var utils = require('./utils');
var cluster = require('./cluster');
var datatype = require('./datatype');
var command = datatype.command;


var query_redis = {
	const_limit_default: 50,
};


var separator_key = datatype.getKeySeparator();
var separator_detail = datatype.getDetailSeparator();
var collision_breaker = datatype.getCollisionBreaker();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();

var label_lua_nil = '_nil';					// passing raw <nil> to lua is not exactly possible



processPaddingForZsetUID = function processPaddingForZsetUID(struct, type, val){
	// NB: it is crucial to leave NULL values untouched; '' may mean a something for callers
	if(val != null){
		if(struct == 'zset' && (type == 'float' || type == 'integer')){
			val = Array((Math.floor(val)+'').length).join('a') + val;	// e.g. 13.82 -> a13.82
		}else{
			val = ''+val;
		}
	}
	return val;
}
// TODO most of the computations on configs could be cached in redislayer to reduce parse computations
query_redis.parseIndexToStorageAttributes = function parseQueryIndexToRedisStorageAttributes(key, cmd, index){
	// for <xid>, distinguish between 0 and null for zrangebyscore
	// that is xid=null would be a hint that zrangebylex should be used instead
	var usual = 'usual';
	var branch = 'branch';
	var keyBranches = [];
	var mainFieldBranch = null;
	var xid = {usual: null, branch: []};
	var luaArgs = {usual: [], branch: []}; 
	var keySuffixes = {usual: [], branch: []}; 
	var uidPrefixes = {usual: [], branch: []};
	var xidPrefixes = {usual: [], branch: []};
	var keyConfig = datatype.getKeyConfig(key);
	var struct = datatype.getConfigStructId(keyConfig);
	var offsets = datatype.getConfigIndexProp(keyConfig, 'offsets') || [];
	var indexFields = Object.keys(index);
	var fields = datatype.getConfigIndexProp(keyConfig, 'fields') || [];			// fields sorted by index
	// using the defined fields is a better idea than using the currently provided possibly-partial or even extraneous fields
	// it uniformly treats every index argument with complete field-set, albeit some having NULL values
	// this is safer; fields have fixed ordering and positions!
	// for example, note that absent fields may still make an entry in keySuffixes!!
	for(var i=0; i < fields.length; i++){
		var field = fields[i];
		if(!(field in index)){
			continue;
		}
		var fieldIndex =  i;
		var fieldVal = index[field];
		var splits = datatype.splitConfigFieldValue(keyConfig, index, field) || [];
		// key suffixes and branches
		var type = usual;
		var fld = splits[2];
		// NB: this must be parsed even if the value of the index.$field is <null>
		// else how do you direct/indicate choice of key for getter queries?!
		if(fld != null && indexFields.indexOf(fld) >= 0){	// unusual case
			type = branch;
			keyBranches.push(fld);
		}
		if(splits[0] != null){
			// don't prefix nulls
			// NB: empty strings are still concatenated
			// use fieldIndex for ordering but remember there may be gaps (i.e. ['', ,'234']) to be removed
			var offsetGroupIndex = datatype.getConfigPropFieldIdxValue(keyConfig, 'offsetgroups', fieldIndex);
			if(offsetGroupIndex == null){
				offsetGroupIndex = fieldIndex;
			}
			// NB: only set once; keysuffix of other offsetgroups would be ignored!
			// 	datatype.splitConfigFieldValue takes care of returning the keysuffix of the first non-null offsetgroup field
			// this means once any offsetgroup field is given a value, it must be sufficient to define the keysuffix
			if(keySuffixes[type][offsetGroupIndex] == null){
				keySuffixes[type][offsetGroupIndex] = splits[0];
			}
		}
		// xid and uid
		if(datatype.isConfigFieldUIDPrepend(keyConfig, fieldIndex)){
			// use fieldIndex for ordering but remember there may be gaps i.e. ['', ,'234'] to be removed
			// in case of <null> use empty-string concats in order to prevent order mismatches
			// for zset, prefix floats with 'a's so they maintain their sorting; see datatype.getConfigOrdering
			var mytype = datatype.getConfigPropFieldIdxValue(keyConfig, 'types', fieldIndex);
			var pad = processPaddingForZsetUID(struct, mytype, splits[1]) || '';
			uidPrefixes[type][fieldIndex] = pad;
		}
		if(struct == 'zset'){
			if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
				var addend = datatype.getFactorizedConfigFieldValue(keyConfig, field, splits[1]);
				if(addend != null){
					if(type == usual){
						xid[type] = (xid[type] || 0) + addend;
					}else{
						xid[type][fld] = (xid[type][fld] || 0) + addend; 
					}
				}
			}
		// datatype.isConfigFieldStrictlyUIDPrepend (lexicographical member) does not really apply to non-zsets; NOT TRUE!
		// add more constraints to fish out wrong evaluations of nulls or empty strings
		// NULLs cannot simply be ignored too
		// NB: keep in mind storing e.g. details of a person with NULL entries => 'firstname::28:'
		// the <cmd> seems to be the only hint as to when to ignore <xid>, at least when NULL
		}else if(!datatype.isConfigFieldStrictlyUIDPrepend(keyConfig, fieldIndex)){	// i.e. definition of XID vs. UID
			// non-zsets values/scores/xids just have to be joined
			xidPrefixes[type][fieldIndex] = (splits[1] != null ? splits[1]+'' : '');
		}
		// NB: upsert can NOT cause changes in key/keysuffix
		// since keySuffixes+UID are required to be complete in order to identify target
		// => in case upsert is tried on such fields, only the non-keysuffix portion is updated
		//	as it turns out, this prevents offsetgroups from updating keysuffixes which subsumes their contribution
		if(utils.startsWith(command.getType(cmd), 'upsert')){
			if(struct == 'zset'){
				if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
					var outOfBounds = 1000*redisMaxScoreFactor;		// just large enough
					var pvf = datatype.getConfigPropFieldIdxValue(keyConfig, 'factors', fieldIndex);
					var next_pvf = datatype.getConfigFieldPrefactor(keyConfig, fieldIndex) || outOfBounds;
					luaArgs[type][fieldIndex] = [splits[1]||0, pvf, next_pvf];
				}
			}else{
				luaArgs[type][fieldIndex] = (splits[1] != null ? [splits[1]] : [label_lua_nil]);
			}
		}
	}
	// passover fieldIndexes once again in order to build structures
	// use index=0 of the same structures above to compound progressive values
	for(var i=1; i < fields.length; i++){		// i=0 is done by virtue of it being used as compound storage
		var field = fields[i];
		var fieldIndex = i; 
		var fieldVal = index[field];
		var os = offsets[fieldIndex];
		var isBranch = datatype.isConfigFieldBranch(keyConfig, fieldIndex);
		var type = (isBranch ? branch : usual);
		var addend = null;
		if(!isBranch){
			// for 'usual' fields, append non-null values to both 'usual' ...
			var structs = [keySuffixes, uidPrefixes, xidPrefixes];
			var delimiters = [separator_key, separator_detail, separator_detail];
			for(s=0; s < structs.length; s++){
				var str = structs[s];
				var del = delimiters[s];
				if(str[type][fieldIndex] != null){
					addend = (str[usual][0] != null ? str[type][0] + del : '');
					str[type][0] = addend + str[type][fieldIndex];
					//  ... and append also to 'branches'
					for(j=0; j < i; j++){
						if(datatype.isConfigFieldBranch(keyConfig, j)){
							addend = (str[branch][j] != null ? str[branch][j] + del : '');
							str[branch][j] = addend + str[type][fieldIndex];
						}
					}
				}
			}
			if(luaArgs[type][fieldIndex] != null){
				if(luaArgs[usual][0] != null){
					[].push.apply(luaArgs[usual][0], luaArgs[type][fieldIndex]);
				}else{
					luaArgs[usual][0] = luaArgs[type][fieldIndex].concat([]);
				}
				for(j=0; j < i; j++){
					if(datatype.isConfigFieldBranch(keyConfig, j)){
						if(luaArgs[branch][j] != null){
							[].push.apply(luaArgs[branch][j], luaArgs[type][fieldIndex]);
						}else{
							luaArgs[branch][j] = luaArgs[type][fieldIndex].concat([]);
						}
					}
				}
			}
		}else{
			// for 'branch' fields, append non-null values in 'usual' to 'branches'
			var structs = [keySuffixes, uidPrefixes, xidPrefixes];
			var delimiters = [separator_key, separator_detail, separator_detail];
			for(s=0; s < structs.length; s++){
				var str = structs[s];
				var del = delimiters[s];
				if(str[usual][0] != null){
					addend = (str[type][fieldIndex] != null ? del + str[type][fieldIndex] : '');
					str[type][fieldIndex] = str[usual][0] + addend;
				}
			}
			if(xid[usual] != null){
				xid[type][field] = xid[usual] + (xid[type][field] || 0);
			}
			if(luaArgs[usual][0] != null){
				if(luaArgs[type][fieldIndex] == null){
					luaArgs[type][fieldIndex] = luaArgs[usual][0].concat([]);
				}else{
					[].push.apply(luaArgs[type][fieldIndex], luaArgs[usual][0]);
				} 
			}
		}
	}
	var type = branch;
	if((keyBranches || []).length == 0){
		type = usual;
		keyBranches = [mainFieldBranch];
	}
	var storageAttr = {};
	for(i=0; i < keyBranches.length; i++){
		var kb = keyBranches[i];
		var idx = datatype.getConfigFieldIdx(keyConfig, kb) || 0;	// i.e. branchIndex or compoundIndex
		var sa = {};
		sa.keySuffixes = keySuffixes[type][idx];
		sa.luaArgs = luaArgs[type][idx];
		sa.uid = uidPrefixes[type][idx];
		if(struct == 'zset'){
			sa.xid = (type == usual ? xid[type] : xid[type][kb]);
		}else{
			sa.xid = xidPrefixes[type][idx];
		}
		storageAttr[kb] = sa;
	}
	return storageAttr;
};

removePaddingFromZsetUID = function removePaddingFromZsetUID(val){
	if(val){
		var len = val.indexOf('.');
		if(len < 0){
			len = val.length;
		}
		val = val.slice(Math.floor((len-1)/2));
	}else{
		val = null;
	}
	return val;
};

getFieldBranchFUID = function getFieldBranchFUID(fieldIndex, keyLabel, mySuffixCount, keyParts, fkeyPartIndexes, uidParts, fuidPartIndexes){
	// create a unique id across all field-branch keys in order to unify records across the branched calls
	// keyLabel and any admissible non-field-branch field should make contributions towards this
	// hence, just subtract field-branch contributions to the <keyText> and <uid> arguments
	var neutralKeyParts = keyParts.slice(0-mySuffixCount);
	if(fkeyPartIndexes[fieldIndex] != null){
		neutralKeyParts.splice(fkeyPartIndexes[fieldIndex], 1);
	}
	var neutralUIDParts = uidParts.concat([]);
	if(fuidPartIndexes[fieldIndex] != null){
		neutralUIDParts.splice(fuidPartIndexes[fieldIndex], 1);
	}
	var fuid = [keyLabel].concat(neutralKeyParts).concat(neutralUIDParts).join(separator_key);
	return fuid;
};

// TODO most of the computations on configs could be cached in redislayer to reduce parse computations
// NB: the uid here may be synthetic i.e. the raw input for the query
//	in the case of no query results, synthetic values help fix other properties e.g. fuid
query_redis.parseStorageAttributesToIndex = function parseRedisStorageAttributesToIndex(cmd, key, key_text, xid, uid, field_branch){
	// NB: if <xid> is null, <uid> still has to be parsed
	var index = {};
	var fld = null;
	var fuid = null;
	var keyConfig = datatype.getKeyConfig(key);
	var keyLabel = datatype.getKeyLabel(key);
	var struct = datatype.getConfigStructId(keyConfig);
	var fields = datatype.getConfigIndexProp(keyConfig, 'fields') || [];
	var factors = datatype.getConfigIndexProp(keyConfig, 'factors') || [];
	var offsets = datatype.getConfigIndexProp(keyConfig, 'offsets') || [];
	var keyParts = key_text.split(separator_key);
	var uidParts = (uid != null ? String(uid).split(separator_detail) : []);
	var xidParts = (xid != null ? String(xid).split(separator_detail) : []);
	// TODO this caching should be done once onload of configs
	// make cache of info to be used
	var keySuffixCount = 0;
	var uidPrependCount = 0;
	var fuidPartIndexes = [];	// fkeyPartIndexes[j] = number of uidPrepends preceding fieldIndex-j
	var fkeyPartIndexes = [];	// fkeyPartIndexes[j] = number of keySuffixes preceding fieldIndex-j
	if(field_branch != null){
		for(var j=0; j < fields.length; j++){
			// other field-branches are not involved since they are stored separately
			// for now all field-branches are excluded, and conditionally added later
			// this is because this code-path runs only once and a cached value is used thereafter
			if(datatype.isConfigFieldKeySuffix(keyConfig, j)){
				fkeyPartIndexes[j] = keySuffixCount;
				if(!datatype.isConfigFieldBranch(keyConfig, j)){
					keySuffixCount++;
				}
			}
			if(datatype.isConfigFieldUIDPrepend(keyConfig, j)){
				fuidPartIndexes[j] = uidPrependCount;
				if(!datatype.isConfigFieldBranch(keyConfig, j)){
					uidPrependCount++;
				}
			}
		}
	}

	// if a get-command doesn't have an XID, no result should be returned
	// this is handled specially since otherwise index={...} will be returned instead of a NULL
	// 	since at least one field can be constructed with the input key_text/xid/uid
	var noReturn = (utils.startsWith(command.getType(cmd), 'get') && xid == null);
	for(var i=0; i < fields.length; i++){
		var field = fields[i];
		var fieldIndex = i;
		var type = datatype.getConfigPropFieldIdxValue(keyConfig, 'types', fieldIndex);
		var fieldOffset = offsets[fieldIndex];
		if(datatype.isConfigFieldBranch(keyConfig, fieldIndex)){
			// NB: field-branch value itself (i.e. apart from the property) could count towards keySuffixes
			var mySuffixCount = keySuffixCount + (datatype.isConfigFieldKeySuffix(keyConfig, fieldIndex) ? 1 : 0);
			if(field_branch != field){
				continue;
			}else{
				var fuid = getFieldBranchFUID(fieldIndex, keyLabel, mySuffixCount, keyParts, fkeyPartIndexes, uidParts, fuidPartIndexes);
				if(noReturn){
					return {index:null, field:field, fuid:fuid};
				}
			}
		}else if(noReturn){
			var fuid = null;
			if(field_branch != null){
				var fbIndex = datatype.getConfigFieldIdx(keyConfig, field_branch);
				var mySuffixCount = keySuffixCount + (datatype.isConfigFieldKeySuffix(keyConfig, fbIndex) ? 1 : 0);
				fuid = getFieldBranchFUID(fbIndex, keyLabel, mySuffixCount, keyParts, fkeyPartIndexes, uidParts, fuidPartIndexes);
			}
			return {index:null, field:field_branch, fuid:fuid};
		}
		if(datatype.isConfigFieldStrictlyUIDPrepend(keyConfig, fieldIndex)
			|| (datatype.isConfigFieldUIDPrepend(keyConfig, fieldIndex) && xid == null)){
			// can't find it elsewhere
			// otherwise values could be found in keytext and/or score
			var uidIndex = 0;
			for(var h=0; h < fieldIndex; h++){
				if(datatype.isConfigFieldUIDPrepend(keyConfig, h)
					&& !datatype.isConfigFieldBranch(keyConfig, h)){
					uidIndex++;
				}
			}
			var fieldVal = uidParts[uidIndex];
			// for zsets, remove padding a's in the case of numerics 
			if(struct == 'zset' && (type == 'integer' || type == 'float')){
				fieldVal = removePaddingFromZsetUID(fieldVal);
			}
			index[field] = fieldVal;
		}else if(xid != null){
			if(struct != 'zset'){
				// count xid prepends across field-branches => exclude field-branch prepends
				var xidIndex = 0;
				for(var h=0; h < fieldIndex; h++){
					if(!datatype.isConfigFieldStrictlyUIDPrepend(keyConfig, h)
						&& !datatype.isConfigFieldBranch(keyConfig, h)){
						xidIndex++;
					}
				}
				index[field] = xidParts[xidIndex];
			}else if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
				// basic splicing of field's component from score
				var fact = factors[fieldIndex];
				var preFact = datatype.getConfigFieldPrefactor(keyConfig, fieldIndex);
				var val = Math.floor((preFact ? xid % preFact : xid) / fact);
				index[field] = val;
			}
		}else if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
			delete index[field];	// xid is required to complete the value
			continue;
		}
		// check if addendums come from the key_text
		if(datatype.isConfigFieldKeySuffix(keyConfig, fieldIndex)){
			if(key_text != null){
				// NB: keys may already have long separated parts; hence count from the rhs
				// find exact position of this field's addendum
				// check subsequent/tailing fields if they have addendums
				// NB: critical to have addendums ordered by fieldIndex
				var keyIndex = 0;
				var fieldBranchCount = 0;
				// handle offgroups
				var offsetGroupIndex = datatype.getConfigPropFieldIdxValue(keyConfig, 'offsetgroups', fieldIndex);
				var keySuffixOffsets = offsets;
				var isOffsetGroup = false;
				if(offsetGroupIndex != null){
					isOffsetGroup = true;
					var offsetGroups = datatype.getConfigIndexProp(keyConfig, 'offsetgroups');
					keySuffixOffsets = offsetGroups;
				}else{
					offsetGroupIndex = fieldIndex;
				}
				// count keysuffix offset
				for(var h=keySuffixOffsets.length-1; h > offsetGroupIndex; h--){
					var idx = (isOffsetGroup ? keySuffixOffsets[h] : h);
					if(idx != h){
						// this eliminates unsorted duplicates in offsetgroups
						// e.g. [3,2,2,3,3] --> [skip,skip,2,3,skip] i.e. sorted and unique just like offsets
						continue;
					}
					if(datatype.isConfigFieldKeySuffix(keyConfig, idx)){
						// do not count more than a single fieldBranch
						// since fieldBranches are stored separately
						if(!datatype.isConfigFieldBranch(keyConfig, idx) || (fieldBranchCount == 0
							&& (!datatype.isConfigFieldBranch(keyConfig, offsetGroupIndex) || idx == offsetGroupIndex))){
								keyIndex++;
							if(datatype.isConfigFieldBranch(keyConfig, idx)){
								fieldBranchCount++;
							}
						}
					}
				}
				var suffix = keyParts[keyParts.length-1-keyIndex];
				index[field] =  datatype.unsplitFieldValue([suffix, index[field]], fieldOffset);
			}else{
				delete index[field];		// key_text is required to complete value
				continue;
			}
		}
		// typecasting
		if(index[field] != null && (type == 'integer' || type == 'float')){
			index[field] = parseFloat(index[field], 10);
		}
	}
	return {index:(utils.isObjectEmpty(index) ? null : index), field:field_branch, fuid:fuid};
};


// ranges are inclusive
getKeyRangeMaxByProp = function getQueryKeyRangeMaxByProp(key, index, score, prop){
	var maxScore = null;
	var max = (index || {}).max;
	var keyConfig = datatype.getKeyConfig(key);
	var propIndex = datatype.getConfigPropFieldIdxValue(keyConfig, 'fields', prop);
	var propFactor = datatype.getConfigPropFieldIdxValue(keyConfig, 'factors', propIndex);
	if(score == null){
		maxScore = '+inf';
	}else if(prop == null || propFactor == null){
		maxScore = score;
	}else if(max == null){
		maxScore = (score - (score % propFactor) + propFactor - 1);		// -1 since ranges are inclusive
	}else{
		var preFactor = datatype.getConfigFieldPrefactor(keyConfig, propIndex);
		var addend = (1+max) * propFactor;
		maxScore = score - (score % (preFactor || Infinity)) + addend - 1;	// -1 since ranges are inclusive
	}
	return maxScore;
};

getKeyRangeMinByProp = function getQueryKeyRangeMinByProp(key, range_config, score, prop){
	var min = range_config.boundValue;
	var keyConfig = datatype.getKeyConfig(key);
	var propIndex = datatype.getConfigPropFieldIdxValue(keyConfig, 'fields', prop);
	var propFactor = datatype.getConfigPropFieldIdxValue(keyConfig, 'factors', propIndex);
	if(score == null){
		minScore = '-inf';
	}else if(prop == null || propFactor == null){
		minScore = score;
	}else if(min == null){
		minScore = score - (score % propFactor);
	}else{
		var preFactor = datatype.getConfigFieldPrefactor(keyConfig, propIndex);
		var addend = min * propFactor;
		minScore = score - (score % (preFactor || Infinity)) + addend;
	}
	return minScore;
};

getKeyRangeStartScore = function getQueryKeyRangeStartScore(range_order, key, range_config, score){
	var startScore = null;
	var startBound = (range_config.excludeCursor ? '(' : '');
	if(range_order == asc){
		startScore = getKeyRangeMinByProp(key, range_config, score, range_config.startProp);
	}else if(range_order == desc){
		startScore = getKeyRangeMaxByProp(key, range_config, score, range_config.startProp);
	}
	return startBound+startScore;
};

getKeyRangeStopScore = function getQueryKeyRangeStopScore(range_order, key, range_config, score){
	var stopScore = null;
	if(range_order == asc){
		stopScore = getKeyRangeMaxByProp(key, range_config, score, range_config.stopProp);
	}else if(range_order == desc){
		stopScore = getKeyRangeMinByProp(key, range_config, score, range_config.stopProp);
	}
	return stopScore;
};

getKeyRangeStartMember = function getQueryKeyRangeStartMember(range_order, key, range_config, member){
	var startMember = member;
	var startProp = range_config.startProp;
	var keyConfig = datatype.getKeyConfig(key);
	var startPropIndex = datatype.getConfigFieldIdx(keyConfig, startProp);
	var startBound = (range_config.excludeCursor ? '(' : '[');
	if(member != null){
		// stripe off from member-parts, all prop-suffixes until the startProp
		// NB: the algorithm here assumes the index holds all props till the startProp
		//	else ranging is not possible!
		var offsets = 0;
		if(startProp == null || !datatype.isConfigFieldUIDPrepend(keyConfig, startPropIndex)){
			startPropIndex = -1;
			offsets = Infinity;	// don't truncate in case of no startProp/startPropIndex
		}
		for(var i=0; i <= startPropIndex; i++){
			if(datatype.isConfigFieldUIDPrepend(keyConfig, i)){
				offsets++;
			}
		}
		var tray = member.split(separator_detail).splice(0, offsets);
		// increment the last character in case of descending
		if(range_order != asc && !range_config.excludeCursor){
			var incr = 1;
			var lastStr = tray[tray.length-1];
			var nextStr = lastStr.slice(0, -1)+String.fromCharCode(lastStr.charCodeAt(lastStr.length-1)+incr);
			tray[tray.length-1] =  nextStr;
		}
		// terminate search string so it doesn't prefix another member which shouldn't have been in the range
		// e.g. aa:a:...   vs.  aa:aZ:...
		// but this should only be done in case the member-tray was truncated
		// otherwise the resulting string would not be a prefix anymore
		var offsetPrependsUID = datatype.getConfigIndexProp(keyConfig, 'offsetprependsuid') || [];
		var isLastPrepend = (offsetPrependsUID.lastIndexOf(true) == startPropIndex);
		if(startProp != null && datatype.isConfigFieldUIDPrepend(keyConfig, startPropIndex) && !isLastPrepend){
			tray.push('');						// character preceding all others
		}
		startMember = tray.join(separator_detail);
	}
	if(range_order == asc){
		startMember = (startMember ? startBound+startMember : '-');
	}else if(range_order == desc){
		startMember = (startMember ? '('+startMember : '+');		// '(' since last character was increased/excluded
	}
	return startMember;
};

getKeyRangeStopMember = function getQueryKeyRangeStopMember(range_order, key, range_config, member){
	var stopMember = null;
	var stopProp = range_config.stopProp;
	var keyConfig = datatype.getKeyConfig(key);
	var stopPropIndex = datatype.getConfigFieldIdx(keyConfig, stopProp);
	if(member != null){
		var offsets = 0;
		if(stopProp == null || !datatype.isConfigFieldUIDPrepend(keyConfig, stopPropIndex)){
			stopPropIndex = -1;
			offsets = Infinity;
		}
		for(var i=0; i <= stopPropIndex; i++){
			if(datatype.isConfigFieldUIDPrepend(keyConfig, i)){
				offsets++;
			}
		}
		var tray = member.split(separator_detail).splice(0, offsets);
		// NB: boundValue not used in getKeyRangeStartMember since index can be used to represent that
		// NB: in case of a boundValue argument, we have to deal with a string instead of char
		// NB: UID numerics are padded to maintain ordering
		var struct = datatype.getConfigStructId(keyConfig);
		var type = datatype.getConfigPropFieldIdxValue(keyConfig, 'types', stopPropIndex);
		var stop = processPaddingForZsetUID( struct, type, range_config.boundValue);
		if(stop != null){
			tray[tray.length-1] = String(stop);
		}
		// increment the last char of string in case of ascending order
		if(range_order == asc){
			var incr = 1;
			var lastStr = tray[tray.length-1];
			var nextStr = lastStr.slice(0, -1)+String.fromCharCode(lastStr.charCodeAt(lastStr.length-1)+incr);
			tray[tray.length-1] =  nextStr;
		}
		var offsetPrependsUID = datatype.getConfigIndexProp(keyConfig, 'offsetprependsuid') || [];
		var isLastPrepend = (offsetPrependsUID.lastIndexOf(true) == stopPropIndex);
		if(stopProp != null && datatype.isConfigFieldUIDPrepend(keyConfig, stopPropIndex) && !isLastPrepend){
			tray.push('');
		}
		stopMember = tray.join(separator_detail);
		
	}
	if(range_order == asc){
		stopMember = (stopMember ? '('+stopMember : '+');
	}else if(range_order == desc){
		stopMember = (stopMember ? '['+stopMember : '-');
	}
	return stopMember;
};


query_redis.getCIQA = function getCIQA(cluster_instance, cmd, keys, index, rangeConfig, attribute, field, storage_attribute){
	var key = keys[0];
	var keyConfig = datatype.getKeyConfig(key);
	var keyLabel = datatype.getKeyLabel(key);
	var struct = datatype.getConfigStructId(keyConfig);
	var lua = null;
	var xid = storage_attribute.xid;
	var uid = storage_attribute.uid;
	var luaArgs = storage_attribute.luaArgs || [];
	var keySuffixes = storage_attribute.keySuffixes;
	var args = [];
	var limit = (attribute || {}).limit;
	switch(cluster.getInstanceType(cluster_instance)){
	default:
		switch(struct){
		case 'string':
			if(utils.startsWith(command.getType(cmd), 'upsert')){
				lua = 'local xid = redis.call("get", KEYS[1]);'
					+ 'local xidList = string.gmatch(xid'+separator_detail+', "([^'+separator_detail+']+)'+separator_detail+'");'
					+ 'local xidStr = "";'
					+ 'for i=1, #xidList, 1 do'
					+ '    if i > 1 then'
					+ '        xidStr = xidStr.."'+separator_detail+'"'
					+ '    end;'
					+ '    if ARGV[i] ~= "'+label_lua_nil+'" then'
					+ '        xidStr = xidStr..ARGV[i];'
					+ '    else'
					+ '        xidStr = xidStr..xidList[i];'
					+ '    end;'
					+ 'end;'
					+ 'if xid ~= xidStr then'
					+ '    return redis.call("set", KEYS[1], xidStr);'
					+ 'else'
					+ '    return 0;'
					+ 'end;';
			}else{
				if(command.requiresXID(cmd)){
					args.unshift(xid || '');
				}
				if(command.requiresUID(cmd)){
					args.unshift(uid || '');
				}
			}
			break;
		case 'set':
		case 'hash':
			if(utils.startsWith(command.getType(cmd), 'upsert')){
				lua = 'local xid = redis.call("hget", KEYS[1], ARGV[1]);'
					+ 'local xidList = string.gmatch(xid'+separator_detail+', "([^'+separator_detail+']+)'+separator_detail+'");'
					+ 'local xidStr = "";'
					+ 'for i=2, #xidList, 1 do'				// ARGV[1] holds UID
					+ '    if i > 1 then'
					+ '        xidStr = xidStr.."'+separator_detail+'"'
					+ '    end;'
					+ '    if ARGV[i] ~= "'+label_lua_nil+'" then'
					+ '        xidStr = xidStr..ARGV[i];'
					+ '    else'
					+ '        xidStr = xidStr..xidList[i];'
					+ '    end;'
					+ 'end;'
					+ 'if xid ~= xidStr then'
					+ '    return redis.call("hset", KEYS[1], ARGV[1], xidStr);'
					+ 'else'
					+ '    return 0;'
					+ 'end;';
				luaArgs.unshift(uid);
			}else{
				if(command.requiresXID(cmd)){
					args.unshift(xid || '');
				}
				if(command.requiresUID(cmd)){
					args.unshift(uid || '');
				}
			}
			break;
		case 'zset':
			var rangeOrder = command.getOrder(cmd) || asc;
			// decide between the different type of ranges
			if(command.getType(cmd) == 'rangez' || command.getType(cmd) == 'delrangez' || command.getType(cmd) == 'countz'){
				cmd = command.getMode(cmd)[datatype.getZRangeSuffix(keyConfig, index, args, xid, uid)];
			}
			if(utils.startsWith(command.getType(cmd), 'upsert')){
				lua = ' local score = redis.call("zscore", KEYS[1], ARGV[1]);'
					+ 'if score == nil then'
					+ '    score = 0;'
					+ 'end;'
					+ 'local scoreStr = score;'
					+ 'for i=2, #ARGV, 3 do'			// ARGV[1] is the member i.e. args[0]
					+ '     local lhs = scoreStr - (scoreStr % ARGV[i+2]);'
					+ '     local mid = ARGV[i] * ARGV[i+1];'
					+ '     local rhs = scoreStr % ARGV[i+1];'
					+ '     scoreStr = lhs + mid + rhs;'
					+ 'end;'
					+ 'if score ~= scoreStr then'
					+ '    return redis.call("zadd", KEYS[1], scoreStr, ARGV[1]);'
					+ 'else'
					+ '    return 0;'
					+ 'end;';
				luaArgs.unshift(uid);
			}else if(utils.startsWith(command.getType(cmd), 'rankbyscorelex')){
				// NB: a partial-score may be present so score==null is false
				// .bylex requires args so when missing => zcard
				// .byscore should also have args together with the score
				if(/*score == null &&*/ uid == null){
					cmd = command.getMode(datatype.getConfigCommand(keyConfig).count).bykey;
				}else{
					var startScore = getKeyRangeStartScore(rangeOrder, key, rangeConfig, xid);
					if(startScore == Infinity || startScore == -Infinity){
						startScore = null;
					}
					var scorerank = {}; scorerank[asc] = '"zrevrank"'; scorerank[desc] = '"zrevrank"';
					lua = ' local startScore = ARGV[1];'
						+ 'local member = ARGV[2];'
						+ 'local score = nil;'
						+ 'local rank = nil;'
						+ 'if startScore ~= "'+label_lua_nil+'" then'				// verify unchanged score-member
						+ '    score = redis.call("zscore", KEYS[1], member);'			// check score tally first
						+ '    if score == startScore then'					// then rank can be set
						+ '        rank = redis.call('+scorerank[command.getOrder(cmd)]+', KEYS[1], member);'
						+ '    else'
						+ '        member = member.."'+collision_breaker+'";'
						+ '        score = startScore;'
						+ '        local count = redis.call("zadd", KEYS[1], "NX", score, member);'
						+ '        rank = redis.call('+scorerank[command.getOrder(cmd)]+', KEYS[1], member);'
						+ '        if count > 0 then'
						+ '            redis.call("zrem", KEYS[1], member);'
						+ '        end;'
						+ '    end;'
						+ 'else'
						+ '    rank = redis.call('+scorerank[command.getOrder(cmd)]+', KEYS[1], member);'	//no score to seek with
						+ 'end;'
						+ 'return rank;'
					luaArgs.unshift(uid);
					luaArgs.unshift(startScore || label_lua_nil);
				}
			}else if(utils.startsWith(command.getType(cmd), 'rangebyscorelex')){
				// FYI: zrank and zrevrank are symmetric so a single code works!
				// NB: redis excluding/bound operator is not required/allowed here
				// so [member] argument can be tweaked for the desired result
				// NB: if startScore must be used, range [prop] must be provided
				var startScore = getKeyRangeStartScore(rangeOrder, key, rangeConfig, xid);
				if(startScore == Infinity || startScore == -Infinity){
					startScore = null;
				}
				//var stopScore = getKeyRangeStopScore(rangeOrder, key, rangeConfig, xid);
				// TODO add countbyscorelex; //TODO implement stopScore
				var zrank = {}; zrank[asc] = '"zrank"'; zrank[desc] ='"zrevrank"';
				var zrange = {}; zrange[asc] = '"zrange"'; zrange[desc] ='"zrevrange"';
				var zlimit = {}; zlimit[asc] = '-1'; zlimit[desc] = '0';
				lua = ' local rank = nil;'
					+ 'local startScore = ARGV[1];'
					+ 'local member = ARGV[2];'
					+ 'local score = nil;'
					+ 'if startScore ~= "'+label_lua_nil+'" then'						// verify unchanged score-member
					+ '    score = redis.call("zscore", KEYS[1], member);'					// check score tally first
					+ '    if score == startScore then'							// then rank can be set
					+ '        rank = redis.call('+zrank[command.getOrder(cmd)]+', KEYS[1], member);'
					+ '    else'
					+ '        member = member.."'+collision_breaker+'";'
					+ '        score = startScore;'
					+ '        local count = redis.call("zadd", KEYS[1], "NX", score, member);'
					+ '        rank = redis.call('+zrank[command.getOrder(cmd)]+', KEYS[1], member);'	// infer the previous position
					+ '        if count > 0 then'
					+ '             redis.call("zrem", KEYS[1], member);'
					+ '        end;'
					+ '    end;'
					+ 'else'
					+ '    rank = redis.call('+zrank[command.getOrder(cmd)]+', KEYS[1], member);'		// no score to seek with
					+ 'end;'
					+ 'if rank ~= nil then'
					+ '     return redis.call('+zrange[command.getOrder(cmd)]+', KEYS[1], rank, '
						+ (limit != null ? 'rank+'+limit+'-1' : zlimit[command.getOrder(cmd)])
						+ (attribute.withscores ? ', "withscores");' : ');')
					+ 'else'
					+ '     return {};'
					+ 'end;'
				luaArgs.unshift(uid);
				// TODO rewrite lua scripts not to used normal ranging functions
				// 	for now hack around example.js issue
				if(startScore && startScore[0] == '('){
					startScore = 1 + startScore.slice(1);
				}
				luaArgs.unshift(startScore || label_lua_nil);
			}else if(utils.startsWith(command.getType(cmd), 'countbylex') || utils.startsWith(command.getType(cmd), 'rangebylex')
					|| utils.startsWith(command.getType(cmd), 'delrangebylex')){
				if(utils.startsWith(command.getType(cmd), 'countbylex') && uid == null){
					cmd = command.getMode(datatype.getConfigCommand(keyConfig).count).bykey;
				}else{
					var startMember = getKeyRangeStartMember(rangeOrder, key, rangeConfig, uid);
					var stopMember = getKeyRangeStopMember(rangeOrder, key, rangeConfig, uid);
					args.unshift(stopMember);
					args.unshift(startMember);
				}
			}else if(utils.startsWith(command.getType(cmd), 'countbyscore') || utils.startsWith(command.getType(cmd), 'rangebyscore')
					|| utils.startsWith(command.getType(cmd), 'delrangebyscore')){
				if(utils.startsWith(command.getType(cmd), 'countbyscore') && (xid == null || xid == '')){
					cmd = command.getMode(datatype.getConfigCommand(keyConfig).count).bykey;
				}else{
					var startScore = getKeyRangeStartScore(rangeOrder, key, rangeConfig, xid);
					var stopScore = getKeyRangeStopScore(rangeOrder, key, rangeConfig, xid);
					args.unshift(stopScore);
					args.unshift(startScore);
				}
			}else{
				if(command.requiresUID(cmd)){
					args.unshift(uid || '');
				}
				if(command.requiresXID(cmd)){
					args.unshift(xid || 0);		// || 0 ... typically for zset.lex
				}
			}
			break;
		default:
			throw new Error('FATAL: query.query: unknown keytype!!');
		}
	}
	var keyText = keyLabel+(field != null ? separator_key+field : '')+(keySuffixes != null ? separator_key+keySuffixes : '');
	if((luaArgs || []).length > 0 && (utils.startsWith(command.getType(cmd), 'upsert')
		|| utils.startsWith(command.getType(cmd), 'rangebyscorelex') || utils.startsWith(command.getType(cmd), 'rankbyscorelex'))){
		luaArgs.unshift(keyText);
		luaArgs.unshift(1);
		luaArgs.unshift(lua);
		args = luaArgs;
		attribute = attribute;
	}
	return {command:cmd, keytext:keyText, args:args};
};


module.exports = query_redis;
