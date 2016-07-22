var async = require('async');
var utils = require('./utils');
var cluster = require('./cluster');
var datatype = require('./datatype');
var command = datatype.command;


var query = {};
	// CONFIGURATIONS
var	const_limit_default = 50,				// result limit for performing internal routines
	label_lua_nil = '_nil',					// passing raw <nil> to lua is not exactly possible
	label_functions = '_functions',				// passed function(s) are passed here
	label_function_args = '_function_args',			// when functions are passed the args are held here
	label_attribute_idxs = '_attribute_idxs',		// when function are passed the attribute index location(s) are held here
	label_cursor_idxs = '_cursor_idxs',			// when function args are passed, possible cursor location(s) are held here
	label_comparator = '_comparator',
	label_comparator_prop = '_comparator_prop',
	label_start_prop = '_start_prop',
	label_stop_prop = '_stop_prop',
	label_exclude_cursor = '_exclude_cursor';

query.getDefaultBatchLimit = function(){
	return const_limit_default;
};
// TODO deprecate the following two functions
//	=> provide instead functions to readily read the target objects
query.getComparatorLabel = function(){
	return label_comparator;
};
query.getExcludeCursorLabel = function(){
	return label_exclude_cursor;
};

var separator_key = datatype.getKeySeparator();
var separator_detail = datatype.getDetailSeparator();
var collision_breaker = datatype.getCollisionBreaker();
var redisMaxScoreFactor = datatype.getRedisMaxScoreFactor();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();


parseIndexToStorageAttributes = function(key, cmd, index){
	// for <xid>, distinguish between 0 and null for zrangebyscore
	// that is xid=null would be a hint that zrangebylex should be used instead
	var usual = 'usual';
	var branch = 'branch';
	var keyBranches = [];
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
	// using the defined fields is a better idea than using the currently provided possibly-partial fields
	// it uniformly treats every index argument with complete field-set, albeit some having NULL values
	// this is safer; fields have fixed ordering and positions!
	for(var i=0; i < fields.length; i++){
		var field = fields[i];
		var fieldIndex =  i;								// datatype.getConfigFieldIdx(keyConfig, field)
		var fieldVal = index[field];
		var splits = datatype.splitConfigFieldValue(keyConfig, field, fieldVal) || [];
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
			keySuffixes[type][fieldIndex] = splits[0];
		}
		// xid and uid
		if(datatype.isConfigFieldUIDPrepend(keyConfig, fieldIndex)){
			// use fieldIndex for ordering but remember there may be gaps i.e. ['', ,'234'] to be removed
			// in case of <null> use empty-string concats in order to prevent order mismatches
			uidPrefixes[type][fieldIndex] = (splits[1] != null ? splits[1]+'' : '');		// 0 => '0'
		}
		if(struct == 'zset'){
			if(splits[1] != null){
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
		// upsert must/can not be used for updates which cause key changes
		// since key-change update must first know/fetch old-value
		// (in which case good-old delete+insert suffice)
		// => keySuffixes+UID are required to be complete for upsert to function correctly
		if(utils.startsWith(command.getType(cmd), 'upsert')){
			if(struct == 'zset'){
				if(splits[1] != null){
					var pvf = datatype.getConfigPropFieldIdxValue(keyConfig, 'factors', fieldIndex);
					var outOfBounds = 1000*redisMaxScoreFactor;		// just large enough
					var next_pvf = datatype.getConfigFieldPrefactor(keyConfig, fieldIndex) || outOfBounds;
					luaArgs[type][fieldIndex] = [fieldVal, pvf, next_pvf];
				}
			}else{
				luaArgs[type][fieldIndex] = (splits[1] != null ? [fieldVal] : [label_lua_nil]);
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
		keyBranches = [label_lua_nil];
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

parseStorageAttributesToIndex = function(key, key_text, xid, uid){
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
	var keySuffixCount = null;
	var fuidPartIndexes = null;
	var fkeyPartIndexes = null;
	for(var i=0; i < fields.length; i++){
		var field = fields[i];
		var fieldIndex = i;
		var fieldOffset = offsets[fieldIndex];
		if(datatype.isConfigFieldBranch(keyConfig, fieldIndex)){
			// check if string at the leftmost end of keyPrefixes matches this field
			// if not skip this field
			if(keySuffixCount == null){		// otherwise use cached count
				keySuffixCount = 0;
				fuidPartIndexes = [];
				fkeyPartIndexes = [];
				var uidPrependCount = 0;
				for(var j=0; j < fields.length; j++){
					// other field-branches are not involved since they are stored separately
					// for now all field-branches are excluded, and conditionally added later
					// this is because this code-path runs only once and a cached value is use thereafter
					if(datatype.isConfigFieldKeySuffix(keyConfig, j)){
						fkeyPartIndexes[j] = keySuffixCount;					// NB: reverse index
						if(!datatype.isConfigFieldBranch(keyConfig, j)){
							keySuffixCount++;
						}
					}
					// take care to account for no more than a single field-branch
					// since different field-branches are not stored together
					if(datatype.isConfigFieldUIDPrepend(keyConfig, j)){
						fuidPartIndexes[j] = uidPrependCount;					// forward index
						uidPrependCount++;
						if(datatype.isConfigFieldBranch(keyConfig, j)){
							uidPrependCount--;
						}
					}
				}
			}
			// field-branch value could count towards keySuffixes
			var mySuffixCount = keySuffixCount + (datatype.isConfigFieldKeySuffix(keyConfig, fieldIndex) ? 1 : 0);
			var keyPrefix = [keyLabel, field].join(separator_key);
			if(keyPrefix != keyParts.slice(0, 0 - mySuffixCount).join(separator_key)){
				continue;
			}else{
				fld = field;
				// every command requires either xid or uid components, so one of them is given
				// the output of the command is the other component
				// so if that other is null, the return should be null
				// actually, given existing commands, UID cannot be unknown! is incomplete though in the case of field-branches
				// XID can be incomplete!!
				// this is handled specially since otherwise and index={...} may be returned
				// since some fields can be constructed with the available component
				if(xid == null || uid == null){
					return {index:null, field:null, fuid:fld};
				}
				// create a unique id across all field-branch keys in order to unify records across calls
				// NB: see discussion in dtree.js about the need to have or UID fields besides field-branches
				// keyLabel and any non-field-branch field which passes should make contributions towards this
				// hence, just subtract field-branch contributions to the <keyText> and <uid> arguments
				var neutralKeyParts = keyParts.slice(0-mySuffixCount)
				neutralKeyParts.splice(fkeyPartIndexes[fieldIndex], 1);
				var neutralUIDParts = uidParts.concat([]);
				neutralUIDParts.splice(fuidPartIndexes[fieldIndex], 1);
				fuid = [keyLabel].concat(neutralKeyParts).concat(neutralUIDParts).join(separator_key);
			}
		}
		if(datatype.isConfigFieldStrictlyUIDPrepend(keyConfig, fieldIndex)){	// can't find it elsewhere
			// NB: keys may already have long separated parts; hence count from the lhs
			// find exact position of this field's addendum
			// check preceding fields if they have addendums in the uid
			// NB: critical to have addendums ordered by fieldIndex
			var uidIndex = 0;
			for(var h=0; h < fieldIndex; h++){
				if(datatype.isConfigFieldUIDPrepend(keyConfig, h) && !datatype.isConfigFieldBranch(keyConfig, h)){
					uidIndex++;
				}
			}
			index[field] = uidParts[uidIndex];
		}else if(xid != null){
			if(struct != 'zset'){
				var xidIndex = 0;
				// count xid prepends across field-branches => exclude field-branch prepends
				for(var h=0; h < fieldIndex; h++){
					if(!datatype.isConfigFieldStrictlyUIDPrepend(keyConfig, h) && !datatype.isConfigFieldBranch(keyConfig, h)){
						xidIndex++;
					}
				}
				index[field] = xidParts[xidIndex];
			}else if(factors[fieldIndex] != null){
				// basic splicing of field's uid from score
				var fact = factors[fieldIndex];
				var preFact = datatype.getConfigFieldPrefactor(keyConfig, fieldIndex);
				var val = Math.floor((preFact ? xid % preFact : xid) / fact);
				index[field] = val;
			}
		}else if((factors[fieldIndex] || 0) != 0){	// xid is required to complete the value
			delete index[field];
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
				var fieldBranchCounts = 0;
				for(var h=offsets.length-1; h > fieldIndex; h--){
					if(datatype.isConfigFieldKeySuffix(keyConfig, h)){
						// do not count more than a single fieldBranch
						// since fieldBranches are stored separately
						if(!datatype.isConfigFieldBranch(keyConfig, h) || (fieldBranchCounts == 0
							&& (!datatype.isConfigFieldBranch(keyConfig, fieldIndex)
								|| (datatype.isConfigFieldBranch(keyConfig, fieldIndex) && h == fieldIndex)))){
								keyIndex++;
							if(datatype.isConfigFieldBranch(keyConfig, h)){
								fieldBranchCounts++;
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
	}
	return {index:(index == {} ? null : index), field:fld, fuid:fuid};
};


// ranges are inclusive
getKeyRangeMaxByProp = function(key, index, score, prop){
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

getKeyRangeMinByProp = function(key, index, score, prop){
	var min = (index || {}).min;
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

getKeyRangeStartScore = function(range_order, key, index, score){
	var startScore = null;
	var startBound = (index[label_exclude_cursor] ? '(' : '');
	if(range_order == asc){
		startScore = getKeyRangeMinByProp(key, index, score, (index||{})[label_start_prop]);
	}else if(range_order == desc){
		startScore = getKeyRangeMaxByProp(key, index, score, (index||{})[label_start_prop]);
	}
	return startBound+startScore;
};

getKeyRangeStopScore = function(range_order, key, index, score){
	var stopScore = null;
	if(range_order == asc){
		stopScore = getKeyRangeMaxByProp(key, index, score, (index||{})[label_stop_prop]);
	}else if(range_order == desc){
		stopScore = getKeyRangeMinByProp(key, index, score, (index||{})[label_stop_prop]);
	}
	return stopScore;
};

getKeyRangeStartMember = function(range_order, key, index, member){
	var startMember = member;
	var startProp = (index||{})[label_start_prop];
	var keyConfig = datatype.getKeyConfig(key);
	var startPropIndex = datatype.getConfigFieldIdx(keyConfig, startProp);
	var startBound = (index[label_exclude_cursor] ? '(' : '[');
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
		if(range_order != asc && !index[label_exclude_cursor]){
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
			tray.push('');							// character preceding all others
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

getKeyRangeStopMember = function(range_order, key, index, member){
	var stopMember = null;
	var stopProp = (index||{})[label_stop_prop];
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
		// NB: min/max prop not used in getKeyRangeStartMember since index can be used to represent that
		// NB: in case of a <min/max> argument, we have to deal with a string instead of char
		var stop = (range_order == desc ? (index || {}).min : (index || {}).max);	// max/min allowed value for stopProp
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


// get next set of keyChains after the index's key
// In the case of keySuffixes, command.isOverRange must be run across a set of keyChains
// NB: depending on attribute.limit, execution may be touch only some preceding keys (even for $count command)
// hence attribute.limit is very much recommended
getKeyChain = function(cmd, keys, key_type, key_field, key_suffixes, limit){
	if(!((key_suffixes || []).length > 0)){
		return [];
	}
	var key = keys[0];
	var keySuffixes = key_suffixes[0];
	// fetch indexes of key-suffix props
	var props = datatype.getConfigIndexProp(key_type, 'fields');
	var suffixPropIndexes = [];
	for(var i=0; i < props.length; i++){
		if(datatype.isConfigFieldKeySuffix(key_type, i)){
			if(!datatype.isConfigFieldBranch(key_type, i)){
				suffixPropIndexes.push(i);
			}else if(props[i] == key_field){		// if keyText is the field-branch
				suffixPropIndexes.unshift(i);
			}
		}
	}
	var cmdOrder = command.getOrder(cmd); 
	var keyChain = [];
	for(var suffixPropIdx=0; suffixPropIdx < suffixPropIndexes.length && keyChain.length < limit; suffixPropIdx++){
		var propIdx = suffixPropIndexes[suffixPropIdx];
		var prop = props[propIdx];
		var propBoundChain = datatype.getKeyFieldBoundChain(key, prop, cmdOrder);
		do{
			var previousKeySuffixes = keyChain[keyChain.length-1] || key_suffixes;
			var propCurrentChain = previousKeySuffixes[suffixPropIdx];
			var propNextChain = datatype.getKeyFieldNextChain(key, prop, cmdOrder, propCurrentChain);
			var isChainWithinBound = datatype.isKeyFieldChainWithinBound(key, prop, cmdOrder, propNextChain, propBoundChain);
			if(isChainWithinBound){
				var nextKeySuffixes = previousKeySuffixes.concat([]);
				nextKeySuffixes[suffixPropIdx] = propNextChain;
				keyChain.push(nextKeySuffixes);
			}
		}while(isChainWithinBound && keyChain.length < limit);
	}
	return keyChain;	
};

// decide here which database instance should be queried
getQueryDBInstance = function(cmd, keys, index, args, field){
	var key = keys[0];
	var keyConfig = datatype.getKeyConfig(key);
	var instanceDecisionFunction = datatype.getKeyClusterInstanceGetter(key);
	return instanceDecisionFunction.apply(this, [cmd, keys, index, args, field]);
};

// TODO: limit-offset across keys is not yet supported
queryDBInstance = function(instance, cmd, keys, key_type, key_field, key_suffixes, index, args, attribute, then){
	var ret = {code:1};
	var key = keys[0];
	var keyLabels = [datatype.getKeyLabel(key)];
	var commandType = command.getType(cmd);
	var keyCommand = command.toRedis(cmd);
	var suffixes = [];
	var prefixes = [];
	var chainedKeySuffixes = [key_suffixes || []];
	var chainedArgs = [args];
	var keyFieldArray = (key_field != null ? [key_field] : []);
	attribute = attribute || {};
	var limit = attribute.limit;
	var offset = attribute.offset;
	var nx = attribute.nx;
	var withscores = attribute.withscores;
	switch(instance.type){
	default:
		if(limit && !utils.startsWith(command.getType(cmd), 'count')){
			[].push.apply(suffixes, ['LIMIT', offset || 0, limit]);
		}
		if(withscores && !utils.startsWith(commandType, 'rangebylex') && !utils.startsWith(commandType, 'count')){
			suffixes.unshift('WITHSCORES');
		}
		if(nx && utils.startsWith(commandType, 'add')){
			prefixes.unshift('NX');
		}
		if(keyCommand == 'eval'){
			keyLabels = [];
			suffixes = [];
			prefixes = [];
		}
	}
	var resultset = null;
	var dataLen = 0;
	var idx = 0;
	var baseCaseDone = false;
	async.doWhilst(
	function(callback){
		switch(instance.type){
		default:
			var myKeySuffixes = chainedKeySuffixes[idx] || [];
			var myArgs = chainedArgs[idx];
			var keyTexts = [];
			if(keyLabels.length > 0){
				keyTexts.push((keyLabels.concat(keyFieldArray).concat(myKeySuffixes || [])).join(separator_key));
			}
			var queryArgs = keyTexts.concat(prefixes, myArgs, suffixes);
			instance.val[keyCommand](queryArgs, function(err, output){
				var errorMessage = 'FATAL: query.queryDB: args: '+keyCommand+'->'+queryArgs;
				if(!utils.logError(err, errorMessage)){
					if(!command.isOverRange(cmd)){
						resultset = output;
					}else{
						if(utils.startsWith(command.getType(cmd), 'count')){
							resultset = (resultset || 0) + output;
							dataLen = resultset;
						}else if(utils.startsWith(command.getType(cmd), 'range')){
							resultset = resultset || [];
							[].push.apply(resultset, output);
							dataLen = resultset.length;
						}
					}
				}
				callback(err);
			});
		}
	},
	function(){
		if(dataLen < (limit || Infinity)){
			return false;
		}
		// refill keyChain
		if(idx < chainedKeySuffixes.length - 1){
			idx++;
		}else{
			if(command.isOverRange(cmd)){
				// if there's a keyChain, the next ranges should begin from the beginning of the storage
				if(!baseCaseDone){
					baseCaseDone = true;
					var rangeMode = command.getRangeMode(cmd);
					var startArgIndex = 0;
					if(rangeMode == 'byscorelex'){						// keyCommand is <eval> in this case
						var evalArgScoreIndex = 3;
						var evalArgRankIndex = 5;
						startArgIndex = evalArgRankIndex;
					}
					if(['byrank', 'bylex', 'byscore', 'byscorelex'].indexOf(rangeMode) >= 0){
						var order = command.getOrder(cmd) || asc;
						// NB: byrank and byscorelex have start position aligned with the direction
						// => no need for changes based on range direction
						if(order == asc){
							args[startArgIndex] = ({byrank:0, bylex:'-', byscore:'-inf', byscorelex:0})[rangeMode];
						}else{
							args[startArgIndex] = ({byrank:0, bylex:'+', byscore:'+inf', byscorelex:0})[rangeMode];
						}
					}
				}
				// get next keys in the chain
				// TODO offset-ing across keyChains is not handled
				// 	--> simply decrement current offset value with the count of targets in last key
				var keyChain = getKeyChain(cmd, keys, key_type, key_field, chainedKeySuffixes[chainedKeySuffixes.length-1], limit);
				chainedKeySuffixes = [];
				chainedArgs = [];
				idx = 0;
				var evalArgKeyIndex = 2;
				for(var j=0; j < keyChain.length; j++){
					var kc = keyChain[j];
					if(keyCommand == 'eval'){
						// change the eval-args key
						var newArgs = args.concat([]);
						newArgs[evalArgKeyIndex] = kc;
						chainedArgs.push(newArgs);
						kc = [];					// eval requires no key prefix
					}
					chainedKeySuffixes.push(kc);
				}
			}
		}
		return (idx < chainedKeySuffixes.length);
	},
	function(err){
		if(!utils.logError(err, 'queryDBInstance')){
			ret = {code:0, data:resultset};
		}
		then(err, ret);
	});
};

getClusterInstanceQueryArgs = function(cluster_instance, cmd, keys, index, args, field, storage_attribute){
	var key = keys[0];
	var keyConfig = datatype.getKeyConfig(key);
	var keyLabel = datatype.getKeyLabel(key);
	var struct = datatype.getConfigStructId(keyConfig);
	var lua = null;
	var xid = storage_attribute.xid;
	var uid = storage_attribute.uid;
	var luaArgs = storage_attribute.luaArgs || [];
	var keySuffixes = storage_attribute.keySuffixes;
	switch(cluster.getClusterInstanceType(cluster_instance)){
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
				if(xid != null /*&& command.requiresXID(cmd)*/){
					args.unshift(xid);
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
				if(uid != null){
					if(xid != null && command.requiresXID(cmd)){
						args.unshift(xid);
					}
					args.unshift(uid);
				}
			}
			break;
		case 'zset':
			var rangeOrder = command.getOrder(cmd) || asc;
			// decide between the different type of ranges
			if(command.getType(cmd) == 'rangez' || command.getType(cmd) == 'delrangez' || command.getType(cmd) == 'countz'){
				cmd = cmd[datatype.getZRangeSuffix(keyConfig, index, args, xid, uid)];
			}
			// TODO test update function
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
					cmd = datatype.getCommandMode(datatype.getConfigCommand(keyConfig).count).bykey;
				}else{
					var startScore = getKeyRangeStartScore(rangeOrder, key, index, xid);
					if(startScore == Infinity || startScore == -Infinity){
						startScore = null;
					}
					var scorerank = {}; scorerank[asc] = '"zrevrank"'; scorerank[desc] = '"zrevrank"';
					lua = ' local startScore = ARGV[1];'
						+ 'local member = ARGV[2];'
						+ 'local score = nil;'
						+ 'local rank = nil;'
						+ 'if startScore ~= "'+label_lua_nil+'" then'			// verify unchanged score-member
						+ '     score = redis.call("zscore", KEYS[1], member);'			// check score tally first
						+ '     if score == startScore then'					// then rank can be set
						+ '          rank = redis.call('+scorerank[command.getOrder(cmd)]+', KEYS[1], member);'
						+ '     else'
						+ '          member = member.."'+collision_breaker+'";'
						+ '          score = startScore;'
						+ '          local count = redis.call("zadd", KEYS[1], "NX", score, member);'
						+ '          rank = redis.call('+scorerank[command.getOrder(cmd)]+', KEYS[1], member);'
						+ '          if count > 0 then'
						+ '               redis.call("zrem", KEYS[1], member);'
						+ '          end;'
						+ '     end;'
						+ 'else'
						+ '     rank = redis.call('+scorerank[command.getOrder(cmd)]+', KEYS[1], member);'	//no score to seek with
						+ 'end;'
						+ 'return rank;'
					luaArgs.unshift(uid);
					luaArgs.unshift(startScore || label_lua_nil);
				}
			}else if(utils.startsWith(command.getType(cmd), 'rangebyscorelex')){			// TODO add countbyscorelex
				// FYI: zrank and zrevrank are symmetric so a single code works!
				// NB: redis excluding/bound operator is not required/allowed here
				// 	TODO make use of new _exclusion flag
				// so [member] argument can be tweaked for the desired result
				var limit = attribute.limit;
				// NB: if startScore must be used, range [prop] must be provided
				var startScore = getKeyRangeStartScore(rangeOrder, key, index, xid);
				if(startScore == Infinity || startScore == -Infinity){
					startScore = null;
				}
				//TODO implement stopScore
				var rank = null;		// keyChains use this value to continue ranging across keys 
				var exclude = {}; exclude[asc] = '1'; exclude[desc] = '0';
				var zrank = {}; zrank[asc] = '"zrank"'; zrank[desc] ='"zrevrank"';
				var zrange = {}; zrange[asc] = '"zrange"'; zrange[desc] ='"zrevrange"';
				var zlimit = {}; zlimit[asc] = '-1'; zlimit[desc] = '0';
				lua = ' local rank = nil;'
					+ 'local startScore = ARGV[1];'
					+ 'local member = ARGV[2];'
					+ 'local score = nil;'
					+ 'if startScore ~= "'+label_lua_nil+'" then'						// verify unchanged score-member
					+ '     score = redis.call("zscore", KEYS[1], member);'					// check score tally first
					+ '     if score == startScore then'							// then rank can be set
					+ '          rank = redis.call('+zrank[command.getOrder(cmd)]+', KEYS[1], member);'
					+ '     else'
					+ '          member = member.."'+collision_breaker+'";'
					+ '          score = startScore;'
					+ '          local count = redis.call("zadd", KEYS[1], "NX", score, member);'
					+ '          rank = redis.call('+zrank[command.getOrder(cmd)]+', KEYS[1], member);'	// infer the previous position
					+ '          if count > 0 then'
					+ '               redis.call("zrem", KEYS[1], member);'
					+ '          end;'
					+ '     end;'
					+ 'else'
					+ '     rank = redis.call('+zrank[command.getOrder(cmd)]+', KEYS[1], member);'		// no score to seek with
					+ 'end;'
					+ 'if rank ~= nil then'
					+ '     rank = rank - '+exclude[command.getOrder(cmd)]+';'
					+ '     return redis.call('+zrange[command.getOrder(cmd)]+', KEYS[1], rank, '
						+ (limit != null ? 'rank+'+limit+'-1' : zlimit[command.getOrder(cmd)])
						+ (attribute.withscores ? ', "withscores");' : ');')
					+ 'else'
					+ '     return {};'
					+ 'end;'
				luaArgs.unshift(rank);		// TODO implement preset rank
				luaArgs.unshift(uid);
				luaArgs.unshift(startScore || label_lua_nil);
			}else if(utils.startsWith(command.getType(cmd), 'countbylex') || utils.startsWith(command.getType(cmd), 'rangebylex')
					|| utils.startsWith(command.getType(cmd), 'delrangebylex')){
				if(utils.startsWith(command.getType(cmd), 'countbylex') && uid == null){
					cmd = datatype.getCommandMode(datatype.getConfigCommand(keyConfig).count).bykey;
				}else{
					var startMember = getKeyRangeStartMember(rangeOrder, key, index, uid);
					var stopMember = getKeyRangeStopMember(rangeOrder, key, index, uid);
					args.unshift(stopMember);
					args.unshift(startMember);
				}
			}else if(utils.startsWith(command.getType(cmd), 'countbyscore') || utils.startsWith(command.getType(cmd), 'rangebyscore')
					|| utils.startsWith(command.getType(cmd), 'delrangebyscore')){
				if(utils.startsWith(command.getType(cmd), 'countbyscore') && (xid == null || xid == '')){
					cmd = datatype.getCommandMode(datatype.getConfigCommand(keyConfig).count).bykey;
				}else{
					var startScore = getKeyRangeStartScore(rangeOrder, key, index, xid);
					var stopScore = getKeyRangeStopScore(rangeOrder, key, index, xid);
					args.unshift(stopScore);
					args.unshift(startScore);
				}
			}else if(uid != null){
				if(command.requiresUID(cmd)){
					args.unshift(uid);
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
	return {command:cmd, keyText:keyText, args:args};
};

getResultSet = function(cmd, keys, qData_list, then){
	var ret = {code:1};
	var len = (qData_list || []).length;
	var instanceKeySet = {};
	var key = keys[0];
	var keyConfig = datatype.getKeyConfig(key);
	// PREPROCESS: prepare instanceKeySet for bulk query execution
	for(var i=0; i < len; i++){
		var elem = qData_list[i];
		var index = elem.index || {};
		// in contrast to args, attributes are placed just after the key before all other args and indexes
		var attribute = elem.attribute || {};							// limit, offset, nx, etc
		var storageAttr = parseIndexToStorageAttributes(key, cmd, index);
		var saKeys = Object.keys(storageAttr ||{});
		// process different field-branches
		for(j=0; j < saKeys.length; j++){
			// currently no need for input args in most cases; everything goes into [index]
			// args are required when several bits of info beyond xid and uid is involved
			// e.g. commands on string keys
			var args = (elem.args || []).concat([]);					// make input arguments immutable
			var field = saKeys[j];
			var fsa = storageAttr[field];
			if(field == label_lua_nil){
				field = null;
			}
			var dbInstance = getQueryDBInstance(cmd, keys, index, args, field);
			var dbInstanceArgs = getClusterInstanceQueryArgs(dbInstance, cmd, keys, index, args, field, fsa);
			cmd = dbInstanceArgs.command;
			args = dbInstanceArgs.args;
			var keyText = dbInstanceArgs.keyText;
			// bulk up the different key-storages for bulk-execution
			var dbInstanceFlag = dbInstance.label;
			if(!instanceKeySet[dbInstanceFlag]){
				instanceKeySet[dbInstanceFlag] = {_dbinstance: dbInstance, _keytext: {}};
			}
			if(!instanceKeySet[dbInstanceFlag]._keytext[keyText]){
				instanceKeySet[dbInstanceFlag]._keytext[keyText] = {};
				instanceKeySet[dbInstanceFlag]._keytext[keyText].attribute = attribute;
				instanceKeySet[dbInstanceFlag]._keytext[keyText].field = field;
				instanceKeySet[dbInstanceFlag]._keytext[keyText].keySuffixes = fsa.keySuffixes;
				instanceKeySet[dbInstanceFlag]._keytext[keyText].fieldBranchCount = saKeys.length;
				instanceKeySet[dbInstanceFlag]._keytext[keyText].index = index;
				instanceKeySet[dbInstanceFlag]._keytext[keyText].args = [];
				instanceKeySet[dbInstanceFlag]._keytext[keyText].indexes = [];
			}
			[].push.apply(instanceKeySet[dbInstanceFlag]._keytext[keyText].args, args);
		}
	}
	var resultset = null;
	var fuidIdx = {};
	var instanceFlags = Object.keys(instanceKeySet);
	async.each(instanceFlags, function(dbInstanceFlag, callback){
		var dbInstance = instanceKeySet[dbInstanceFlag]._dbinstance;
		var ks = Object.keys(instanceKeySet[dbInstanceFlag]._keytext);
		async.each(ks, function(keyText, cb){
			var newKeys = instanceKeySet[dbInstanceFlag]._keytext[keyText].newKeys;
			var field = instanceKeySet[dbInstanceFlag]._keytext[keyText].field;
			var keySuffixes = instanceKeySet[dbInstanceFlag]._keytext[keyText].keySuffixes;
			var fieldBranchCount = instanceKeySet[dbInstanceFlag]._keytext[keyText].fieldBranchCount;
			var index = instanceKeySet[dbInstanceFlag]._keytext[keyText].index;
			var args = instanceKeySet[dbInstanceFlag]._keytext[keyText].args;
			var attribute = instanceKeySet[dbInstanceFlag]._keytext[keyText].attribute;
			queryDBInstance(dbInstance, cmd, keys, keyConfig, field, keySuffixes, index, args, attribute, function(err, result){
				// POST-PROCESS
				if(!utils.logCodeError(err, result)){
					var withscores = (attribute || {}).withscores;
					if(Array.isArray(result.data)){
						var isRangeCommand = command.isOverRange(command);
						resultset = resultset || {};	// dict is used to maitain positioning when elements are deleted
						for(var i=0; i < result.data.length; i++){
							var detail = null;
							var uid = null;
							var xid = null;
							// zset indexes need withscores for completion
							if(command.getType(cmd).slice(-1) == 'z'){
								xid = null;
								uid = result.data[i];
								// withscores is only applicable to zset ranges except bylex
								if(withscores && utils.startsWith(command.getType(cmd), 'range') 
									&& !utils.startsWith(command.getType(cmd), 'rangebylex')){
									i++;
									xid = result.data[i];
								}
							}else{
								// TODO handle e.g. hgetall: does something similar to withscores
								xid = result.data[i];
								uid = args[i];
							}
							// for zsets having no withscores only uid prop can be guaranteed complete
							// but parseStorageAttributesToIndex should be smart enough to do this more correctly
							//if(!withscores){
							//	detail = {uid: detail.uid};
							//}
							detail = parseStorageAttributesToIndex(key, keyText, xid, uid);
							// querying field-branches can result in separated resultsets; merge them into a single record
							if(detail.field != null){
								var fuid = detail.fuid;
								var field = detail.field;
								var idx = fuidIdx[fuid];
								if(idx == null){
									// NB: if no result was found don't return any result, not even NULL
									// BUT this applies only to ranges; getters should still return NULL
									// else add first occurence of branches
									fuidIdx[fuid] = i;
									if(detail.index == null){
										if(!isRangeCommand){
											resultset[i] = null;
										}
									}else{
										resultset[i] = (detail.index);
									}
								}else{
									// NB: if no result was found remove possibly existing partial index
									// BUT this applies only to ranges; getters should still return NULL
									// else update first occurence of branches
									if(detail.index == null){
										if(isRangeCommand){
											delete resultset[idx];
										}else{
											resultset[idx] = null;
										}
									}else if(resultset[idx] != null){
										// fuid is not orphaned; index can be updated
										resultset[idx][field] = detail.index[field];
									}
								}
							}else{
								resultset[i] = detail.index;
							}
						}
					}else{
						// NB: different keys may have different returns e.g. hmset NX
						//	take care to be transparent about this
						var xid = result.data;
						var uid = args[0];
						if(utils.startsWith(command.getType(cmd), 'get')){
							var detail = parseStorageAttributesToIndex(key, keyText, xid, uid);
							// querying field-branches can result is separated resultsets; merge them into a single record
							if(detail.field != null){
								var field = detail.field;
								var fuid = detail.fuid;
								var idx = fuidIdx[fuid];
								if(idx == null){
									resultset = detail.index;
									fuidIdx[fuid] = true;
								}else{
									if(detail.index == null){
										resultset = detail.index;
									}else if(resultset != null){
										// fuid is not orphaned; index can be updated
										resultset[field] = detail.index[field];
									}
								}
							}else{
								resultset = detail.index;
							}
						}else{
							if(utils.isInt(xid)){
								// NB: when zcount/etc is used on field-branches, the average count is returned!!
								var count = parseInt(xid, 10);
								resultset = (resultset || 0) + count/fieldBranchCount;
							}else{
								// e.g. 'OK' response; null in case of conflicts e.g. for field branches??
								// TODO recheck bulk-response cases
								resultset = ((resultset || xid) == xid ? xid : label_lua_nil);
							}
						}
					}
				}
				cb(err);
			});
		}, function(err){
			callback(err);
		});
	}, function(err){
		if(Array.isArray(resultset)){
			var sortedKeys = Object.keys(resultset || {}).sort(function(a,b){return parseInt(a,10) > parseInt(b, 10);});
			ret.data = [];
			for(var i=0; i < sortedKeys.length; i++){
				ret.data.push(resultset[sortedKeys[i]]);
			}
		}else{
			ret.data = (resultset == label_lua_nil ? null : resultset);
		}
		// sometimes it's necessary to examined query results; see e.g. join.joinRange
		ret.keys = keys;
		if(!utils.logError(err, 'FATAL: query.getResultSet')){
			ret.code = 0;
		}else{
			ret.code = 1;	// announce partial success/failure
		}
		then (err, ret);
	});
}
shallowCopy = function(myDict){
	var myClone = {};
	for(var key in (myDict || {})){
		myClone[key] = myDict[key];
	}
	return myClone;
};
getComparatorProp = function(comparator, prop){
	return ((comparator || {})[prop] || {})[label_comparator_prop] || prop;
}
//var comparator = {keytext:[], score:[], uid:[]};
getComparison = function(order, comparator, index, ref, index_data, ref_data){
	order = order || database.asc;
	// NB: sometimes <keys> prop is not available, but if there's a single prop <keys> is not required
	var indexKeyType = index_data.keytype;
	var refKeyType = ref_data.keytype;
	var indexComparator = index_data[label_comparator] || {};
	var refComparator = ref_data[label_comparator] || {};
	// check keytext
	for(var i=0; i < comparator.keytext.length; i++){
		var prop = comparator.keytext[i];
		// indexes may have different translations of comparator props
		var indexProp = getComparatorProp(indexComparator, prop);
		var refProp = getComparatorProp(refComparator, prop);
		var indexPropIndex = datatype.getConfigFieldIdx(indexKeyType, indexProp);
		var refPropIndex = datatype.getConfigFieldIdx(refKeyType, refProp);
		// NB: key suffixes are checked as integers, not strings
		var indexVal = (index[indexProp] == null ? -Infinity : parseInt(datatype.splitConfigFieldValue(indexKeyType, indexProp, index[indexProp])[0], 10));
		var refVal = (ref[refProp] == null ? -Infinity : parseInt(datatype.splitConfigFieldValue(refKeyType, refProp, ref[refProp])[0], 10));
		if(indexVal < refVal){
			return (order == asc ? '<' : '>');
		}else if(indexVal > refVal){
			return (order == asc ? '>' : '<');
		}else if(indexVal != refVal){
			utils.logError('WARNING:', 'getComparison, keytext, %s, %s, %s', comparator, indexVal, refVal);
			return null;
		}
	}
	// check score
	for(var i=0; i < comparator.score.length; i++){
		var prop = comparator.score[i];
		// indexes may have different translations of comparator props
		var indexProp = getComparatorProp(indexComparator, prop);
		var refProp = getComparatorProp(refComparator, prop);
		var indexPropIndex = datatype.getConfigFieldIdx(indexKeyType, indexProp);
		var refPropIndex = datatype.getConfigFieldIdx(refKeyType, refProp);
		var indexPropFactor = datatype.getConfigPropFieldIdxValue(indexKeyType, 'factors', indexPropIndex);
		var refPropFactor = datatype.getConfigPropFieldIdxValue(refKeyType, 'factors', refPropIndex);
		var indexVal = (indexPropFactor || 0) * (datatype.splitConfigFieldValue(indexKeyType, indexProp, index[indexProp])[1] || 0);
		var refVal = (refPropFactor || 0) * (datatype.splitConfigFieldValue(refKeyType, refProp, ref[refProp])[1] || 0);
		if(indexVal < refVal){
			return (order == asc ? '<' : '>');
		}else if(indexVal > refVal){
			return (order == asc ? '>' : '<');
		}else if(indexVal != refVal){
			utils.logError('WARNING:', 'getComparison, score, %s, %s, %s', comparator, indexVal, refVal);
			return null;
		}
	}
	// check uid
	for(var i=0; i < comparator.uid.length; i++){
		var prop = comparator.uid[i];	
		// indexes may have different translations of comparator props
		var indexProp = getComparatorProp(indexComparator, prop);
		var refProp = getComparatorProp(refComparator, prop);
		var indexPropIndex = datatype.getConfigFieldIdx(indexKeyType, indexProp);
		var refPropIndex = datatype.getConfigFieldIdx(refKeyType, refProp);
		// like in redis, uid is ordered as a string
		var indexVal = ''+ (datatype.splitConfigFieldValue(indexKeyType, indexProp, index[indexProp])[1] || '');
		var refVal = ''+ (datatype.splitConfigFieldValue(refKeyType, refProp, ref[refProp])[1] || '');
		if(indexVal < refVal){
			return (order == asc ? '<' : '>');
		}else if(indexVal > refVal){
			return (order == asc ? '>' : '<');
		}else if(indexVal != refVal){
			utils.logError('WARNING','getComparison, uid, %s, %s, %s', comparator, indexVal, refVal);
			return null;
		}
	}
	var matchSymbol = '=';
	return matchSymbol;
}

query.singleIndexQuery = function(cmd, keys, index, args, attribute, then){
	// querying fields with partitions is tricky
	// execute the different partitions separately and merge the results
	// NB: partitions allow use of e.g. flags without breaking ordering
	var key = keys[0];
	var keyConfig = datatype.getKeyConfig(key);
	var cmdType = command.getType(cmd);
	var partitions = datatype.getConfigIndexProp(keyConfig, 'partitions') || [];
	var partitionCrossJoins = [];
	var indexClone = null;						// prevent mutating <index>
	var comparator = null;
	index = index || {};
	if(utils.startsWith(cmdType, 'range') || utils.startsWith(cmdType, 'count')){
		indexClone = {};
		// if the property is out-of-bounds (i.e. shouldn't play a role in range) ignore it altogether
		var comparator = datatype.getConfigFieldOrdering(keyConfig);
		var startProp = index[label_start_prop];
		var stopProp = index[label_stop_prop];
		var startPropScoreIdx = comparator.score.indexOf(startProp);
		var startPropUIDIdx = comparator.uid.indexOf(startProp);
		var stopPropScoreIdx = comparator.score.indexOf(stopProp);
		var stopPropUIDidx = comparator.uid.indexOf(stopProp);
		for(var prop in index){								// NB: some of these props are stray/unknown
			var propIndex = datatype.getConfigFieldIdx(keyConfig, prop);
			var propValue = index[prop];
			indexClone[prop] = propValue;						// log <index> props i.e. shallow copy
			if(Array.isArray(propValue)){
				if((propValue || []).length <= 1){				// sanitize propValue
					indexClone[prop] = propValue[0];			// null or singular value
				}else if(partitions[propIndex] == null){	// without declaration, it's assumed array is indeed raw value
					continue;
				}else if(!(comparator.keytext.indexOf(prop) >= 0)			// prop is not essential to resolving the keyText
					&& (startPropScoreIdx >= 0 && startPropScoreIdx > comparator.score.indexOf(prop))	// out-of-bounds
					&& (startPropUIDIdx >= 0 && startPropUIDIdx > comparator.uid.indexOf(prop))
					&& (stopPropScoreIdx >= 0 && stopPropScoreIdx > comparator.score.indexOf(prop))
					&& (stopPropScoreIdx >=0 && stopPropScoreIdx > comparator.uid.indexOf(prop))){
					delete indexClone[prop];				// don't use prop
					continue;
				}else{ // cross-join the values of the different partitioned props with array values
					if(partitionCrossJoins.length == 0){
						// initialize
						partitionCrossJoins = propValue.map(function(a){var b = {}; b[prop] = a; return b;});
						continue;
					}
					var multiBucket = null;
					for(var i=0; i < propValue.length; i++){
						var bucket = (i==0 ? partitionCrossJoins : partitionCrossJoins.map(function(a){return shallowCopy(a);}));
						for(var j=0; j < bucket.length; j++){
							var pcj = bucket[j];
							pcj[prop] = propValue[i];
						}
						if(i != 0){
							if((multiBucket || []).length == 0 ){
								multiBucket = bucket;
							}else{
								[].push.apply(multiBucket, bucket);
							}
						}
					}
					if((multiBucket || []).length > partitionCrossJoins.length){
						[].push.apply(multiBucket, partitionCrossJoins);
						partitionCrossJoins = multiBucket;
					}else{
						[].push.apply(partitionCrossJoins, multiBucket || []);
					}
				}
			}
		}
	}
	var singleIndexList = [{index:(indexClone || index), args:args, attribute:attribute}];
	if(partitionCrossJoins.length == 0){
		return getResultSet(cmd, keys, singleIndexList, then);
	}else{
		// merge query results in retrieval order
		comparator = comparator || datatype.getConfigFieldOrdering(keyConfig);
		var partitionResults = [];
		var partitionIdx = [];
		var boundIndex = null;
		var boundPartition = null;
		var pcjIndexes = Object.keys(partitionCrossJoins);
		var mergeData = [];
		async.each(pcjIndexes, function(idx, callback){
			if(partitionResults[idx] != null){				// query is not empty
				return(callback(null));
			}
			var pcj = partitionCrossJoins[idx];
			for(var prop in pcj){
				indexClone[prop] = pcj[prop];
			}
			singleIndexList[0].index = boundIndex || indexClone;		// bootstrap with indexClone
			getResultSet(cmd, keys, singleIndexList, function(err, result){
				partitionResults[idx] = (result || []).data;
				callback(err);
			});
		}, function(err){
			var ret = {code:1};
			if(!utils.logError(err, 'query.singleIndexQuery')){
				var alive = true;
				if(utils.startsWith(cmdType, 'count')){
					mergeData = partitionResults.reduce(function(a,b){return a+b;});
					alive = false;
				}
				while(alive){
					alive = false;
					// reset boundIndex for next comparison cycle
					// else how could higher indexes be less-than the previous boundIndex
					boundIndex = null;
					boundPartition = null;
					// compare next values across the partitions and pick the least
					for(var i=0; i < pcjIndexes.length; i++){
						var idx = partitionIdx[i] || 0;			// indexes are initially null
						if(idx >= (partitionResults[i] || []).length){
							// if a partition runs out of results, search continues as usual with others
							continue;
						}
						var index = partitionResults[i][idx];
						var indexData = {keytype: keyConfig};			// no need for comparators here
						var reln = getComparison(command.getOrder(cmd), comparator, index, boundIndex||{}, indexData, indexData);
						if(boundIndex == null || reln == '<'){
							boundPartition = i;
							boundIndex = index;
							alive = true;
						}else if(reln == '='){
							// this should NOT happen but in case the values match,
							// just skip this partition's value
							utils.logError('WARNING: query.singleIndexQuery');
							partitionIdx[i] = 1 + (partitionIdx[i] || 0);
						}
					}
					if(alive){	// there has been a change in the boundIndex
						// boundIndex holds the least index across the partitions
						mergeData.push(boundIndex);
						partitionIdx[boundPartition] = 1 + (partitionIdx[boundPartition] || 0);
					}
				}
				ret = {code:0, data:mergeData};
			}
			then(err, ret);	
		});
	}
}
// TODO indexList allows callers to potentially send strange but understandable queries
//	e.g. zdelrangebyscore index1 index2 ... indexN
//	the question is whether this can end up in the same queryKeySet, and what happens then??
//	this interface should not be allowed for calls which don't take multiple indexes
query.indexListQuery = function(cmd, keys, index_list, then){
	getResultSet(cmd, keys, index_list, then);
}


module.exports = query;
