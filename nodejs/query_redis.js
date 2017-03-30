var utils = require('./utils');
var datatype = require('./datatype');
var command = datatype.command;


var query_redis = {
	const_limit_default: 50,
};


var separator_key = datatype.getKeySeparator();
var separator_detail = datatype.getDetailSeparator();
var collision_breaker = datatype.getCollisionBreaker();
var redisMaxScoreFactor = datatype.getRedisMaxScoreFactor();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();
var nullChar = datatype.getNullCharacter();
var emptyChar = datatype.getEmptyCharacter();
// these values are used to represent +/- Infinity
var firstUnicode = datatype.getJSFirstUnicode();
var lastUnicode = datatype.getJSLastUnicode();
var redisMinScore = datatype.getRedisMinScore();
var redisMaxScore = datatype.getRedisMaxScore();

var label_lua_nil = '_nil';					// passing raw <nil> to lua is not exactly possible


registerStructElement = function registerStructElement(type, struct, elem, fieldidx, key_type){
	if(elem != null){
		if(type == 'usual'){
			// for 'usual' fields, append non-null values to both 'usual' ...
			if(struct.usual == null){
				struct.usual = [];
			}
			struct.usual.push(elem);
			//  ... and append also to preceding 'branches'
			// field-branches after current field remain unprocessed
			for(j=0; j < fieldidx; j++){
				if(datatype.isConfigFieldBranch(key_type, j)){
					if(struct.branch[j] == null){
						struct.branch[j] = [];
					}
					struct.branch[j].push(elem);
				}
			}
		}else if(type == 'branch'){
			// for 'branch' fields, append non-null values in 'usual' to 'branches'
			struct.branch[fieldidx] = [elem];
			if(struct.usual != null){
				// unshift => the cumulative data preceed that of the current field-branch
				[].unshift.apply(struct.branch[fieldidx], struct.usual);
			}
		}
	}
};
// NB: special chars are used to encode null and empty string; however these cannot be stored in scores
query_redis.parseIndexToStorageAttributes = function parseQueryIndexToRedisStorageAttributes(key, meta_cmd, index, rangeConfig){
	// NB: rangeConfig.startValue is mostly useful for the zrange query
	// if specified along with rangeConfig.index, update the index's value with startValue
	// this is necessary for the correct start keytext/keysuffixes
	if(rangeConfig && rangeConfig.startValue != null && rangeConfig.startProp in (index || {})){
		index = utils.shallowCopy(index);			// NB: index variable is detached from rangeConfig.index
		index[rangeConfig.startProp] = rangeConfig.startValue;
	}
	var usual = 'usual';
	var branch = 'branch';
	var keyBranches = [];
	var mainFieldBranch = null;
	var luaArgs = {usual: [], branch: []}; 
	var keyFieldSuffixes = {usual: [], branch: []}; 
	var uidPrefixes = {usual: [], branch: []};
	var xidPrefixes = {usual: [], branch: []};
	var keyConfig = datatype.getKeyConfig(key);
	var struct = datatype.getConfigStructId(keyConfig);
	var fields = datatype.getConfigIndexProp(keyConfig, 'fields') || [];			// fields sorted by index
	var offsets = datatype.getConfigIndexProp(keyConfig, 'offsets') || [];
	var offsetgroups = datatype.getConfigIndexProp(keyConfig, 'offsetgroups') || [];
	var doneOffsetGroups = {};
	// using the defined fields is a better idea than using the currently provided possibly-partial or even extraneous fields
	// it uniformly treats every index argument with complete field-set, albeit some having NULL values
	// this is safer; fields have fixed ordering and positions!
	// for example, note that absent fields may still make an entry in keyFieldSuffixes!!
	for(var i=0; i < fields.length; i++){
		var field = fields[i];
		var fieldIndex =  i;
		var fieldVal = index[field];
		var isFieldProvided = (field in (index || {}));
		var splits = datatype.splitConfigFieldValue(keyConfig, index, field) || [];
		// key suffixes and branches
		var fieldBranch = splits[2];
		// NB: this must be parsed even if the value of the index.$field is <null>
		// else how do you direct/indicate choice of key (for fieldBranches) for getter queries?!
		var type = usual;
		if(fieldBranch != null){
			type = branch;
			if(fieldBranch in (index || {})){
				keyBranches.push(fieldBranch);
			}
		}
		if(datatype.isConfigFieldKeySuffix(keyConfig, fieldIndex)){
			var offsetGroupIndex = offsetgroups[fieldIndex];
			if(offsetGroupIndex == null){
				offsetGroupIndex = fieldIndex;
			}
			// NB: only set once; keysuffix of other offsetgroups would be ignored!
			// 	datatype.splitConfigFieldValue takes care of returning the keysuffix of the first non-null offsetgroup field
			// this means once any offsetgroup field is given a value, it must be sufficient to define the keysuffix
			if(!doneOffsetGroups[offsetGroupIndex]){
				doneOffsetGroups[offsetGroupIndex] = true;
				var valueFieldIdx = splits[3];	// the field within the offsetgroup whose value was used
				var val = datatype.encodeVal(splits[0], type, struct, 'keysuffix', (offsetGroupIndex != null & offsetGroupIndex >= 0));
				var elem = {keysuffix:val, fieldidx:fieldIndex, valuefieldidx:valueFieldIdx};
				registerStructElement(type, keyFieldSuffixes, elem, fieldIndex, keyConfig);
			}
		}
		// xid and uid
		// for <xid>, distinguish between 0 and null for zrangebyscore and zrangebylex respectively
		if(datatype.isConfigFieldUIDPrepend(keyConfig, fieldIndex)){
			// for zset, prefix floats with special-characters so they maintain their sorting; see datatype.getConfigOrdering
			var mytype = datatype.getConfigPropFieldIdxValue(keyConfig, 'types', fieldIndex);
			var pad = datatype.processPaddingForZsetUID(struct, mytype, splits[1]);
			var val = datatype.encodeVal(pad, type, struct, 'uid', isFieldProvided);
			var elem = {uid:val, fieldidx:fieldIndex};
			registerStructElement(type, uidPrefixes, elem, fieldIndex, keyConfig);
		}
		if(struct == 'zset'){
			if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
				if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
					var val = datatype.encodeVal(splits[1], type, struct, 'xid', isFieldProvided); 
					var elem = {xid:val, fieldidx:fieldIndex};
					registerStructElement(type, xidPrefixes, elem, fieldIndex, keyConfig);
				}
			}
		}else if(!datatype.isConfigFieldStrictlyUIDPrepend(keyConfig, fieldIndex)){	// i.e. definition of XID vs. UID
			var val = datatype.encodeVal(splits[1], type, struct, 'xid', isFieldProvided);
			var elem = {xid:val, fieldidx:fieldIndex};
			registerStructElement(type, xidPrefixes, elem, fieldIndex, keyConfig);
		}
		// NB: upsert can NOT cause changes in key/keysuffix
		// since keyFieldSuffixes+UID are required to be complete in order to identify target
		// => in case upsert is tried on such fields, only the non-keysuffix portion is updated
		//	as it turns out, this fortunately prevents offsetgroups from updating keysuffixes which subsumes their contribution
		if(utils.startsWith(command.getType(meta_cmd), 'upsert')){
			// update if field is explicitly mentioned; else the stored value would be retained
			// NB: when fieldBranches are mentioned, they are always updated
			//	this is okay since the branch is essentially for the value of the fieldbranch
			if(struct == 'zset'){
				if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
					var pvf = datatype.getConfigPropFieldIdxValue(keyConfig, 'factors', fieldIndex);
					var next_pvf = datatype.getConfigFieldPrefactor(keyConfig, fieldIndex);
					if(next_pvf == null){
						var outOfBounds = 1000*redisMaxScoreFactor;		// just large enough
						next_pvf = outOfBounds;
					}
					// NB: for Scores 0 and NULL cannot be distinguished
					var val = datatype.encodeVal(splits[1], type, struct, 'xid', isFieldProvided);
					if(val == null){
						val = label_lua_nil;
					}
					var elem = [val, pvf, next_pvf];
					registerStructElement(type, luaArgs, elem, fieldIndex, keyConfig);
				}
			}else{
				var val = datatype.encodeVal(splits[1], type, struct, 'xid', isFieldProvided);
				if(val == null){
					val = label_lua_nil;
				}
				var elem = [val];
				registerStructElement(type, luaArgs, elem, fieldIndex, keyConfig);
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
		var idx = datatype.getConfigFieldIdx(keyConfig, kb);
		var sa = {};
		if(type == 'usual'){
			sa.keyfieldsuffixes = keyFieldSuffixes.usual || [];
			sa.luaargs = luaArgs.usual;
			sa.uidprefixes = uidPrefixes.usual;
			sa.xidprefixes = xidPrefixes.usual;
		}else if(type == 'branch'){
			// fallback on struct.usual since some branches never get to call registerStructElement
			sa.keyfieldsuffixes = keyFieldSuffixes.branch[idx] || keyFieldSuffixes.usual || [];
			sa.luaargs = (luaArgs.branch[idx] == null ? luaArgs.usual : luaArgs.branch[idx]);
			sa.uidprefixes = (uidPrefixes.branch[idx] == null ? uidPrefixes.usual : uidPrefixes.branch[idx]);
			sa.xidprefixes = (xidPrefixes.branch[idx] == null ? xidPrefixes.usual : xidPrefixes.branch[idx]);
		}
		storageAttr[kb] = sa;
	}
	return storageAttr;
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
		var rescaleScoreAddend = false;
		var field = fields[i];
		var fieldIndex = i;
		var type = datatype.getConfigPropFieldIdxValue(keyConfig, 'types', fieldIndex);
		var fieldOffset = offsets[fieldIndex];
		var facet = null;
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
			facet = 'uid';
			// for zsets, remove padding special-characters in the case of numerics 
			fieldVal = datatype.removePaddingFromZsetUID(struct, type, fieldVal);
			index[field] = fieldVal;
		}else if(xid != null){
			// NB: splitting sometimes precedes the RHS value with significant zeros (e.g. [123, 00045])
			// hence score values may have to be rescaled based on the offset
			rescaleScoreAddend = true;
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
				facet = 'xid';
			}else if(datatype.isConfigFieldScoreAddend(keyConfig, fieldIndex)){
				// basic splicing of field's component from score
				var fact = factors[fieldIndex];
				var preFact = datatype.getConfigFieldPrefactor(keyConfig, fieldIndex);
				var val = Math.floor((preFact ? xid % preFact : xid) / fact);
				index[field] = val;
				facet = 'xid';
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
				// handle offsetgroups
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
				// suffix may need rescaling due to lost significant preceding zeros of score addends
				if(rescaleScoreAddend){
					var prefixInfoLen = fieldOffset%100;
					var trailSuffix = suffix.slice(prefixInfoLen);
					if(parseInt(trailSuffix || '0', 10) != 0){
						var trailInfoLen = Math.floor(fieldOffset/100);
						var deficit = trailInfoLen - ((index[field] || 0)+'').length;
						if(deficit > 0){
							suffix = suffix+(Array(deficit+1).join('0'));	// pad with significant zeros
						}
					}
				}
				suffix = datatype.decodeVal(suffix, type, struct, 'keysuffix');
				index[field] = datatype.decodeVal(index[field], type, struct, facet)
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


getNextCodeWord = function getNextCodeWord(word){
	if(word){
		return word.slice(0,-1)+String.fromCharCode(1+word.charCodeAt(word.length-1))
	}else{
		return null;
	}
};
getPreviousCodeWord = function getPreviousCodeWord(word){
	if(word){
		return word.slice(0,-1)+String.fromCharCode(-1+word.charCodeAt(word.length-1))
	}else{
		return null;
	}
};

// excludecursor is used instead of range_config.excludeCursor since in the case of .byscorelex it shouldn't affect score
// hence callers must specify accordingly
getKeyRangeBoundValue = function getQueryKeyRangeBoundValue(range_order, key_config, range_config, valprefixes, valprop, keysuffixes
						, ascExtremePosition, descExtremePosition, boundPropFlag, boundValueFlag, valuetype, excludecursor){
	// NB: this function may be called with valprefixes (e.g. from keychains) which do not correspond to the range_config.index
	//	=> do not make any correspondence between the two
	if(range_config == null){
		return (range_order == asc ? ascExtremePosition : descExtremePosition);
	}else if(range_config[boundValueFlag] != null && range_config[boundPropFlag] == null){
		// NB: without an index-prop, there's no config reference for processing (e.g. for keysuffix)
		var value = range_config[boundValueFlag];
		if(valuetype == 'score'){
			value = (range_config.excludeCursor ? '(' : '') + value;
		}else if(valuetype == 'member'){
			// prepending bound operator is required by redis API for .bylex commands
			value = (range_config.excludeCursor ? '(' : '[') + value;
		}
		return value;
	}else if(range_config.index == null){
		return (range_order == asc ? ascExtremePosition : descExtremePosition);
	}
	var boundProp = range_config[boundPropFlag];
	var boundValue = range_config[boundValueFlag];
	var boundPropIdx = datatype.getConfigFieldIdx(key_config, boundProp);
	if(boundPropIdx == null || boundPropIdx < 0){
		boundPropIdx = Infinity;
	}
	var isBoundPropProcessed = false;	// track if boundProp refers to a member of valprefixes
	var missingFields = 0;
	var err = 'missing fields in range definition; config:= '+datatype.getConfigId(key_config)+', before field:= ';
	var value = null;
	var truncated = false;			// indicates whether <value> was truncated
	for(var i=0; i < (valprefixes || []).length; i++){
		var elem = valprefixes[i];
		var prefix = (value != null ? (value + separator_detail) : '');
		if(elem[valprop] != null && missingFields > 0){
			throw new Error(err + datatype.getConfigPropFieldIdxValue(key_config, 'fields', elem.fieldidx));
		}else if(elem.fieldidx == boundPropIdx){
			isBoundPropProcessed = true;
			// NB: boundValue is raw and unprocessed, hence may resolve to an entirely different keysuffix
			// in which case the boundPropDelimiter would have to be before and not after the boundValue
			// 	this is equivalently implemented by descExtremePosition/ascExtremePosition
			// 	the keychain search module would take care of ranging to other keysuffixes
			// NB: sometimes boundValue is NULL but the range has entered a new keysuffix e.g. happens with startProp
			if(datatype.isConfigFieldKeySuffix(key_config, boundPropIdx)){
				// find the keysuffix corresponding to the boundProp
				var boundKS = null;
				var boundOSGIdx = datatype.getConfigPropFieldIdxValue(key_config, 'offsetgroups', boundPropIdx);
				for(var j=0; j < keysuffixes.length; j++){
					if(keysuffixes[j].fieldidx == boundPropIdx || keysuffixes[j].fieldidx == boundOSGIdx){
						boundKS = keysuffixes[j].keysuffix;
						break;
					}
				}
				var boundIndex = utils.shallowCopy(range_config.index);
				if(boundValue != null){
					boundIndex[boundProp] = boundValue;
				}
				var splits = datatype.splitConfigFieldValue(key_config, boundIndex, boundProp);
				var origKS = splits[0];
				var isBefore = datatype.isKeyFieldChainWithinBound(key_config, boundPropIdx, range_order, origKS, boundKS);
				var isAfter = datatype.isKeyFieldChainWithinBound(key_config, boundPropIdx, range_order, boundKS, origKS);
				if(isBefore && isAfter){								// i.e. equality
					boundValue = splits[1];
				}else if(boundPropFlag == 'startProp' ? isBefore : isAfter){				// match everything within the prop's scope
					if(valuetype == 'score'){
						var boundPrefactor = datatype.getConfigFieldPrefactor(key_config, boundPropIdx);
						var boundFactor = datatype.getConfigPropFieldIdxValue(key_config, 'factors', boundPropIdx);
						if(boundPrefactor == null){
							boundPrefactor = 10*redisMaxScoreFactor;
						}
						var maxDiff = Math.floor((boundPrefactor - boundFactor)/boundFactor);	// e.g. 999
						if(boundPropFlag == 'startProp'){
							boundValue = (range_order == asc ? 0 : maxDiff);		// TODO incompatible with -ve values
						}else if(boundPropFlag == 'stopProp'){
							boundValue = (range_order == asc ? maxDiff : 0);		// TODO incompatible with -ve values
						}
					}else if(valuetype == 'member'){
						boundValue = (range_order == asc ? ascExtremePosition : descExtremePosition);
					}
				}else if(boundPropFlag == 'startProp' ? isAfter : isBefore){				// match nothing
					value = (range_order == asc ? descExtremePosition : ascExtremePosition);
					break;
				}
			}else{
				boundValue = elem[valprop];
			}
			if(valuetype == 'score'){
				value = value + datatype.getFactorizedConfigFieldValue(key_config, elem.fieldidx, boundValue);
			}else if(valuetype == 'member'){
				value = prefix + boundValue;
			}
		}else if(elem.fieldidx > boundPropIdx){
			truncated = true;
			break;
		}else if(elem[valprop] == null){
			missingFields++;
		}else{
			if(valuetype == 'score'){
				var addend = datatype.getFactorizedConfigFieldValue(key_config, elem.fieldidx, elem[valprop]);
				if(addend != null){			// null valprefixes should yield value=null => lexicographical ordering
					value = value + addend;
				}
			}else if(valuetype == 'member'){
				value = prefix + elem[valprop];
			}
		}
	}
	if(value != null){
		if(valuetype == 'score'){
			// computations could overflow redis-score limits
			if(value > redisMaxScore){
				value = redisMaxScore;
			}else if(value < redisMinScore){
				value = redisMinScore;
			}
			// it would be nice not to have to use the '(' operator
			// lua scripts could also use this output without having to remove '('
			// but this cannot be implemented by just skipping the score value
			// skipping the score would be wrong for .byscorelex commands
			// since it is the uid/member value which should be skipped in this case
			// excludecursor instead of range_config.excludeCursor is introduced to regulate this
			if(truncated){
				var high = 0;
				if((boundPropFlag == 'startProp' && !excludecursor) || (boundPropFlag == 'stopProp' && excludecursor)){
					high = (range_order == asc ? 0 : 1);
				}else if((boundPropFlag == 'stopProp' && !excludecursor) || (boundPropFlag == 'startProp' && excludecursor)){
					high = (range_order == asc ? 1 : 0);
				}
				var low = 0;
				if((boundPropFlag == 'startProp' && range_order == asc) || (boundPropFlag == 'stopProp' && range_order != asc)){
					low = 0;
				}else if((boundPropFlag == 'stopProp' && range_order == asc) || (boundPropFlag == 'startProp' && range_order != asc)){
					low = -1;
				}
				value = value + datatype.getFactorizedConfigFieldValue(key_config, boundPropIdx, high) + low;
			}else{
				if(excludecursor){
					if(boundPropFlag == 'startProp'){
						value = value + (range_order == asc ? 1 : -1);
					}else if(boundPropFlag == 'stopProp'){
						value = value + (range_order == asc ? -1 : 1);
					}
				}
			}
		}else if(valuetype == 'member'){
			// prepending bound operator is required by redis API for .bylex commands
			// it would have been preferable to tweak member element instead
			// since lua scripts can't take these bound operators
			// nonetheless tweak member, so that lua scripts just have to cut off operator
			value = '['+value;
			if(truncated){	// use the left and right char of the separator_detail to delimit range
				var delimiter = '';
				if((boundPropFlag == 'startProp' && !excludecursor) || (boundPropFlag == 'stopProp' && excludecursor)){
					delimiter = (range_order == asc ? getPreviousCodeWord(separator_detail) : getNextCodeWord(separator_detail));
				}else if((boundPropFlag == 'stopProp' && !excludecursor) || (boundPropFlag == 'startProp' && excludecursor)){
					delimiter = (range_order == asc ? getNextCodeWord(separator_detail) : getPreviousCodeWord(separator_detail));
				}
				value = value + delimiter;
			}else{	// just (in/de)crement last char of value in case of excludecursor
				if(excludecursor){
					if(boundPropFlag == 'startProp'){
						value = (range_order == asc ? getNextCodeWord(value) : getPreviousCodeWord(value));
					}else if(boundPropFlag == 'stopProp'){
						value = (range_order == asc ? getPreviousCodeWord(value) : getNextCodeWord(value));
					}
				}
			}
		}
	}
	return value;
};

getKeyRangeStartScore = function getQueryKeyRangeStartScore(range_order, key_config, range_config, xidprefixes, keysuffixes, excludecursor){
	return getKeyRangeBoundValue(range_order, key_config, range_config, xidprefixes, 'xid', keysuffixes
					, redisMinScore, redisMaxScore, 'startProp', 'startValue', 'score', excludecursor);
};

getKeyRangeStopScore = function getQueryKeyRangeStopScore(range_order, key_config, range_config, xidprefixes, keysuffixes, excludecursor){
	return getKeyRangeBoundValue(range_order, key_config, range_config, xidprefixes, 'xid', keysuffixes
					, redisMaxScore, redisMinScore, 'stopProp', 'stopValue', 'score', excludecursor);
};

getKeyRangeStartMember = function getQueryKeyRangeStartMember(range_order, key_config, range_config, uidprefixes, keysuffixes, excludecursor){
	return getKeyRangeBoundValue(range_order, key_config, range_config, uidprefixes, 'uid', keysuffixes
					, firstUnicode, lastUnicode, 'startProp', 'startValue', 'member', excludecursor);
};

getKeyRangeStopMember = function getQueryKeyRangeStopMember(range_order, key_config, range_config, uidprefixes, keysuffixes, excludecursor){
	return getKeyRangeBoundValue(range_order, key_config, range_config, uidprefixes, 'uid', keysuffixes
					, lastUnicode, firstUnicode, 'stopProp', 'stopValue', 'member', excludecursor);
	
};


getKeyText = function getKeyText(key, keysuffixes, field){
	var keyLabel = datatype.getKeyLabel(key);
	var ks = keysuffixes.map(function(kfs){return kfs.keysuffix;});
	var keyText = [keyLabel].concat(field != null ? [field] : [], ks).join(separator_key);
	return keyText;
};

query_redis.getCIQA = function getCIQA(meta_cmd, keys, rangeConfig, attribute, field, storage_attribute){
	var key = keys[0];
	var cmd = meta_cmd;
	var keyConfig = datatype.getKeyConfig(key);
	var struct = datatype.getConfigStructId(keyConfig);
	var lua = null;
	var xidPrefixes = storage_attribute.xidprefixes || [];
	var uidPrefixes = storage_attribute.uidprefixes || [];
	var xid = xidPrefixes.map(function(a){return (a.xid == null ? '' : a.xid);}).join(separator_detail);		// except for zset
	var uid = uidPrefixes.map(function(a){return (a.uid == null ? '' : a.uid);}).join(separator_detail);		// except for zset
	var luaArgs = (storage_attribute.luaargs || []).reduce(function(a,b){return a.concat(b);}, []);
	var keySuffixes = storage_attribute.keyfieldsuffixes;
	var args = [];
	var limit = (attribute || {}).limit;
	switch(struct){
	case 'string':
		if(utils.startsWith(command.getType(cmd), 'upsert')){
			lua = 'local xid = redis.call("get", KEYS[1]);'
				+ 'local xidList = string.gmatch(xid'+separator_detail+', "([^'+separator_detail+']+)'+separator_detail+'");'
				+ 'local xidStr = "";'
				+ 'local i = 0;'
				+ 'for val in xidList do'
				+ '    i = i + 1;'
				+ '    if i > 1 then'
				+ '        xidStr = xidStr.."'+separator_detail+'"'
				+ '    end;'
				+ '    if ARGV[i] ~= "'+label_lua_nil+'" then'
				+ '        xidStr = xidStr..ARGV[i];'
				+ '    else'
				+ '        xidStr = xidStr..val;'
				+ '    end;'
				+ 'end;'
				+ 'if xid ~= xidStr then'
				+ '    return redis.call("set", KEYS[1], xidStr);'
				+ 'else'
				+ '    return 0;'
				+ 'end;';
		}else{
			if(command.requiresXID(cmd)){
				args.unshift(xid == null ? '' : xid);
			}
			if(command.requiresUID(cmd)){
				args.unshift(uid == null ? '' : uid);
			}
		}
		break;
	case 'set':
	case 'hash':
		if(utils.startsWith(command.getType(cmd), 'upsert')){
			lua = 'local xid = redis.call("hget", KEYS[1], ARGV[1]);'
				+ 'local xidList = string.gmatch(xid'+separator_detail+', "([^'+separator_detail+']+)'+separator_detail+'");'
				+ 'local xidStr = "";'
				+ 'local i = 1;'				// ARGV[1] holds UID; to be skipped
				+ 'for val in xidList do'
				+ '    i = i + 1;'
				+ '    if i > 2 then'
				+ '        xidStr = xidStr.."'+separator_detail+'"'
				+ '    end;'
				+ '    if ARGV[i] ~= "'+label_lua_nil+'" then'
				+ '        xidStr = xidStr..ARGV[i];'
				+ '    else'
				+ '        xidStr = xidStr..val;'
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
				args.unshift(xid == null ? '' : xid);
			}
			if(command.requiresUID(cmd)){
				args.unshift(uid == null ? '' : uid);
			}
		}
		break;
	case 'zset':
		// decide between the different type of ranges
		if(command.getType(cmd) == 'rangez' || command.getType(cmd) == 'delrangez' || command.getType(cmd) == 'countz'){
			cmd = command.getMode(cmd)[datatype.getZRangeSuffix(keyConfig, rangeConfig, xidPrefixes, uidPrefixes)];
		}
		xid = (xidPrefixes || []).reduce(function(a,b){
						return (a + datatype.getFactorizedConfigFieldValue(keyConfig, b.fieldidx, b.xid));
					}, 0);
		uid = (uidPrefixes || []).reduce(function(a,b){
						return ((a == null ? '' : a+separator_detail)+datatype.encodeVal(b.uid, null, 'zset', 'uid', true));
					}, null);
		var cmdOrder = command.getOrder(cmd) || asc;
		var cmdType = command.getType(cmd);
		if(utils.startsWith(cmdType, 'upsert')){
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
		}else if(utils.startsWith(cmdType, 'rankbyscorelex')){
			// NB: unlike the ranging variants, rankbyscorelex is an entirely different command
			// it is as-such not resolved but rather specified by the caller
			var scorerank = {}; scorerank[asc] = '"zrevrank"'; scorerank[desc] = '"zrevrank"';
			lua = ' local startScore = ARGV[1];'
				+ 'local member = ARGV[2];'
				+ 'local score = nil;'
				+ 'local rank = nil;'
				+ 'if startScore ~= "'+label_lua_nil+'" then'					// verify unchanged score-member
				+ '    score = redis.call("zscore", KEYS[1], member);'				// check score tally first
				+ '    if score == startScore then'						// then rank can be set
				+ '        rank = redis.call('+scorerank[cmdOrder]+', KEYS[1], member);'
				+ '    else'
				+ '        member = member.."'+collision_breaker+'";'
				+ '        score = startScore;'
				+ '        local count = redis.call("zadd", KEYS[1], "NX", score, member);'
				+ '        rank = redis.call('+scorerank[cmdOrder]+', KEYS[1], member);'
				+ '        if count > 0 then'
				+ '            redis.call("zrem", KEYS[1], member);'
				+ '        end;'
				+ '    end;'
				+ 'else'
				+ '    rank = redis.call('+scorerank[cmdOrder]+', KEYS[1], member);'		//no score to seek with
				+ 'end;'
				+ 'return rank;'
			var startScore = getKeyRangeStartScore(cmdOrder, keyConfig, rangeConfig, xidPrefixes, keySuffixes, false);
			var scoreMap = {null:0, '-inf':redisMinScore, '+inf':redisMaxScore};			// zlex has score=0
			if(startScore && startScore[0] == '('){
				startScore = parseFloat(startScore.slice(1), 10);
			}
			startScore = scoreMap[startScore] || startScore;
			var startMember = getKeyRangeStartMember(cmdOrder, keyConfig, rangeConfig, uidPrefixes, keySuffixes, rangeConfig.excludeCursor);
			var front = (cmdOrder == asc ? firstUnicode : lastUnicode);				// if null => first member of given score
			var memberMap = {null:front, '-':firstUnicode, '+':lastUnicode};
			if(startMember && startMember[0] == '['){
				startMember = startMember.slice(1);
			}
			startMember = memberMap[startMember] || startMember;
			luaArgs.unshift(startMember);
			luaArgs.unshift(startScore);
		}else if(utils.startsWith(cmdType, 'rangebyscorelex')
				|| utils.startsWith(cmdType, 'countbyscorelex')){
			// NB: in contrast to .rankbyscorelex, such range commands are resolved depending on the storage structure
			//	=> hence the caller is normally not specifying the type of range to be used; defaults should be chosen carefully
			// FYI: zrank and zrevrank are symmetric so a single code works!
			var zrank = {}; zrank[asc] = '"zrank"'; zrank[desc] ='"zrevrank"';
			var zrange = {}; zrange[asc] = '"zrange"'; zrange[desc] ='"zrevrange"';
			var zlimit = {}; zlimit[asc] = '-1'; zlimit[desc] = '0';
			lua = ' local startRank = nil;'
				+ 'local stopRank = nil;'
				+ 'local startScore = ARGV[1];'
				+ 'local startMember = ARGV[2];'
				+ 'local stopScore = ARGV[3];'
				+ 'local stopMember = ARGV[4];'
				+ 'local score = nil;'
				+ 'if startScore ~= "'+label_lua_nil+'" then'					// verify unchanged score-member
				+ '    score = redis.call("zscore", KEYS[1], startMember);'			// check score tally first
				+ '    if score == startScore then'						// then rank can be set
				+ '        startRank = redis.call('+zrank[cmdOrder]+', KEYS[1], startMember);'
				+ '    else'
				+ '        startMember = startMember.."'+collision_breaker+'";'
				+ '        local count = redis.call("zadd", KEYS[1], "NX", startScore, startMember);'
				+ '        startRank = redis.call('+zrank[cmdOrder]+', KEYS[1], startMember);'	// infer the previous position
				+ '        if count > 0 then'
				+ '             redis.call("zrem", KEYS[1], startMember);'
				+ '        end;'
				+ '    end;'
				+ 'else'
				+ '    startRank = redis.call('+zrank[cmdOrder]+', KEYS[1], startMember);'	// no score to seek with
				+ 'end;'
				+ 'if stopScore ~= "'+label_lua_nil+'" then'					// verify unchanged score-member
				+ '    score = redis.call("zscore", KEYS[1], stopMember);'			// check score tally first
				+ '    if score == stopScore then'						// then rank can be set
				+ '        stopRank = redis.call('+zrank[cmdOrder]+', KEYS[1], stopMember);'
				+ '    else'
				+ '        stopMember = stopMember.."'+collision_breaker+'";'
				+ '        local count = redis.call("zadd", KEYS[1], "NX", stopScore, stopMember);'
				+ '        stopRank = redis.call('+zrank[cmdOrder]+', KEYS[1], stopMember);'	// infer the previous position
				+ '        if count > 0 then'
				+ '             redis.call("zrem", KEYS[1], stopMember);'
				+ '        end;'
				+ '    end;'
				+ 'else'
				+ '    stopRank = redis.call('+zrank[cmdOrder]+', KEYS[1], stopMember);'	// no score to seek with
				+ 'end;'
				+ 'if startRank ~= nil and stopRank ~= nil then'
				+ '    if '+(utils.startsWith(cmdType, 'rangebyscorelex') ? 'false' : 'true')+' then'
				+ '        return math.max(0, stopRank-startRank+1);'
				+ '    else'
				+ '        return redis.call('+zrange[cmdOrder]+', KEYS[1], startRank, '
						+ (limit != null ? 'math.min(startRank+'+limit+'-1, stopRank)' : 'stopRank')
						+ (attribute.withscores ? ', "withscores");' : ');')
				+ '    end;'
				+ 'else'
				+ '    return '+(utils.startsWith(cmdType, 'rangebyscorelex') ? '{}' : '0')+';'
				+ 'end;'
			var startScore = getKeyRangeStartScore(cmdOrder, keyConfig, rangeConfig, xidPrefixes, keySuffixes, false);
			// NB: in contract to rankbyscorelex, ranging would resolve to the .bylex variant in case of lexicographical order
			//  => null-score should not be resolved to 0 or any value
			var scoreMap = {null:label_lua_nil, '-inf':redisMinScore, '+inf':redisMaxScore};
			if(startScore && startScore[0] == '('){
				startScore = parseFloat(startScore.slice(1), 10);
			}
			startScore = scoreMap[startScore] || startScore;
			var stopScore = getKeyRangeStopScore(cmdOrder, keyConfig, rangeConfig, xidPrefixes, keySuffixes, null);
			if(stopScore && stopScore[0] == '('){
				stopScore = parseFloat(stopScore.slice(1), 10);
			}
			stopScore = scoreMap[stopScore] || stopScore;
			var startMember = getKeyRangeStartMember(cmdOrder, keyConfig, rangeConfig, uidPrefixes, keySuffixes, rangeConfig.excludeCursor);
			var front = (cmdOrder == asc ? firstUnicode : lastUnicode);				// if null => first member of given score
			var memberMap = {null:front, '-':firstUnicode, '+':lastUnicode};
			if(startMember && startMember[0] == '['){
				startMember = startMember.slice(1);
			}
			startMember = memberMap[startMember] || startMember;
			var stopMember = getKeyRangeStopMember(cmdOrder, keyConfig, rangeConfig, uidPrefixes, keySuffixes, null);
			if(stopMember && stopMember[0] == '['){
				stopMember = stopMember.slice(1);
			}
			stopMember = memberMap[stopMember] || stopMember;
			luaArgs.unshift(stopMember);
			luaArgs.unshift(stopScore);
			luaArgs.unshift(startMember);
			luaArgs.unshift(startScore);
		}else if(utils.startsWith(cmdType, 'countbylex')
				|| utils.startsWith(cmdType, 'rangebylex')
				|| utils.startsWith(cmdType, 'delrangebylex')){
			var negInf = (cmdOrder == asc ? '-' : '+');
			var posInf = (cmdOrder == asc ? '+' : '-');
			var startMember = getKeyRangeStartMember(cmdOrder, keyConfig, rangeConfig, uidPrefixes, keySuffixes, rangeConfig.excludeCursor);
			var stopMember = getKeyRangeStopMember(cmdOrder, keyConfig, rangeConfig, uidPrefixes, keySuffixes, null);
			if(utils.startsWith(cmdType, 'countbylex') && startMember == negInf && stopMember == posInf){
				cmd = command.getMode(datatype.getConfigCommand(keyConfig).count).bykey;
			}else{
				args.unshift(stopMember);
				args.unshift(startMember);
			}
		}else if(utils.startsWith(cmdType, 'countbyscore')
				|| utils.startsWith(cmdType, 'rangebyscore')
				|| utils.startsWith(cmdType, 'delrangebyscore')){
			var negInf = (cmdOrder == asc ? '-inf' : '+inf');
			var posInf = (cmdOrder == asc ? '+inf' : '-inf');
			var startScore = getKeyRangeStartScore(cmdOrder, keyConfig, rangeConfig, xidPrefixes, keySuffixes, rangeConfig.excludeCursor);
			var stopScore = getKeyRangeStopScore(cmdOrder, keyConfig, rangeConfig, xidPrefixes, keySuffixes, null);
			if(utils.startsWith(cmdType, 'countbyscore') && startScore == negInf && stopScore == posInf){
				cmd = command.getMode(datatype.getConfigCommand(keyConfig).count).bykey;
			}else{
				args.unshift(stopScore);
				args.unshift(startScore);
			}
		}else{
			if(command.requiresUID(cmd)){
				args.unshift(uid || '');
			}
			if(command.requiresXID(cmd)){
				args.unshift(xid || 0);
			}
		}
		break;
	default:
		throw new Error('FATAL: query.query: unknown keytype!!');
	}
	var keyText = getKeyText(key, keySuffixes, field);
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
