var cluster = require('./cluster');
var utils = require('./utils');


	// CONFIGURATIONS
var	separator_key = ':',								// separator for parts of keys
	separator_detail = '\u0000',							// separator for parts of values i.e. UID and XID
	collision_breaker = '\u0000?*$#\u0000\u0000?#-*',				// appending this to a UID guarantees it doesn't collide with another
	offset_id_trail_info = 6,
	redis_max_score_factor = Math.pow(10,15),
	asc = '_label_ascending',							// direction of ranging -- typically over sorted-sets
	desc = '_label_descending',							// direction of ranging -- typically over sorted-sets
	access_code = '_do_not_access_fields_with_this',				// used to lock internal-configs or leaf nodes of dicts see utils.wrap
	label_cluster_instance_getter = '_cluster_instance_function',
	default_get_cluster_instance = (function(cmd, keys, index, args, field){return cluster.getDefaultInstance();}),
	// functions relating to configs with key-suffixes
	// keys should have these functions attached if the datatype has key-suffixes; in order to compute getKeyChains
	label_field_min_value_getter = '_field_min_value_getter_function',		// {field1: function1,..}; see default_get_field_min_value
	label_field_max_value_getter = '_field_max_value_getter_function',		// {field1: function1,..}; see default_get_field_max_value
	label_field_next_chain_getter = '_field_next_chain_getter_function',		// {field1: function1,..}; see default_get_next_chain
	label_field_previous_chain_getter = '_field_previous_chain_getter_function',	// {field1: function1,..}; see default_get_previous_chain
	label_field_is_less_than_comparator = '_field_is_less_than_comparator',		// {field1: function1,..}; see default_is_chain_less_than
	// these defaults assume inputs are natural numbers
	// more optimal implementations should be given, in order to produce only the required keychain
	default_get_field_min_value = (function(){return 0;}),				// returns current min value of field
	default_get_field_max_value = (function(){return 0;}),				// returns current max value of field
	default_get_next_chain = (function(suffix){					// returns next chain after given value
				return ''+parseInt(suffix || 0, 10)+1;
			}),
	default_get_previous_chain = (function(suffix){					// return previous chain after given value
				var prev = parseInt(suffix, 10)-1;
				return (prev >= 0 ? ''+prev : '');
			}),
	default_is_chain_less_than = (function(suffix1, suffix2){			// makes comparison between key-chains
				return parseInt(suffix1 || 0, 10) < parseInt(suffix2 || 0, 10);
			});



var datatype = {};

var structure = {
	// CRUD
	datatype: {
		struct: [{ 			// [{}] => .getStruct()
			set: {			// {} => .set
				command: [{
					// TODO [type] property should be an array of tags?? affords non-linear dimensions
					add: {type:'adds', val:'sadd'},
					del: {type:'dels', val:'srem'},
					count: {type:'counts', val: 'count',
						mode: [{
							bylex: {type: 'countbylexs', val: 'sismember'},
							bykey: {type: 'countbykeys', val: 'scard'}}]}}],
				config: [{}]},
			string: {
				command: [{
					add: {type: 'addk', val: 'mset'},
					get: {type: 'getk', val: 'get'},
					mget: {type: 'getmk', val: 'mget'},
					upsert: {type: 'upsertk', val: 'set'},
			 		del: {type: 'delk', val: 'del'}}],
				config: [{}]},
			hash: {
				command: [{
					add: {type: 'addh', val: 'hmset'},
					get: {type: 'geth', val: 'hget'},
					mget: {type: 'getmh', val: 'hmget'},
					upsert: {type: 'upserth', val: 'hmset'},
					del: {type: 'delh', val: 'hdel'},
					incrby: {type: 'incrbyh', val: 'hincrby'}}],
				config: [{}]},
			zset: {
				command: [{
					add: {type: 'addz', val: 'zadd'},
					get: {type: 'getz', val: 'zscore'},
					upsert: {type: 'upsertz', val: 'zadd'},
					del: {type: 'delz', val: 'zrem'},
					delrange: {	type: 'delrangez', val: 'delrange',
							mode: [{
								byrank: {type: 'delrangebyrankz', val: 'zremrangebyrank'},
								byscore: {type: 'delrangebyscorez', val: 'zremrangebyscore'},
								bylex: {type: 'delrangebylexz', val: 'zremrangebylex'}}]},
					rangeasc: {	type: 'rangez', val: 'rangeasc', order: asc,
							mode: [{
								byrank: {type: 'rangebyrankz', val: 'zrange', order: asc},
								byscore: {type: 'rangebyscorez', val: 'zrangebyscore', order: asc},
								bylex: {type: 'rangebylexz', val: 'zrangebylex', order: asc},
								byscorelex: {type: 'rangebyscorelexz', val: 'eval', order: asc}}]},
					rangedesc: {	type: 'rangez', val: 'rangedesc', order: desc,
							mode: [{
								byrank: {type: 'rangebyrankz', val: 'zrevrange', order: desc},
								byscore: {type: 'rangebyscorez', val: 'zrevrangebyscore', order: desc},
								bylex: {type: 'rangebylexz', val: 'zrevrangebylex', order: desc},
								byscorelex: {type: 'rangebyscorelexz', val: 'eval', order: desc}}]},
					count: {type: 'countz', val: 'count',
						mode: [{
					      		byscore: {type: 'countbyscorez', val: 'zcount'},
							bylex: {type: 'countbylexz', val: 'zlexcount'},
							bykey: {type: 'countbykeyz', val: 'zcard'}}]},
					rankasc: {	type: 'rankz', val: 'indexasc', order: asc,
							mode: [{
					      			byscorelex: {type: 'rankbyscorelexz', val: 'eval', order: asc},
								bylex: {type: 'rankbylexz', val: 'zrank', order: asc}}]},
					rankdesc: {	type: 'rankz', val: 'indexdesc', order: desc,
							mode: [{
								byscorelex: {type: 'rankbyscorelexz', val: 'eval', order: desc},
								bylex: {type: 'rankbylexz', val: 'zrevrank', order: desc}}]}}],
				config: [{}]},
			}],
		},
	};

unwrap = function(caller){
	return caller(access_code);
}

getKeyLabelFunction = function(caller, label, field){
	var obj = unwrap(caller)[label];
	obj = (field != null ? (obj || {})[field] : obj);				// function for specific field
	if(obj == null){
		// fallback to config in case of key
		var _type = caller.getType();
		if(_type == 'key'){
			var conf = caller.getConfig();
			caller = conf;
			obj = unwrap(caller)[label];
			obj = (field != null ? (obj || {})[field] : obj);		// function for specific field
		}
		if(obj == null){
			// fallback to struct in case of config
			_type = caller.getType();
			if(_type == 'config'){
				var struct = caller.getStruct();
				caller = struct;
				obj = unwrap(caller)[label];				// struct defaults
			}
			// fallback to defaults in case of struct
			if(obj == null){
				obj = unwrap(datatype)[label];				// datatype defaults
			}
		}
	}
	return obj;
};

setKeyLabelFunction = function(caller, label, obj, field){
	var dict = unwrap(caller);
	if(field == null){
		dict[label] = obj;
	}else{
		if(dict[label] == null){
			dict[label] = {};
		}
		dict[label][field] = obj;
	}
};

// functions pertaining to structs/configs/keys with keyChains i.e. if datatype.isConfigKeyChain
var kcf = ['getFieldMinValueGetter','getFieldMaxValueGetter','getFieldNextChainGetter','getFieldPreviousChainGetter','getFieldIsLessThanComparator'
	,'setFieldMinValueGetter','setFieldMaxValueGetter','setFieldNextChainGetter','setFieldPreviousChainGetter','setFieldIsLessThanComparator'];

getClusterInstanceGetter = function(){
	return getKeyLabelFunction(this, label_cluster_instance_getter, null);
};
getFieldMinValueGetter = function(field){
	return getKeyLabelFunction(this, label_field_min_value_getter, field);
};
getFieldMaxValueGetter = function(field){
	return getKeyLabelFunction(this, label_field_max_value_getter, field);
};
getFieldNextChainGetter = function(field){
	return getKeyLabelFunction(this, label_field_next_chain_getter, field);
};
getFieldPreviousChainGetter = function(field){
	return getKeyLabelFunction(this, label_field_previous_chain_getter, field);
};
getFieldIsLessThanComparator = function(field){
	return getKeyLabelFunction(this, label_field_is_less_than_comparator, field);
};

setClusterInstanceGetter = function(func){
	return setKeyLabelFunction(this, label_cluster_instance_getter, func, null);
};
setFieldMinValueGetter = function(obj, field){
	return setKeyLabelFunction(this, label_field_min_value_getter, obj, field);
};
setFieldMaxValueGetter = function(obj, field){
	return setKeyLabelFunction(this, label_field_max_value_getter, obj, field);
};
setFieldNextChainGetter = function(obj, field){
	return setKeyLabelFunction(this, label_field_next_chain_getter, obj, field);
};
setFieldPreviousChainGetter = function(obj, field){
	return setKeyLabelFunction(this, label_field_previous_chain_getter, obj, field);
};
setFieldIsLessThanComparator = function(obj, field){
	return setKeyLabelFunction(this, label_field_is_less_than_comparator, obj, field);
};

// default and zset require possibility to set functions
structure[access_code] = {_all: kcf.concat(['setClusterInstanceGetter', 'getClusterInstanceGetter'])};
structure.datatype.struct[0][access_code] = {_all: ['setClusterInstanceGetter', 'getClusterInstanceGetter'], zset: kcf};

datatype = utils.wrap(structure, {api:{}, keyconfig:{}, keywrap:{}}, access_code, null, null, null, null).datatype;
// set defaults
datatype.setClusterInstanceGetter(default_get_cluster_instance);
datatype.setFieldMinValueGetter(default_get_field_min_value);
datatype.setFieldMaxValueGetter(default_get_field_max_value);
datatype.setFieldNextChainGetter(default_get_next_chain);
datatype.setFieldPreviousChainGetter(default_get_previous_chain);
datatype.setFieldIsLessThanComparator(default_is_chain_less_than);


datatype.getIdTrailInfoOffset = function(offset){
	return offset_id_trail_info;
};

datatype.setIdTrailInfoOffset = function(offset){
	offset_id_trail_info = offset;
};

datatype.getKeySeparator = function(){
	return separator_key;
};

datatype.getDetailSeparator = function(){
	return separator_detail;
};

datatype.getCollisionBreaker = function(){
	return collision_breaker;
};

datatype.getIdIncrement = function(){
	return Math.pow(10, offset_id_trail_info);
};

datatype.getRedisMaxScoreFactor = function(){
	return redis_max_score_factor;
};

datatype.getConfigIndexProp = function(config, prop){
	return ((unwrap(config).indexconfig || {})[prop] || []);
};

hasKeyChainOffset = function(offsets){
	var suffixes = (offsets || []).filter(function(n){return (n != null && n != 0);});
	return suffixes.length > 0;
}
datatype.isConfigKeyChain = function(config){
	var offsets = datatype.getConfigIndexProp(config, 'offsets');
	return hasKeyChainOffset(offsets);
};


var dataConfig = {};
var dataKey = {};

datatype.getConfig = function(){
	return dataConfig;
};
datatype.getKey = function(){
	return dataKey;
};


getIndexConfig = function(){
	return unwrap(this).indexconfig;
};

datatype.createConfig = function(id, struct, index_config){
	var structConfig = struct.getConfig();
	var store = {api:structConfig, keyconfig:{}, keywrap:{}};
	var dict = {};
	dict[id] = {key:[{}], indexconfig: index_config};
	dict[access_code] = {_all: ['getIndexConfig', 'getClusterInstanceGetter', 'setClusterInstanceGetter']};
	var offsets = index_config.offsets;
	if(hasKeyChainOffset(offsets)){
		[].push.apply(dict[access_code]._all, kcf);
	}
	var api = utils.wrap(dict, store, access_code, 'config', 'struct', struct, true);
	dataConfig[id] = api[id];
	return dataConfig[id];
};

getLabel = function(){
	return unwrap(this).label;
};
getCommand = function(){	// shortcut
	return this.getConfig().getStruct().getCommand();
};

datatype.createKey = function(id, label, key_config){
	var configKey = key_config.getKey();
	var store = {api:configKey, keyconfig:{}, keywrap:{}};
	var dict = {};
	dict[id] = {label: label};
	dict[access_code] = {_all: ['getLabel', 'getCommand', 'getClusterInstanceGetter', 'setClusterInstanceGetter']};
	if(datatype.isConfigKeyChain(key_config)){
		[].push.apply(dict[access_code]._all, kcf);
	}
	api = utils.wrap(dict, store, access_code, 'key', 'config', key_config, true);
	dataKey[id] = api[id];
	return dataKey[id];
};


datatype.loadtree = function(dtree){
	var fldGetter = {'clusterinstance': 'setClusterInstanceGetter',
			'minvalue': 'setFieldMinValueGetter',
			'maxvalue': 'setFieldMaxValueGetter',
			'nextchain': 'setFieldNextChainGetter',
			'previouschain': 'setFieldPreviousChainGetter',
			'islessthancomparator': 'setFieldIsLessThanComparator'};
	var structs = dtree.structs;
	var defaultGetter = dtree.defaultgetter || {};
	for(var fld in fldGetter){
		var getter = defaultGetter[fld];
		if(getter != null){
			if(fld == 'clusterinstance'){
				datatype[fldGetter[fld]](getter);
			}else{
				datatype[fldGetter[fld]](getter, null);
			}
		}
	}
	for(var i=0; i < structs.length; i++){
		var str = structs[i];
		var strObj = datatype.getStruct()[str.id];
		var configs = str.configs;
		var structGetter = str.structgetter || {};
		for(var fld in fldGetter){
			var getter = structGetter[fld];
			if(getter != null){
				if(fld == 'clusterinstance'){
					strObj[fldGetter[fld]](getter);
				}else{
					strObj[fldGetter[fld]](getter, null);
				}
			}
		}
		for(var j=0; j < configs.length; j++){
			var cfg = configs[j];
			var cfgObj = datatype.createConfig(cfg.id, strObj, cfg.index);
			var keys = cfg.keys;
			var fieldGetter = cfg.fieldgetter || {};
			var fieldList = datatype.getConfigIndexProp(cfgObj, 'fields');
			for(var fld in fldGetter){
				if(fld == 'clusterinstance'){
					cfgObj[fldGetter[fld]](getter);
				}else{
					var getterList = fieldGetter[fld] || [];
					for(var k=0; k < fieldList.length; k++){
						var getter = getterList[k];
						if(getter != null){
							cfgObj[fldGetter[fld]](getter, fieldList[k]);
						}
					}
				}
			}
			for(var k=0; k < keys.length; k++){
				var key = keys[k];
				var keyObj = datatype.createKey(key.id, key.label, cfgObj);
				var keyGetter = key.keygetter || {};
				for(var fld in fldGetter){
					if(fld == 'clusterinstance'){
						keyObj[fldGetter[fld]](getter);
					}else{
						var getterList = keyGetter[fld] || [];
						for(var k=0; k < fieldList.length; k++){
							var getter = getterList[k];
							if(getter != null){
								keyObj[fldGetter[fld]](getter, fieldList[k]);
							}
						}
					}
				}
			}
		}
	}
};


datatype.getKeyFieldBoundChain = function(key, field, cmd_order){
	var fieldBoundValueGetter = null;
	if(cmd_order == desc){
		fieldBoundValueGetter = key.getFieldMinValueGetter(field);
	}else{
		fieldBoundValueGetter = key.getFieldMaxValueGetter(field);
	}
	return fieldBoundValueGetter();
};

datatype.getKeyFieldNextChain = function(key, field, cmd_order, chain){
	index = index || {};
	var fieldNextChainGetter = null;
	if(cmd_order == desc){
		fieldNextChainGetter = key.getFieldPreviousChainGetter(field);
	}else{
		fieldNextChainGetter = key.getFieldNextChainGetter(field);
	} 
	return fieldNextChainGetter(chain);
};

datatype.isKeyFieldChainWithinBound = function(key, field, cmd_order, chain, bound_chain){
	index = index || {};
	var fieldChainLessThanComparator = key.getFieldIsLessThanComparator(field);
	if(cmd_order == desc){
		return !fieldChainLessThanComparator(chain, bound_chain);
	}else{
		return !fieldChainLessThanComparator(bound_chain, chain);
	}
};


datatype.getConfigFieldIdx = function(config, field){
	var idx = datatype.getConfigIndexProp(config, 'fields').indexOf(field);
	return (idx >= 0 ? idx : null);
};

datatype.getConfigPropFieldIdxValue = function(config, prop, field_idx){
	return datatype.getConfigIndexProp(config, prop)[field_idx];
};

datatype.getConfigFieldOrdering = function(config){
	// TODO NB: ordering across keys should be admissible only if split[0] is an integer e.g. time
	// else, the semantics of ordering is a bit shrouded
	//	use the value provided to check isNumber(val)
	//	NO! instead clarify that the split config is for handling increasing integers/unicode-ordering
	//	does newly introduced ordering function solve this?
	var fieldIndexOrder = datatype.getConfigIndexProp(config, 'fields');
	var comparator = {keytext:[], score:[], uid:[]};
	var keyFactors = datatype.getConfigIndexProp(config, 'factors').concat([]).sort(function(a,b){return (b||-Infinity)-a;});	// put <nulls> to the end
	var keyFactorSplices = 0;
	// iteration is done in fieldIndexOrder since e.g. member- and key- prepends are also done in this order
	for(var i=0; i < fieldIndexOrder.length; i++){
		if(datatype.getConfigPropFieldIdxValue(config, 'partitions', i)){
			// partitions are not used for ordering
			// adjust keyFactors to ignore partitions
			keyFactors.splice(i-keyFactorSplices, 1);
			keyFactorSplices++;
			continue;
			
		}
		var field = fieldIndexOrder[i];
		var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', i);
		var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', i);
		var isPrepend = datatype.getConfigPropFieldIdxValue(config, 'offsetprependsuid', i);
		if(offset != null && offset != 0){
			comparator.keytext.push(field);
		}
		if(factor != null && factor != 0){
			var ord = keyFactors.indexOf(factor);
			if(ord >= 0){
				// in case factors coincide (such configs not advised), find next spot
				while(comparator.score[ord]){
					ord++;
				}
				comparator.score[ord] = field;
			}
		}
		if(isPrepend){
			comparator.uid.push(field);
		}
	}
	return comparator;
};


datatype.isConfigFieldKeySuffix = function(config, field_idx){
	return !(!datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx));
}
datatype.isConfigFieldBranch = function(config, field_idx){
	return datatype.getConfigPropFieldIdxValue(config, 'fieldprependskey', field_idx) == true;
}
datatype.isConfigFieldPartitioned = function(config, field_idx){
	return datatype.getConfigPropFieldIdxValue(config, 'partitions', field_idx) == true;
}
datatype.isConfigFieldUIDPrepend = function(config, field_idx){
	var offsetPrependsUID = datatype.getConfigPropFieldIdxValue(config, 'offsetprependsuid', field_idx);
	return offsetPrependsUID == true;
}
// UID datatype.isConfigFieldStrictlyUIDPrepend whereas XID !datatype.isConfigFieldStrictlyUIDPrepend
datatype.isConfigFieldStrictlyUIDPrepend = function(config, field_idx){
	var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', field_idx);
	var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx);
	var isPrepend = datatype.isConfigFieldUIDPrepend(config, field_idx);
	return isPrepend && offset != 0 && (factor==0 || factor==null);
}

// decide which hybrid of zrange to use
datatype.getZRangeSuffix = function(config, index, args, xid, uid){
	// NB: byrank applies to all hybrids
	// it applies when UID and XID are both not given, and args holds the min/max
	if((xid == null || xid == '') && (uid == null || uid == '')){
		if((args || []).length >= 2){
			return 'byrank';
		}else{
			return 'byscore';		// any stable default; full range
		}
	}
	var factors = datatype.getConfigIndexProp(config, 'factors') || [];
	if(factors.filter(function(a){return (a != null)}).length == 0){
		return 'bylex';
	}
	// byscore, if not a single field which datatype.isConfigFieldStrictlyUIDPrepend is provided
	var exists = false;
	for(field in index){
		var fieldIndex = datatype.getConfigFieldIdx(config, field);
		if(datatype.isConfigFieldStrictlyUIDPrepend(config, fieldIndex)){
			exists = true;
			break;
		}
	}
	if(!exists){
		return 'byscore';
	}else{
		return 'byscorelex';
	}

};

splitID = function (id, offset){ 
	offset = (offset != null ? Math.abs(offset) : offset);	// ignore polarity here
	if(offset == Infinity || offset == null){		// index property not included in key
		return [null, id];				// '' to allow empty-string concats
	}else if(offset <= 0 && offset != null){		// index property is not included into score
		return [id, null];
	}else if(id != null && offset != null){
		id = id.toString();
		var num = 0 - offset;
		var a = id.slice(0, num);
		var b = id.slice(num);
		return [a,b];
	}else if(offset != null){
		// when id=null return ['', null] for the key and score respectively
		// '' would force concatenation to key
		return ['', id];
	}else{
		return [null, null];
	}
};
datatype.splitConfigFieldValue = function(config, field, val){
	// offset=null/0  =>  retain everything
	// offset=+/-Infinity => send everything to key
	var fieldIndex = datatype.getConfigFieldIdx(config, field);
	var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fieldIndex);
	var trailOffset = (Math.abs(offset || Infinity) == Infinity ? offset || 0 : offset%10);	// how much to join to key
	var split = splitID(val, trailOffset);
	var stem = (String(split[0]) == '' ? null : split[0]);					// <null> since '' is not yet for key-suffixing
	var trailInfo = split[1];
	// check if field should be added to LHS
	var fieldPrefix = [];
	if(datatype.isConfigFieldBranch(config, fieldIndex)){
		offset = (offset != null ? 0 - offset : offset);
		fieldPrefix = [field];
	}
	offset = ((Math.abs(offset) == Infinity ? 0 : Math.floor(offset)/10 || Infinity));	// how much to leave untouched
	split = splitID(stem, offset);
	split[0] = (split[0] != null || trailInfo != null ? (split[0]||'') + (trailInfo||'') : null);
	[].push.apply(split, fieldPrefix);
	return split;
};

datatype.unsplitFieldValue = function(split, offset){
	var trailOffset = (Math.abs(offset || Infinity) == Infinity ? offset || 0 : offset%10);
	var innerSplit = splitID(split[0], trailOffset);
	var stemSplit = (innerSplit[0] != null ? innerSplit[0] : '');
	var trailInfo = (innerSplit[1] != null ? innerSplit[1] : '');
	return (stemSplit+(split[1] != null ? split[1] : '')+trailInfo);
};

datatype.getFactorizedConfigFieldValue = function(config, field, value){
	var fieldIndex = datatype.getConfigFieldIdx(config, field);
	var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', fieldIndex);
	var isStrictlyValueField = datatype.isConfigFieldStrictlyUIDPrepend(config, fieldIndex);
	if(value != null && factor != null && !isStrictlyValueField){
		return value * factor;
	}else{
		return null;
	}
};

datatype.getConfigFieldPrefactor = function(config, field_idx){
	var preFactor = null;
	for(var i=field_idx-1; i >= 0; i--){
		preFactor = datatype.getConfigPropFieldIdxValue(config, 'factors', i);
		if(preFactor){
			break;
		}
	}
	return preFactor;
};



var command = {};

command.getAscendingOrderLabel = function(){
	return asc;
};

command.getDescendingOrderLabel = function(){
	return desc;
};

command.toRedis = function(cmd){
	return unwrap(cmd).val;
};

command.getType = function(cmd){
	return unwrap(cmd).type;
};

command.getOrder = function(cmd){
	return unwrap(cmd).order;
};


command.getRangeMode = function(cmd){
	var cmdType = command.getType(cmd);
	if(cmdType.slice(-11) == 'byscorelexz'){
		return 'byscorelex';
	}else if(cmdType.slice(-6) == 'bylexz'){
		return 'bylex';
	}else if(cmdType.slice(-8) == 'byscorez'){
		return 'byscore';
	}else if(cmdType.slice(-7) == 'byrankz'){
		return 'byrank';
	}else{
		return null;
	}
};

// command which run over a certain range
command.isOverRange = function(cmd){
	var cmdType = command.getType(cmd);
	return (cmdType.slice(-1) == 'z' && (utils.startsWith(cmdType, 'range') || utils.startsWith(cmdType, 'count')));
};

command.requiresXID = function(cmd){
	var cmdType = command.getType(cmd);
	if(  utils.startsWith(cmdType, 'add')
		|| utils.startsWith(cmdType, 'upsert')
		|| utils.startsWith(cmdType, 'rangebyscore')
		|| utils.startsWith(cmdType, 'countbyscore')
		|| utils.startsWith(cmdType, 'rankbyscore')
		|| utils.startsWith(cmdType, 'delrangebyscore')
	){
		return true;
	}
	return false;
};

command.requiresUID = function(cmd){
	var cmdType = command.getType(cmd);
	if(  utils.startsWith(cmdType, 'add')
		|| utils.startsWith(cmdType, 'get')
		|| utils.startsWith(cmdType, 'upsert')
		|| utils.startsWith(cmdType, 'rangebylex')
		|| utils.startsWith(cmdType, 'countbylex')
		|| utils.startsWith(cmdType, 'rankbylex')
		|| utils.startsWith(cmdType, 'delrangebylex')
	){
		return true;
	}
	return false;
};

datatype.command = command;


// internal routines do not rely on external interfaces; just in case the external ones are deprecated
// hence redefinitions here!
datatype.getKeyConfig = function(key){
	return key.getConfig();
};
datatype.getConfigStructId = function(config){
	return config.getStruct().getId();
};
datatype.getConfigCommand = function(config){
	return config.getStruct().getCommand();
};
datatype.getCommandMode = function(cmd){
	return cmd.getMode();
};
datatype.getKeyLabel = function(key){
	return key.getLabel();
};
datatype.getKeyClusterInstanceGetter = function(key){
	return key.getClusterInstanceGetter();
};


module.exports = datatype;
