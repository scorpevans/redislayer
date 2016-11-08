var cluster = require('./cluster');
var utils = require('./utils');


var	asc = '_label_ascending',							// direction of ranging -- typically over sorted-sets
	desc = '_label_descending',							// direction of ranging -- typically over sorted-sets
	access_code = 'datatype._do_not_access_fields_with_this',			// used to lock internal-configs or leaf nodes of dicts see utils.wrap
	redis_max_score_factor = Math.pow(10,15),
	label_key_separator = '_separator_key',
	label_detail_separator = '_separator_detail',
	label_collision_breaker = '_collision_breaker',
	label_cluster_instance_getter = '_cluster_instance_function',
	label_field_min_value_getter = '_field_min_value_getter_function',		// {field1: function1,..}; see default_get_field_min_value
	label_field_max_value_getter = '_field_max_value_getter_function',		// {field1: function1,..}; see default_get_field_max_value
	label_field_next_chain_getter = '_field_next_chain_getter_function',		// {field1: function1,..}; see default_get_next_chain
	label_field_previous_chain_getter = '_field_previous_chain_getter_function',	// {field1: function1,..}; see default_get_previous_chain
	default_key_separator = ':',							// separator for parts of keys
	default_detail_separator = '\u0000',						// separator for parts of values i.e. UID and XID
	default_collision_breaker = '\u0000?*$#\u0000\u0000?#-*',			// appending this to a UID guarantees it doesn't collide with another
	default_get_cluster_instance = (function(cmd, keys, keysuffix_index, key_field){
					return cluster.getDefault().master;}),
	// functions relating to configs with key-suffixes
	// keys should have these functions attached if the datatype has key-suffixes; in order to compute getKeyChains
	// these defaults assume inputs are natural numbers
	// more optimal implementations should be given, in order to produce only the required keychain
	default_get_field_min_value = (function(){return 0;}),				// returns current min value of field
	default_get_field_max_value = (function(){return 0;}),				// returns current max value of field
	default_get_next_chain = (function(suffix){					// returns next chain after given value
				return ''+parseFloat(suffix || 0, 10)+1;
			}),
	default_get_previous_chain = (function(suffix){					// return previous chain after given value
				var prev = parseFloat(suffix, 10)-1;
				return (prev >= 0 ? ''+prev : '');
			});



var unwrap = function datatypeUnwrap(caller){
	return caller(access_code);
};

var command = {};

command.getAscendingOrderLabel = function getCommandAscendingOrderLabel(){
	return asc;
};

command.getDescendingOrderLabel = function getCommandDescendingOrderLabel(){
	return desc;
};

command.toRedis = function commandToRedis(cmd){
	return unwrap(cmd).val;
};

command.getType = function getCommandType(cmd){
	return unwrap(cmd).type;
};

command.getOrder = function getCommandOrder(cmd){
	return unwrap(cmd).order;
};


var structure = {
	// CRUD
	datatype: {
		struct: [{ 			// [{}] => .getStruct()
			set: {			// {} => .set
				command: [{
					// TODO [type] property should be an array of tags?? affords non-linear dimensions
					add: {type:'adds', val:'sadd', multi: true},
					del: {type:'dels', val:'srem', multi: true},
					randmember: {type:'randmembers', val:'srandmember'},
					ismember:{type: 'ismembers', val: 'sismember'},
					count: {type:'counts', val: 'count',
						mode: [{bykey: {type: 'countbykeys', val: 'scard'}}]}}],
				config: [{}]},
			string: {
				command: [{
					add: {type: 'addk', val: 'mset', multi: true},
					get: {type: 'getk', val: 'get'},
					mget: {type: 'getmk', val: 'mget', multi: true},
					upsert: {type: 'upsertk', val: 'eval'},
			 		del: {type: 'delk', val: 'del', multi: true},
					incrby: {type: 'incrbyk', val: 'incrby'}}],
				config: [{}]},
			hash: {
				command: [{
					add: {type: 'addh', val: 'hmset', multi: true},
					get: {type: 'geth', val: 'hget'},
					mget: {type: 'getmh', val: 'hmget', multi: true},
					upsert: {type: 'upserth', val: 'eval'},
					del: {type: 'delh', val: 'hdel', multi: true},
					incrby: {type: 'incrbyh', val: 'hincrby'},
					incrbyfloat: {type: 'incrbyfloath', val: 'hincrbyfloat'}}],
				config: [{}]},
			zset: {
				command: [{
					add: {type: 'addz', val: 'zadd', multi: true},
					get: {type: 'getz', val: 'zscore'},
					upsert: {type: 'upsertz', val: 'eval'},
					del: {type: 'delz', val: 'zrem', multi: true},
					incrby: {type: 'incrbyz', val: 'zincrby'},
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
							bykey: {type: 'countbykeyz', val: 'zcard'},
							byscorelex: {type: 'countbyscorelexz', val: 'eval'}}]},
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


getKeyLabelFunction = function getDatatypeKeyLabelFunction(caller, label, field){
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

setKeyLabelFunction = function setDatatypeKeyLabelFunction(caller, label, obj, field){
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

// functions pertaining to structs/configs/keys with keyChains
var kcf = ['getFieldMinValueGetter','getFieldMaxValueGetter','getFieldNextChainGetter','getFieldPreviousChainGetter'
	,'setFieldMinValueGetter','setFieldMaxValueGetter','setFieldNextChainGetter','setFieldPreviousChainGetter'];

getKeySeparator = function getDatatypeKeySeparator(){
	return getKeyLabelFunction(this, label_key_separator, null);
};
getDetailSeparator = function getDatatypeDetailSeparator(){
	return getKeyLabelFunction(this, label_detail_separator, null);
};
getCollisionBreaker = function getDatatypeCollisionBreaker(){
	return getKeyLabelFunction(this, label_collision_breaker, null);
};
getClusterInstanceGetter = function getClusterInstanceGetter(){
	return getKeyLabelFunction(this, label_cluster_instance_getter, null);
};
getFieldMinValueGetter = function getFieldMinValueGetter(field){
	return getKeyLabelFunction(this, label_field_min_value_getter, field);
};
getFieldMaxValueGetter = function getFieldMaxValueGetter(field){
	return getKeyLabelFunction(this, label_field_max_value_getter, field);
};
getFieldNextChainGetter = function getFieldNextChainGetter(field){
	return getKeyLabelFunction(this, label_field_next_chain_getter, field);
};
getFieldPreviousChainGetter = function getFieldPreviousChainGetter(field){
	return getKeyLabelFunction(this, label_field_previous_chain_getter, field);
};

setKeySeparator = function setKeySeparator(func){
	setKeyLabelFunction(this, label_key_separator, func, null);;
};
setDetailSeparator = function setDetailSeparator(func){
	setKeyLabelFunction(this, label_detail_separator, func, null);;
};
setCollisionBreaker = function setCollisionBreaker(func){
	setKeyLabelFunction(this, label_collision_breaker, func, null);;
};
setClusterInstanceGetter = function setClusterInstanceGetter(func){
	setKeyLabelFunction(this, label_cluster_instance_getter, func, null);
};
setFieldMinValueGetter = function setFieldMinValueGetter(obj, field){
	setKeyLabelFunction(this, label_field_min_value_getter, obj, field);
};
setFieldMaxValueGetter = function setFieldMaxValueGetter(obj, field){
	setKeyLabelFunction(this, label_field_max_value_getter, obj, field);
};
setFieldNextChainGetter = function setFieldNextChainGetter(obj, field){
	setKeyLabelFunction(this, label_field_next_chain_getter, obj, field);
};
setFieldPreviousChainGetter = function setFieldPreviousChainGetter(obj, field){
	setKeyLabelFunction(this, label_field_previous_chain_getter, obj, field);
};


// methods for commands
// for ClusterInstanceGetter, user may require info of whether command is read or write
var setterCommandPrefixes = ['add', 'del', 'upsert', 'incr'];
isWriter = function isCommandWriter(){
	var cmdType = command.getType(this);
	for(var i=0; i < setterCommandPrefixes.length; i++){
		var prefix = setterCommandPrefixes[i];
		if(utils.startsWith(cmdType, prefix)){
				return true;
		}
	}
	return false;
};

isReader = function isCommandReader(){
	return !this.isWriter;
};

isMulti = function isCommandMulti(){
	return (unwrap(this).multi == true);
};

// attach methods to structures
var structMethods = kcf.concat(['setClusterInstanceGetter', 'getClusterInstanceGetter', 'setKeySeparator', 'getKeySeparator'
				, 'setDetailSeparator', 'getDetailSeparator', 'setCollisionBreaker', 'getCollisionBreaker']);
var commandMethods = ['isMulti', 'isWriter', 'isReader'];
structure[access_code] = {_all: structMethods};
structure.datatype.struct[0][access_code] = {_all: structMethods};
structure.datatype.struct[0].set.command[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].string.command[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].hash.command[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].zset.command[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].set.command[0].count.mode[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].zset.command[0].delrange.mode[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].zset.command[0].rangeasc.mode[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].zset.command[0].rangedesc.mode[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].zset.command[0].count.mode[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].zset.command[0].rankasc.mode[0][access_code] = {_all: commandMethods};
structure.datatype.struct[0].zset.command[0].rankdesc.mode[0][access_code] = {_all: commandMethods};

// create interface
datatype = utils.wrap(structure, {api:{}, keyconfig:{}, keywrap:{}}, access_code, null, null, null, null).datatype;

// set defaults
datatype.setClusterInstanceGetter(default_get_cluster_instance);
datatype.setFieldMinValueGetter(default_get_field_min_value);
datatype.setFieldMaxValueGetter(default_get_field_max_value);
datatype.setFieldNextChainGetter(default_get_next_chain);
datatype.setFieldPreviousChainGetter(default_get_previous_chain);
datatype.setKeySeparator(default_key_separator);
datatype.setDetailSeparator(default_detail_separator);
datatype.setCollisionBreaker(default_collision_breaker);

datatype.getRedisMaxScoreFactor = function getRedisMaxScoreFactor(){
	return redis_max_score_factor;
};

datatype.getConfigIndexProp = function getDatatypeConfigIndexProp(config, prop){
	return ((unwrap(config).indexconfig || {})[prop] || []);
};


var dataConfig = {};
var dataKey = {};

datatype.getConfig = function getDatatypeConfig(){
	return dataConfig;
};
datatype.getKey = function getDatatypeKey(){
	return dataKey;
};


getIndexConfig = function getConfigIndexConfig(){
	return unwrap(this).indexconfig;
};
getFieldIdx = function getConfigFieldIdx(field){
	return datatype.getConfigFieldIdx(this, field);
};
getPropFieldIdxValue = function getConfigPropFieldIdxValue(prop, field_idx){
	return datatype.getConfigPropFieldIdxValue(this, prop, field_idx);
};

datatype.createConfig = function createDatatypeConfig(id, struct, index_config, on_tree){
	var structConfig = (on_tree == false || struct == null ? {} : struct.getConfig());
	var store = {api:structConfig, keyconfig:{}, keywrap:{}};
	var dict = {};
	dict[id] = {key:[{}], indexconfig: index_config};
	dict[access_code] = {_all: kcf.concat(['getIndexConfig', 'getFieldIdx', 'getPropFieldIdxValue'
						, 'getClusterInstanceGetter', 'setClusterInstanceGetter'])};
	var api = utils.wrap(dict, store, access_code, 'config', 'struct', struct, true);
	if(on_tree == false){
		api[id].getStruct = function(){return struct;};
	}else{
		dataConfig[id] = api[id];
	}
	return api[id];
};

getLabel = function getKeyLabel(){
	return unwrap(this).label;
};
getCommand = function getKeyCommand(){	// shortcut
	return this.getConfig().getStruct().getCommand();
};

datatype.createKey = function createDatatypeKey(id, label, key_config, on_tree){
	// allow struct to take nulls, in which case the Key is not bound to singleton datatype structure
	var configKey = (on_tree == false || key_config == null ? {} : key_config.getKey());
	var store = {api:configKey, keyconfig:{}, keywrap:{}};
	var dict = {};
	dict[id] = {label: label};
	dict[access_code] = {_all: kcf.concat(['getLabel', 'getCommand', 'getClusterInstanceGetter', 'setClusterInstanceGetter'])};
	api = utils.wrap(dict, store, access_code, 'key', 'config', key_config, true);
	if(on_tree == false){
		api[id].getConfig = function(){return key_config;};
	}else{
		dataKey[id] = api[id];
	}
	return api[id];
};


// TODO add checkers to e.g. inform user of malformed configurations, etc
datatype.loadTree = function loadDatatypeTree(dtree){
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
			var cfgObj = datatype.createConfig(cfg.id, strObj, cfg.index, true);
			var keys = cfg.keys;
			var configGetter = cfg.configgetter || {};
			var fieldList = datatype.getConfigIndexProp(cfgObj, 'fields');
			for(var fld in fldGetter){
				var getter = configGetter[fld];
				if(fld == 'clusterinstance'){
					cfgObj[fldGetter[fld]](getter);
				}else{
					var getterList = configGetter[fld] || [];
					for(var k=0; k < fieldList.length; k++){
						getter = getterList[k];
						if(getter != null){
							cfgObj[fldGetter[fld]](getter, fieldList[k]);
						}
					}
				}
			}
			for(var k=0; k < keys.length; k++){
				var key = keys[k];
				var keyObj = datatype.createKey(key.id, key.label, cfgObj, true);
				var keyGetter = key.keygetter || {};
				for(var fld in fldGetter){
					var getter = keyGetter[fld];
					if(fld == 'clusterinstance'){
						keyObj[fldGetter[fld]](getter);
					}else{
						var getterList = keyGetter[fld] || [];
						for(var l=0; l < fieldList.length; l++){
							getter = getterList[l];
							if(getter != null){
								keyObj[fldGetter[fld]](getter, fieldList[l]);
							}
						}
					}
				}
			}
		}
	}
};


datatype.getKeyFieldBoundChain = function getDatatypeKeyFieldBoundChain(key, field, cmd_order){
	var fieldBoundValueGetter = null;
	if(cmd_order == desc){
		fieldBoundValueGetter = key.getFieldMinValueGetter(field);
	}else{
		fieldBoundValueGetter = key.getFieldMaxValueGetter(field);
	}
	return fieldBoundValueGetter();
};

datatype.getKeyFieldNextChain = function getDatatypeKeyFieldNextChain(key, field, cmd_order, chain){
	var fieldNextChainGetter = null;
	if(cmd_order == desc){
		fieldNextChainGetter = key.getFieldPreviousChainGetter(field);
	}else{
		fieldNextChainGetter = key.getFieldNextChainGetter(field);
	} 
	return fieldNextChainGetter(chain);
};

datatype.isKeyFieldChainWithinBound = function isDatatypeKeyFieldChainWithinBound(config, fieldIdx, cmd_order, chain, bound_chain){
	var type = datatype.getConfigPropFieldIdxValue(config, 'types', fieldIdx);
	if(type == 'integer' || type == 'float'){
		chain = parseFloat(chain);
		bound_chain = parseFloat(bound_chain);
	}else{
		chain += '';
		bound_chain += '';
	}
	if(cmd_order == desc){
		return (chain >= bound_chain);
	}else{
		return (chain <= bound_chain);
	}
};


datatype.getConfigFieldIdx = function getDatatypeConfigFieldIdx(config, field){
	var idx = datatype.getConfigIndexProp(config, 'fields').indexOf(field);
	return (idx >= 0 ? idx : null);
};

datatype.getConfigPropFieldIdxValue = function getDatatypeConfigPropFieldIdxValue(config, prop, field_idx){
	return datatype.getConfigIndexProp(config, prop)[field_idx];
};


datatype.isConfigFieldScoreAddend = function isDatatypeConfigFieldScoreAddend(config, field_idx){
	return !(!datatype.getConfigPropFieldIdxValue(config, 'factors', field_idx));
}
datatype.isConfigFieldKeySuffix = function isDatatypeConfigFieldKeySuffix(config, field_idx){
	// offset=null  =>  retain everything
	// offset=0	=>  send everything to key
	return (datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx) != null);
}
datatype.isConfigFieldStrictlyKeySuffix = function isDatatypeConfigFieldStrictlyKeySuffix(config, field_idx){
	return (datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx) == 0);
}
datatype.isConfigFieldBranch = function isDatatypeConfigFieldBranch(config, field_idx){
	return datatype.getConfigPropFieldIdxValue(config, 'fieldprependskey', field_idx) == true;
}
datatype.isConfigFieldPartitioned = function isDatatypeConfigFieldPartitioned(config, field_idx){
	return datatype.getConfigPropFieldIdxValue(config, 'partitions', field_idx) == true;
}
datatype.isConfigPartitioned = function isDatatypeConfigPartitioned(config){
	var partitions = datatype.getConfigIndexProp(config, 'partitions') || [];
	for(var i=0; i < partitions.length; i++){
		if(datatype.isConfigFieldPartitioned(config, i)){
			return true;
		}
	}
	return false;
}
datatype.isConfigFieldUIDPrepend = function isDatatypeConfigFieldUIDPrepend(config, field_idx){
	var offsetPrependsUID = datatype.getConfigPropFieldIdxValue(config, 'offsetprependsuid', field_idx);
	return offsetPrependsUID == true;
}
// UID datatype.isConfigFieldStrictlyUIDPrepend whereas XID !datatype.isConfigFieldStrictlyUIDPrepend
datatype.isConfigFieldStrictlyUIDPrepend = function isDatatypeConfigFieldStrictlyUIDPrepend(config, field_idx){
	var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', field_idx);
	var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx);
	var isPrepend = datatype.isConfigFieldUIDPrepend(config, field_idx);
	return isPrepend && !datatype.isConfigFieldStrictlyKeySuffix(config, field_idx) && !datatype.isConfigFieldScoreAddend(config, field_idx);
}

// decide which hybrid of zrange to use
datatype.getZRangeSuffix = function getDatatypeZRangeSuffix(config, index, args, xid, uid){
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

splitID = function datatypeSplitID(id, offset){
	id = (id != null ? String(id) : id); 		// !!0 != !!'0'
	if(offset == null){				// index property not included in key
		return [null, id];
	}else if(offset == 0){				// full index property is included only in key
		return [id, null];
	}else if(id != null){
		id = id.toString();
		var num = 0 - offset;
		var a = id.slice(0, num);
		var b = id.slice(num);
		return [a,b];
	}else{
		// when id=null return ['', null] for the key and score respectively
		// '' would force concatenation to key
		return ['', id];
	}
};
datatype.splitConfigFieldValue = function datatypeSplitConfigFieldValue(config, index, field){
	// offset=null  =>  retain everything
	// offset=0	=>  send everything to key
	var val = index[field];
	var fieldIndex = datatype.getConfigFieldIdx(config, field);
	var split = null;
	if(!datatype.isConfigFieldKeySuffix(config, fieldIndex)){
		split = [null, val];
	}else{
		var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fieldIndex);
		if(val == null){
			var offsetgroups = datatype.getConfigIndexProp(config, 'offsetgroups');
			if(offsetgroups[fieldIndex] >= 0){
				var fields = datatype.getConfigIndexProp(config, 'fields');
				// get a non-null offsetgroup-field as [val]
				// also get the corresponding [offset]; some offsetgroup members subsume others
				for(var i=0; i < offsetgroups.length; i++){
					var fldIdx = offsetgroups[i];
					if(fldIdx == offsetgroups[fieldIndex]){
						var fld = fields[fldIdx];
						if(index[fld] != null){
							val = index[fld];
							offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fldIdx);
							break;
						}
					}
				}
			}
		}
		var prefixOffset = (offset != null ? -1*(offset%100) : null);				// prefix-info chars to join to key
		var split = splitID(val, prefixOffset);
		var prefixInfo = (prefixOffset == null || prefixOffset < 0 ? split[0] : null);
		var trailInfo = (prefixOffset == null || prefixOffset < 0 ? null : split[1]);
		var stem = (prefixOffset == null || prefixOffset < 0 ? split[1] : split[0]) || null;	// ||<null> since '' is not yet for key-suffixing
		var os = (offset != null ? Math.abs(Math.floor(offset/100)) : offset);			// how much of value to retain
		if(os == 0 && offset != 0){
			// this 0 is actually the absence of a specified value
			// if offset was not 0 in the first-place, then this means keep everything
			os = null;
		}
		split = splitID(stem, os);
		split[0] = (split[0] != null || trailInfo != null || prefixInfo != null ?
				(prefixInfo||'') + (split[0]||'') + (trailInfo||'') : null);
		if(val != index[field]){
			split[1] = null;				// non-null val was taken from an offsetgroup-field
		}
	}
	// check if field should be added to LHS
	var fieldPrefix = [];
	if(datatype.isConfigFieldBranch(config, fieldIndex)){
		fieldPrefix.push(field);
	}
	[].push.apply(split, fieldPrefix);
	return split;
};

datatype.unsplitFieldValue = function datatypeUnsplitFieldValue(split, offset){
	// some keysuffixes subsume keysuffixes of some members of the offsetgroups
	// so it must be determined whether both Info and main-offset parts are both applicable
	// i.e. both offset%100 and offset/100 parts have to be considered
	if(offset == 0){
		return split.join('');
	}
	offset = -1 * offset;
	// is there the prefix/trailer info, plus the main offset info?
	var isMultiSplit = (offset != null && (offset == 0 || Math.trunc(offset/100) != 0));
	if(offset < 0){
		var prefixOffset = (offset == null || offset == -Infinity ? offset : offset%100);
		var innerSplit = splitID(split[0], prefixOffset);
		var prefixInfo = (innerSplit[0] != null ? innerSplit[0] : '');
		var stemSplit = '';
		if(isMultiSplit){
			stemSplit = (innerSplit[1] != null ? innerSplit[1] : '');
		}
		return (prefixInfo+stemSplit+(split[1] != null ? split[1] : ''));
	}else if(offset > 0){
		var trailOffset = (offset == null || offset == Infinity ? offset : offset%100);
		var innerSplit = splitID(split[0], trailOffset);
		var stemSplit = '';
		if(isMultiSplit){
			stemSplit = (innerSplit[0] != null ? innerSplit[0] : '');
		}
		var trailInfo = (innerSplit[1] != null ? innerSplit[1] : '');
		return (stemSplit+(split[1] != null ? split[1] : '')+trailInfo);
	}
};

datatype.getFactorizedConfigFieldValue = function getFactorizedDatatypeConfigFieldValue(config, field, value){
	var fieldIndex = datatype.getConfigFieldIdx(config, field);
	var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', fieldIndex);
	var isStrictlyValueField = datatype.isConfigFieldStrictlyUIDPrepend(config, fieldIndex);
	if(value != null && factor != null && !isStrictlyValueField){
		return parseInt(value, 10) * factor;
	}else{
		return null;
	}
};

datatype.getConfigFieldPrefactor = function getDatatypeConfigFieldPrefactor(config, field_idx){
	var preFactor = null;
	for(var i=field_idx-1; i >= 0; i--){
		preFactor = datatype.getConfigPropFieldIdxValue(config, 'factors', i);
		if(preFactor){
			break;
		}
	}
	return preFactor;
};


command.getRangeMode = function getCommandRangeMode(cmd){
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

// TODO consider appending such flags directly to the command dict; especially command.requiresXID and command.requiresUID

// command which run over a certain range
command.isOverRange = function isCommandOverRange(cmd){
	var cmdType = command.getType(cmd);
	return (cmdType.slice(-1) == 'z' && (utils.startsWith(cmdType, 'range')
						|| utils.startsWith(cmdType, 'count') || utils.startsWith(cmdType, 'delrange')));
};

var xidCommandPrefixes = ['add', 'incrby', 'decrby', 'upsert', 'randmember', 'rangebyscore', 'countbyscore', 'rankbyscore', 'delrangebyscore'];
command.requiresXID = function requiresCommandXID(cmd){
	var cmdType = command.getType(cmd);
	if(cmdType == 'adds'){
		return false;
	}
	for(var i=0; i < xidCommandPrefixes.length; i++){
		var prefix = xidCommandPrefixes[i];
		if(utils.startsWith(cmdType, prefix)){
			return true;
		}
	}
	return false;
};

var uidCommandPrefixes = ['add', 'incr', 'decr', 'get', 'upsert', 'rangebylex', 'countbylex', 'rankbylex', 'delrangebylex'];
command.requiresUID = function requiresCommandUID(cmd){
	var cmdType = command.getType(cmd);
	// string/key struct doesn't take UID
	if(cmdType.slice(-1) == 'k'){
		return false;
	}
	for(var i=0; i < uidCommandPrefixes.length; i++){
		var prefix = uidCommandPrefixes[i];
		if(  utils.startsWith(cmdType, prefix)){
			return true;
		}
	}
	return false;
};


// internal routines do not rely on external interfaces; just in case the external ones are deprecated
// hence redefinitions here!
datatype.getConfigId = function getDatatypeConfigId(config){
	return config.getId();
};
datatype.getKeyConfig = function getDatatypeKeyConfig(key){
	return key.getConfig();
};
datatype.getConfigStructId = function getDatatypeStructId(config){
	return config.getStruct().getId();
};
datatype.getConfigCommand = function getDatatypeConfigCommand(config){
	return config.getStruct().getCommand();
};
datatype.getKeyLabel = function getDatatypeKeyLabel(key){
	return key.getLabel();
};
datatype.getKeyClusterInstanceGetter = function getDatatypeKeyClusterInstanceGetter(key){
	return key.getClusterInstanceGetter();
};
// TODO: this and other such routines can be switched off iff cluster-instance choices don't need it
datatype.getKeyFieldSuffixIndex = function getDatatypeKeyFieldSuffixIndex(key, field, index){
	var keySuffixIndex = {};
	var config = key.getConfig();
	var index = index || {};
	// NB: always essential to used config fields since the index itself may hold extraneous data
	var indexFields = datatype.getConfigIndexProp(config, 'fields');
	for(var i=0; i < indexFields.length; i++){
		var fld = indexFields[i];
		if(fld in index){
			var fldIdx = datatype.getConfigFieldIdx(config, fld);
			// in field-branches, only UID components which are not field branches, and the branched field are applicable
			if(field == null || fld == field
				|| (!datatype.isConfigFieldBranch(config, fldIdx) && datatype.isConfigFieldUIDPrepend(config, fldIdx))){
				var ks = datatype.splitConfigFieldValue(config, index, fld)[0];
				keySuffixIndex[fld] = ks;
			}
		}
	}
	return keySuffixIndex;
};

command.getMode = function getCommandMode(cmd){
	return cmd.getMode();
};
command.isMulti = function getDatatypeIsCommandMulti(cmd){
	return cmd.isMulti();
};


datatype.command = command;

module.exports = datatype;
