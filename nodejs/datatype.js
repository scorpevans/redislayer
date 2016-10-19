var cluster = require('./cluster');
var utils = require('./utils');


	// CONFIGURATIONS
var	separator_key = ':',								// separator for parts of keys
	separator_detail = '\u0000',							// separator for parts of values i.e. UID and XID
	collision_breaker = '\u0000?*$#\u0000\u0000?#-*',				// appending this to a UID guarantees it doesn't collide with another
	redis_max_score_factor = Math.pow(10,15),
	asc = '_label_ascending',							// direction of ranging -- typically over sorted-sets
	desc = '_label_descending',							// direction of ranging -- typically over sorted-sets
	access_code = 'datatype._do_not_access_fields_with_this',			// used to lock internal-configs or leaf nodes of dicts see utils.wrap
	label_cluster_instance_getter = '_cluster_instance_function',
	default_get_cluster_instance = (function(cmd, keys, keysuffix_index, key_field){
					return cluster.getDefault().master;}),
	// functions relating to configs with key-suffixes
	// keys should have these functions attached if the datatype has key-suffixes; in order to compute getKeyChains
	label_field_min_value_getter = '_field_min_value_getter_function',		// {field1: function1,..}; see default_get_field_min_value
	label_field_max_value_getter = '_field_max_value_getter_function',		// {field1: function1,..}; see default_get_field_max_value
	label_field_next_chain_getter = '_field_next_chain_getter_function',		// {field1: function1,..}; see default_get_next_chain
	label_field_previous_chain_getter = '_field_previous_chain_getter_function',	// {field1: function1,..}; see default_get_previous_chain
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
					count: {type:'counts', val: 'count',
						mode: [{
							bylex: {type: 'countbylexs', val: 'sismember'},
							bykey: {type: 'countbykeys', val: 'scard'}}]}}],
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

// functions pertaining to structs/configs/keys with keyChains
var kcf = ['getFieldMinValueGetter','getFieldMaxValueGetter','getFieldNextChainGetter','getFieldPreviousChainGetter'
	,'setFieldMinValueGetter','setFieldMaxValueGetter','setFieldNextChainGetter','setFieldPreviousChainGetter'];

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


// methods for commands
// for ClusterInstanceGetter, user may require info of whether command is read or write
var setterCommandPrefixes = ['add', 'del', 'upsert', 'incr'];
isWriter = function(){
	var cmdType = command.getType(this);
	for(var i=0; i < setterCommandPrefixes.length; i++){
		var prefix = setterCommandPrefixes[i];
		if(utils.startsWith(cmdType, prefix)){
				return true;
		}
	}
	return false;
};

isReader = function(){
	return !this.isWriter;
};

isMulti = function(){
	return (unwrap(this).multi == true);
};

// attach methods to structures
var structMethods = kcf.concat(['setClusterInstanceGetter', 'getClusterInstanceGetter']);
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


datatype.getKeySeparator = function(){
	return separator_key;
};

datatype.getDetailSeparator = function(){
	return separator_detail;
};

datatype.getCollisionBreaker = function(){
	return collision_breaker;
};

datatype.getRedisMaxScoreFactor = function(){
	return redis_max_score_factor;
};

datatype.getConfigIndexProp = function(config, prop){
	return ((unwrap(config).indexconfig || {})[prop] || []);
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
getFieldIdx = function(field){
	return datatype.getConfigFieldIdx(this, field);
};
getPropFieldIdxValue = function(prop, field_idx){
	return datatype.getConfigPropFieldIdxValue(this, prop, field_idx);
};
getFieldOrdering = function(joint_map, joints){
	return datatype.getConfigFieldOrdering(this, joint_map, joints);
};

datatype.createConfig = function(id, struct, index_config, on_tree){
	var structConfig = (on_tree == false || struct == null ? {} : struct.getConfig());
	var store = {api:structConfig, keyconfig:{}, keywrap:{}};
	var dict = {};
	dict[id] = {key:[{}], indexconfig: index_config};
	dict[access_code] = {_all: kcf.concat(['getIndexConfig', 'getFieldIdx', 'getPropFieldIdxValue'
						, 'getClusterInstanceGetter', 'setClusterInstanceGetter', 'getFieldOrdering'])};
	var api = utils.wrap(dict, store, access_code, 'config', 'struct', struct, true);
	if(on_tree == false){
		api[id].getStruct = function(){return struct;};
	}else{
		dataConfig[id] = api[id];
	}
	return api[id];
};

getLabel = function(){
	return unwrap(this).label;
};
getCommand = function(){	// shortcut
	return this.getConfig().getStruct().getCommand();
};

datatype.createKey = function(id, label, key_config, on_tree){
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
datatype.loadTree = function(dtree){
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
				if(fld == 'clusterinstance'){
					cfgObj[fldGetter[fld]](getter);
				}else{
					var getterList = configGetter[fld] || [];
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
				var keyObj = datatype.createKey(key.id, key.label, cfgObj, true);
				var keyGetter = key.keygetter || {};
				for(var fld in fldGetter){
					if(fld == 'clusterinstance'){
						keyObj[fldGetter[fld]](getter);
					}else{
						var getterList = keyGetter[fld] || [];
						for(var l=0; l < fieldList.length; l++){
							var getter = getterList[l];
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
	var fieldNextChainGetter = null;
	if(cmd_order == desc){
		fieldNextChainGetter = key.getFieldPreviousChainGetter(field);
	}else{
		fieldNextChainGetter = key.getFieldNextChainGetter(field);
	} 
	return fieldNextChainGetter(chain);
};

datatype.isKeyFieldChainWithinBound = function(config, fieldIdx, cmd_order, chain, bound_chain){
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


datatype.getConfigFieldIdx = function(config, field){
	var idx = datatype.getConfigIndexProp(config, 'fields').indexOf(field);
	return (idx >= 0 ? idx : null);
};

datatype.getConfigPropFieldIdxValue = function(config, prop, field_idx){
	return datatype.getConfigIndexProp(config, prop)[field_idx];
};


datatype.isConfigFieldScoreAddend = function(config, field_idx){
	return !(!datatype.getConfigPropFieldIdxValue(config, 'factors', field_idx));
}
datatype.isConfigFieldKeySuffix = function(config, field_idx){
	// offset=null  =>  retain everything
	// offset=0	=>  send everything to key
	return (datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx) != null);
}
datatype.isConfigFieldStrictlyKeySuffix = function(config, field_idx){
	return (datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx) == 0);
}
datatype.isConfigFieldBranch = function(config, field_idx){
	return datatype.getConfigPropFieldIdxValue(config, 'fieldprependskey', field_idx) == true;
}
datatype.isConfigFieldPartitioned = function(config, field_idx){
	return datatype.getConfigPropFieldIdxValue(config, 'partitions', field_idx) == true;
}
datatype.isConfigPartitioned = function(config){
	var partitions = datatype.getConfigIndexProp(config, 'partitions') || [];
	for(var i=0; i < partitions.length; i++){
		if(datatype.isConfigFieldPartitioned(config, i)){
			return true;
		}
	}
	return false;
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
	return isPrepend && !datatype.isConfigFieldStrictlyKeySuffix(config, field_idx) && !datatype.isConfigFieldScoreAddend(config, field_idx);
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
datatype.splitConfigFieldValue = function(config, index, field){
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

datatype.unsplitFieldValue = function(split, offset){
	// some keysuffixes subsume keysuffixes of some members of the offsetgroups
	// so it must be determined whether both Info and main-offset parts are both applicable
	// i.e. both offset%100 and offset/100 parts have to be considered
	if(offset == 0){
		return split.join('');
	}
	offset = -1 * offset;
	var isMultiSplit = (offset != null && (offset == 0 || Math.floor(offset/100) != 0));
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

datatype.getFactorizedConfigFieldValue = function(config, field, value){
	var fieldIndex = datatype.getConfigFieldIdx(config, field);
	var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', fieldIndex);
	var isStrictlyValueField = datatype.isConfigFieldStrictlyUIDPrepend(config, fieldIndex);
	if(value != null && factor != null && !isStrictlyValueField){
		return parseInt(value, 10) * factor;
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

var label_comparator_prop = '_comparator_prop';
datatype.jointMap = function(){
	var jm = {};
	var myjm = function(key){return (key == access_code ? jm : null);};
	myjm.addPropMap = function(prop, prop_map){
		jm[prop] = jm[prop] || {};
		jm[prop][label_comparator_prop] = prop_map;
	};
	myjm.view = function(){
		return JSON.stringify(jm);
	};
	return myjm;
};
datatype.getJointMapDict = function(joint_map){
	return (joint_map != null ? unwrap(joint_map) : null);
};
datatype.getJointMapMask = function(joint_map, prop){
	var jm = datatype.getJointMapDict(joint_map);
	return (((jm || {})[prop] || {})[label_comparator_prop] || prop);
};
datatype.addJointMapMask = function(joint_map, prop, prop_map){
	var jm = datatype.getJointMapDict(joint_map);
	jm[prop] = jm[prop] || {};
	jm[prop][label_comparator_prop] = prop_map;
};

// TODO this actually only indirectly related to Joins
//	it seems more like datatype.configFieldOrd
//	then, getConfigFieldOrdering seems not to need joints and joint_map arguments
//	then, it seems datatype.jointMap belongs to join.js
datatype.jointOrd = function(){
	// keytext, score & uid hold props from joints
	// fieldconfig:{fmap1:{config:#, field:#}, fmap2:{..}, ..}
	// 	mask holds fields (which have a mapping to joints) and their respective configs
	//	these fields could be mangled if they conflict on the left and right side of a merge-join
	//	hence the need to retain the original field-name under the [field] property
	var ord = {keytext:[], score:[], uid:[], mask:{}};
	var myord = function(key){return (key == access_code ? ord : null);};
	myord.addMask = function(prop, field, config){
		if(field == null || config == null){
			throw new Error('joint-ord mask cannot have NULL values');
		}
		ord.mask[prop] = ord.mask[prop] || {};
		ord.mask[prop].config = config;
		ord.mask[prop].field = field;
	};
	// TODO deprecate
	myord.addMaskFromClone = function(prop, mask){
		var pm = ord.mask[mask] || {};
		var field = pm.field,
			config = pm.config;
		if(field == null || config == null){
			throw new Error('the prop ['+mask+'] does not have a valid mask in the joint-ord');
		}
		ord.mask[prop] = ord.mask[prop] || {};
		ord.mask[prop].config = config;
		ord.mask[prop].field = field;
	};
	myord.getMaskPropField = function(prop){
		return ((ord.mask || {})[prop] || {}).field;
	};
	myord.getMaskPropConfig = function(prop){
		return ((ord.mask || {})[prop] || {}).config;
	};
	return myord;
};
getJointOrdDict = function(joint_ord){
	return (joint_ord != null ? unwrap(joint_ord) : null);
};
datatype.getJointOrdProp = function(joint_ord, prop){
	var jm = getJointOrdDict(joint_ord);
	return (jm || {})[prop];
};
datatype.resetJointOrdMask = function(joint_ord){
	var jm = getJointOrdDict(joint_ord);
	jm.mask = {};
};
datatype.getJointOrdMaskPropField = function(joint_ord, prop){
	return (joint_ord != null ? joint_ord.getMaskPropField(prop) : null);
};
datatype.getJointOrdMaskPropConfig = function(joint_ord, prop){
	return (joint_ord != null ? joint_ord.getMaskPropConfig(prop) : null);
};
datatype.addJointOrdMask = function(joint_ord, fmask, field, config){
	if(joint_ord != null){
		joint_ord.addMask(fmask, field, config);
	}
};

datatype.normalizeOrd = function(ord){
      	// the ord should be normalized so seemingly different keys can be seen to have the same comparison
	// this also reduces unnecessary comparisons
	// merge the edges of the 3 ordParts
	var keytext = datatype.getJointOrdProp(ord, 'keytext');
	var score = datatype.getJointOrdProp(ord, 'score');
	var uid = datatype.getJointOrdProp(ord, 'uid');
	var firstScoreProp = (score[0] || [])[0];
	var lastKeytextProp = (keytext[keytext.length-1] || [])[0];
	// the consecutive occurence of a prop makes it <full>; no offsetting required
	// the first occurence must be the keysuffix
	// if lastKeytextProp is null, push firstScoreProp into keytext i.e. normalize
	//	i.e. there's no suffix, hence firstScoreProp must be complete without offset
	// if lastKeytextProp == firstScoreProp, again firstScoreProp must be complete without offset
	if(firstScoreProp != null && (lastKeytextProp == null || firstScoreProp == lastKeytextProp)){
		var idx = (keytext.length == 0 ? 0 : keytext.length-1);
		keytext[idx] = [lastKeytextProp || firstScoreProp, null];	// no offset now => complete prop
		score.splice(0,1);
		lastKeytextProp = keytext[keytext.length-1][0];			// update
	}
	var lastScoreProp = (score[score.length-1] || [])[0];
	var firstUIDProp = (uid[0] || [])[0];
	if(firstUIDProp != null && firstUIDProp == lastScoreProp){
		uid.splice(0, 1);						// i.e. entry is represented in score
	}else if(firstUIDProp != null && lastScoreProp == null && (lastKeytextProp == null || firstUIDProp == lastKeytextProp)){
		var idx = (keytext.length == 0 ? 0 : keytext.length-1);
		keytext[idx] = [lastKeytextProp || firstUIDProp, null];			// no offset now => complete prop
		uid.splice(0,1);
	}
	return ord;
};

/*
 return an ordering scheme (ord) which tells how the objects stored on a key are ordered
 the ord should be normalized so seemingly different keys can be seen to have the same comparison
 in order to improve the match of ords, PrefixInfo should be prefered to trailInfo (see splitConfigFieldValue)
	because when the prefixInfo is offsetted, the field's normalized ord is still the same as if it wasn't used as a keySuffix
	this increases the chances of making a merge join (which requires similar ordering) among the keys
 also, for floats keysuffixes and scores both orders with integer semantics; however uid strings violate this order
	so once again to improved ordering matches regardless on where values are stored, floats should be padded whenever they are placed in UIDs
	the float '123.321' should be stored as 'aa123.321'; the 'a' padding per tenth degree reinstates the float ordering
*/
datatype.getConfigFieldOrdering = function(config, joint_map, joints){
	var ord = new datatype.jointOrd();
	// convert encoding to [fieldIdx, cmp_field] pairs
	var cmpProps = joints;
	if((cmpProps || []).length > 0){
		cmpProps.sort(function(a,b){return datatype.getConfigFieldIdx(config, datatype.getJointMapMask(joint_map, a))
							- datatype.getConfigFieldIdx(config, datatype.getJointMapMask(joint_map, b))});
	}else{
		// NB: iteration is done in field-index order since e.g. uid- and key- prepends are also done in this order
		cmpProps = datatype.getConfigIndexProp(config, 'fields');
		joint_map = null;						// no mapping in this case
	}
	// we are interested in all fields between the min and max indexes of joints
	// if a non-comparator field appears between these in ord, joints is not sufficient for ordering
	var startFldIdx = datatype.getConfigFieldIdx(config, datatype.getJointMapMask(joint_map, cmpProps[0]));
	var endFldIdx = datatype.getConfigFieldIdx(config, datatype.getJointMapMask(joint_map, cmpProps[cmpProps.length-1]));
	if(cmpProps.length-1 != endFldIdx - startFldIdx){
		throw new Error('The comparator props submitted are not sufficient for ordering'
				+ '; ensure there are no missing props between the given ones!');
	}
	var scoreFieldIdx = {};
	var keytext = datatype.getJointOrdProp(ord, 'keytext');
	var score = datatype.getJointOrdProp(ord, 'score');
	var uid = datatype.getJointOrdProp(ord, 'uid');
	for(var i=0; i < cmpProps.length; i++){
		var fieldIdx = i+startFldIdx;
		var prop = cmpProps[i];
		var field = datatype.getJointMapMask(joint_map, prop);
		if(datatype.isConfigFieldPartitioned(config, fieldIdx)){
			// partitions are not used for ordering
			continue;
		}
		var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fieldIdx);
		var cmp = [prop, offset];	// this encoding is useful for datatype.normalizeOrd
		// mask
		datatype.addJointOrdMask(ord, prop, field, config);
		// keytext
	       	if(datatype.isConfigFieldKeySuffix(config, fieldIdx)){
			if(datatype.isConfigFieldStrictlyKeySuffix(config, fieldIdx)){
				cmp[1] = null;
			}
			keytext.push(cmp);
		}
		if(!datatype.isConfigFieldStrictlyKeySuffix(config, fieldIdx)){
			// score
			if(datatype.isConfigFieldScoreAddend(config, fieldIdx)){
				score.push(cmp);
				scoreFieldIdx[prop] = fieldIdx;
			}
			// uid
			if(datatype.isConfigFieldUIDPrepend(config, fieldIdx)){
				uid.push(cmp);
			}
		}
	}
	// sort ord.score in descending order of the Factors; put nulls as lowest (i.e. not participating)
	score.sort(function(a,b){return (datatype.getConfigPropFieldIdxValue(config, 'factors', scoreFieldIdx[b]) || -Infinity)
						- datatype.getConfigPropFieldIdxValue(config, 'factors', scoreFieldIdx[a]);});
	return datatype.normalizeOrd(ord);
};

//ord => {keytext:[], score:[], uid:[]};
datatype.getComparison = function(order, unmasked_fields, ord, index, ref, index_comparator, ref_comparator){
	order = order || datatype.getAscendingOrderLabel();
	var unmaskField = function(fld){return (unmasked_fields || [])[fld] || fld;};
	var numeric = ['integer', 'float'];
	// check keytext
	var keytext = datatype.getJointOrdProp(ord, 'keytext');
	for(var i=0; i < keytext.length; i++){
		var prop = keytext[i][0];
		var offset = keytext[i][1];
		// indexes may have different translations of ord props
		var indexOrdProp = datatype.getJointMapMask(index_comparator, prop);
		var indexConfig = datatype.getJointOrdMaskPropConfig(ord, indexOrdProp);
		var indexField = datatype.getJointOrdMaskPropField(ord, indexOrdProp);
		var indexProp = unmaskField(indexOrdProp);
		// indexOrdProp must have an entry in the Ord, and it's field-reference must exist in the Index
		if(indexConfig == null || indexField == null || !(indexProp in index)){
			throw new Error('missing index-mask-field or index-mask-config (for ['+indexOrdProp+']) or join-prop (for ['+indexProp+']).');
		}
		var refOrdProp = datatype.getJointMapMask(ref_comparator, prop);
		var refConfig = datatype.getJointOrdMaskPropConfig(ord, refOrdProp);
		var refField = datatype.getJointOrdMaskPropField(ord, refOrdProp);
		var refProp = unmaskField(refOrdProp);
		if(refConfig == null || refField == null || !(refProp in ref)){
			throw new Error('missing index-mask-field or index-mask-config (for ['+refOrdProp+']) or join-prop (for ['+refProp+']).');
		}
		var indexFieldIdx = datatype.getConfigFieldIdx(indexConfig, indexField);
		var refFieldIdx = datatype.getConfigFieldIdx(refConfig, refField);
		// before using index/ref within functions, translate mangled fields back to config-fields
		var primitiveIndex = {};
		primitiveIndex[indexField] = index[indexProp];
		var primitiveRef = {};
		primitiveRef[refField] = ref[refProp];
		// normalization of ord can cause props to be pushed to the keytext
		// if offset=null in ord, use the full value
		var indexVal = (offset == null ? index[indexProp] : datatype.splitConfigFieldValue(indexConfig, primitiveIndex, indexField)[0]);
		var refVal = (offset == null ? ref[refProp] : datatype.splitConfigFieldValue(refConfig, primitiveRef, refField)[0]);
		var indexFieldType = datatype.getConfigPropFieldIdxValue(indexConfig, 'types', indexFieldIdx);
		var refFieldType = datatype.getConfigPropFieldIdxValue(refConfig, 'types', refFieldIdx);
		if(numeric.indexOf(indexFieldType) >= 0 && numeric.indexOf(refFieldType) >= 0){
			indexVal = (indexVal != null && indexVal != '' ? parseFloat(indexVal, 10) : -Infinity);
			refVal = (refVal != null && indexVal != '' ? parseFloat(refVal, 10) : -Infinity);
		}else{
			indexVal = (indexVal != null ? ''+indexVal : '');
			refVal = (refVal != null ? ''+refVal : '');
		}
		if(indexVal < refVal){
			return (order == asc ? '<' : '>');
		}else if(indexVal > refVal){
			return (order == asc ? '>' : '<');
		}else if(indexVal != refVal){
			utils.logError('datatype.getComparison, keytext, '+keytext+', '+indexVal+', '+refVal, 'FATAL:');
			return null;
		}
	}
	// check score
	var score = datatype.getJointOrdProp(ord, 'score');
	for(var i=0; i < score.length; i++){
		var prop = score[i][0];
		// indexes may have different translations of ord props
		var indexOrdProp = datatype.getJointMapMask(index_comparator, prop);
		var indexConfig = datatype.getJointOrdMaskPropConfig(ord, indexOrdProp);
		var indexField = datatype.getJointOrdMaskPropField(ord, indexOrdProp);
		var indexProp = unmaskField(indexOrdProp);
		// indexOrdProp must have an entry in the Ord, and it's field-reference must exist in the Index
		if(indexConfig == null || indexField == null || !(indexProp in index)){
			throw new Error('missing index-mask-field or index-mask-config (for ['+indexOrdProp+']) or join-prop (for ['+indexProp+']).');
		}
		var refOrdProp = datatype.getJointMapMask(ref_comparator, prop);
		var refConfig = datatype.getJointOrdMaskPropConfig(ord, refOrdProp);
		var refField = datatype.getJointOrdMaskPropField(ord, refOrdProp);
		var refProp = unmaskField(refOrdProp);
		if(refConfig == null || refField == null || !(refProp in ref)){
			throw new Error('missing index-mask-field or index-mask-config (for ['+refOrdProp+']) or join-prop (for ['+refProp+']).');
		}
		var indexFieldIdx = datatype.getConfigFieldIdx(indexConfig, indexField);
		var refFieldIdx = datatype.getConfigFieldIdx(refConfig, refField);
		var indexFieldFactor = datatype.getConfigPropFieldIdxValue(indexConfig, 'factors', indexFieldIdx);
		var refFieldFactor = datatype.getConfigPropFieldIdxValue(refConfig, 'factors', refFieldIdx);
		// before using index/ref within functions, translate mangled fields back to config-fields
		var primitiveIndex = {};
		primitiveIndex[indexField] = index[indexProp];
		var primitiveRef = {};
		primitiveRef[refField] = ref[refProp];
		var indexVal = (indexFieldFactor || 0)
				* (datatype.splitConfigFieldValue(indexConfig, primitiveIndex, indexField)[1] || 0);
		var refVal = (refFieldFactor || 0)
				* (datatype.splitConfigFieldValue(refConfig, primitiveRef, refField)[1] || 0);
		if(indexVal < refVal){
			return (order == asc ? '<' : '>');
		}else if(indexVal > refVal){
			return (order == asc ? '>' : '<');
		}else if(indexVal != refVal){
			utils.logError('datatype.getComparison, score, '+score+', '+indexVal+', '+refVal, 'FATAL:');
			return null;
		}
	}
	// check uid
	var uid = datatype.getJointOrdProp(ord, 'uid');
	for(var i=0; i < uid.length; i++){
		var prop = uid[i][0];
		// indexes may have different translations of ord props
		var indexOrdProp = datatype.getJointMapMask(index_comparator, prop);
		var indexConfig = datatype.getJointOrdMaskPropConfig(ord, indexOrdProp);
		var indexField = datatype.getJointOrdMaskPropField(ord, indexOrdProp);
		var indexProp = unmaskField(indexOrdProp);
		// indexOrdProp must have an entry in the Ord, and it's field-reference must exist in the Index
		if(indexConfig == null || indexField == null || !(indexProp in index)){
			throw new Error('missing index-mask-field or index-mask-config (for ['+indexOrdProp+']) or join-prop (for ['+indexProp+']).');
		}
		var refOrdProp = datatype.getJointMapMask(ref_comparator, prop);
		var refConfig = datatype.getJointOrdMaskPropConfig(ord, refOrdProp);
		var refField = datatype.getJointOrdMaskPropField(ord, refOrdProp);
		var refProp = unmaskField(refOrdProp);
		if(refConfig == null || refField == null || !(refProp in ref)){
			throw new Error('missing index-mask-field or index-mask-config (for ['+refOrdProp+']) or join-prop (for ['+refProp+']).');
		}
		// before using index/ref within functions, translate mangled fields back to config-fields
		var primitiveIndex = {};
		primitiveIndex[indexField] = index[indexProp];
		var primitiveRef = {};
		primitiveRef[refField] = ref[refProp];
		var indexFieldType = datatype.getConfigPropFieldIdxValue(indexConfig, 'types', indexFieldIdx);
		var refFieldType = datatype.getConfigPropFieldIdxValue(refConfig, 'types', refFieldIdx);
		var indexVal = datatype.splitConfigFieldValue(indexConfig, primitiveIndex, indexField)[1];
		var refVal = datatype.splitConfigFieldValue(refConfig, primitiveRef, refField)[1];
		// NB: integers stored in UIDs are prefixed in a way such that they are still correctly ordered
		if(numeric.indexOf(indexFieldType) >= 0 && numeric.indexOf(refFieldType) >= 0){
			indexVal = (indexVal != null && indexVal != '' ? parseFloat(indexVal, 10) : -Infinity);
			refVal = (refVal != null && indexVal != '' ? parseFloat(refVal, 10) : -Infinity);
		}else{
			indexVal = (indexVal != null ? ''+indexVal : '');
			refVal = (refVal != null ? ''+refVal : '');
		}
		if(indexVal < refVal){
			return (order == asc ? '<' : '>');
	       	}else if(indexVal > refVal){
			return (order == asc ? '>' : '<');
		}else if(indexVal != refVal){
			utils.logError('datatype.getComparison, uid, '+uid+', '+indexVal+', '+refVal, 'FATAL:');
			return null;
		}
	}
	var matchSymbol = '=';
	return matchSymbol;
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

// TODO consider appending such flags directly to the command dict; especially command.requiresXID and command.requiresUID

// command which run over a certain range
command.isOverRange = function(cmd){
	var cmdType = command.getType(cmd);
	return (cmdType.slice(-1) == 'z' && (utils.startsWith(cmdType, 'range')
						|| utils.startsWith(cmdType, 'count') || utils.startsWith(cmdType, 'delrange')));
};

var xidCommandPrefixes = ['add', 'incrby', 'decrby', 'upsert', 'randmember', 'rangebyscore', 'countbyscore', 'rankbyscore', 'delrangebyscore'];
command.requiresXID = function(cmd){
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
command.requiresUID = function(cmd){
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
datatype.getConfigId = function(config){
	return config.getId();
};
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
// TODO: this and other such routines can be switched off iff cluster-instance choices don't need it
datatype.getKeyFieldSuffixIndex = function(key, field, index){
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

command.isMulti = function(cmd){
	return cmd.isMulti();
};


datatype.command = command;

module.exports = datatype;
