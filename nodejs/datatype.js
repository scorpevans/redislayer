var async =  require('async');
var cluster = require('./cluster');
var utils = require('./utils');


// NB:  control-codes (U+0000–U+001F and U+007F–U+009F) are used internally; storing them may lead to data corruption

var	asc = '_label_ascending',						// direction of ranging -- typically over sorted-sets
	desc = '_label_descending',						// direction of ranging -- typically over sorted-sets
	access_code = 'datatype._do_not_access_fields_with_this',		// used to lock internal-configs or leaf nodes of dicts see utils.wrap
	redis_max_score_factor = Math.pow(10,15),
	redis_min_score = -9007199254740992,					// used to implement -Infinity
	redis_max_score = 9007199254740992,					// used to implement +Infinity
	// all characters must fall strictly within first_js_unicode and last_js_unicode
	// since they are sometimes (in/de)cremented
	first_js_unicode = '\u0000',
	last_js_unicode = '\uffff',
	positive_number_prefix = '~';						// prefixes in order to order numbers as strings
	negative_number_prefix = '-';
	key_separator = ':',							// separator for parts of keys
	// for following better choose values which can be incremented and decremented without javascript string complications
	detail_separator = '\u001c',						// separator for parts of values; NB: A < B <=> A+separator < B
	null_character = '\u001e',						// preceeds all data-characters
	empty_character = '\u001f',						// makes life regular and easier to have empty-char
	collision_breaker = 'ascii\uffff?*$#\u0000\u001f?#-*',			// appending this to a UID guarantees it doesn't collide with another
	label_cluster_instance_getter = '_cluster_instance_function',
	default_get_cluster_instance = (function(arg){return cluster.getDefault().master;}),
	// functions relating to configs with key-suffixes
	// keys should have these functions attached if the datatype has key-suffixes; in order to compute getKeyChains
	// there's no way to provide an easy default (e.g. redis.scan), especially considering that keys may be across several servers
	label_field_min_value_getter = '_field_min_value_getter_function',	// {field1: function1,..}; see default_get_field_min_value
	label_field_max_value_getter = '_field_max_value_getter_function',	// {field1: function1,..}; see default_get_field_max_value
	label_field_next_chain_getter = '_field_next_chain_getter_function',	// {field1: function1,..}; see default_get_field_next_chain
	default_get_field_min_value = (function(then){				// returns current min value of field
					then(null, null);
				}),
	default_get_field_max_value = (function(then){				// returns current max value of field
					then(null, null);
				}),
	default_get_field_next_chain = (function(arg, then){			// returns next chain after given value
				var key = arg.key;
				var index = arg.index;
				var field = arg.field;
				var keysuffix = arg.keysuffix;			// exclusive keysuffix cursor
				var order = arg.order;
				var limit = arg.limit;
				if(limit == null){
					limit = Infinity;
				}
				var config = key.getConfig();
				var fieldIdx = datatype.getConfigFieldIdx(config, field);
				var type = datatype.getConfigPropFieldIdxValue(config, 'types', fieldIdx);
				var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fieldIdx);
				var span = Math.abs(offset % 100);
				var nullKeySuffix = null_character;
				var keychain = [];
				var start = keysuffix;
				var stop = null;
				async.parallel({
				maximum: function(callback){
					if(order == asc || start == null){
						var maxFieldValueGetter = key.getFieldMaxValueGetter(field);
						maxFieldValueGetter(function(err, output){
							if(order == asc){
								stop = output;
							}else if(start == null){
								start = output;
							}
							callback(null, output);
						});
					}else{
						callback(null, null);
					}
				},
				minimum: function(callback){
					if(order == desc || start == null){
						var minFieldValueGetter = key.getFieldMinValueGetter(field);
						minFieldValueGetter(function(err, output){
							if(order == desc){
								stop = output;
							}else if(start == null){
								start = output;
								keychain.push(nullKeySuffix);		// null precedes all
							}
							callback(null, output);
						});
					}else{
						callback(null, null);
					}
				}}, function(err, bound){
					if(start == null || stop == null){
						var err = 'unable to infer start/stop of keychain; see key='+key.getId();
						return then(err, null);
					}
					// if input keysuffix== null, process generated start-value
					var startKS = start;
					if(keysuffix == null){
						index[field] = start;
						var keySuffixIndex = datatype.getKeyFieldSuffixIndex(key, field, index);
						startKS = keySuffixIndex[field];
						keychain.push(startKS);
					}
					// get the keysuffix corresponding to the stop-value
					index[field] = stop;
					var keySuffixIndex = datatype.getKeyFieldSuffixIndex(key, field, index);
					var stopKS = keySuffixIndex[field];
					// (in/de)crement keysuffix within bounds of stopKS
					try{
						var nextKS = datatype.getNextKeySuffix(order, startKS, stopKS, type, offset);
						for(var i=keychain.length; (i < limit && nextKS != null); i++){
							keychain.push(nextKS);
							nextKS = datatype.getNextKeySuffix(order, nextKS, stopKS, type, offset);
						}
					}catch(err){
						return then(err, null);
					}
					// desc EOF
					if(order == desc && keychain.length < limit && keysuffix != nullKeySuffix){
						keychain.push(nullKeySuffix);
					}
					then(null, keychain);
				});
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

// functions pertaining to structs/configs/keys with keyChains
var kcf = ['getFieldMinValueGetter','getFieldMaxValueGetter','getFieldNextChainGetter'
		,'setFieldMinValueGetter','setFieldMaxValueGetter','setFieldNextChainGetter'];

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
datatype.setFieldNextChainGetter(default_get_field_next_chain);


datatype.getNullCharacter = function getNullCharacter(){
	return null_character;
};
datatype.getEmptyCharacter = function getEmptyCharacter(){
	return empty_character;
};
datatype.getPositiveNumberPrefix = function getPositiveNumberPrefix(){
	return positive_number_prefix;
};
datatype.getNegativeNumberPrefix= function getNegativeNumberPrefix(){
	return negative_number_prefix;
};
datatype.getJSLastUnicode = function getJSLastUnicode(){
	return last_js_unicode;
};
datatype.getJSFirstUnicode = function getJSFirstUnicode(){
	return first_js_unicode;
};
datatype.getRedisMaxScore= function getRedisMaxScore(){
	return redis_max_score;
};
datatype.getRedisMinScore = function getRedisMinScore(){
	return redis_min_score;
};
datatype.getRedisMaxScoreFactor = function getRedisMaxScoreFactor(){
	return redis_max_score_factor;
};
datatype.getKeySeparator = function getDatatypeKeySeparator(){
	return key_separator;
};
datatype.getDetailSeparator = function getDatatypeDetailSeparator(){
	return detail_separator;
};
datatype.getCollisionBreaker = function getDatatypeCollisionBreaker(){
	return collision_breaker;
};
datatype.setKeySeparator = function setKeySeparator(val){
	key_separator = val;
};
datatype.setCollisionBreaker = function setCollisionBreaker(val){
	collision_breaker = val;
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
	// enforce rules on index_config
	var nonPartitionPassed = false;
	var fieldBranchExists =(index_config.fieldprependskey || []).reduce(function(a,b){return a||b;}, null);
	var factors = index_config.factors || [];
	var lastFactor = null;
	var lastRegion = null;
	var lastGroupOffset = [];
	for(var i=0; i < index_config.fields.length; i++){
		// 1. partition fields should be placed on high-order end i.e. LHS
		// this gives more control for future queries i.e. partitions serve as namespaces
		if((index_config.partitions || [])[i] != true){
			nonPartitionPassed = true;
		}else if(nonPartitionPassed){
			var error = 'partition fields should be put in the left-hand-side before non-partitions; see config: '+id;
			throw new Error(error);
		}
		// 2. all fields should have Types since this is required by some storage types like SQL
		if(!(index_config.types || [])[i]){
			var error = 'provide a type for each field; see config: '+id;
			throw new Error(error);
		}
		// 3. when a config has fieldbranches, ensure that only possible non-fieldbranches are uid-components
		// in this way xid components do not duplicate across fieldbranches; xid is mostly the fieldBranch value
		// this restriction helps with upserts since fieldBranches must be given to determine keytext
		//	=> fieldBranch value would be upserted; this would be problematic if upsert on the fieldBranch is not intended
		if(fieldBranchExists){
			if((index_config.fieldprependskey || [])[i] != true){
				if((index_config.offsetprependsuid || [])[i] != true){
					var error = 'fieldbranches must be accompanied only by uidprepends or other fieldbranches; see config: '+id;
					throw new Error(error);
				}
			}else if((index_config.offsetprependsuid || [])[i] == true){
				var error = 'values of fieldbranches should not prependuid, else they cannot be updated; see config: '+id;
				throw new Error(error);
			}
		}
		// 4. convention: enforce that factors increase in same way as fieldIndex
		// among others, this prevents logical complications with specifying Ranges
		if (factors[i]){
			if(factors[i] > (lastFactor || Infinity)){
				var error = 'score-factors must decrease with field-index; see config: '+id;
				throw new Error(error);
			}else{
				lastFactor = factors[i];
			}
		}
		// 5. convention: fields should be ordered by isConfigFieldPartitioned, isConfigFieldKeySuffix, isConfigFieldScoreAddend, isConfigFieldUIDPrepend
		// this ordering helps to provide range.startProp/stopProp in clear regions
		var error = 'fields should be ordered by partition, keysuffixes, scoreaddends, uidprepends; see config: '+id;
		if((index_config.partitions || [])[i] == true){			// i.e. isConfigFieldPartitioned
			if((lastRegion || 'partition') != 'partition'){
				throw new Error(error);
			}
		}else if((index_config.offsets || [])[i] != null){		// i.e. isConfigFieldKeySuffix
			if(['partition', 'keysuffix'].indexOf(lastRegion || 'keysuffix') < 0){
				throw new Error(error);
			}
			lastRegion = 'keysuffix';
		}else if(!(!(index_config.factors || [])[i])){			// i.e. isConfigFieldScoreAddend
			if(lastRegion == 'uidprepend'){
				throw new Error(error);
			}
			lastRegion = 'scoreaddend';
		}else if((index_config.offsetprependsuid || [])[i] == true){	// i.e. isConfigFieldUIDPrepend
			lastRegion = 'uidprepend';
		}
		// 6. ensure that subsuming groupoffsetidx come first, and warn about subsumptions
		if((index_config.offsetgroups || [])[i] != null){
			var group = (index_config.offsetgroups || [])[i];
			var offset = (index_config.offsets || [])[i];
			if(offset == null){
				var error = 'provide offsets for all fields participating in offsetgroups; see config: '+id;
				throw new Error(error);
			}
			if(lastGroupOffset[group] != null){
				if (!couldOffsetSubsumeRef(lastGroupOffset[group], offset)){
					var error = 'ensure that subsuming-offset fields come first within the offsetgroup; see config: '+id;
					throw new Error(error);
				}
				if(!couldOffsetSubsumeRef(offset, lastGroupOffset[group])){
					console.log('FYI: subsumption is being employed, ensure that this is desirable; see config: '+id);
				}
			}
			lastGroupOffset[group] = offset;
		}
	}
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
	// these methods would be used to set their corresponding field-getters
	var fldGetter = {'getclusterinstance':'setClusterInstanceGetter', 'getnextkeychain':'setFieldNextChainGetter'
			, 'getminfieldvalue':'setFieldMinValueGetter', 'getmaxfieldvalue':'setFieldMaxValueGetter'};
	var structs = dtree.structs;
	var defaultGetter = dtree.defaultgetter || {};
	for(var fld in fldGetter){
		var getter = defaultGetter[fld];
		if(getter != null){
			if(fld == 'getclusterinstance'){
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
				if(fld == 'getclusterinstance'){
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
				if(fld == 'getclusterinstance'){
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
					if(fld == 'getclusterinstance'){
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


datatype.getKeyFieldNextChainGetter = function getKeyFieldNextChainGetter(key, field){
	return key.getFieldNextChainGetter(field);
};

datatype.isKeyFieldChainWithinBound = function isDatatypeKeyFieldChainWithinBound(config, fieldIdx, cmd_order, chain, bound_chain){
	var type = datatype.getConfigPropFieldIdxValue(config, 'types', fieldIdx);
	if(type == 'integer' || type == 'float'){
		var last = (cmd_order == asc ? Infinity : -Infinity);
		var inf = {last_js_unicode: last, first_js_unicode: -1*last};
		chain = inf[chain] || chain;
		bound_chain = inf[bound_chain] || bound_chain;
		if(chain){
			chain = parseFloat(chain);
		}
		if(bound_chain){
			bound_chain = parseFloat(bound_chain);
		}
	}else{
		if(chain){
			chain += '';
		}
		if(bound_chain){
			bound_chain += '';
		}
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
datatype.isConfigKeyChain = function isDatatypeConfigKeyChain(config){
	var offsets = datatype.getConfigIndexProp(config, 'offsets') || [];
	for(var i=0; i < offsets.length; i++){
		if(offsets[i] != null){
			return true;
		}
	}
	return false;
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
// NB: fragments are sometimes inserted into both UID and Scores to achieve certain orderings!
datatype.isConfigFieldStrictlyUIDPrepend = function isDatatypeConfigFieldStrictlyUIDPrepend(config, field_idx){
	var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', field_idx);
	var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', field_idx);
	var isPrepend = datatype.isConfigFieldUIDPrepend(config, field_idx);
	return isPrepend && !datatype.isConfigFieldStrictlyKeySuffix(config, field_idx) && !datatype.isConfigFieldScoreAddend(config, field_idx);
}

// decide which hybrid of zrange to use
// NB: empty-string and null-values are represented by unicode values => NULL here means no values was provides
datatype.getZRangeSuffix = function getDatatypeZRangeSuffix(config, range_config, xidprefixes, uidprefixes){
	var rc = range_config || {};
	// 1. byrank definition
	if(rc.startValue != null && rc.stopValue != null && rc.index == null && rc.startProp == null && rc.stopProp == null){
		return 'byrank';
	}
	// 2. if config doesn't make use of scores it's none other than bylex
	var factors = datatype.getConfigIndexProp(config, 'factors') || [];
	if(factors.filter(function(a){return (a != null)}).length == 0){
		return 'bylex';
	}
	// 3. byscore, if no uid field is provided, or if !datatype.isConfigFieldStrictlyUIDPrepend for all fields in the range
	var existsNonNullUID = false;
	var existsStrictUIDPrepend = false;
	var stopPropIdx = datatype.getConfigFieldIdx(config, rc.stopProp);
	var startPropIdx = datatype.getConfigFieldIdx(config, rc.startProp);
	if(stopPropIdx == null || stopPropIdx < 0){
		stopPropIdx = (datatype.getConfigIndexProp(config, 'fields') || []).length - 1;
	}
	if(startPropIdx == null || startPropIdx < 0){
		startPropIdx = (datatype.getConfigIndexProp(config, 'fields') || []).length - 1;
	}
	var maxPropIdx = Math.max(stopPropIdx, startPropIdx);
	for(var i=0; i <= maxPropIdx; i++){
		if((uidprefixes || [])[i] && uidprefixes[i].fieldidx <= i && uidprefixes[i].uid != null){
			existsStrictUIDPrepend = true;
		}
		if(datatype.isConfigFieldStrictlyUIDPrepend(config, i)){
			existsStrictUIDPrepend = true;
		}
	}
	if(!existsNonNullUID || !existsStrictUIDPrepend){
		return 'byscore';
	}
	return 'byscorelex';
};

couldOffsetSubsumeRef = function couldOffsetSubsumeRef(offset, ref){
	// NB: check for subsumption checks only the position matches
	// actual values for different fields may not coincide!
	// TODO future data-constraint layer may check actual values before insertion
	//	only then can keysuffixes be guaranteed to subsume all members of the offsetgroup!!
	if(offset == null && ref != null){
		return false;
	}
	if(offset < 0){
		offset = Math.abs(offset);
	}
	if(ref < 0){
		ref = Math.abs(ref);
	}
	var sub = false;
	if(Math.floor(offset/100) > 0){
		// overflow part (offset/100) makes subsuming always possible depending on actual values
		sub = true;
	}else if((offset%100) > (ref%100)){
		// any gain in counter for <offset> could ensure that overflow of <ref> is still subsumable
		sub = true;
	}else if(Math.floor(ref/100) <= 0 && (offset%100) >= (ref%100)){
		sub = true;
	}
	// cannot check in case of offset value of 0; length of keysuffixes remain unknown
	return (offset == 0 || ref == 0 || sub);
};
splitID = function datatypeSplitID(id, offset){
	id = id = (id != null ? String(id) : id);
	if(offset == null){					// index property not included in key
		return [null, id];
	}
	if(offset == 0){					// full index property is included only in key
		return [id, null];
	}else if(id != null){
		id = id.toString();
		var num = 0 - offset;
		var a = id.slice(0, num);
		var b = id.slice(num);
		return [a, b];
	}else{
		return [null, id];
	}
};
datatype.splitConfigFieldValue = function datatypeSplitConfigFieldValue(config, index, field){
	// offset=null  =>  retain everything
	// offset=0	=>  send everything to key
	var split = null;
	var val = index[field];
	var fieldIndex = datatype.getConfigFieldIdx(config, field);
	var fieldBranch = (datatype.isConfigFieldBranch(config, fieldIndex) ? field : null);
	if(!datatype.isConfigFieldKeySuffix(config, fieldIndex)){
		split = [null, val, fieldBranch];
	}else{
		var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fieldIndex);
		var valueFieldIdx = fieldIndex;
		if(val == null){
			var offsetgroups = datatype.getConfigIndexProp(config, 'offsetgroups');
			if(offsetgroups[fieldIndex] >= 0){
				var fields = datatype.getConfigIndexProp(config, 'fields');
				// get a non-null offsetgroup-field as [val]
				// also get the corresponding [offset]; in case some offsetgroup members subsume others i.e. generates bigger keysuffix
				// NB: Subsuming offsetgroups must be treated with caution; subsuming fields must be present in order to get keysuffix right
				for(var i=0; i < offsetgroups.length; i++){	// order is important to fetch Subsumes first
					var fldIdx = i;
					var fld = fields[fldIdx];
					var fldOffset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fldIdx);
					var osgIdx = offsetgroups[i];
					if(osgIdx == offsetgroups[fieldIndex] && index[fld] != null && couldOffsetSubsumeRef(fldOffset, offset)){
						val = index[fld];
						offset = fldOffset;
						valueFieldIdx = fldIdx;
						break;
					}
				}
			}
		}
		var prefixOffset = (offset != null ? -1*(offset%100) : null);				// prefix-info chars to join to key
		var split = splitID(val, prefixOffset);
		var prefixInfo = (prefixOffset == null || prefixOffset < 0 ? split[0] : null);
		var trailInfo = (prefixOffset == null || prefixOffset < 0 ? null : split[1]);
		var stem = (prefixOffset == null || prefixOffset < 0 ? split[1] : split[0]) || null;
		var os = (offset != null ? Math.abs(Math.floor(offset/100)) : offset);			// how much of value to retain
		if(os == 0 && offset != 0){
			// if offset!=0 in the first-place (i.e. send everything to keysuffix)
			// ...then this os=0 means keep everything
			os = null;
		}
		split = splitID(stem, os);
		split[0] = (split[0] != null || trailInfo != null || prefixInfo != null
				? (prefixInfo||'') + (split[0]||'') + (trailInfo||'')
				: null);
		if(val != index[field]){
			split[1] = null;								// non-null val was taken from an offsetgroup-field
		}
	}
	return {keysuffix:split[0], offsetvalue:split[1], fieldbranch:fieldBranch, valuefieldidx:valueFieldIdx};
};

datatype.unsplitFieldValue = function datatypeUnsplitFieldValue(split, offset){
	// NB: values passed to this function are already decoded by caller
	if(split.keysuffix == null || split.offsetvalue == null){
		return null;		// if NULL is stored as keysuffix => total value was null
	}
	// some keysuffixes subsume keysuffixes of some members of the offsetgroups
	// so it must be determined whether Info and main-offset parts are both applicable
	// i.e. both offset%100 and offset/100 parts have to be considered
	if(offset == 0){
		return split.join('');
	}
	offset = -1 * offset;
	// is there the prefix/trailer info, plus the main offset info?
	var hasInfo =  (offset != null && offset != 0 && offset%100 != 0);
	var isOffsetted = (offset != null && (offset == 0 || parseInt(offset/100, 10) != 0));
	var isMultiSplit = (hasInfo && isOffsetted);
	if(offset < 0){
		var prefixOffset = (offset == null || offset == -Infinity ? offset : offset%100);
		var innerSplit = splitID(split.keysuffix, prefixOffset);
		var prefixInfo = (innerSplit[0] != null ? innerSplit[0] : '');
		var stemSplit = '';
		if(isMultiSplit){
			stemSplit = (innerSplit[1] != null ? innerSplit[1] : '');
		}
		return (prefixInfo+stemSplit+(split.offsetvalue != null ? split.offsetvalue : ''));
	}else if(offset > 0){
		var trailOffset = (offset == null || offset == Infinity ? offset : offset%100);
		var innerSplit = splitID(split.keysuffix, trailOffset);
		var stemSplit = '';
		if(isMultiSplit){
			stemSplit = (innerSplit[0] != null ? innerSplit[0] : '');
		}
		var trailInfo = (innerSplit[1] != null ? innerSplit[1] : '');
		return (stemSplit+(split.offsetvalue != null ? split.offsetvalue : '')+trailInfo);
	}
};

datatype.getFactorizedConfigFieldValue = function getFactorizedDatatypeConfigFieldValue(config, field_index, value){
	var factor = datatype.getConfigPropFieldIdxValue(config, 'factors', field_index);
	if(datatype.isConfigFieldScoreAddend(config, field_index)){
		return parseInt(value || 0, 10) * factor;
	}else{
		return null;
	}
};

datatype.getConfigFieldPrefactor = function getDatatypeConfigFieldPrefactor(config, field_idx){
	var preFactor = null;
	for(var i=field_idx-1; i >= 0; i--){
		if(datatype.isConfigFieldScoreAddend(config, i)){
			preFactor = datatype.getConfigPropFieldIdxValue(config, 'factors', i);
			break;
		}
	}
	return preFactor;
};


// TODO consider appending such flags directly to the command dict; especially command.requiresXID and command.requiresUID

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

var uidCommandPrefixes = ['add', 'del', 'incr', 'decr', 'get', 'upsert', 'ismember', 'rangebylex', 'countbylex', 'rankbylex', 'delrangebylex'];
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

invertFloatStr = function invertFloat(val){
	// NB: invertion is done textually inorder to prevent floating point anormalies
	return val.split('').map(function(a){if(utils.isInt(a)){return 9-a;}else{return a;}}).join('');
};
datatype.processPaddingForZsetUID = function processPaddingForZsetUID(struct, type, val){
	// NB: it is crucial to leave NULL values untouched; '' may mean something for callers
	if(val != null){
		val = val+'';
		if(struct == 'zset' && (type == 'float' || type == 'integer')){
			// NB: preceding zeros are significant, since in case of splits these may actually not be preceding
			//      in case of no prefix splits, numbers are not expected to be preceeded by zeros
			var dotIdx = (val+'.').indexOf('.');
			if(val[0] == '-'){
				// for negatives, besides the padding, the digits have to be inverted
				// and end the val with positiveNumberPrefix to take care of decimals; consider -0.999 vs -0.9
				val = invertFloatStr(val);
				val = Array(dotIdx).join(negative_number_prefix) + val.slice(1) + positive_number_prefix;
			}else{
				val = Array(dotIdx).join(positive_number_prefix) + val;
			}
		}
	}
	return val;
};

datatype.removePaddingFromZsetUID = function removePaddingFromZsetUID(struct, type, val){
	if(struct == 'zset' && (type == 'integer' || type == 'float')){
		if(val){
			// just remove preceding special-characters
			// NB: don't parse to int/float since it might be a chain of zeros, but still significant with keysuffix
			if(val[0] == positive_number_prefix){
				val = val.slice(1+val.lastIndexOf(positive_number_prefix));
			}else if(val[0] == negative_number_prefix){
				val = val.slice(1+val.lastIndexOf(negative_number_prefix));
				// for negatives, the val is also inverted
				// and suffixes with positiveNumberPrefix
				val = '-'+invertFloatStr(val.slice(0, -1));
			}
		}
	}
	return val;
};

// (de/en)codeVal help to store NULL/empty characters
var charCodes = {null:null_character, '':empty_character};
datatype.encodeVal = function encodeChar(val, type, struct, facet, isfieldprovided){
	if(!isfieldprovided){
		// meta-info that $field was not provided
		return null;
	}else if(struct == 'zset' && facet == 'xid'){
		// scores cannot distinguish null and 0
		// FYI: facet=keysuffix is encoded as a usual char
		// this enables NULLs in zset scores to be spotted iff keysuffix exists
		return (val || 0);
	}else{
		return (val in charCodes ? charCodes[val] : val);
	}
};

var inverseCharCodes = {};
inverseCharCodes[null_character] = null;
inverseCharCodes[empty_character] = '';
datatype.decodeVal = function encodeChar(val, type, struct, facet){
	return (val in inverseCharCodes ? inverseCharCodes[val] : val);
};

// TODO: this and other such routines can be switched off iff cluster-instance choices don't need it
datatype.getKeyFieldSuffixIndex = function getDatatypeKeyFieldSuffixIndex(key, field_branch, index){
	var keySuffixIndex = {};
	var config = key.getConfig();
	var index = index || {};
	// NB: always essential to use config fields since the index itself may hold extraneous data
	var indexFields = datatype.getConfigIndexProp(config, 'fields');
	for(var i=0; i < indexFields.length; i++){
		var fld = indexFields[i];
		if(fld in index){
			var fldIdx = datatype.getConfigFieldIdx(config, fld);
			// in field-branches, only UID components which are not field branches, and the branched field are applicable
			if(field_branch == null || fld == field_branch
				|| (!datatype.isConfigFieldBranch(config, fldIdx) && datatype.isConfigFieldUIDPrepend(config, fldIdx))){
				var ks = datatype.splitConfigFieldValue(config, index, fld).keysuffix;
				var type = datatype.getConfigPropFieldIdxValue(config, 'types', fldIdx);
				var struct = datatype.getConfigStructId(config);
				keySuffixIndex[fld] = datatype.encodeVal(ks, type, struct, 'keysuffix', true);
			}
		}
	}
	return keySuffixIndex;
};

datatype.getNextKeySuffix = function getDatatypeNextKeySuffix(order, startKS, stopKS, type, offset){
	var startCode = datatype.decodeVal(startKS, type, null, 'keysuffix');
	var stopCode = datatype.decodeVal(stopKS, type, null, 'keysuffix');
	var isOffsetted = (offset != null && (offset == 0 || parseInt(offset/100, 10) != 0));
	if(type != 'integer' || startCode == null || stopCode == null || !isOffsetted){
		return null;
	}
	// in the case of isMultiSplit 'jumps' are created in keysuffixes e.g. 999 ...  9991 ...  9992 (offset=603)
	// keychains should be handled to skip jumped keysuffixes
	// => (in/de)crement only offsets not prefixInfo/trailInfo
	// NB: (in/de)crementing values is tricky especially around '-' and '' i.e. -0/+0
	// 	consider: ... '-2', '-1', '-', '', '1', '2' ...
	// trick: use Math.ceil and Math.floor after adding a decimal
	var infoOffset = -1 * (offset % 100);
	var offsetIdx = (offset <= 0 ? 0 : 1);
	var splits = splitID(startKS, infoOffset);
	var incrFunc = (order == asc ? Math.ceil : Math.floor);
	splits[offsetIdx] = incrFunc((splits[offsetIdx] || '0') + '.5');
	splits[offsetIdx] = splits[offsetIdx] || '';				// 0 => ''
	startCode = splits.join('');
	var nextCode = parseInt(startCode, 10);
	stopCode = parseInt(stopCode, 10);
	if(order == asc){
		return (nextCode > stopCode ? null : nextCode);
	}else{
		return (nextCode < stopCode ? null : nextCode);
	}
};


command.getMode = function getCommandMode(cmd){
	return cmd.getMode();
};
command.isMulti = function getDatatypeIsCommandMulti(cmd){
	return cmd.isMulti();
};


datatype.command = command;

module.exports = datatype;
