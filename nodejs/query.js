var async = require('async');
var utils = require('./utils');
var cluster = require('./cluster');
var comparison = require('./comparison');
var datatype = require('./datatype');
var command = datatype.command;
var queryRedis = require('./query_redis');

var query = {
	const_limit_default: 50,				// result limit for performing internal routines
};


var separator_key = datatype.getKeySeparator();
var separator_detail = datatype.getDetailSeparator();
var collision_breaker = datatype.getCollisionBreaker();
var redisMaxScoreFactor = datatype.getRedisMaxScoreFactor();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();

var label_lua_nil = '_nil';					// passing raw <nil> to lua is not exactly possible


query.getDefaultBatchLimit = function getQueryDefaultBatchLimit(){
	return query.const_limit_default;
};


query.rangeConfig = function queryRangeConfig(index){
	this.index = index;		// an index representing an object
	this.startProp = null;		// the prop which should be used to compute the starting point based on the Index
	this.stopProp = null;		// the prop which should be used to compute the stopping point
	this.boundValue = null;		// inclusive max/min value for stopProp; if NULL, ranging stops when value change from Index's stopProp value
	this.excludeCursor = false;	// in case of paging, used to specify if cursor position should be skipped
	this.cursorMatchOffset = null;	// in case of joins, used to specify amount of already returned values which match the cursor position 
	var that = this;
	this.clone = function cloneRangeConfig(){
		var rc = new query.rangeConfig(that.index);
		rc.startProp = that.startProp;
		rc.stopProp = that.stopProp;
		rc.boundValue = that.boundValue;
		rc.excludeCursor = that.excludeCursor;
		rc.cursorMatchOffset = that.cursorMatchOffset;
		return rc;
	};
};

query.cloneRangeConfig = function cloneQueryRangeConfig(rc){
	if(rc == null){
		return null;
	}
	return rc.clone();
};


// decide here which cluster_instance should be queried
getQueryDBInstance = function getQueryDBInstance(meta_cmd, keys, index, key_field){
	var key = keys[0];
	var instanceDecisionFunction = datatype.getKeyClusterInstanceGetter(key);
	var keyFieldSuffixIndex = datatype.getKeyFieldSuffixIndex(key, key_field, index);
	var arg = {metacmd:meta_cmd, key:keys, keysuffixindex:keyFieldSuffixIndex, keyfield:key_field};
	return instanceDecisionFunction.apply(this, [arg]);
};

// get next set of keyChains after the index's key
// In the case of keySuffixes, command.isOverRange must be run across a set of keyChains
// NB: depending on attribute.limit, execution may be touch only some preceding keys (even for $count command)
// hence attribute.limit is very much recommended
getKeyChain = function getQueryKeyChain(cmd, keys, key_type, key_field, key_suffixes, limit){
	if(!((key_suffixes || []).length > 0)){
		return [];
	}
	var key = keys[0];
	var config = datatype.getKeyConfig(key);
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
			var isChainWithinBound = datatype.isKeyFieldChainWithinBound(config, propIdx, cmdOrder, propNextChain, propBoundChain);
			if(isChainWithinBound){
				var nextKeySuffixes = previousKeySuffixes.concat([]);
				nextKeySuffixes[suffixPropIdx] = propNextChain;
				keyChain.push(nextKeySuffixes);
			}
		}while(isChainWithinBound && keyChain.length < limit);
	}
	return keyChain;	
};

// TODO: limit-offset across keyChains is not yet supported
queryDBInstance = function queryDBInstance(cluster_instance, cmd, keys, key_type, key_field, key_suffixes, index, args, attribute, then){
	var ret = {code:1};
	var key = keys[0];
	var keyLabels = [datatype.getKeyLabel(key)];
	var chainedKeySuffixes = [key_suffixes || []];
	var chainedArgs = [args];
	var keyFieldArray = (key_field != null ? [key_field] : []);
	attribute = attribute || {};
	var limit = attribute.limit;
	var offset = attribute.offset;
	var nx = attribute.nx;
	var withscores = attribute.withscores;
	var cmdType = command.getType(cmd);
	var keyCommand = command.toRedis(cmd);
	var suffixes = [];
	var prefixes = [];
	var clusterInstance = cluster_instance;
	// preprocess attributes for different types of storages
	switch(cluster.getInstanceType(clusterInstance)){
	default:
		if(limit && !utils.startsWith(command.getType(cmd), 'count')){
			[].push.apply(suffixes, ['LIMIT', offset || 0, limit]);
		}
		if(withscores && !utils.startsWith(cmdType, 'rangebylex') && !utils.startsWith(cmdType, 'count')){
			suffixes.unshift('WITHSCORES');
		}
		if(nx && utils.startsWith(cmdType, 'add')){
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
		var myKeySuffixes = chainedKeySuffixes[idx] || [];
		var myArgs = chainedArgs[idx];
// TODO clusterInstance has to be updated per keyChain
		switch(cluster.getInstanceType(clusterInstance)){
		default:
			var keyTexts = [];
			if(keyLabels.length > 0){
				keyTexts.push((keyLabels.concat(keyFieldArray).concat(myKeySuffixes || [])).join(separator_key));
			}
			var queryArgs = keyTexts.concat(prefixes, myArgs, suffixes);
			cluster_instance.getProxy()[keyCommand](queryArgs, function(err, output){
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
		if(idx < chainedKeySuffixes.length - 1){
			idx++;
		}else{	// refill keyChain
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

getClusterInstanceQueryArgs = function getClusterInstanceQueryArgs(cluster_instance, cmd, keys, index, rangeConfig, attribute, field, storage_attribute){
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
		return queryRedis.getCIQA(cluster_instance, cmd, keys, index, rangeConfig, attribute, field, storage_attribute);
	}
};

getIndexFieldBranches = function getQueryIndexFieldBranches(config, index){
	var fieldBranches = []; //[null];	// TODO ALWAYS make a main branch even if there are fieldBranches i.e. fieldbranch=null
	if(index == null){
		index = {};
	}
	for(var field in index){
		var fieldIndex = datatype.getConfigFieldIdx(config, field);
		if(datatype.isConfigFieldBranch(config, fieldIndex)){
			fieldBranches.push(field);
		}
	}
	// TODO remove this clause once above todo is done
	if(fieldBranches.length == 0){
		fieldBranches.push(null);
	}
	return fieldBranches;
};
parseStorageAttributesToIndex = function parseQueryStorageAttributesToIndex(cluster_instance_type, cmd, key, keyText, xid, uid, field_branch){
	switch(cluster_instance_type){
	default:
		return queryRedis.parseStorageAttributesToIndex(cmd, key, keyText, xid, uid, field_branch)
	}
}
getResultSet = function getQueryResultSet(original_cmd, keys, qData_list, then){
	var ret = {code:1};
	var len = (qData_list || []).length;
	var instanceKeySet = {};
	var key = keys[0];
	var keyLabel = datatype.getKeyLabel(key);
	var keyConfig = datatype.getKeyConfig(key);
	var isRangeCommand = command.isOverRange(original_cmd);
	// PREPROCESS: prepare instanceKeySet for bulk query execution
	for(var i=0; i < len; i++){
		var elem = qData_list[i];
		var rangeConfig = (isRangeCommand ? elem.rangeconfig : null);
		var index = (isRangeCommand ? rangeConfig.index : elem.index) || {};
		var attribute = elem.attribute || {};							// limit, offset, nx, etc
		var fieldBranches = getIndexFieldBranches(keyConfig, index);
		var cache = {storageAttr:{}, clusterInstance:{}, clusterInstanceId:{}};
		// process different field-branches; vertical partitioning
		for(j=0; j < fieldBranches.length; j++){
			var fieldBranch = fieldBranches[j];
			var clusterInstance = cache.clusterInstance[fieldBranch];
			if(clusterInstance == null){
				clusterInstance = getQueryDBInstance(original_cmd, keys, index, fieldBranch);
				cache.clusterInstance[fieldBranch] = clusterInstance;
			}
			var clusterInstanceId = cache.clusterInstanceId[fieldBranch];
			if(clusterInstanceId == null){
				clusterInstanceId = cluster.getInstanceId(clusterInstance);
				cache.clusterInstanceId[fieldBranch] = clusterInstanceId;
			}
			var fsa = cache.storageAttr[fieldBranch];
			if(fsa == null){
				switch(cluster.getInstanceType(clusterInstance)){
				default:
					var storageAttr = queryRedis.parseIndexToStorageAttributes(key, original_cmd, index);
					fsa = storageAttr[fieldBranch];
					cache.storageAttr = storageAttr;	// returns already dict of all fields
				}
			}
			var dbInstanceArgs = getClusterInstanceQueryArgs(clusterInstance, original_cmd, keys, index, rangeConfig, attribute, fieldBranch, fsa);
			
			// bulk up the different key-storages for bulk-execution
			var keyText = dbInstanceArgs.keytext;
			if(!instanceKeySet[clusterInstanceId]){
				instanceKeySet[clusterInstanceId] = {_dbinstance: clusterInstance, _keytext: {}};
			}
			if(!instanceKeySet[clusterInstanceId]._keytext[keyText]){
				instanceKeySet[clusterInstanceId]._keytext[keyText] = {};
				instanceKeySet[clusterInstanceId]._keytext[keyText].attribute = attribute;
				instanceKeySet[clusterInstanceId]._keytext[keyText].fieldBranch = fieldBranch;
				instanceKeySet[clusterInstanceId]._keytext[keyText].keySuffixes = fsa.keySuffixes;
				instanceKeySet[clusterInstanceId]._keytext[keyText].fieldBranchCount = fieldBranches.length;
				instanceKeySet[clusterInstanceId]._keytext[keyText].cmd = dbInstanceArgs.command;
				instanceKeySet[clusterInstanceId]._keytext[keyText].index = index;
				instanceKeySet[clusterInstanceId]._keytext[keyText].args = [];
				instanceKeySet[clusterInstanceId]._keytext[keyText].indexes = [];
			}
			[].push.apply(instanceKeySet[clusterInstanceId]._keytext[keyText].args, dbInstanceArgs.args);
		}
	}
	var resultset = null;
	var resultsetLength = 0;
	var resultType = null;	// 'array', 'object', 'scalar'
	var fuidIdx = {};
	var instanceFlags = Object.keys(instanceKeySet);
	async.each(instanceFlags, function(clusterInstanceId, callback){
		var clusterInstance = instanceKeySet[clusterInstanceId]._dbinstance;
		var ks = Object.keys(instanceKeySet[clusterInstanceId]._keytext);
		async.each(ks, function(keyText, cb){
			var newKeys = instanceKeySet[clusterInstanceId]._keytext[keyText].newKeys;
			var fieldBranch = instanceKeySet[clusterInstanceId]._keytext[keyText].fieldBranch;
			var keySuffixes = instanceKeySet[clusterInstanceId]._keytext[keyText].keySuffixes;
			var fieldBranchCount = instanceKeySet[clusterInstanceId]._keytext[keyText].fieldBranchCount;
			var cmd = instanceKeySet[clusterInstanceId]._keytext[keyText].cmd;
			var index = instanceKeySet[clusterInstanceId]._keytext[keyText].index;
			var args = instanceKeySet[clusterInstanceId]._keytext[keyText].args;
			var attribute = instanceKeySet[clusterInstanceId]._keytext[keyText].attribute;
			queryDBInstance(clusterInstance, cmd, keys, keyConfig, fieldBranch, keySuffixes, index, args, attribute, function(err, result){
				// POST-PROCESS
				if(!utils.logCodeError(err, result)){
					var clusterInstanceType = cluster.getInstanceType(clusterInstance);
					var withscores = (attribute || {}).withscores;
					if(Array.isArray(result.data)){
						resultType = 'array';
						isRangeCommand = command.isOverRange(cmd);
						// although resultset is actually an array i.e. with integer indexes,
						// dict is used to maintain positioning/indexing when elements are deleted
						// 	fuidIdx references these positions/indexes
						resultset = resultset || {};
						for(var i=0; i < result.data.length; i++){
							var detail = null;
							var uid = null;
							var xid = null;
							if(['z', 's'].indexOf(command.getType(cmd).slice(-1)) >= 0){
								xid = null;
								uid = result.data[i];
								// zset indexes need withscores for completion of XID
								// withscores is only applicable to zset ranges except bylex
								if(withscores && utils.startsWith(command.getType(cmd), 'range') 
									&& !utils.startsWith(command.getType(cmd), 'rangebylex')){
									i++;
									xid = result.data[i];
								}
							}else{
								// TODO handle e.g. hgetall: does something similar to withscores
								xid = result.data[i];
								uid = args[i];		// TODO generally, how so?? think a bit about this!
							}
							detail = parseStorageAttributesToIndex(clusterInstanceType, cmd, key, keyText, xid, uid, fieldBranch);
							// querying field-branches can result in separated resultsets; merge them into a single record
							if(detail.field != null){
								var fuid = detail.fuid;
								var field = detail.field;
								var idx = fuidIdx[fuid];
								if(idx == null){
									// NB: if no result was found don't return any result, not even NULL
									// BUT this applies only to ranges; getters should still return NULL
									// else add first occurence of branches
									if(detail.index == null){
										if(!isRangeCommand){
											resultset[resultsetLength] = null;
											fuidIdx[fuid] = resultsetLength;
											resultsetLength++;
										}
									}else{
										resultset[resultsetLength] = detail.index;
										fuidIdx[fuid] = resultsetLength;
										resultsetLength++;
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
								resultset[resultsetLength] = detail.index;
								resultsetLength++;
							}
						}
					}else{
						// NB: different keys may have different returns e.g. hmset NX
						//	take care to be transparent about this
						var xid = result.data;
						if(utils.startsWith(command.getType(cmd), 'get')){
							resultType = 'object';
							var uid = args[0];
							var detail = parseStorageAttributesToIndex(clusterInstanceType, cmd, key, keyText, xid, uid, fieldBranch);
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
							resultType = 'scalar';
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
		if(resultType == 'array'){
			// ensure sorting since dicts do not guaranteed keys are returned in sorted order
			ret.data = [];
			var len = Object.keys(resultset || []).length;
			for(var i=0; i < len; i++){
				ret.data[i] = resultset[''+i];
			}
		}else{
			ret.data = (resultset == label_lua_nil ? null : resultset);
		}
		ret.keys = keys;	// sometimes this is required in order to examined query results; see e.g. join.mergeStreams
		if(!utils.logError(err, 'FATAL: query.getResultSet')){
			ret.code = 0;
		}else{
			ret.code = 1;	// announce partial success/failure
		}
		then (err, ret);
	});
}

query.singleIndexQuery = function getSingleIndexQuery(cmd, key, index_or_rc, attribute, then){
	var keys = [key];				// TODO deprecate
	var keyConfig = datatype.getKeyConfig(key);
	var cmdType = command.getType(cmd);
	var partitionCrossJoins = [];
	var indexClone = null;						// prevent mutating <index>
	var ord = null;
	var isRangeQuery = command.isOverRange(cmd);
	var rangeConfig = (isRangeQuery ? index_or_rc : null);
	var index = (isRangeQuery ? (rangeConfig || {}).index : index_or_rc) || {};
	if(datatype.isConfigPartitioned(keyConfig) && isRangeQuery){
		// querying fields with partitions is tricky
		// execute the different partitions separately and merge the results
		// NB: partitions allow use of e.g. flags without breaking ordering
		var cmdOrder = command.getOrder(cmd);
		var startProp = rangeConfig.startProp;
		var startPropIndex = datatype.getConfigFieldIdx(keyConfig, startProp);
		var startPropIsAddend = datatype.isConfigFieldScoreAddend(keyConfig, startPropIndex);
		var startPropScoreAddend = datatype.getConfigIndexProp(keyConfig, 'factors', startPropIndex);
		var startPropPrependsUID = datatype.isConfigFieldUIDPrepend(keyConfig, startPropIndex);
		var stopProp = rangeConfig.stopProp;
		var stopPropIndex = datatype.getConfigFieldIdx(keyConfig, stopProp);
		var stopPropIsAddend = datatype.isConfigFieldScoreAddend(keyConfig, stopPropIndex);
		var stopPropScoreAddend = datatype.getConfigIndexProp(keyConfig, 'factors', stopPropIndex);
		var stopPropPrependsUID = datatype.isConfigFieldUIDPrepend(keyConfig, stopPropIndex);
		indexClone = {};
		for(var prop in index){								// NB: some of these props are stray/unknown
			var propIndex = datatype.getConfigFieldIdx(keyConfig, prop);
			var propValue = index[prop];
			indexClone[prop] = propValue;						// log <index> props i.e. shallow copy
			// handle partition queries
			if(Array.isArray(propValue)){
				if((propValue || []).length <= 1){				// sanitize propValue
					indexClone[prop] = (propValue || [])[0];		// null or singular value
				}else if(!datatype.isConfigFieldPartitioned(keyConfig, propIndex)){
					// without declaration, it's assumed array is indeed raw value
					// this should also apply to stray/extraneous props
					continue;
				}else{
					// partitions must be handled well w.r.t. the query-range else duplicate fetches would be made
					/*	   p1   start    p2   stop     p3
						---|------|------|------|------|--->
						   p1	stop'    p2'  start'   p3
						p1 is well within scope, p3 is out-of-scope, p2 requires only max/min value and distinct keysuffixes
					*/
					var propIsAddend = datatype.isConfigFieldScoreAddend(keyConfig, propIndex);
					var propScoreAddend = datatype.getConfigIndexProp(keyConfig, 'factors', propIndex);
					var propPrependsUID = datatype.isConfigFieldUIDPrepend(keyConfig, propIndex);
					var isPropKeySuffix = datatype.isConfigFieldKeySuffix(keyConfig, propIndex);
					var outOfStartScope = startProp
								&& (!propIsAddend || (startPropIsAddend && startPropScoreAddend > propScoreAddend))
								&& (!propPrependsUID || (startPropPrependsUID && startPropIndex < propIndex));
					var outOfStopScope = stopProp
								&& (!propIsAddend || (stopPropIsAddend && stopPropScoreAddend > propScoreAddend))
								&& (!propPrependsUID || (stopPropPrependsUID && stopPropIndex < propIndex));
					if(outOfStartScope || outOfStopScope){
						if(outOfStartScope && outOfStopScope){	// case p3
							if(!isPropKeySuffix){
								continue;		// NB: prop could still be a fieldbranch
							}else{	// prop is required to resolve keyText
								// retain only enough values to cover distinct keysuffixes
								var uniq = [];
								var keysuffixes = {};
								var myIndex = {};
								for(var j=0; j < propValue.length; j++){
									myIndex[prop] = propValue[j];
									var splits = datatype.splitConfigFieldValue(keyConfig, myIndex, prop);
									if(!(splits[0] in keysuffixes)){
										keysuffixes[splits[0]] = true;
										uniq.push(propValue[j]);
									}
								}
								propValue = uniq;
							}
						}else{					// cases p2 & p2'
							var error = 'cannot decipher range query with partitions between start and stop props';
							return then(error);
						}
					}
					// cross-join the values of the different partitioned props with array values
					if(partitionCrossJoins.length == 0){
						// initialize
						partitionCrossJoins = propValue.map(function(a){var b = {}; b[prop] = a; return b;});
						continue;
					}
					var multiBucket = null;
					for(var i=0; i < propValue.length; i++){
						var bucket = (i==0 ? partitionCrossJoins : partitionCrossJoins.map(function(a){return utils.shallowCopy(a);}));
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
	var rangeConfigClone = null;
	var singleIndex = {attribute:attribute};
	if(isRangeQuery){
		rangeConfigClone = (rangeConfig != null ? query.cloneRangeConfig(rangeConfig) : new query.rangeConfig(null));
		singleIndex.rangeconfig = rangeConfigClone;
	}else{
		singleIndex.index = index;
	}
	var singleIndexList = [singleIndex];
	if(partitionCrossJoins.length == 0){
		return getResultSet(cmd, keys, singleIndexList, then);
	}else{
		// merge query results in retrieval order
		var partitionResults = [];
		var partitionIdx = [];
		var pcjIndexes = Object.keys(partitionCrossJoins);
		var limit = (attribute || {}).limit || Infinity;
		var mergeData = null;
		async.each(pcjIndexes, function(idx, callback){
			var pcj = partitionCrossJoins[idx];
			for(var prop in pcj){
				indexClone[prop] = pcj[prop];
			}
			singleIndex.rangeconfig.index = indexClone;
			getResultSet(cmd, keys, singleIndexList, function(err, result){
				partitionResults[idx] = (result || []).data;
				callback(err);
			});
		}, function(err){
			var ret = {code:1};
			if(!utils.logError(err, 'query.singleIndexQuery')){
				if(utils.startsWith(cmdType, 'count')){
					mergeData = partitionResults.reduce(function(a,b){return a+b;});
				}else{	// compare next values across the partitions and pick the least
					var boundIndex = null;
					var boundPartition = null;
					var cmdOrder = command.getOrder(cmd);
					var ord = comparison.getConfigFieldOrdering(keyConfig, null, null);
					mergeData = [];
					// it's essential to iterate backwards, due to slicing
					for(var i=pcjIndexes.length-1; true; i--){
						if(pcjIndexes.length == 0){
							break;
						}
						if(i == -1){
							i = pcjIndexes.length-1;	// reset cycle
							if(boundIndex != null){		// return the least index across the partitions during the last cycle
								mergeData.push(boundIndex);
								// reset boundIndex for next comparison cycle
								// else how could higher indexes be less-than the previous boundIndex
								partitionIdx[boundPartition] = 1 + (partitionIdx[boundPartition] || 0);
								boundIndex = null;
								boundPartition = null;
								//if(mergeData.length >= limit){// don't waste resultsets this way
								//	break;
								//}
							}else{	// eof
								break;
							}
						}
						var part = pcjIndexes[i];
						var idx = partitionIdx[part] || 0;	// indexes are initially null
						if(idx >= (partitionResults[part] || []).length){
							// if a partition runs out of results
							// we can continue with other partitions IFF the current partition is completely out
							// otherwise further results may present some prior object to values in remaining partitions
							// hence putting any of the remaining values next may be false
							if((partitionResults[part] || []).length < (limit || Infinity)){
								pcjIndexes.splice(part, 1);
								continue;
							}else{
								break;
							}
						}
						var index = partitionResults[part][idx];
						var indexJM = null;			// same fields; no need for jointmap here
						var reln = null;
						if(boundIndex != null){
							reln = comparison.getComparison(cmdOrder, ord, index, boundIndex, indexJM, indexJM, null, null);
						}
						// NB: if reln == '=', it would be processed later
						if(boundIndex == null || reln == '<'){
							boundPartition = part;
							boundIndex = index;
						}
					}
				}
			}
			ret = {code:0, data:mergeData, keys:keys};
			then(err, ret);	
		});
	}
}
// indexList allows callers to potentially send strange but understandable queries
// e.g. zdelrangebyscore index1 index2 ... indexN
// this interface should not be allowed for calls which don't take multiple indexes
query.indexListQuery = function getIndexListQuery(cmd, key, index_list, then){
	if(!command.isMulti(cmd)){
		return then(new Error('This command does not take multiple indexes!'));
	}
	getResultSet(cmd, [key], index_list, then);		// TODO make [key] non-array argument
}


module.exports = query;
