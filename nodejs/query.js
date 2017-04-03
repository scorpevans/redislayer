var async = require('async');
var utils = require('./utils');
var cluster = require('./cluster');
var comparison = require('./comparison');
var datatype = require('./datatype');
var command = datatype.command;
var queryRedis = require('./query_redis');

var query = {
	const_limit_default: 100,	// result limit for performing internal routines
};


var separator_key = datatype.getKeySeparator();
var separator_detail = datatype.getDetailSeparator();
var collision_breaker = datatype.getCollisionBreaker();
var redisMaxScoreFactor = datatype.getRedisMaxScoreFactor();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();

var label_lua_nil = '_nil';		// passing raw <nil> to lua is not exactly possible


query.getDefaultBatchLimit = function getQueryDefaultBatchLimit(){
	return query.const_limit_default;
};


query.rangeConfig = function queryRangeConfig(index){
	this.index = index;		// an index representing an object
	this.startProp = null;		// the prop which should be used to compute the starting point based on the Index
	this.stopProp = null;		// the prop which should be used to compute the stopping point, ranging stops when value changes
	this.startValue = null;		// inclusive min/max value of startProp or command if startProp is NULL
	this.stopValue = null;		// inclusive min/max value of stopProp or command if stopProp is NULL
	this.excludeCursor = false;	// in case of paging, used to specify if cursor position should be skipped
	this.cursorMatchOffset = null;	// in case of joins, used to specify amount of already returned values which match the cursor position 
	var that = this;
	this.clone = function cloneRangeConfig(){
		var rc = new query.rangeConfig(that.index);
		rc.startProp = that.startProp;
		rc.stopProp = that.stopProp;
		rc.startValue = that.startValue;
		rc.stopValue = that.stopValue;
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
getQueryDBInstance = function getQueryDBInstance(instance_decision_function, meta_cmd, keys, key_field_suffix_index, key_field){
	var arg = {metacmd:meta_cmd, key:keys, keysuffixindex:key_field_suffix_index, keyfield:key_field};
	return instance_decision_function.apply(this, [arg]);
};

getClusterInstanceQueryArgs = function getClusterInstanceQueryArgs(cluster_instance_type, meta_cmd, keys, rangeConfig, attribute, field, storage_attr){
	switch(cluster_instance_type){
	default:
		return queryRedis.getCIQA(meta_cmd, keys, rangeConfig, attribute, field, storage_attr);
	}
};

parseClusterInstanceIndexToStorageAttributes = function parseClusterInstanceIndexToStorageAttributes(cluster_instance_type, key, meta_cmd, index, range_config){
	switch(cluster_instance_type){
	default:
		return queryRedis.parseIndexToStorageAttributes(key, meta_cmd, index, range_config);
	}
};

// get next set of keyChains after the index's key
// In the case of keyFieldSuffixes, command.isOverRange must be run across a set of keyChains
// NB: depending on attribute.limit, execution may touch only some keyChains (even for $count command)
// hence attribute.limit is very much recommended
// NB: the order of key_field_suffixes is crucial
//	it should reflect the same case as if only a single key was used
//	hence Score fields should precede Member fields
getKeyChain = function getQueryKeyChain(cmd, index, keys, key_type, key_field, key_field_suffixes, limit, then){
	if(!((key_field_suffixes || []).length > 0)){
		return [];
	}
	if(limit == null || limit > query.const_limit_default){
		limit = query.const_limit_default;
	}
	var indexCopy = utils.shallowCopy(index);
	var key = keys[0];
	var fields = datatype.getConfigIndexProp(key_type, 'fields');
	var config = datatype.getKeyConfig(key);
	var cmdOrder = command.getOrder(cmd); 
	var keyChain = [];
	var arg = {key:key, index:indexCopy, limit:limit, order:cmdOrder};
	var keyChainGetters = [];
	var idx = key_field_suffixes.length-1;
	var resetIdxCounter = [];
	key_field_suffixes = key_field_suffixes.map(function(ks){return utils.shallowCopy(ks);});	// remove potential references, prepare for mutation
	async.whilst(
	function(){
		return (idx >= 0 && keyChain.length < limit);
	},function(callback){
			var kfs = key_field_suffixes[idx];
			var keysuffix = datatype.decodeVal(kfs.keysuffix, null, 'zset', 'keysuffix');	// functions may not expect encodings
			var fieldIdx = kfs.fieldidx;
			var field = fields[fieldIdx];
			arg.field = field;
			arg.keysuffix = (resetIdxCounter[idx] ? null : keysuffix);
			var keyChainGetter = keyChainGetters[idx];
			if(!keyChainGetter){
				keyChainGetters[idx] = datatype.getKeyFieldNextChainGetter(key, field);
				keyChainGetter = keyChainGetters[idx];
			}
			keyChainGetter(arg, function(err, nextChain){
				if(!utils.logError(err, 'keyChainGetter error')){
					var chainLen = (nextChain || []).length;
					var nextMinorKeySuffix = key_field_suffixes[idx+1];
					var secondMinorKeySuffix = key_field_suffixes[idx+2];
					for(var i=0; i < chainLen; i++){
						if(!nextMinorKeySuffix){				// the least significant keysuffix
							var bucket = key_field_suffixes.map(function(ks){return utils.shallowCopy(ks);});
							bucket[idx].keysuffix = datatype.encodeVal(nextChain[i], null, 'zset', 'keysuffix', true);
							keyChain.push(bucket);
						}else{	// for higher significant keysuffixes, just (in/de)crement keysuffix
							kfs.keysuffix = nextChain[i];
							break;
						}
					}
					// counting should be reset when next move is made to next significant keysuffix
					// e.g. 1997, 1998, 1999, 2000, 2001, ..., 2002,
					if(chainLen <= 0 && resetIdxCounter[idx]){
						var noSuffixFound = 'cannot retrieve any keysuffix for field: '+field;
						return then(noSuffixFound, null);
					}else if(chainLen < arg.limit){
						resetIdxCounter[idx] = true;
						idx = idx-1;						// move to next significant keysuffix
						arg.limit = 1;						// only one element required to reset counter
					}else if(nextMinorKeySuffix){
						resetIdxCounter[idx] = false;
						idx = idx+1; 						// move to the next less significant keysuffix
						arg.limit = (secondMinorKeySuffix ? 1 : limit); 	// limit is restored on the least significant keysuffix
					}
				}
				callback(err);
			});
	}, function(err){
		ret = {code:1};
		if(!utils.logError(err, 'query.getKeyChain')){
			ret = {code:0, data:keyChain};
		}
		then(err, ret);
	});
};

// TODO: limit-offset across keyChains is not yet supported
executeDecodeQuery = function executeDecodeQuery(meta_cmd, keys, key_field, index, range_config, attribute, misc, then){
	var ret = {code:1};
	var key = keys[0];
	var keyConfig = datatype.getKeyConfig(key);
	var fields = datatype.getConfigIndexProp(keyConfig, 'fields');
	var keyLabels = [datatype.getKeyLabel(key)];
	var keyFieldArray = (key_field != null ? [key_field] : []);
	attribute = attribute || {};
	var nx = attribute.nx;
	var limit = (attribute.limit != null ? attribute.limit : Infinity);
	var offset = attribute.offset;
	var withscores = attribute.withscores;
	var chainedKeyFieldSuffixes = [misc.fsa.keyfieldsuffixes || []];
	var instanceDecisionFunction = misc.instancedecisionfunction;
	var keySuffixIndex = utils.shallowCopy(misc.keysuffixindex);
	var keyChainActive = false;
	var stopKeySuffixes = null;
	var resultset = null;
	var dataLen = 0;
	var idx = 0;
	var whileCondition = null;
	async.doWhilst(
	function(callback){
		var keyFieldSuffixes = chainedKeyFieldSuffixes[idx] || [];
		if(keyChainActive){
			// clusterInstance has to be updated per keyChain
			// FYI: this happens only for command.isOverRange, in which case !command.isMulti
			// construct new keySuffixIndex
			for(var i=0; i < keyFieldSuffixes.length; i++){
				var kfs = keyFieldSuffixes[i];
				var kfsField = fields[kfs.fieldidx];
				keySuffixIndex[kfsField] = kfs.keysuffix;
			}
			misc.clusterinstance = getQueryDBInstance(instanceDecisionFunction, meta_cmd, keys, keySuffixIndex, key_field);
		}
		var clusterInstance = misc.clusterinstance;
		var clusterInstanceType = cluster.getInstanceType(clusterInstance);
		if(keyChainActive){
			// clusterInstanceType, hence other properties, may change
			// normally this would have to be done only if the cluster-instance-type changed with the new keychain
			// but there are a lot of tiny details with rangeconfigs to be sorted out; so always called for new details
			// w.r.t. ranging, updating the fsa is enough trigger for the keyfieldsuffixes, uidprefixes & xidprefixes to be adjusted
			// overwrite keysuffixes of misc.fsa, with new suffixes
			misc.fsa.keyfieldsuffixes = keyFieldSuffixes;
			var dbInstanceArgs = getClusterInstanceQueryArgs(clusterInstanceType, meta_cmd, keys, range_config, attribute, key_field, misc.fsa);
			misc.cmd = dbInstanceArgs.command;
			misc.args = dbInstanceArgs.args;
		}
		var cmd = misc.cmd;
		var cmdType = command.getType(cmd);
		var cmdOrder = command.getOrder(cmd);
		var isKeyChainCommand = command.isOverRange(cmd) && datatype.isConfigKeyChain(keyConfig);
		var args = misc.args;
		var ks = keyFieldSuffixes.map(function(kfs){return kfs.keysuffix;});
		var keyTexts = [keyLabels.concat(keyFieldArray, ks).join(separator_key)];
		async.series([
		function(cb){
			switch(clusterInstanceType){
			default:
				var keyText = keyTexts[0];
				var clusterCommand = command.toRedis(cmd);
				var suffixes = [];
				var prefixes = [];
				if(limit && limit != Infinity && !utils.startsWith(command.getType(cmd), 'count')){
					[].push.apply(suffixes, ['LIMIT', offset || 0, limit]);
				}
				if(withscores && !utils.startsWith(cmdType, 'rangebylex') && !utils.startsWith(cmdType, 'count')){
					suffixes.unshift('WITHSCORES');
				}
				if(nx && utils.startsWith(cmdType, 'add')){
					prefixes.unshift('NX');
				}
				if(clusterCommand == 'eval'){
					keyTexts = [];
					suffixes = [];
					prefixes = [];
				}
				var queryArgs = keyTexts.concat(prefixes, args, suffixes);
				var client = null;
				try{
					client = clusterInstance.getProxy();
					var clab = cluster.getInstanceLabel(clusterInstance); 
					if(!client){
						throw Error('cannot establish connection, cluster:'+clab);
					}else if(typeof client[clusterCommand] != 'function'){
						throw Error(clusterCommand+' is not a command of the '+clusterInstanceType+' client');
					}
					client[clusterCommand](queryArgs, function(err, output){
						var errorMessage = 'FATAL: query.queryDB: args: '+clusterCommand+'->'+queryArgs;
						if(utils.logError(err, errorMessage)){
							return cb(err);
						}
						if(Array.isArray(output)){
							resultset = resultset || [];
							for(var i=0; i < output.length; i++){
								var detail = null;
								var uid = null;
								var xid = null;
								if(['z', 's'].indexOf(command.getType(cmd).slice(-1)) >= 0){
									xid = null;
									uid = output[i];
									// zset indexes need withscores for completion of XID
									// withscores is only applicable to zset ranges except bylex
									if(withscores && command.isOverRange(cmd)
										&& !utils.startsWith(command.getType(cmd), 'rangebylex')){
										i++;
										xid = output[i];
									}
								}else{
									// TODO handle e.g. hgetall: does something similar to withscores
									xid = output[i];
									uid = args[i];		// TODO generally, how so?? think a bit about this!
								}
								var detail = parseStorageAttributesToIndex(clusterInstanceType, cmd, key, keyText, xid, uid, key_field);
								resultset.push(detail);
							}
							dataLen = resultset.length;
						}else if(utils.startsWith(command.getType(meta_cmd), 'get')){
							// NB: different keys may have different returns e.g. hmset NX
							//	take care to be transparent about this
							var xid = output;
							var uid = args[0];
							resultset = parseStorageAttributesToIndex(clusterInstanceType, cmd, key, keyText, xid, uid, key_field);
							dataLen = 1;
						}else if(utils.startsWith(command.getType(cmd), 'count')){
								resultset = (resultset || 0) + output;
								dataLen = resultset;
						}else{
							resultset = output;
						}
						cb(err);
					});
				}catch(err){
					return cb(err);
				}
			}
		},function(cb){
			// TODO offset-ing across keyChains is not handled
			// --> simply decrement current offset value with the count of targets in last keychain
			// NB: more correctly, keychain operations should be made atomic with lua
			// 	but this is only possible on a single server
			//	hence this cross-server approximation has been made
			// FYI: for offsetgroups, the first group-member fixes the keychain
			if(dataLen < limit && isKeyChainCommand){
				var lastKeyFieldSuffixes = chainedKeyFieldSuffixes[chainedKeyFieldSuffixes.length-1];
				if(!keyChainActive){
					keyChainActive = true;
					var stopProp = range_config.stopProp;
					var stopPropIdx = (stopProp != null ? datatype.getConfigFieldIdx(keyConfig, stopProp) : -1);
					// NB: the order of key_field_suffixes is crucial; see comparison.getConfigFieldOrdering
					// make a first-time ordering of the keysuffix fields according to storage ordering
					// FYI: this procedure also filters off keysuffixes excluded from the range e.g. partition fields
					var configOrd = comparison.getConfigFieldOrdering(keyConfig, null, null);
					var keySuffixOrder = comparison.getOrdProp(configOrd, 'order');
					var orderedKeyFieldSuffixes = [];
					for(var i=0; i < keySuffixOrder.length; i++){
						for(var j=0; j < lastKeyFieldSuffixes.length; j++){
							var fldIdx = lastKeyFieldSuffixes[j].fieldidx;
							if(fldIdx == keySuffixOrder[i].fieldidx){
								orderedKeyFieldSuffixes.push(lastKeyFieldSuffixes[j]);
								break;
							}
						}
					}
					chainedKeyFieldSuffixes[chainedKeyFieldSuffixes.length-1] = orderedKeyFieldSuffixes;
					lastKeyFieldSuffixes = orderedKeyFieldSuffixes;
					// it is possible to stop further search by comparison with the stopKeyFieldSuffixes; prepare one
					// else searches with stopValue behind startValue, would keep ranging on and on away from stopValue
					stopKeyFieldSuffixes = orderedKeyFieldSuffixes.map(function(a){return utils.shallowCopy(a);});
					var bound = (cmdOrder == asc ? datatype.getJSLastUnicode() : datatype.getJSFirstUnicode());
					for(var i=stopKeyFieldSuffixes.length-1; i >= 0; i--){
						var skfs = stopKeyFieldSuffixes[i];
						var fldIdx = skfs.fieldidx;
						if(fldIdx > stopPropIdx){
							skfs.keysuffix = bound;
							continue;
						}else if(fldIdx == stopPropIdx){
							if(range_config.stopValue != null){
								var stopIndex = utils.shallowCopy(index);
								stopIndex[stopProp] = range_config.stopValue;
								skfs.keysuffix = datatype.splitConfigFieldValue(keyConfig, stopIndex, stopProp).keysuffix;
							}
						}
						break;
					}
				}
				//  ensure that keychains are converging, not diverging, to stopValue 
				for(var i=0; i < stopKeyFieldSuffixes.length; i++){
					var stopKS = stopKeyFieldSuffixes[i].keysuffix;
					var lastKS = lastKeyFieldSuffixes[i].keysuffix;
					var fldIdx = lastKeyFieldSuffixes[i].fieldidx;	//= stopKeyFieldSuffixes[i].fieldidx
					if(!datatype.isKeyFieldChainWithinBound(keyConfig, fldIdx, cmdOrder, stopKS, lastKS)){
						break; 			// i.e. lastKS < stopKS
					}else if(!datatype.isKeyFieldChainWithinBound(keyConfig, fldIdx, cmdOrder, lastKS, stopKS)){
						whileCondition = false;
						return(cb(null));	// i.e. lastKS > stopKS
					}else if(i == stopKeyFieldSuffixes.length-1){
						whileCondition = false;
						return(cb(null));	// => complete equality on all keysuffixes
					}
				}
				if(idx >= chainedKeyFieldSuffixes.length-1){
					// get next keys in the chain
					idx = 0;
					getKeyChain(cmd, index, keys, keyConfig, key_field, lastKeyFieldSuffixes, limit, function(err, result){
						if(!utils.logCodeError(err, result)){
							chainedKeyFieldSuffixes = result.data;
							var newKeyFieldSuffixes = chainedKeyFieldSuffixes[0] || [];
							//  ensure that keychains are progressing i.e. prevent infinite loop
							for(var i=0; i < newKeyFieldSuffixes.length; i++){
								var newKS = newKeyFieldSuffixes[i].keysuffix;
								var lastKS = lastKeyFieldSuffixes[i].keysuffix;
								var fldIdx = lastKeyFieldSuffixes[i].fieldidx;	//= newKeyFieldSuffixes[i].fieldidx
								if(!datatype.isKeyFieldChainWithinBound(keyConfig, fldIdx, cmdOrder, newKS, lastKS)){
									break; 			// i.e. lastKS < newKS
								}else if(!datatype.isKeyFieldChainWithinBound(keyConfig, fldIdx, cmdOrder, lastKS, newKS)){
									var err = 'infinite loop detected; keychain values are not progression';
									return(cb(err));	// i.e. lastKS > newKS
								}else if(i == newKeyFieldSuffixes.length-1){
									var err = 'infinite loop detected; keychain values are not progression';
									return(cb(err));	// => complete equality on all keysuffixes
								}
							}
							whileCondition = (idx < chainedKeyFieldSuffixes.length);
						}
						cb(err);
					});
				}else{
					idx++;
					whileCondition = (idx < chainedKeyFieldSuffixes.length);
					cb(null);
				}
			}else{
				whileCondition = false;
				cb(null);
			}
		}],function(err){
			callback(err);
		});
	}, function(){
		return whileCondition;
	}, function(err){
		if(!utils.logError(err, 'executeDecodeQuery')){
			ret = {code:0, data:resultset};
		}
		then(err, ret);
	});
};

getIndexFieldBranches = function getQueryIndexFieldBranches(config, index){
	var fieldBranches = [];
	if(index == null){
		index = {};
	}
	for(var field in index){
		var fieldIndex = datatype.getConfigFieldIdx(config, field);
		if(datatype.isConfigFieldBranch(config, fieldIndex)){
			fieldBranches.push(field);
		}
	}
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
batchMergeQuery = function batchMergeQuery(meta_cmd, keys, qData_list, then){
	var ret = {code:1};
	var len = (qData_list || []).length;
	var instanceKeySet = {};
	var key = keys[0];
	var keyLabel = datatype.getKeyLabel(key);
	var keyConfig = datatype.getKeyConfig(key);
	var instanceDecisionFunction = datatype.getKeyClusterInstanceGetter(key);
	var isRangeCommand = command.isOverRange(meta_cmd);
	// PREPROCESS: prepare instanceKeySet for bulk query execution
	for(var i=0; i < len; i++){
		var elem = qData_list[i];
		var rangeConfig = (isRangeCommand ? elem.rangeconfig : null);
		var index = (isRangeCommand ? rangeConfig.index : elem.index) || {};
		var attribute = elem.attribute || {};					// limit, offset, nx, etc
		var fieldBranches = getIndexFieldBranches(keyConfig, index);
		// parseIndexToStorageAttributes returns results for all fieldBranches, so cache
		var storageAttrCache = {};
		// process different field-branches; vertical partitioning
		for(j=0; j < fieldBranches.length; j++){
			var fieldBranch = fieldBranches[j];
			if(fieldBranch != null && !(fieldBranch in index)){		// NB: fieldBranch can be NULL
				continue;
			}
			var keySuffixIndex = datatype.getKeyFieldSuffixIndex(key, fieldBranch, index);
			var clusterInstance = getQueryDBInstance(instanceDecisionFunction, meta_cmd, keys, keySuffixIndex, fieldBranch);
			var clusterInstanceId = cluster.getInstanceId(clusterInstance);
			var clusterInstanceType = cluster.getInstanceType(clusterInstance);
			if(storageAttrCache[fieldBranch] == null){
				// returns already dict of all fieldBranches
				storageAttrCache = parseClusterInstanceIndexToStorageAttributes(clusterInstanceType, key, meta_cmd, index, rangeConfig);
			}
			var fsa = storageAttrCache[fieldBranch];
			var dbInstanceArgs = getClusterInstanceQueryArgs(clusterInstanceType, meta_cmd, keys, rangeConfig, attribute, fieldBranch, fsa);
			// bulk up the different key-storages for bulk-execution
			var keyText = dbInstanceArgs.keytext;
			if(!instanceKeySet[clusterInstanceId]){
				instanceKeySet[clusterInstanceId] = {};
			}
			if(!instanceKeySet[clusterInstanceId][keyText]){
				var misc = {clusterinstance: clusterInstance,
						instancedecisionfunction: instanceDecisionFunction,
						cmd: dbInstanceArgs.command,
						fsa: fsa,
						keysuffixindex: keySuffixIndex,
						args: [],
					};
				instanceKeySet[clusterInstanceId][keyText] = {};
				instanceKeySet[clusterInstanceId][keyText].attribute = attribute;
				instanceKeySet[clusterInstanceId][keyText].fieldBranch = fieldBranch;
				instanceKeySet[clusterInstanceId][keyText].fieldBranchCount = fieldBranches.length;
				instanceKeySet[clusterInstanceId][keyText].misc = misc;
				// index/rangeConfig make sense only for !command.isMulti
				// else only first index is logged
				instanceKeySet[clusterInstanceId][keyText].index = index;
				instanceKeySet[clusterInstanceId][keyText].rangeconfig = rangeConfig;
			}
			[].push.apply(instanceKeySet[clusterInstanceId][keyText].misc.args, dbInstanceArgs.args);
		}
	}
	var resultset = null;
	var resultsetLength = 0;
	var resultType = null;	// 'array', 'object', 'scalar'
	var fuidIdx = {};
	var instanceIds = Object.keys(instanceKeySet);
	async.each(instanceIds, function(clusterInstanceId, callback){
		var ks = Object.keys(instanceKeySet[clusterInstanceId]);
		async.each(ks, function(keyText, cb){
			var newKeys = instanceKeySet[clusterInstanceId][keyText].newKeys;
			var attribute = instanceKeySet[clusterInstanceId][keyText].attribute;
			var fieldBranch = instanceKeySet[clusterInstanceId][keyText].fieldBranch;
			var fieldBranchCount = instanceKeySet[clusterInstanceId][keyText].fieldBranchCount;
			var index = instanceKeySet[clusterInstanceId][keyText].index;
			var rangeConfig = instanceKeySet[clusterInstanceId][keyText].rangeconfig;
			var misc = instanceKeySet[clusterInstanceId][keyText].misc;
			var clusterInstance = misc.clusterinstance;
			var cmd = misc.cmd;
			var args = misc.args;
			executeDecodeQuery(meta_cmd, keys, fieldBranch, index, rangeConfig, attribute, misc, function(err, result){
				// POST-PROCESS
				if(!utils.logCodeError(err, result)){
					var clusterInstanceType = cluster.getInstanceType(clusterInstance);
					var withscores = (attribute || {}).withscores;
					if(Array.isArray(result.data)){
						resultType = 'array';
						isRangeCommand = command.isOverRange(cmd);
						// although resultset is actually an array i.e. with integer indexes,
						// dict is used in order to maintain positioning/indexing when elements are deleted
						// 	fuidIdx references these positions/indexes
						resultset = resultset || {};
						for(var i=0; i < result.data.length; i++){
							var detail = result.data[i];
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
						if(utils.startsWith(command.getType(cmd), 'get')){
							resultType = 'object';
							var detail = result.data;
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
							var xid = result.data;
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
		if(!utils.logError(err, 'FATAL: query.batchMergeQuery')){
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
					// this should also apply to stray/extraneous props; they are ignored
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
									var split = datatype.splitConfigFieldValue(keyConfig, myIndex, prop);
									if(!(split.keysuffix in keysuffixes)){
										keysuffixes[split.keysuffix] = true;
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
		return batchMergeQuery(cmd, keys, singleIndexList, then);
	}else{
		// merge query results in retrieval order
		var partitionResults = [];
		var partitionIdx = [];
		var pcjIndexes = Object.keys(partitionCrossJoins);
		var limit = (attribute || {}).limit;
		if(limit == null){
			limit = Infinity;
		}
		var mergeData = null;
		async.each(pcjIndexes, function(idx, callback){
			var pcj = partitionCrossJoins[idx];
			for(var prop in pcj){
				indexClone[prop] = pcj[prop];
			}
			singleIndex.rangeconfig.index = indexClone;
			batchMergeQuery(cmd, keys, singleIndexList, function(err, result){
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
	batchMergeQuery(cmd, [key], index_list, then);		// TODO make [key] non-array argument
}


module.exports = query;
