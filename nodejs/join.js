var async = require('async');
var utils = require('./utils');
var query = require('./query');
var datatype = require('./datatype');
var command = datatype.command;


var join = {
	nsp_separator:	'.',
};

var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();
var defaultLimit = query.getDefaultBatchLimit();

var access_code = 'join._do_not_access_fields_with_this';


join.getNamespaceSeparator = function getJoinNamespaceSeparator(){
	return join.nsp_separator;
};
join.setNamespaceSeparator = function setJoinNamespaceSeparator(nsp_separator){
	join.nsp_separator = nsp_separator;
};

var unwrap = function unwrap(caller){
        return caller(access_code);
};


join.joints = function joinJoints(joints){
	var myJoints = null;
	this.getJoints = function getJoints(){
		return myJoints.concat([]);
	};
	this.setJoints = function setJoins(js){
		var uniq = {};
		js = js || [];
		for(var i=0; i < js.length; i++){
			uniq[js[i]] = true;
		}
		myJoints = Object.keys(uniq);
	};
	this.setJoints(joints);
};

join.getJoints = function getJoinJoints(joints){
	return joints.getJoints();
};


join.streamConfig = function streamConfig(){
	this.func = null;					// passed function(s) are passed here
	this.args = null; 					// when functions are passed the args are held here
	this.namespace = null;					// required in order to prevent field clashes in joins, and to decipher cursors
	this.jointMap = null;					// if a stream returns different fields, a mapping from the join-fields may be required
	this.attributeIndex = null;				// when function are passed the attribute index location(s) are held here
	this.cursorIndex = null;				// when function args are passed, possible cursor location(s) are held here
};

join.joinConfig = function joinConfig(){
	var jc = {
		type: null,	// fulljoin or innerjoin
		mode: null,	// count or range
		order: null,
		};
	var myjc = function(ac){return (ac == access_code ? jc : null);};

	myjc.joints = null;
	myjc.limit = null;
	myjc.streamConfigs = null;

	myjc.setFulljoin = function(){jc.type = 'fulljoin';};
	myjc.setInnerjoin = function(){jc.type = 'innerjoin';};
	myjc.setModeCount = function(){jc.mode = 'count';};
	myjc.setModeList = function(){jc.mode = 'range';};
	myjc.setOrderAsc = function(){jc.order = asc;};
	myjc.setOrderDesc = function(){jc.order = desc;};

	return myjc;
};


// since join-streams pass namespaces downwards, hence return namespaced fields,
// subsequent calls receive namespaced-fields
// the respective namespaced-fields must be trimmed and used as cursor
// NB: namespaces are required to uniquely identify a join-streams even across calls
//	using array indexes as namespaces, for example, is a bad idea!
join.unNamespaceCursor = function unNamespaceJoinCursor(namespace, cursor, jointmap){
	if(cursor == null){
		return new query.rangeConfig(null);
	}
	cursorClone = query.cloneRangeConfig(cursor);
	if(!namespace){
		return cursorClone;
	}
	var index = cursorClone.index;
	var nspIndex = {};
	for(var mangle in (index || {})){
		// if there's no separator or namespace prefixes the field-name, admit field into stream's cursor
		var mask = null;
		if(mangle.indexOf(join.nsp_separator) < 0){
			mask = mangle;
		}else if(utils.startsWith(mangle, namespace + join.nsp_separator)){
			mask = mangle.slice(namespace.length + join.nsp_separator.length);
		}
		// fields may require mapping
		// but do this iff there is no expression on the mapping
		// else it should be disregarded since it's a computed field
		if(datatype.getMaskExpression(jointmap, mask) != null){
			continue;
		}
		var fld = datatype.getMaskField(jointmap, mask);
		nspIndex[fld] = index[mangle];
	}
	cursorClone.index = (cursor.index != null ? nspIndex : null);
	return cursorClone;
};


// all ords are equivalent except for the fieldmask and configmask props, which have to be merged
mergeOrds = function mergeOrds(ord, myord, namespace, stream_joint_mangle){
	// make references before possible reset of masks
	var ordFieldMasks = datatype.getOrdFieldMasks(myord);
	var propmask = datatype.getOrdProp(myord, 'propmask');
	var fieldmask = datatype.getOrdProp(myord, 'fieldmask');
	var configmask = datatype.getOrdProp(myord, 'configmask');
	var ordPropMask = null;
	var ordFieldMask = null;
	var ordConfigMask = null;
	if(ord == null){
		ord = myord;
		datatype.resetOrdMasks(ord);
		ordPropMask = propmask;
		ordFieldMask = fieldmask;
		ordConfigMask = configmask;
	}else{
		ordConfigMask = datatype.getOrdProp(myord, 'configmask');
	}
	for(var i=0; i < ordFieldMasks.length; i++){
		var fieldMask = ordFieldMasks[i];
		var mangledMask = fieldMask;
		var prop = datatype.getMaskField(propmask, fieldMask);
		var field = datatype.getMaskField(fieldmask, fieldMask);
		var config = datatype.getMaskConfig(configmask, fieldMask);
		var existProp = datatype.getMaskField(ordPropMask, fieldMask);
		var existField = datatype.getMaskField(ordFieldMask, fieldMask);
		var existConfig = datatype.getMaskConfig(ordConfigMask, fieldMask);
		var isDifferent = (existProp != prop || existField != field 
					|| (existConfig && datatype.getConfigId(existConfig) != datatype.getConfigId(config)));
		if(namespace != null || isDifferent){
			mangledMask = (namespace != null ? namespace+join.nsp_separator+fieldMask : '_'+j+'_'+fieldMask);
			// update streamJointMangles
			stream_joint_mangle[fieldMask] = mangledMask;
		}
		// transfer masks from myord into unified join ord
		datatype.addOrdMaskPropFieldConfig(ord, mangledMask, fieldMask, field, config);
	}
	return ord;
}

createJoinOrd = function createJoinOrd(joinType, joints, streamIndexes, streamProps, streamConfigs, streamJointMangles, streamJointMaps){
	// requires that provided functions return results with keys or Ords
	// NB: in case of mangling for the config prop of ord, be sure to update the respective jointMap
	var ord = null;
	var ords = [];
	for(var j=streamIndexes.length-1; j >= 0; j--){
		// if there were no results, we may not get Ord/Key for joining
		// remove such streams straight-away
		if(streamProps[j].results.length == 0){
			if(joinType == 'innerjoin'){		// initiate termination
				streamIndexes.splice(0);
				break;
			}else if(joinType == 'fulljoin'){	// search continues as usual with others
				streamIndexes.splice(j, 1);	// index <j> is not to be access anymore
				if(streamIndexes.length > 0){
					continue;
				}else{
					break;
				}
			}
		}
		var myord = streamProps[j].ord;
		// update the resultset jointMap with stream's jointMap
		var jointMap = datatype.mergeFieldMasks(streamProps[j].jointMap, streamConfigs[j].jointMap);
		streamJointMaps[j] = jointMap;			// new effective jointMap
		if(myord == null){
			var key = streamProps[j].key;
			if(key == null){
				var func = streamConfigs[j].func.name || '';
				var err = 'no [ord] or [key] was returned by '+func+'(stream['+j+'])';
				throw new Error(err);
			}
			var config = datatype.getKeyConfig(key);
			myord = datatype.getConfigFieldOrdering(config, jointMap, joints);		// may throw an Error
		}
		ords.push(myord);
		// merge ords into a single ord
		streamJointMangles[j] = {};
		var namespace = streamConfigs[j].namespace;
		ord = mergeOrds(ord, myord, namespace, streamJointMangles[j]);
		// check that ords of all streamConfigs are in harmony
		if(ords.length > 1){
			// this check is based on a normalized ord i.e. datatype.normalizeOrd
			var prevOrd = ords[ords.length-2];
			var k1 = datatype.getOrdProp(myord, 'keytext');
			var s1 = datatype.getOrdProp(myord, 'score');
			var u1 = datatype.getOrdProp(myord, 'uid');
			var k2 = datatype.getOrdProp(prevOrd, 'keytext');
			var s2 = datatype.getOrdProp(prevOrd, 'score');
			var u2 = datatype.getOrdProp(prevOrd, 'uid');
			var isCongruent = (k1.length == k2.length && s1.length == s2.length && u1.length == u2.length);
			for(var k=0; isCongruent && k < k1.length; k++){
				if(k1[k][0] != k2[k][0] || k1[k][1] != k2[k][1]){
					isCongruent = false;
				}
			}
			for(var k=0; isCongruent && k < s1.length; k++){
				if(s1[k][0] != s2[k][0] || s1[k][1] != s2[k][1]){
					isCongruent = false;
				}
			}
			for(var k=0; isCongruent && k < u1.length; k++){
				if(u1[k][0] != u2[k][0] || u1[k][1] != u2[k][1]){
					isCongruent = false;
				}
			}
			if(!isCongruent){
				var func1 = streamConfigs[j-1].func.name || '';
				var func2 = streamConfigs[j].func.name || '';
				var err = func1+' (streams '+(j-1)+') and '+func2+' (stream '+j+') do not have the same ordering';
				throw new Error(err);
			}
		}
	}
	return ord;
};


// LIMITATION: this join is ONLY on equality and on unique ranges
//	complex joins can be handled in the input-functions or on the mergeRange output
// NB: merge joins assume inputs are sorted in the same order
join.mergeStreams = function mergeStreams(join_config, then){
	var joinConfigDict = unwrap(join_config);
	var joinType = joinConfigDict.type,
		joinMode = joinConfigDict.mode,
		streamOrder = joinConfigDict.order,
		streamConfigs = join_config.streamConfigs,
		joints = join_config.joints.getJoints(),
		limit = join_config.limit;
	var streamProps = [];
	for(var i=0; i < streamConfigs.length; i++){
		streamProps[i] = {};
		streamProps[i].argsCopy = (streamConfigs[i].args || []).concat([]);    		 		    	// make a copy
		var cursorIdx = streamConfigs[i].cursorIndex;
		streamProps[i].cursorIdx = cursorIdx;
		var cursor = join.unNamespaceCursor(streamConfigs[i].namespace, streamProps[i].argsCopy[cursorIdx], streamConfigs[i].jointMap);
		streamProps[i].argsCopy[cursorIdx] = cursor;
		var attrIdx = streamConfigs[i].attributeIndex;
		streamProps[i].attributeIdx = attrIdx;
		if((streamProps[i].argsCopy[attrIdx]||{}).limit == null){
			streamProps[i].argsCopy[attrIdx] = utils.shallowCopy(streamProps[i].argsCopy[attrIdx]) || {};	// make a copy
			streamProps[i].argsCopy[attrIdx].limit = 1;//limit || defaultLimit;
		}else if(streamProps[i].argsCopy[attrIdx].limit < limit){
			streamProps[i].argsCopy[attrIdx].limit = limit || defaultLimit;
		}
		streamProps[i].argsCopy.push('/*callback-place-holder*/');
	}
	// NB: count still has to perform range since count can only be made after the joins
	// => callers should pass mode=range to the function_args
	var isRangeCount = false;
	var joinData = [];
	if(joinMode == 'count'){
		isRangeCount = true;
		joinData = 0;
	}
	var streamIndexes = [];
	var i=0;
	while(i < streamConfigs.length){
		streamIndexes.push(i++);
	}
	var boundIndex = null;
	var boundIndexIdx = -1;
	var boundIndexMask = {};
	var innerJoinCount = 0;
	var refreshIdx = null;						// the index of streamIndexes where joins are left off to fetch data
	var ord = null;							// the merges ord of the rangeconfigs
	var streamJointMangles = [];					// mangle info while merging ords
	var streamJointMaps = [];					// the merge of the inner and outer jointMaps
	limit = limit || Infinity;					// NB: not to be confused with limit in <attribute>
	if(limit <= 0){
		return then(null, {code:0, data:[]});
	}
	async.whilst(
	function(){return (((isRangeCount && joinData < limit) || joinData.length < limit) && streamIndexes.length > 0);},
	function(callback){
		async.each((refreshIdx != null ? [refreshIdx] : streamIndexes), function(idx, cb){
			var next = function(err, result){
				result = (result || {});
				streamProps[idx].results = result.data  || [];	// NB: count is not expected in joins
				streamProps[idx].key = (result.keys || [])[0];
				streamProps[idx].ord = result.ord;
				streamProps[idx].jointMap = result.jointMap;
				cb(err);
			};
			var len = streamProps[idx].argsCopy.length;
			streamProps[idx].argsCopy[len-1] = next;
			streamConfigs[idx].func.apply(this, streamProps[idx].argsCopy);
		}, function(err){
			if(!utils.logError(err, 'join.mergeStreams.1')){
				// set up a new joint-Ord, if not done already
				if(ord == null){
					try{
					      ord = createJoinOrd(joinType, joints, streamIndexes, streamProps, streamConfigs
							, streamJointMangles, streamJointMaps);
					}catch(err){
						utils.logError(err, 'join.mergeStreams.2');
						return then(err);
					}
				}
				// if the re-fetch doesn't progress the cursor we're not going to terminate
				if(refreshIdx != null && streamIndexes.length > 0){
					var str = streamIndexes[refreshIdx];
					var cursorIdx = streamProps[str].cursorIdx;
					var cursor = streamProps[str].argsCopy[cursorIdx];
					var cursorIndex = cursor.index;
					var firstIndex = (streamProps[str].results || [])[0];
					if(firstIndex){
						var fm = streamJointMaps[str];
						var sm =  streamJointMangles[str];
						var reln = null;
						try{
							reln = datatype.getComparison(streamOrder, ord, firstIndex, cursorIndex, fm, fm, sm, sm);
						}catch(error){
							utils.logError(err, 'join.mergeStreams.3');
							return then(error);
						}
						if(reln != '>'){
							var func = streamConfigs[str].func.name || '';
							var err = 'repetition detected; the cursor for '+func+'(stream['+str+']) is not behind next-fetch';
							return then(err);
						}
					}
				}
				// compare next values across the partitions and pick the least
				// direction of iteration is due to possible splicing-out of exhausted streamIndexes
				// iteration starts at refreshIdx i.e. where it was left off for fresh data
				refreshIdx = (refreshIdx != null && refreshIdx >= 0 ? refreshIdx : streamIndexes.length-1);
				for(var i=refreshIdx; true; i--){
					if(streamIndexes.length <= 0){
						// initiate termination
						streamIndexes = [];
						break;
					}
					if(i == -1){
						// reset index and process past cycle
						i = streamIndexes.length-1;
						// check if any joins have been registered
						// for fulljoin, the boundIndex is returned if it remains the min/max throughout the cycle
						// for innerjoin, the boundIndex is returned if it makes all others in the cycle
						if(joinType == 'fulljoin' || (joinType == 'innerjoin' && innerJoinCount == streamConfigs.length)){
							if(isRangeCount){
								// NB: when convenient counting is terminated if limit is reached
								// useful to prevent going after info which spreads across keys/servers
								joinData++;
							}else{
								joinData.push(boundIndexMask);
							}
							// terminate if enough results are accounted for
							var accounted = (isRangeCount ? joinData : joinData.length);
							if(accounted >= limit){
								// initiate termination
								streamIndexes = [];
								break;
							}
						}
						// reset registers
						if(joinType == 'fulljoin'){
							streamProps[boundIndexIdx].resultsIdx = 1 + (streamProps[boundIndexIdx].resultsIdx || 0);
						}else if(joinType == 'innerjoin'){
							innerJoinCount = 0;
						}
						boundIndex = null;
						boundIndexIdx = -1;
						boundIndexMask = {};
					}
					var str = streamIndexes[i];
					var idx = streamProps[str].resultsIdx || 0;		// indexes are initially null
					if(idx >= (streamProps[str].results || []).length){
						var attrIdx = streamProps[str].attributeIdx;
						var rangeLimit = streamProps[str].argsCopy[attrIdx].limit;
						// if a stream runs out of results
						if((streamProps[str].results || []).length < rangeLimit){
							if(joinType == 'innerjoin'){ i		// initiate termination
								streamIndexes = [];
								break;
							}else if(joinType == 'fulljoin'){	// search continues as usual with others
								streamIndexes.splice(i, 1);	// str (i.e. index <i>) is not to be access anymore
								if(streamIndexes.length > 0){
									continue;
								}else{
									break;
								}
							}
						}else{	// if stream runs out of batch, the search pauses for refresh
							// update the cursor of the args
							var newCursorIndex = streamProps[str].results[idx-1];	// refresh should proceed beyond this
							var cursorIdx = streamProps[str].cursorIdx;
							var oldCursor = streamProps[str].argsCopy[cursorIdx];
							if(oldCursor != null){
								if(oldCursor.index == null){
									oldCursor.index = {};
								}
								// NB: keep partitioned fields (e.g. [0,1]) intact
								for(var fld in newCursorIndex){
									if(oldCursor.index[fld] == null){
										oldCursor.index[fld] = newCursorIndex[fld];
									}else{
										var fldMangle = streamJointMangles[str][fld] || fld;
										var fldConfig = datatype.getOrdMaskConfig(ord, fldMangle);
										var fldField = datatype.getOrdMaskField(ord, fldMangle);
										var fldIdx = datatype.getConfigFieldIdx(fldConfig, fldField);
										if(!datatype.isConfigFieldPartitioned(fldConfig, fldIdx)){
											oldCursor.index[fld] = newCursorIndex[fld];
										}
									}
								}
							}else{
								oldCursor = new query.rangeConfig(newCursorIndex);
								streamProps[str].argsCopy[cursorIdx] = oldCursor;
							}
							oldCursor.excludeCursor = true;
							// initiate refresh
							refreshIdx = i;						// continue from this iteration
							streamProps[str].results = null;
							streamProps[str].resultsIdx = 0;
							break;
						}
					}
					var index = streamProps[str].results[idx];
					var indexData = {};
					var boundData = {};
					indexJM = streamJointMaps[str];
					boundJM = streamJointMaps[boundIndexIdx];
					indexSM = streamJointMangles[str];
					boundSM = streamJointMangles[boundIndexIdx];
					var reln = null;
					if(boundIndex != null){
						try{
							reln = datatype.getComparison(streamOrder, ord, index, boundIndex, indexJM, boundJM, indexSM, boundSM);
						}catch(error){
							utils.logError(err, 'join.mergeStreams.4');
							return then(error);
						}
					}
					if(boundIndex == null
						|| joinType == 'innerjoin'
						|| (joinType == 'fulljoin' && !(reln == '>'))){
						if(boundIndex == null
							|| (joinType == 'innerjoin' && reln == '>')
							|| (joinType == 'fulljoin' && reln == '<')){
							boundIndex = index;
							boundIndexIdx = str;
							if(!isRangeCount){
								boundIndexMask = {};
							}
						}
						if(reln == '=' || joinType == 'innerjoin'){		// always progressive
							streamProps[str].resultsIdx = 1 + (streamProps[str].resultsIdx || 0);	
						}
						if(joinType == 'innerjoin'){
							if(boundIndexIdx == str || reln == '>'){
								innerJoinCount = 1;
							}else if(reln == '='){
								innerJoinCount++;
							}
						}
						// NB: the different range-functions would yield different objects, hence props
						// hence the merged-results has to be unified
						// one idea is to push only ord props
						// but callers may be interested in other fields e.g. for further joins
						// so the props have to be merged; clashes have to be resolved with a namespace
						if(!isRangeCount){
							// if boundIndexMask is empty add the joints
							if(utils.isObjectEmpty(boundIndexMask)){
								for(var k=0; k < joints.length; k++){
									var jmask = joints[k];
									var fieldMask = datatype.getMaskField(streamJointMaps[str], jmask);
									var fieldMangle = streamJointMangles[str][fieldMask] || fieldMask;
									var unmask = datatype.getOrdMaskProp(ord, fieldMangle);
									// when join expressions exist, joints have to be expressed further
									// boundIndexMask would be set to that result instead
									var expression = datatype.getMaskExpression(streamConfigs[str].jointMap, jmask);
									if(expression == null){
										boundIndexMask[jmask] = index[unmask];
										// update ord with info about synthetic field
										datatype.addOrdMaskFromClone(ord, jmask, fieldMangle);
									}/*else{ expressions are not implemented
									}*/
								}
							}
							for(var fld in index){
								var fieldMangle = streamJointMangles[str][fld] || fld;
								if(!(fieldMangle in boundIndexMask)){
									boundIndexMask[fieldMangle] = index[fld];
								}else if(boundIndexMask[fieldMangle] != index[fld]){
									var err = 'FATAL: field ['+fieldMangle+'] of '+func+'(stream['+str+']) conflicts'
											+' with the value of another field with the same name;'
											+' use a namespace to resolve this.';
									return then(err);
								}
							}
						}
					}
				}
			}
			callback(err);
		});
	},
	function(err){
		var ret = {code:1};
		if(!utils.logError(err, 'join.mergeStreams')){
			ret = {code:0, data:joinData, ord:ord};
		}
		then(err, ret);	
	}
	);
};


module.exports = join;
