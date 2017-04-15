var async = require('async');
var utils = require('./utils');
var query = require('./query');
var datatype = require('./datatype');
var comparison = require('./comparison');
var command = datatype.command;


var join = {
	nsp_separator:	'.',
	max_loops: 100000,		// in the event of cursor blunders, prevent infinite loops
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

	myjc.joint = null;
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
		var mask = null;
		if(mangle.indexOf(join.nsp_separator) < 0){
			// if there's no separator or namespace prefixes in the field-name, admit field into stream's cursor
			// this may put extraneous fields, but this should be okay
			mask = mangle;
		}else if(utils.startsWith(mangle, namespace + join.nsp_separator)){
			mask = mangle.slice(namespace.length + join.nsp_separator.length);
		}else{	// field belongs to another namespace
			continue;
		}
		// fields may require mapping
		// but do this iff there is no expression on the mapping
		// else it should be disregarded since it's a computed field
		if(comparison.getMaskExpression(jointmap, mask) != null){
			continue;
		}
		var fld = comparison.getMaskField(jointmap, mask);
		nspIndex[fld] = index[mangle];
	}
	cursorClone.index = (cursor.index != null ? nspIndex : null);
	return cursorClone;
};


// all ords are equivalent except for the fieldmask and configmask props, which have to be merged
mergeOrds = function mergeOrds(ord, myord, namespace, stream_joint_mangle, stream_idx){
	// make references before possible reset of masks
	var ordFieldMasks = comparison.getOrdFieldMasks(myord);
	var fieldmask = comparison.getOrdProp(myord, 'fieldmask');
	var configmask = comparison.getOrdProp(myord, 'configmask');
	var ordFieldMask = null;
	if(ord == null){
		ord = myord;
		comparison.resetOrdMasks(ord);
	}else{
		ordFieldMask = comparison.getOrdProp(ord, 'fieldmask');
		// ords can be merged only if their orderings are comparable
		var fieldOrder = comparison.getOrdProp(ord, 'order');
		var myFieldOrder = comparison.getOrdProp(myord, 'order');
		for(var k=0; k < fieldOrder.length; k++){
			if(myFieldOrder[k].mask != fieldOrder[k].mask){
				var err = 'stream '+stream_idx+' does not have the same ordering with previous streams';
				throw new Error(err);
			}
			// merged ords may have different fieldidx for the field-mappings
			// fieldidx prop in field-orders is invalidated
			if('fieldidx' in fieldOrder[k]){
				delete fieldOrder[k].fieldidx;
			}
		}
	}
	for(var i=0; i < ordFieldMasks.length; i++){
		var fieldMask = ordFieldMasks[i];
		var mangledMask = fieldMask;
		var field = comparison.getMaskField(fieldmask, fieldMask);
		var config = comparison.getMaskConfig(configmask, fieldMask);
		var existField = comparison.getMaskField(ordFieldMask, fieldMask);
		if(namespace != null || existField){
			mangledMask = (namespace != null ? namespace+join.nsp_separator+fieldMask : '_'+stream_idx+join.nsp_separator+fieldMask);
			// update streamJointMangles
			stream_joint_mangle[fieldMask] = mangledMask;
		}
		// transfer masks from myord into unified join ord
		comparison.addOrdMaskPropFieldConfig(ord, mangledMask, fieldMask, field, config);
	}
	return ord;
}

createJoinOrd = function createJoinOrd(joinType, joint, streamIndexes, streamProps, streamConfigs, streamJointMangles, streamJointMaps){
	// requires that provided functions return results with keys or Ords
	// NB: in case of mangling for the config prop of ord, be sure to update the respective jointMap
	var ord = null;
	var ords = [];
	// it's essential to iterate backwards due to possible slicing
	for(var j=streamIndexes.length-1; j >= 0; j--){
		// if there were no results, we may not get Ord/Key for joining
		// remove such streams straight-away
		if(streamProps[j].results.length == 0){
			if(joinType == 'innerjoin'){
				streamIndexes.splice(0);	// initiate termination
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
		var str = streamIndexes[j];
		var myord = streamProps[str].ord;
		// update the resultset jointMap with stream's jointMap
		var jointMap = comparison.mergeFieldMasks(streamProps[str].jointMap, streamConfigs[str].jointMap);
		streamJointMaps[str] = jointMap;			// new effective jointMap
		if(myord == null){
			var key = streamProps[str].key;
			if(key == null){
				var func = streamConfigs[str].func.name || '';
				var err = 'no [ord] or [key] was returned by '+func+'(stream['+str+'])';
				throw new Error(err);
			}
			var config = datatype.getKeyConfig(key);
			myord = comparison.getConfigFieldOrdering(config, jointMap, joint);		// may throw an Error
		}
		ords.push(myord);
		// merge ords into a single ord
		streamJointMangles[str] = {};
		var namespace = streamConfigs[str].namespace;
		ord = mergeOrds(ord, myord, namespace, streamJointMangles[str], str);
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
		joint = join_config.joint,
		limit = (join_config.limit == '' ? null : join_config.limit);
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
			streamProps[i].argsCopy[attrIdx].limit = (limit == null? defaultLimit : limit);
		}else if(streamProps[i].argsCopy[attrIdx].limit < limit){
			streamProps[i].argsCopy[attrIdx].limit = (limit == null? defaultLimit : limit);
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
	var boundIndexStr = null;
	var isBounded = false;
	var cursorReln = null;				// when refetch returns this relationship should exist b/n cursor and head element
	var boundIndexMask = {};
	var innerJoinCount = 0;
	var refreshIdx = null;				// the index of streamIndexes where joins are left off to fetch data
	var ord = null;					// the merged ord of the rangeconfigs
	var streamJointMangles = [];			// mangle info while merging ords
	var streamJointMaps = [];			// the merge of the inner and outer jointMaps
	var getMaskedMangledStreamField = function(jmask, str){
		var maskFld = comparison.getMaskField(streamJointMaps[str], jmask);
		var mangleFld = streamJointMangles[str][maskFld] || maskFld;
		var streamFld = comparison.getOrdMaskProp(ord, mangleFld);
		return {maskFld:maskFld, mangleFld:mangleFld, streamFld:streamFld};
	};
	var joinFields = null;
	if(limit == null){
		limit = Infinity;			// NB: not to be confused with limit in <attribute>
	}else if(limit <= 0){
		return then(null, {code:0, data:[]});
	}
	var loopCount = 0;
	async.whilst(
	function(){
		return (((isRangeCount && joinData < limit) || joinData.length < limit) && streamIndexes.length > 0);
	},
	function(callback){
		loopCount++;
		if(loopCount > join.max_loops){
			var error = 'join.mergestreams max_loops has been breached';
			return then(error);
		}
		async.each((refreshIdx != null ? [streamIndexes[refreshIdx]] : streamIndexes), function(idx, cb){
			var next = function(err, result){
				result = (result || {});
				streamProps[idx].results = result.data  || [];	// NB: count is not expected in joins
				streamProps[idx].resultsIdx = 0;
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
						ord = createJoinOrd(joinType, joint, streamIndexes, streamProps, streamConfigs
									, streamJointMangles, streamJointMaps);
						joinFields = comparison.getOrdProp(ord, 'joints');
					}catch(err){
						utils.logError(err, 'join.mergeStreams.2');
						return then(err);
					}
				}
				// if the re-fetch doesn't progress the cursor, we're not going to terminate
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
							reln = comparison.getComparison(streamOrder, ord, firstIndex, cursorIndex, fm, fm, sm, sm);
						}catch(error){
							utils.logError(err, 'join.mergeStreams.3');
							return then(error);
						}
						reln = reln || '<';
						if(reln == '<' || (cursorReln == '=' && reln != '>')){
							var func = streamConfigs[str].func.name || '';
							var err = 'repetition detected; new fetch for '+func+'(stream['+str+']) does not progress forward';
							return then(err);
						}
					}
				}
				// compare next values across the partitions and pick the least
				// iteration starts at refreshIdx i.e. where it was left off for fresh data
				// iterate backwards due to possible slicing
				for(var i=refreshIdx; true; i--){
					if(streamIndexes.length <= 0){
						// initiate termination
						streamIndexes = [];
						break;
					}
					var headStrIdx = streamIndexes.length-1;
					if(i == null || i == -1 || i == streamIndexes.length){
						i = headStrIdx;	// begin loop
					}
					var str = streamIndexes[i];
					if(isBounded && ((i == headStrIdx && joinType == 'fulljoin') || (str == boundIndexStr && joinType == 'innerjoin'))){
						// process past cycle
						// check if any joins have been registered
						// for fulljoin, the boundIndex is returned if it remains the min/max throughout the cycle
						// for innerjoin, the boundIndex is returned if it matches all others in the cycle
						if(joinType == 'fulljoin' || (joinType == 'innerjoin' && innerJoinCount == streamConfigs.length)){
							if(isRangeCount){
								// NB: when convenient counting is terminated if limit is reached
								// useful to prevent going after info which spreads across keys/servers
								joinData++;
							}else{
								joinData.push(boundIndexMask);
							}
							// reset registers
							streamProps[boundIndexStr].resultsIdx = 1 + streamProps[boundIndexStr].resultsIdx;	
							boundIndexMask = {};
							isBounded = false;
							//boundIndex = null;	// boundIndex retained as a cursor point; may be required for refreshes
							//boundIndexStr = null;
							// terminate if enough results are accounted for
							//var accounted = (isRangeCount ? joinData : joinData.length);
							//if(accounted >= limit){		// don't waste resultsets this way
							//	// initiate termination
							//	streamIndexes = [];
							//	break;
							//}
						}else{	// i.e. incomplete innerjoin; other streams are still behind
							// should not happen since each stream is progressed until >= boundIndex
							var error = 'join.mergestream.1 unexpected condition';
							return then(err);
						}
					}
					var idx = streamProps[str].resultsIdx;
					if(idx >= (streamProps[str].results || []).length){
						var attrIdx = streamProps[str].attributeIdx;
						var rangeLimit = streamProps[str].argsCopy[attrIdx].limit;
						// if a stream runs out of results
						if((streamProps[str].results || []).length < rangeLimit){
							if(joinType == 'innerjoin'){		// initiate termination
								streamIndexes = [];
								break;
							}else if(joinType == 'fulljoin'){	// search continues as usual with others
								streamIndexes.splice(i, 1);	// str (i.e. index <i>) is not to be accessed anymore
								if(streamIndexes.length > 0){
									continue;
								}else{
									break;
								}
							}
						}else{	// if stream runs out of batch, the search pauses for refresh
							// update the cursor of the args
							var cursorIdx = streamProps[str].cursorIdx;
							var newCursorIndex = null;
							if(joinType == 'fulljoin'){
								newCursorIndex = streamProps[str].results[idx-1];
								cursorReln = '=';		// next resultset has to be '>' newCursorIndex TODO assumes 1 to 1 joins
							}else if(joinType == 'innerjoin'){
								if(str == boundIndexStr){
									newCursorIndex = boundIndex;
									cursorReln = '=';
								}else{
									cursorReln = '<';
									newCursorIndex = streamProps[str].results[idx-1];
									// advance newCursorIndex fields with boundIndex
									// NB: mapping required for fields from boundIndexStr to str
									for(var j=0; j < joinFields.length; j++){
										var fld = joinFields[j];
										var strFld = getMaskedMangledStreamField(fld, str).streamFld;
										var boundStrFld = getMaskedMangledStreamField(fld, boundIndexStr).streamFld;
										newCursorIndex[strFld] = boundIndex[boundStrFld];
									}
								}
							}
							var oldCursorClone = streamProps[str].argsCopy[cursorIdx].clone();	// use a shallowCopy for mutation
							streamProps[str].argsCopy[cursorIdx] = oldCursorClone;			// attached clone
							// use entire index-cursor point
							oldCursorClone.startProp = null;
							oldCursorClone.startValue = null;
							if(oldCursorClone == null){
								oldCursorClone = new query.rangeConfig(newCursorIndex);
								streamProps[str].argsCopy[cursorIdx] = oldCursorClone;
							}else if(utils.isObjectEmpty(oldCursorClone.index)){
								oldCursorClone.index = newCursorIndex;
							}else{	// NB: keep partitioned-field values (e.g. [0,1]) intact
								// FYI: mutate newCursorIndex instead; oldCursorClone points to other objects
								for(var fld in oldCursorClone.index){
									// copy partitioned-field values from old cursor
									var fldMangle = streamJointMangles[str][fld] || fld;
									var fldConfig = comparison.getOrdMaskConfig(ord, fldMangle);
									// NB: indexes may contain extraneous fields; bypass them
									if(fldConfig == null){
										continue;
									}
									var fldField = comparison.getOrdMaskField(ord, fldMangle);
									var fldIdx = datatype.getConfigFieldIdx(fldConfig, fldField);
									if(datatype.isConfigFieldPartitioned(fldConfig, fldIdx)){
										newCursorIndex[fld] = oldCursorClone.index[fld];
									}
								}
								oldCursorClone.index = newCursorIndex;
							}
							if(cursorReln == '<'){
								oldCursorClone.excludeCursor = false;
							}else{
								oldCursorClone.excludeCursor = true;
							}
							// initiate refresh
							refreshIdx = i;		// continue from this iteration
							streamProps[str].results = null;
							streamProps[str].resultsIdx = 0;
							break;
						}
					}
					var index = streamProps[str].results[idx];
					var indexJM = streamJointMaps[str];
					var indexSM = streamJointMangles[str];
					var boundJM = streamJointMaps[boundIndexStr];
					var boundSM = streamJointMangles[boundIndexStr];
					var reln = null;
					if(isBounded){
						try{
							reln = comparison.getComparison(streamOrder, ord, index, boundIndex, indexJM, boundJM, indexSM, boundSM);
						}catch(error){
							utils.logError(err, 'join.mergeStreams.5');
							return then(error);
						}
					}
					if(isBounded == false
						|| (joinType == 'fulljoin' && (reln == '<' || reln == '='))
						|| (joinType == 'innerjoin' && (reln == '>' || reln == '='))){
						if(isBounded == false || (joinType == 'innerjoin' && reln == '>') || (joinType == 'fulljoin' && reln == '<')){
							// begin a new cycle
							isBounded = true;
							boundIndex = index;
							boundIndexStr = str;
							if(!isRangeCount){
								boundIndexMask = {};
							}
							if(joinType == 'innerjoin'){
								innerJoinCount = 1;
							}
						}else if(joinType == 'innerjoin'){
							if(reln == '='){
								innerJoinCount++;
							}
							streamProps[str].resultsIdx = 1 + streamProps[str].resultsIdx;	
						}else if(joinType == 'fulljoin' && reln == '='){
							streamProps[str].resultsIdx = 1 + streamProps[str].resultsIdx;	
						}
						// NB: the different range-functions would yield different objects, hence props
						// hence the merged-results has to be unified
						// one idea is to push only ord props
						// but callers may be interested in other fields e.g. for further joins
						// so the props have to be merged; clashes have to be resolved with a namespace
						// joint-fields should be represented by the joint-field names
						// others by the namespace - provided or generated
						if(!isRangeCount){
							var expression = comparison.getMaskExpression(streamConfigs[str].jointMap, jmask);
							// if boundIndexMask is empty add the joints
							if(utils.isObjectEmpty(boundIndexMask)){
								for(var k=0; k < joinFields.length; k++){
									var jmask = joinFields[k];
									var mmsf = getMaskedMangledStreamField(jmask, str);
									var mangleFld = mmsf.mangleFld;
									var streamFld = mmsf.streamFld;
									// when join expressions exist, joints have to be computed further
									// boundIndexMask would be set to that result instead
									// TODO expressions should yield new separate fields instead
									if(expression == null){
										boundIndexMask[jmask] = index[streamFld];
										// update ord with info about synthetic field
										comparison.addOrdMaskFromClone(ord, jmask, mangleFld);
									}/*else{ TODO expressions are not implemented
									}*/
								}
							}
							for(var fld in index){
								var jointImage = comparison.getMaskFieldRev(streamJointMaps[str], fld);
								// maintain jointmap labels iff there's no associated jointmapping
								var from = (joint || {}).from;
								var to = (joint || {}).to;
								if((from == fld || to == fld) && fld == comparison.getMaskField(streamJointMaps[str], fld)){
									jointImage = fld;
								}
								var fldMangle = streamJointMangles[str][fld] || fld;
								if(expression == null && joinFields.indexOf(jointImage) >= 0 ){
									// taken care of by adding jointFields
									continue;
								}else if(!(fldMangle in boundIndexMask)){
									boundIndexMask[fldMangle] = index[fld];
								}else{	// not expected
									var error = 'join.mergestream.2 unexpected condition';
									utils.logError(error, 'WARNING');
								}
							}
						}
					}else if(joinType == 'innerjoin' && reln == '<'){
						i++;			// repeat this stream until >= boundIndex
						cursorReln = reln;
						streamProps[str].resultsIdx = 1 + streamProps[str].resultsIdx;	
						continue;
					}else if(reln == null){
						error = 'joint.mergestreams.3: reln is null';
						return then(error);
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
