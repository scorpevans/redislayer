var async = require('async');
var utils = require('./utils');
var query = require('./query');
var datatype = require('./datatype');
var command = datatype.command;


var join = {};

	// CONFIGURATIONS
var	access_code = 'join._do_not_access_fields_with_this';



var excludeCursorLabel = query.getExcludeCursorLabel();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();
var defaultLimit = query.getDefaultBatchLimit();

unwrap = function unwrap(caller){
        return caller(access_code);
}

join.streamConfig = function streamConfig(){
	this.func = null;					// passed function(s) are passed here
	this.args = null; 					// when functions are passed the args are held here
	this.namespace = null;					// required in order to prevent field clashes in joins, and to decipher cursors
	this.jointMap = null;					// if a stream returns different fields, a mapping to the join-fields is required
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
//	using array indexes, for example, is a bad idea!
join.getNamespaceCursor = function(namespace, cursor){
	if(cursor == null){
		return cursor;
	}else if(!namespace){
		return utils.shallowCopy(cursor);
	}
	var nspCursor = {};
	var joinAttributes = {'_start_prop': true, '_stop_prop':true};
	joinAttributes[excludeCursorLabel] = true;
	for(k in cursor){
		if(joinAttributes[k] == true){
			nspCursor[k] = cursor[k];
		}else if(utils.startsWith(k, namespace+'.')){
			var kk = k.slice(namespace.length+1);
			nspCursor[kk] = cursor[k];
		}
	}
	return nspCursor;
};

// adhoc Ords for functions which have to participate in a join but lacking .keys prop in their resultset
join.createResultsetOrd = function createResultsetOrd(id, index_config){
	var config = datatype.createConfig(id, null, index_config);
	return datatype.getConfigFieldOrdering(config, null, null);
};

var joinFieldMask = [];
var unMaskedFields = {};
mergeOrds = function mergeOrds(ord, rangeIdx, myord, fields, config, namespace, joint_map, joints){
	var cf = datatype.getJointOrdProp(myord, 'mask');		// save possibly previous fieldconfig for configs
	datatype.resetJointOrdMask(myord);				// offload possibly previous fieldconfig
	var fieldMask = {};
	var jm = datatype.getJointMapDict(joint_map);
	ord = ord || myord;
	for(var i=0; i < fields.length; i++){
		var fmask = fields[i];
		var mangledField = fmask;
		var existConf = datatype.getJointOrdMaskPropConfig(ord, fmask);
		var fld = (cf[fmask] || {field: fmask}).field;		// original-field location in case of existing fieldconfig
		config = config || cf[fmask].config;
		if(namespace != null
			|| (existConf && datatype.getConfigId(existConf) != datatype.getConfigId(config))){
			mangledField = (namespace != null ? namespace+'.'+fmask : '_'+j+'_'+fmask);
			// update the global ord
			datatype.addJointOrdMask(ord, mangledField, fld, config);
			// also update references to fmask in joint_map
			// else insert new map to mangledField
			// TODO WARNING: joint_map is mutated here!!; this mutation is probably informative to the caller??
			var isFound = false;
			for(var fm in jm){
				// NB: expect a single reference; making multiple references doesn't make sense
				if(datatype.getJointMapMask(joint_map, fm) == fmask){
					datatype.addJointMapMask(joint_map, fm, mangledField);
					isFound = true;
					break;
				}
			}
			// NB: this is only relevant for the joints
			if(!isFound && joints.indexOf(fmask) >= 0){
				datatype.addJointMapMask(joint_map, fmask, mangledField);
			}
		}else{
			datatype.addJointOrdMask(ord, fmask, fld, config);
		}
		// record per i & prop, the new mangled fieldname; used for output fields
		fieldMask[fmask] = mangledField;
		unMaskedFields[mangledField] = fmask;
	}
	joinFieldMask[rangeIdx] = fieldMask;
}


// streamConfigs => [{keys:[#], index:{#}, args:[#], attributes:{#}},...]
// type => fulljoin or innerjoin
// LIMITATION: this join is ONLY on equality and on unique ranges
//	complex joins can be handled in the input-functions or on the mergeRange output
// NB: merge joins assume inputs are sorted in the same order
join.mergeStreams = function mergeStreams(join_config, then){
	var joinConfigDict = unwrap(join_config);
	var joinType = joinConfigDict.type,
		joinMode = joinConfigDict.mode,
		streamOrder = joinConfigDict.order,
		streamConfigs = join_config.streamConfigs,
		joints = join_config.joints,
		limit = join_config.limit;
	var streamProps = [];
	for(var i=0; i < streamConfigs.length; i++){
		streamProps[i] = {};
		if(!streamConfigs[i].namespace){
			var func = streamConfigs[i].func.name || '';
			var err = 'no Namespace has been provided for '+func+'(stream['+i+'])';
			return then(err);
		}
		streamProps[i].argsCopy = (streamConfigs[i].args || []).concat([]);    		 		    	// make a copy
		var cursorIdx = streamConfigs[i].cursorIndex;
		streamProps[i].cursorIdx = cursorIdx;
		var cursor = join.getNamespaceCursor(streamConfigs[i].namespace, streamProps[i].argsCopy[cursorIdx]);
		streamProps[i].argsCopy[cursorIdx] = cursor;
		var attrIdx = streamConfigs[i].attributeIndex;
		streamProps[i].attributeIdx = attrIdx;
		if((streamProps[i].argsCopy[attrIdx]||{}).limit == null){
			streamProps[i].argsCopy[attrIdx] = utils.shallowCopy(streamProps[i].argsCopy[attrIdx]) || {};	// make a copy
			streamProps[i].argsCopy[attrIdx].limit = 1; //TODO defaultLimit;
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
	var refreshIdx = null;						// point where joins are left off to fetch data
	var refreshIndex = null;
	var ord = null;							// the merges ord of the rangeconfigs
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
				streamProps[idx].results = result.data || [];
				streamProps[idx].key = (result.keys || [])[0];
				streamProps[idx].ord = result.ord;
				cb(err);
			};
			var len = streamProps[idx].argsCopy.length;
			streamProps[idx].argsCopy[len-1] = next;
			streamConfigs[idx].func.apply(this, streamProps[idx].argsCopy);
		}, function(err){
			if(!utils.logError(err, 'join.mergeStreams')){
				// if the re-fetch doesn't progress the cursor we're not going to terminate
				if(refreshIdx != null){
					var str = streamIndexes[refreshIdx];
					var res = streamProps[str].results || [];
					var lastIndex = res[res.length-1];
					if(lastIndex){
						var jm = streamConfigs[str].jointMap;
						var reln = null;
						try{
							reln = datatype.getComparison(streamOrder, unMaskedFields, ord, lastIndex, refreshIndex, jm, jm);
						}catch(error){
							return then(error);
						}
						if(reln != '>'){
							var func = streamConfigs[str].func.name || '';
							var err = 'loop detected; the cursor for '+func+'(stream['+str+']) does not move further';
							return then(err);
						}
					}
				}
				// compare next values across the partitions and pick the least
				// direction of iteration is due to possible splicing-out of exhausted streamConfigs
				// iteration starts at refreshIdx i.e. where it was left off for fresh data
/* TODO WTF!!!
> null > 0
false
> null == 0
false
> null >= 0
true
*/
				refreshIdx = (refreshIdx != null && refreshIdx >= 0 ? refreshIdx : streamIndexes.length-1);
				for(var i=refreshIdx; true; i--){
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
						// if a partition runs out of results
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
							refreshIdx = i;							// continue from this iteration
							refreshIndex = streamProps[str].results[idx-1];			// refresh should proceed beyond this
							if(refreshIndex){
								refreshIndex[excludeCursorLabel] = true;
							}
							var cursorIdx = streamProps[str].cursorIdx;
							streamProps[str].argsCopy[cursorIdx] = refreshIndex;
							streamProps[str].results = null;				// initiate refresh
							streamProps[str].resultsIdx = 0;
							break;
						}
					}
					var index = streamProps[str].results[idx];
					// set up a new joint-Ord, if not done already
					// requires that provided functions return results with keys or Ords
					// NB: in case of mangling for the config prop of ord, be sure to update the respective joint_map
					if(ord == null){
						var ords = [];
						for(var j=0; j < streamProps.length; j++){
							var myord = streamProps[j].ord;
							var namespace = streamConfigs[j].namespace;
							if(streamConfigs[j].jointMap == null){
								streamConfigs[j].jointMap = new datatype.jointMap();
							}
							var joint_map = streamConfigs[j].jointMap;
							var fields = Object.keys(datatype.getJointOrdProp(myord, 'mask') || {});
							var config = null;
							if(myord == null){
								var key = streamProps[j].key;
								config = datatype.getKeyConfig(key);
								try{
									myord = datatype.getConfigFieldOrdering(config, joint_map, joints);
								}catch(error){
									return then(error);
								}
								datatype.resetJointOrdMask(myord);			// these are possibly only partial fields
								fields = datatype.getConfigIndexProp(config, 'fields');
							}
							ords.push(myord);
							// merge ords into a single ord
							// all ords are equivalent except for the fieldconfig props, which have to be merged
							// initialize ord to that of the first entry
							mergeOrds(ord, j, myord, fields, config, namespace, joint_map, joints);
							if(j == 0){
								ord = ords[0];
							}else{	// check that ords of all streamConfigs are in harmony
								var prevOrd = ords[j-1];
								var k1 = datatype.getJointOrdProp(myord, 'keytext');
								var s1 = datatype.getJointOrdProp(myord, 'score');
								var u1 = datatype.getJointOrdProp(myord, 'uid');
								var k2 = datatype.getJointOrdProp(prevOrd, 'keytext');
								var s2 = datatype.getJointOrdProp(prevOrd, 'score');
								var u2 = datatype.getJointOrdProp(prevOrd, 'uid');
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
									var err = 'join.mergeStreams: the streams '+(j-1)+' and '+j+' do not have the same ordering';
									return then(err);
								}
							}
						}
					}
					var indexData = {};
					var boundData = {};
					indexJM = streamConfigs[str].jointMap;
					boundJM = (streamConfigs[boundIndexIdx]||{}).jointMap;
					var reln = null;
					if(boundIndex != null){
						try{
							reln = datatype.getComparison(streamOrder, unMaskedFields, ord, index, boundIndex, indexJM, boundJM);
						}catch(error){
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
						if(reln == '=' || joinType == 'innerjoin'){	// always progressive
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
							for(var fld in index){
								var fieldMask = joinFieldMask[str][fld];
								boundIndexMask[fieldMask] = index[fld];
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
