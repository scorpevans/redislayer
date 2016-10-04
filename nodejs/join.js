var async = require('async');
var utils = require('./utils');
var query = require('./query');
var datatype = require('./datatype');
var command = datatype.command;


var join = {};
	// CONFIGURATIONS
var	label_namespace = '_namespace',				// always recommended; in order to prevent field clashes in joins
	label_functions = '_functions',			 	// passed function(s) are passed here
	label_function_args = '_function_args',		 	// when functions are passed the args are held here
	label_attribute_idxs = '_attribute_idxs',	       	// when function are passed the attribute index location(s) are held here
	label_cursor_idxs = '_cursor_idxs';		     	// when function args are passed, possible cursor location(s) are held here

var comparatorLabel = datatype.getComparatorLabel();
var comparatorPropLabel = datatype.getComparatorPropLabel();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();
var defaultLimit = query.getDefaultBatchLimit();
var excludeCursorLabel = query.getExcludeCursorLabel();




/*
               var subscriptionRange = {};
                subscriptionRange[commons.label_functions] = users.getFolkSubscriptions;
                // NB: count still has to perform range since count can only be made after the joins
                // => callers should pass mode=range to the function_args
                subscriptionRange[commons.label_function_args] = ['range', folk_id, attribute, scroll_type, scroll_position];
                subscriptionRange[commons.label_attribute_idxs] = 2;
                subscriptionRange[commons.label_cursor_idxs] = 4;
                subscriptionRange[commons.label_namespace] = 'subp';
                subscriptionRange[commons.label_comparator] = {folk: {}};
                subscriptionRange[commons.label_comparator].folk[commons.label_comparator_prop] = 'subscription';
 */


// adhoc Ords for functions which have to participate in a join but lacking .keys prop in their resultset
join.createResultsetOrd = function(id, index_config){
	var config = datatype.createConfig(id, null, index_config);
	return datatype.getConfigFieldOrdering(config, null, null);
};

var joinFieldMask = [];
var unMaskedFields = {};
mergeOrds = function(ord, rangeIdx, myord, fields, config, namespace, joint_map, joints){
	var cf = myord.fieldconfig || {};				// save possibly previous fieldconfig for configs
	myord.fieldconfig = {};						// offload possibly previous fieldconfig
	var fieldMask = {};
	var jm = datatype.getJointMapDict(joint_map);
	for(var i=0; i < fields.length; i++){
		var fmask = fields[i];
		var mangledField = fmask;
		var fld = (cf[fmask] || {field: fmask}).field;		// original-field location in case of existing fieldconfig
		var existConf = (((ord || {}).fieldconfig || {})[fmask] || {}).config;
		ord = ord || myord;
		config = config || cf[fmask].config;
		if(namespace != null
			|| (existConf && datatype.getConfigId(existConf) != datatype.getConfigId(config))){
			mangledField = (namespace != null ? namespace+'.'+fmask : '_'+j+'_'+fmask);
			// update the global ord
			ord.fieldconfig[mangledField] = {config:config, field:fld};
			// also update references to fmask in joint_map
			// else insert new map to mangledField
			// TODO WARNING: joint_map is mutated here!!; this mutation is probably informative to the caller??
			var isFound = false;
			for(var fm in jm){
				// NB: expect a single reference; making multiple references doesn't make sense
				if(jm[fm][comparatorPropLabel] == fmask){
					jm[fm][comparatorPropLabel] = mangledField;
					isFound = true;
					break;
				}
			}
			// NB: this is only relevant for the joints
			if(!isFound && joints.indexOf(fmask) >= 0){
				jm[fmask] = {};
				jm[fmask][comparatorPropLabel] = mangledField;
			}
		}else{
			ord.fieldconfig[fmask] = {config:config, field:fld};
		}
		// record per i & prop, the new mangled fieldname; used for output fields
		fieldMask[fmask] = mangledField;
		unMaskedFields[mangledField] = fmask;
	}
	joinFieldMask[rangeIdx] = fieldMask;
}


// ranges => [{keys:[#], index:{#}, args:[#], attributes:{#}},...]
// type => fulljoin or innerjoin
// LIMITATION: this join is ONLY on equality and on unique ranges
//	complex joins can be handled in the input-functions or on the mergeRange output
// NB: merge joins assume inputs are sorted in the same order
// TODO encapsulate first 5 parameters into Range dict
join.mergeRanges = function(range_mode, range_order, ranges, joints, join_type, limit, then){
	var rangeProps = [];
	for(var i=0; i < ranges.length; i++){
		rangeProps[i] = {};
		rangeProps[i].argsCopy = (ranges[i][label_function_args] || []).concat([]);     		    	// make a copy
		var cursorIdx = ranges[i][label_cursor_idxs];
		rangeProps[i].cursorIdx = cursorIdx;
		rangeProps[i].cursor = utils.shallowCopy(rangeProps[i].argsCopy[cursorIdx]) || {};			// make a copy
		var attrIdx = ranges[i][label_attribute_idxs];
		rangeProps[i].attributeIdx = attrIdx;
		if((rangeProps[i].argsCopy[attrIdx]||{}).limit == null){
			rangeProps[i].argsCopy[attrIdx] = utils.shallowCopy(rangeProps[i].argsCopy[attrIdx]) || {};	// make a copy
			rangeProps[i].argsCopy[attrIdx].limit = defaultLimit;
		}
		rangeProps[i].argsCopy.push('/*callback-place-holder*/');
	}
	var getRangeIdxComparatorProp = (function(idx, prop){
		return datatype.getComparatorProp(ranges[idx][comparatorLabel], prop);
	});
	// NB: count still has to perform range since count can only be made after the joins
	// => callers should pass mode=range to the function_args
	var isRangeCount = false;
	var joinData = [];
	if(range_mode == 'count'){
		isRangeCount = true;
		joinData = 0;
	}
	var rangeIndexes = Object.keys(ranges);
	var boundIndex = null;
	var boundIndexIdx = -1;
	var boundIndexMask = {};
	var innerJoinCount = 0;
	var refreshIdx = null;						// point where joins are left off to fetch data
	var ord = null;							// the merges ord of the ranges
	limit = limit || Infinity;					// NB: not to be confused with limit in <attribute>
	async.whilst(
	function(){return (((isRangeCount && joinData < limit) || joinData.length < limit) && rangeIndexes.length > 0);},
	function(callback){
		async.each((refreshIdx != null ? [refreshIdx] : rangeIndexes), function(idx, cb){
			var next = function(err, result){
				result = (result || {});
				rangeProps[idx].results = result.data || [];
				rangeProps[idx].key = (result.keys || [])[0];
				rangeProps[idx].ord = result.ord;
				cb(err);
			};
			var range = ranges[idx];
			var len = rangeProps[idx].argsCopy.length;
			rangeProps[idx].argsCopy[len-1] = next;
			range[label_functions].apply(this, rangeProps[idx].argsCopy);
		}, function(err){
			if(!utils.logError(err, 'join.mergeRanges')){
				// compare next values across the partitions and pick the least
				// direction of iteration is due to possible splicing-out of exhausted ranges
				// iteration starts at refreshIdx i.e. where it was left off for fresh data
/* TODO WTF!!!
> null > 0
false
> null == 0
false
> null >= 0
true
*/
				refreshIdx = (refreshIdx != null && refreshIdx >= 0 ? refreshIdx : rangeIndexes.length-1);
				for(var i=refreshIdx; true; i--){
					if(i == -1){
						// reset index and process past cycle
						i = rangeIndexes.length-1;
						// check if any joins have been registered
						// for fulljoin, the boundIndex is returned if it remains the min/max throughout the cycle
						// for innerjoin, the boundIndex is returned if it makes all others in the cycle
						if(join_type == 'fulljoin' || (join_type == 'innerjoin' && innerJoinCount == ranges.length)){
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
								rangeIndexes = [];
								break;
							}
						}
						// reset registers
						if(join_type == 'fulljoin'){
							rangeProps[boundIndexIdx].resultsIdx = 1 + (rangeProps[boundIndexIdx].resultsIdx || 0);
						}else if(join_type == 'innerjoin'){
							innerJoinCount = 0;
						}
						boundIndex = null;
						boundIndexIdx = -1;
						boundIndexMask = {};
					}
					var idx = rangeProps[i].resultsIdx || 0;		// indexes are initially null
					if(idx >= (rangeProps[i].results || []).length){
						// if a partition runs out of results, IFF fulljoin, search continues as usual with others
						// but if partition runs out of current batch, search pauses for refresh
						// NB: => boundIndex must have been re-initialized by now for next search
						var resultsLen = (rangeProps[i].results || []).length-1;
						rangeProps[i].cursor = rangeProps[i].results[resultsLen] || {};		// last result
						rangeProps[i].cursor[excludeCursorLabel] = true;
						// update the cursor of the args
						var cursorIdx = rangeProps[i].cursorIdx;
						rangeProps[i].argsCopy[cursorIdx] = rangeProps[i].cursor;
						var attrIdx = rangeProps[i].attributeIdx;
						var rangeLimit = rangeProps[i].argsCopy[attrIdx].limit;
						if((rangeProps[i].results || []).length < rangeLimit){
							if(join_type == 'innerjoin'){
								// initiate termination
								rangeIndexes = [];
								break;
							}else if(join_type == 'fulljoin'){
								// NB: it is tempting to splice indexes out of rangeIndexes
								// instead the easier action is to reduce the list, and splice object from rangeProps
								// async.each would now see different rangeResults
								rangeIndexes.splice(-1);
								// <splice> other parameters to match the reduction in ranges-size
								rangeProps.splice(i, 1);
								boundIndexIdx = (boundIndexIdx == i ? -1  // shouldn't happen, right!
								                : (boundIndexIdx > i ? boundIndexIdx-1 : boundIndexIdx));
								if(rangeProps.length > 0){
									continue;
								}else{
									break;
								}
							}
						}else{						// refresh needed
							rangeProps[i].results = null;		// initiate refresh
							refreshIdx = i;				// continue from this point
							rangeProps[i].resultsIdx = 0;
							break;
						}
					}
					var index = rangeProps[i].results[idx];
					// NB: tricky bit here trying to get the keys from resultset
					// ensure provided functions return results with keys
					// NB: in case of mangling for the config prop of ord, be sure to update the respective joint_map
					// this is done only once
					if(ord == null){
						var ords = [];
						for(var j=0; j < rangeProps.length; j++){
							var myord = rangeProps[j].ord;
							var namespace = ranges[j][label_namespace];
							if(ranges[j][comparatorLabel] == null){
								ranges[j][comparatorLabel] = new datatype.jointMap();
							}
							var joint_map = ranges[j][comparatorLabel];
							var fields = Object.keys((myord || {}).fieldconfig || {});
							var config = null;
							if(myord == null){
								var key = rangeProps[j].key;
								config = datatype.getKeyConfig(key);
								try{
									myord = datatype.getConfigFieldOrdering(config, joint_map, joints);
								}catch(error){
									return then(error);
								}
								myord.fieldconfig = {};			// these are possibly only partial fields
								fields = datatype.getConfigIndexProp(config, 'fields');
							}
							ords.push(myord);
							// merge ords into a single ord
							// all ords are equivalent except for the fieldconfig props, which have to be merged
							// initialize ord to that of the first entry
							mergeOrds(ord, j, myord, fields, config, namespace, joint_map, joints);
							if(j == 0){
								ord = ords[0];
							}
						}
						// TODO check that ords of all ranges are in harmony
						
					}
					var indexData = {};
					var boundData = {};
					indexData[comparatorLabel] = ranges[i][comparatorLabel];
					boundData[comparatorLabel] = (ranges[boundIndexIdx]||{})[comparatorLabel];

					var reln = null;
					if(boundIndex != null){
						try{
							reln = datatype.getComparison(range_order, unMaskedFields, ord, index, boundIndex||{}, indexData, boundData);
						}catch(error){
							return then(error);
						}
					}
					if(boundIndex == null
						|| join_type == 'innerjoin'
						|| (join_type == 'fulljoin' && !(reln == '>'))){
						if(boundIndex == null
							|| (join_type == 'innerjoin' && reln == '>')
							|| (join_type == 'fulljoin' && reln == '<')){
							boundIndex = index;
							boundIndexIdx = i;
							if(!isRangeCount){
								boundIndexMask = {};
							}
						}
						if(reln == '=' || join_type == 'innerjoin'){	// always progressive
							rangeProps[i].resultsIdx = 1 + (rangeProps[i].resultsIdx || 0);	
						}
						if(join_type == 'innerjoin'){
							if(boundIndexIdx == i || reln == '>'){
								innerJoinCount = 1;
							}else if(reln == '='){
								innerJoinCount++;
							}
						}
						// NB: the different range-functions would yield different objects, hence props
						// hence the merged-results has to be unified somehow
						// one idea is to push only ord props
						// but callers may be interested in other fields e.g. for further joins
						// so the props have to be merged; clashes have to be resolved with a namespace
						if(!isRangeCount){
							for(var fld in index){
								var fieldMask = joinFieldMask[i][fld];
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
		if(!utils.logError(err, 'join.mergeRanges')){
			ret = {code:0, data:joinData, ord:ord};
		}
		then(err, ret);	
	}
	);
};


module.exports = join;
