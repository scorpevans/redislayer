var async = require('async');
var utils = require('./utils');
var query = require('./query');
var datatype = require('./datatype');
var command = datatype.command;


var join = {};
	// CONFIGURATIONS
var	label_functions = '_functions',			 	// passed function(s) are passed here
	label_function_args = '_function_args',		 	// when functions are passed the args are held here
	label_attribute_idxs = '_attribute_idxs',	       	// when function are passed the attribute index location(s) are held here
	label_cursor_idxs = '_cursor_idxs';		     	// when function args are passed, possible cursor location(s) are held here

var defaultLimit = query.getDefaultBatchLimit();
var comparatorLabel = query.getComparatorLabel();
var excludeCursorLabel = query.getExcludeCursorLabel();
var asc = command.getAscendingOrderLabel();
var desc = command.getDescendingOrderLabel();

// comparator => {keytext:[...], score:[...], uid:[...]}; see datatype.getKeyConfigPropOrdering
// ranges => [{keys:[#], index:{#}, args:[#], attributes:{#}},...]
// type => fulljoin or innerjoin
// LIMITATION: this join is ONLY on equality and on unique ranges
//	complex joins can be handled in the input-functions or on the mergeRange output
// NB: merge joins assume inputs are sorted in the same order
// TODO encapsulate first 5 parameters into Range dict
join.mergeRanges = function(range_mode, range_order, ranges, comparator, join_type, limit, then){
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
		return query.getComparatorProp(ranges[idx][comparatorLabel], prop);
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
	var cycleIdx = -1;
	var innerJoinCount = 0;
	var refreshIdx = null;						// point where joins are left off to fetch data
	limit = limit || Infinity;					// NB: not to be confused with limit in <attribute>
	async.whilst(
	function(){return (((isRangeCount && joinData < limit) || joinData.length < limit) && rangeIndexes.length > 0);},
	function(callback){
		async.each((refreshIdx != null ? [refreshIdx] : rangeIndexes), function(idx, cb){
			var next = function(err, result){
				rangeProps[idx].results = (result || []).data || [];
				rangeProps[idx].keys = (result || {}).keys;
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
						i = rangeIndexes.length-1;			// reset index
					}
					if(i == cycleIdx && cycleIdx != null){			// process previous cycle
						// check if any joins have been registered
						// for fulljoin, the boundIndex is returned if it remains the min/max throughout the cycle
						// for innerjoin, the boundIndex is returned if it makes all others in the cycle
						if(join_type == 'fulljoin' || (join_type == 'innerjoin' && innerJoinCount == ranges.length)){
							if(isRangeCount){
								// NB: when convenient counting is terminated if limit is reached
								// useful to prevent going after info which spreads across keys/servers
								joinData++;
							}else{
								joinData.push(boundIndex);
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
							rangeProps[cycleIdx].resultsIdx = 1 + (rangeProps[cycleIdx].resultsIdx || 0);
						}else if(join_type == 'innerjoin'){
							innerJoinCount = 0;
						}
						boundIndex = null;
						cycleIdx = -1;
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
								cycleIdx = (cycleIdx == i ? -1	// shouldn't happen, right!
										: (cycleIdx > i ? cycleIdx-1 : cycleIdx));
								continue;
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
					// sometimes <keys> prop is not available e.g. resultset from join.mergeRanges
					// 	but if there's a single prop, <keys> is not required
					var indexKey = (rangeProps[i].keys || [])[0];
					var boundKey = ((rangeProps[cycleIdx]||{}).keys || [])[0];
					var indexData = {key: indexKey};
					indexData[comparatorLabel] = ranges[i][comparatorLabel];
					var boundData = {key: boundKey};
					boundData[comparatorLabel] = (ranges[cycleIdx]||{})[comparatorLabel];
					var reln = (boundIndex == null ? null
							: query.getComparison(range_order, comparator, index, boundIndex||{}, indexData, boundData));
					if(boundIndex == null
						|| join_type == 'innerjoin'						// always progressive
						|| (join_type == 'fulljoin' && !(reln == '>'))){			// > comes later
						if(join_type == 'innerjoin' || reln == '='){
							rangeProps[i].resultsIdx = 1 + (rangeProps[i].resultsIdx || 0);	// always progressive
							if(join_type == 'innerjoin'){
								if(boundIndex == null || reln == '>'){
									cycleIdx = i;
									boundIndex = index;
									innerJoinCount = 1;
								}else if(reln == '='){
									innerJoinCount++;
								}
							}
						}else{	// i.e. fulljoin && reln == '<'
							cycleIdx = i;
							boundIndex = index;
						}
						// NB: the different range-functions would yield different objects, hence props
						// hence the merged-results has to be unified somehow
						// one idea is to push only comparator props
						// but this breaks when merge-results are recalled to their respective functions
						// so the props have to be merged
						// include comparator props as unifying props for convenience of caller
						for(var prop in index){
							if(boundIndex[prop] == null){
								boundIndex[prop] = index[prop];
							}
						}
						for(var j=0; j < (comparator.keytext||[]).length; j++){
							var prop = comparator.keytext[j];
							if(boundIndex[prop] == null){
								var comProp = getRangeIdxComparatorProp(i, prop);
								boundIndex[prop] = index[comProp];
							}
						}
						for(var j=0; j < (comparator.score||[]).length; j++){
							var prop = comparator.score[j];
							if(boundIndex[prop] == null){
								var comProp = getRangeIdxComparatorProp(i, prop);
								boundIndex[prop] = index[comProp];
							}
						}
						for(var j=0; j < (comparator.uid||[]).length; j++){
							var prop = comparator.uid[j];
							if(boundIndex[prop] == null){
								var comProp = getRangeIdxComparatorProp(i, prop);
								boundIndex[prop] = index[comProp];
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
			ret = {code:0, data:joinData};
		}
		then(err, ret);	
	}
	);
};


module.exports = join;
