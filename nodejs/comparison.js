var utils = require('./utils');
var datatype = require('./datatype');
var command = datatype.command;

var comparison = {};
var access_code = 'comparison.do_not_user_this_to_access_fields';
var asc = command.getAscendingOrderLabel();


var unwrap = function unwrap(caller){
	return caller(access_code);
};


// joints are specified with just a <from> and <to> field-names
// because every other field on the LHS of <to> (i.e. high order) is included (except of course partition fields)
// fields between <from> and <to> are the focus of the join
//	other LHS fields are assumed to be ordered, and would not be included in Ord
//	this reduces administration of jointmaps on non-jointfields, especially when non-jointfields are later added to configs
comparison.joint = function comparisonJoint(from, to){
        var partitions = [];				// partitions which should be included in ordering
        var meta = {};                                  // internal stores
        var myj = function(ac){return (ac == access_code ? meta : null);};
        myj.from = from;
	myj.to = to;
        myj.getPartitions = function getPartitions(){
                return partitions.concat([]);
        };
        myj.setPartitions = function setPartitions(js){
                var uniq = {};
                js = js || [];
                for(var i=0; i < js.length; i++){
                        uniq[js[i]] = true;
                }
                partitions = Object.keys(uniq);
        };
        return myj;
};
comparison.getJointPartitions = function getComparisonJointPartition(joint){
	return (joint ? joint.getPartitions() : null);
};


var label_mask = '_label_mask';
var label_expression = '_label_expression';	// currently only equality is allowed
comparison.fieldMask = function comparisonFieldMask(){
	var fm = {field:{}, mask:{}};
	var myfm = function(key){return (key == access_code ? fm : null);};
	myfm.addMaskToField = function addMaskToField(mask, field){
		fm.mask[mask] = fm.mask[mask] || {};
		fm.mask[mask][label_mask] = field;
		fm.field[field] = fm.field[field] || {};
		fm.field[field][label_mask] = mask;
	};
	myfm.getMaskField = function getMaskField(mask){
		return ((fm.mask[mask] || {})[label_mask]  || mask);
	};
	myfm.getMaskFieldRev = function getMaskFieldRev(field){
		// if field has been overwritten by jointmap, it's fallback is null
		var fallback = (this.getMaskField(field) == field ? field : null);
		return ((fm.field[field] || {})[label_mask]  || fallback);
	};
	myfm.getMaskExpression = function getMaskExpression(mask){
		return (fm.mask[mask] || {})[label_expression];
	};
	myfm.view = function(){
		return JSON.stringify(fm);
	};
	return myfm;
};
getFieldMaskDict = function getComparisonFieldMaskDict(fieldmask){
	return (fieldmask != null ? unwrap(fieldmask).mask : {});
};
comparison.getFieldMasks = function getComparisonFieldMasks(fieldmask){
	return Object.keys(getFieldMaskDict(fieldmask));
};
comparison.getMaskField = function getComparisonMaskField(fieldmask, mask){
	return (fieldmask != null ? fieldmask.getMaskField(mask) : mask);
};
comparison.getMaskFieldRev = function getComparisonMaskField(fieldmask, field){
	return (fieldmask != null ? fieldmask.getMaskFieldRev(field) : field);
};
comparison.getMaskConfig = function getComparisonMaskField(fieldmask, mask){
	var fmd = getFieldMaskDict(fieldmask);
	return (((fmd || {})[mask]) || {})[label_mask];
};
comparison.getMaskExpression = function getComparisonMaskExpression(fieldmask, mask){
	return (fieldmask != null ? fieldmask.getMaskExpression(mask) : null);
};
comparison.addMaskToField = function addComparisonMaskToField(fieldmask, mask, field){
	if(fieldmask != null){
		fieldmask.addMaskToField(mask, field);
		return true;
	}
};
// unorthodox use; to store config info
comparison.addMaskToConfig = function addComparisonMaskToConfig(fieldmask, mask, config){
	if(fieldmask != null){
		var fm = getFieldMaskDict(fieldmask);
		fm[mask] = fm[mask] || {};
		fm[mask][label_mask] = config;
		return true;
	}
};
comparison.mergeFieldMasks = function mergeComparisonFieldMasks(outer_fm, inner_fm){
	inner_fm = inner_fm || new comparison.fieldMask();
	var outer = getFieldMaskDict(outer_fm);
	// overwrite inner_map; outer_map takes precedence 
	for(var mask in (outer || {})){
		comparison.addMaskToField(inner_fm, mask, comparison.getMaskField(outer_fm, mask));
	}
	return inner_fm;
};


comparison.fieldOrd = function comparisonFieldOrd(){
	var ord = {	keytext:[], score:[], uid:[],		// ordering of joint fieldmasks
			joints:[],				// all fields to be compared for ordering, ordered by fieldindex
			// it's difficult to decide how multiple key-suffixes are to be ordered
			// assume keysuffixing/splitting is only necessary for some storages or for partitioning
			// hence create an ordering which considers full values in Score and then UID
			// such ordering would transcend partitioning changes and other irrelevant details
			order:[],
			// TODO propmask can be deprecated by using just stripping of possible namespaces
			propmask: new comparison.fieldMask(),	// map from fieldmask to prop from which it was mangled
			fieldmask: new comparison.fieldMask(),	// map from fieldmask to config field
			configmask:new comparison.fieldMask()};	// map from fieldmask to config of field
	var myord = function(key){return (key == access_code ? ord : null);};
	myord.addMaskPropFieldConfig = function(mask, prop, field, config){
		if(!mask || !prop || !field ||  !config){
			throw new Error('invalid input for either mask, prop, field or config');
		}
		comparison.addMaskToField(ord.propmask, mask, prop);
		comparison.addMaskToField(ord.fieldmask, mask, field);
		comparison.addMaskToConfig(ord.configmask, mask, config);
	};
	myord.getMaskProp = function(mask){
		return comparison.getMaskField(ord.propmask, mask);
	};
	myord.getMaskField = function(mask){
		return comparison.getMaskField(ord.fieldmask, mask);
	};
	myord.getMaskConfig = function(mask){
		return comparison.getMaskConfig(ord.configmask, mask);
	};
	return myord;
};
getFieldOrdDict = function getFieldOrdDict(fieldord){
	return (fieldord != null ? unwrap(fieldord) : null);
};
comparison.getOrdProp = function getComparisonOrdProp(ord, prop){
	var fm = getFieldOrdDict(ord);
	return (fm || {})[prop];
};
comparison.getOrdFieldMasks = function getComparisonOrdFieldMasks(fieldord){
	return comparison.getFieldMasks(comparison.getOrdProp(fieldord, 'fieldmask'));
};
comparison.getOrdMaskProp = function getComparisonOrdMaskProp(fieldord, mask){
	return (fieldord != null ? fieldord.getMaskProp(mask) : mask);
};
comparison.getOrdMaskField = function getComparisonOrdMaskField(fieldord, mask){
	return (fieldord != null ? fieldord.getMaskField(mask) : mask);
};
comparison.getOrdMaskConfig = function getComparisonOrdMaskConfig(fieldord, mask){
	return (fieldord != null ? fieldord.getMaskConfig(mask) : null);
};
comparison.addOrdMaskPropFieldConfig = function addComparisonOrdMaskPropFieldConfig(ord, mask, prop, field, config){
	if(ord != null){
		ord.addMaskPropFieldConfig(mask, prop, field, config);
		return true;
	}
};
comparison.addOrdMaskFromClone = function addComparisonOrdMaskFromClone(ord, mask, clone){
	var maskProp = comparison.getOrdMaskProp(ord, clone);
	var maskField = comparison.getOrdMaskField(ord, clone);
	var maskConfig = comparison.getOrdMaskConfig(ord, clone);
	comparison.addOrdMaskPropFieldConfig(ord, mask, maskProp, maskField, maskConfig);
};
comparison.resetOrdMasks = function resetComparisonOrdMasks(ord){
	var fo = getFieldOrdDict(ord);
	if(fo != null){
		fo.propmask = new comparison.fieldMask();
		fo.fieldmask = new comparison.fieldMask();
		fo.configmask = new comparison.fieldMask();
		return true;
	}
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
comparison.getConfigFieldOrdering = function getComparisonConfigFieldOrdering(config, fieldmask, joint){
	var ord = new comparison.fieldOrd();
	var keytext = comparison.getOrdProp(ord, 'keytext');
	var score = comparison.getOrdProp(ord, 'score');
	var uid = comparison.getOrdProp(ord, 'uid');
	var joints = comparison.getOrdProp(ord, 'joints');
	var order = comparison.getOrdProp(ord, 'order');
	var fields = datatype.getConfigIndexProp(config, 'fields');
	var partitions = datatype.getConfigIndexProp(config, 'partitions');
	var jointPartitions = comparison.getJointPartitions(joint) || [];
	var from = (joint || {}).from;
	var to = (joint || {}).to;
	var fromField = comparison.getMaskField(fieldmask, from);
	var toField = comparison.getMaskField(fieldmask, to);
	var fromIdx = datatype.getConfigFieldIdx(config, fromField);
	if(fromIdx == null || fromIdx < 0){
		fromIdx = -Infinity;
	}
	var toIdx = datatype.getConfigFieldIdx(config, toField);
	if(toIdx == null || toIdx < 0){
		toIdx = Infinity;
	}
	// NB: iteration is done in field-index order since e.g. uid- and key- prepends are also done in this order
	for(var i=0; i < fields.length; i++){
		var fieldIdx = i;
		var field = fields[fieldIdx];
		// NB: all fields should be represented in the ord, for masking logistics
		comparison.addOrdMaskPropFieldConfig(ord, field, field, field, config);
		// the order focus is fromIdx to toIdx
		// partition fields are add to ordering only upon request
		if(fieldIdx < fromIdx || fieldIdx > toIdx || (partitions[fieldIdx] == true && jointPartitions.indexOf(field) < 0)){
			continue;
		}
		var mask = comparison.getMaskFieldRev(fieldmask, field);
		if(mask == null){
			throw new Error('the field='+field+' is cutoff by the jointmap');
		}
		// if the field-name is mentioned as joint.from or joint.to,
		//	and there's no jointmap for it, don't use a mask
		//	in this way, indiscriminate jointmap mask are not used as fieldnames
		// 	jointmap masks are only relevant when the joint makes reference to other aliases
		if((from == field || to == field) && field == comparison.getMaskField(fieldmask, field)){
			mask = field;
		}
		var offset = datatype.getConfigPropFieldIdxValue(config, 'offsets', fieldIdx);
		var cmp = [mask, offset, fieldIdx];
		// joints
		joints.push(mask);
		// keytext
	       	if(datatype.isConfigFieldKeySuffix(config, fieldIdx)){
			var offsetGroup = datatype.getConfigPropFieldIdxValue(config, 'offsetgroups', fieldIdx);
			if(offsetGroup == null || offsetGroup == fieldIdx){
				if(datatype.isConfigFieldStrictlyKeySuffix(config, fieldIdx)){
					cmp[1] = null;
				}
				keytext.push(cmp);
			}
		}
		if(!datatype.isConfigFieldStrictlyKeySuffix(config, fieldIdx)){
			// score
			if(datatype.isConfigFieldScoreAddend(config, fieldIdx)){
				score.push(cmp);
			}
			// uid
			if(datatype.isConfigFieldUIDPrepend(config, fieldIdx)){
				uid.push(cmp);
			}
		}
	}
	// sort ord.score in descending order of the Factors; put nulls/0's as lowest (i.e. not participating)
	score.sort(function(a,b){return (datatype.getConfigPropFieldIdxValue(config, 'factors', b[2]) || -Infinity)
						- datatype.getConfigPropFieldIdxValue(config, 'factors', a[2]);});
	// configure ordering
	for(var i=0; i < score.length; i++){
		order.push({mask:score[i][0], fieldidx:score[i][2]});
	}
	for(var i=0; i < uid.length; i++){
		if(order.indexOf(uid[i][2]) < 0){
			order.push({mask:uid[i][0], fieldidx:uid[i][2]});
		}
	}
	return ord;
};

comparison.getMaskFieldMangle = function getComparisonMangleMaskField(fieldmask, mask, stream_map){
	var maskField = comparison.getMaskField(fieldmask, mask);
	return ((stream_map || {})[maskField] || maskField);
};
var numeric = ['integer', 'float'];
comparison.getComparison = function getComparison(order, ord, index, ref, index_fieldmask, ref_fieldmask, index_mangle, ref_mangle){
	order = order || datatype.getAscendingOrderLabel();
	var fieldOrder = comparison.getOrdProp(ord, 'order');
	for(var i=0; i < fieldOrder.length; i++){
		var mask = fieldOrder[i].mask;
		var indexOrdMask = comparison.getMaskFieldMangle(index_fieldmask, mask, index_mangle);
		var indexConfig = comparison.getOrdMaskConfig(ord, indexOrdMask);
		var indexField = comparison.getOrdMaskField(ord, indexOrdMask);
		var indexProp = comparison.getOrdMaskProp(ord, indexOrdMask);
		// indexOrdMask must have an entry in the Ord, and it's field-reference must exist in the Index
		if(indexConfig == null || indexField == null || !(indexProp in index)){
			throw new Error('missing index-mask-field or index-mask-config (for ['+indexOrdMask+']) or index-field (for ['+indexProp+']).');
		}
		var refOrdMask = comparison.getMaskFieldMangle(ref_fieldmask, mask, ref_mangle);
		var refConfig = comparison.getOrdMaskConfig(ord, refOrdMask);
		var refField = comparison.getOrdMaskField(ord, refOrdMask);
		var refProp = comparison.getOrdMaskProp(ord, refOrdMask);
		if(refConfig == null || refField == null || !(refProp in ref)){
			throw new Error('missing ref-mask-field or ref-mask-config (for ['+refOrdMask+']) or ref-field (for ['+refProp+']).');
		}
		var indexVal = index[indexProp];
		var refVal = ref[refProp];
		var indexFieldIdx = datatype.getConfigFieldIdx(indexConfig, indexField);
		var refFieldIdx = datatype.getConfigFieldIdx(refConfig, refField);
		var indexFieldType = datatype.getConfigPropFieldIdxValue(indexConfig, 'types', indexFieldIdx);
		var refFieldType = datatype.getConfigPropFieldIdxValue(refConfig, 'types', refFieldIdx);
		if(numeric.indexOf(indexFieldType) >= 0 && numeric.indexOf(refFieldType) >= 0){
			indexVal = (indexVal != null && indexVal != '' ? parseFloat(indexVal, 10) : -Infinity);	// -Infinity => nulls first
			refVal = (refVal != null && refVal != '' ? parseFloat(refVal, 10) : -Infinity);
		}else{
			indexVal = (indexVal != null ? ''+indexVal : '');
			refVal = (refVal != null ? ''+refVal : '');
		}
		if(indexVal < refVal){
			return (order == asc ? '<' : '>');
		}else if(indexVal > refVal){
			return (order == asc ? '>' : '<');
		}else if(indexVal != refVal){
			utils.logError('comparison.getComparison, '+indexVal+', '+refVal, 'FATAL');
			return null;
		}
	}
	return '=';
};


module.exports = comparison;
