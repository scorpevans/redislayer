var utils = {};

utils.shallowCopy = function(myDict){
	var myClone = {};
	for(var key in (myDict || {})){
		myClone[key] = myDict[key];
	}
	return myClone;
};


isObjectEmpty = function(obj) {
	for (var prop in obj){
		if(obj.hasOwnProperty(prop)){
			return false;
		}
	}
	return true;
};

utils.logError = function(err, message){
	if(err){
		console.error('%s, %s',message, err);
		return true;
	} else{
		return false;
	}
}

utils.logCodeError = function(err, result){
	if(err || !result || result.code != 0){
		return true;
	}
	return false;
}

utils.startsWith = function(str, prefix){
	return (str != null ? str.lastIndexOf(prefix, 0) === 0 : null);
}

utils.isInt = function(val){
	var digits = [0,1,2,3,4,5,6,7,8,9];
	for(var i=0; i < (val || '').length; i++){
		if(digits[val[i]] == null){
			return false;
		}
	}
	return (val?true:null);
}

// used to create interface for datatype structure
// just a way to hide dict behind function calls
// TODO consider reformulating the interface mechanism
/*  storage format
	{
		api: {...},
		keyconfig: {},
		keywrap: {key1: {api: {...},
				keyconfig: {...},
				keywrap: {...},
			},
			...
		},
	}
*/
utils.wrap = function(dict, store, access_code, curr_type_id, prev_type_id, prev_instance, force){
	var methods = dict[access_code] || [];				// {_all: [], key1: [], ...}
	var api = store.api;						// NB: may already hold existing APIs
	if(!force && !isObjectEmpty(api)){
		return api;
	}
	if(typeof dict === 'object' && !Array.isArray(dict)){
		for(var key in dict){
			if(key == access_code){
				continue;
			}
			// i.e. key: {}
			var isOption = (typeof dict[key] === 'object' && !Array.isArray(dict[key]));
			if(isOption){
				store.keyconfig[key] = {};
				var func = eval('(function(ac){return (ac == access_code ? store.keyconfig["'+key+'"] : null);})');
				api[key] = func;
				api[key].getId = eval('(function(){return "'+key+'";})');
				if(curr_type_id != null){
					api[key].getType = (function(){return curr_type_id;});
				}
				if(prev_type_id != null){
					var cmd = 'get'+prev_type_id[0].toUpperCase() + prev_type_id.slice(1);
					api[key][cmd] = (function (){return prev_instance;});
				}
				var keyMethods = (methods._all || []).concat(methods[key] || []);
				for(var i=0; i < keyMethods.length; i++){
					api[key][keyMethods[i]] = eval(keyMethods[i]);
				}
				store.keywrap[key] = {};
				for(var type in dict[key]){
					// i.e. key.type: [{}]
					var isClass = (Array.isArray(dict[key][type]) && dict[key][type].length == 1
							&& typeof (dict[key][type][0]) === 'object' && !Array.isArray(dict[key][type][0]));
					if(isClass){
						store.keywrap[key][type] = {keyconfig:{}, api:{}, keywrap:{}};
						func = eval('(function (){'
								+'return utils.wrap(dict["'+key+'"]["'+type+'"][0], store.keywrap["'+key+'"]["'+type+'"]'
									+', access_code, "'+type+'", curr_type_id, api["'+key+'"], false);})');
						var cmd = 'get'+type.charAt(0).toUpperCase() + type.slice(1);
						api[key][cmd] = func;
					}else{	// <leaf> node
						store.keyconfig[key][type] = dict[key][type];	// NB: connection broken in cases of non-references!
					}
				}
			}else{
				api[key] = dict[key];
			}
		}
		return api;
	}else{
		return dict;
	}
}


module.exports = utils;
