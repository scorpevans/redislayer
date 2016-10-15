var rl = require('./redislayer');
var zset = rl.getStruct().zset.getId();


// 1. create and load a tree to represent your keys and configurations
// 	This can also be done with createKey and createConfig calls; see redislayer.js
// this data-tree sample is taken from dtree.js
// NB: see dtree.js for explanations to the fields and other options
var dtree = {
	id:	'datatype',
	structs:[{
		id:	zset,
		configs: [{
			id:     'zkey_user',
			index:  { fields: ['gender', 'userid', 'firstname', 'lastname']
				, fieldprependskey: [null, null, true, true]		// firstname & lastname are now so-called field-branches
				, offsets: [null, 502, 300, 300]
				, offsetprependsuid: [false, true, true, true]		// userid=true is crucial; see offsetprependsuid in dtree.js
				, partitions: [true]
				, factors: [10000000000000, 1]			
				, types: ['text', 'integer', 'text', 'text']},
			keys:	[{
				id:	'key1',
				label:	'example:employee:detail'}]}]}],
};

rl.loadtree({dtree:dtree});		// load dtree into redislayer


// 2. start querying your data-layer via redislayer; see redislayer.js for API
// Note that there's no reference to where/how to fetch/save the data; see dtree.js for configuration
// 	this means that, in most cases, the storage config can be changed without changing your code
// 	and migration is just a function call


// setters

var key = rl.getKey().key1;
var cmd = key.getCommand().add;
var index = {gender: 1, userid: 12300, firstname: 'firstname1', lastname: 'lastname1'};
var attr = {nx: true};
var arg = {cmd:cmd, key:key, indexorrange:index, attribute:attr};
rl.singleIndexQuery(arg, function(err, result){
	console.log('[1]');
	if(err || result.code != 0){
		console.log('Oops! '+err);
	}else{
		console.log('Hurray! ... '+result.data);
	}
});

var idx1 = {gender: 0, userid: 23400, firstname: 'firstname2', lastname: 'lastname2'};
var idx2 = {gender: 1, userid: 12311, firstname: 'firstname3', lastname: 'lastname3'};
var indexList = [{index:idx1, attribute:attr}, {index:idx2, attribute:attr}];
var arg = {cmd:cmd, key:key, indexlist:indexList};
rl.indexListQuery(arg, function(err, result){
	console.log('[2]');
	if(err || result.code != 0){
		console.log('Oops! '+err);
	}else{
		console.log('Hurray! ... '+result.data);
	}
});

// getters

cmd = key.getCommand().get;
index = {gender: 9,			// irrelevant field i.e. not an element of keytext of UID; returned value may differ
	//lastname: null,		// only field-branches which explicitly exist in Object.keys() are searched/returned
	 userid: 12300,
	 firstname: 'firstname1'};
var arg = {cmd:cmd, key:key, indexorrange:index};
rl.singleIndexQuery(arg, function(err, result){
	console.log('[3]');
	if(err || result.code != 0){
		console.log('Oops! '+err);
	}else{
		console.log(result.data);
	}
});

	// search across field-branches lastname and firstname
index = { userid: 12300,
	 lastname: 'lastname1',
	 firstname: 'firstname1'};
var arg = {cmd:cmd, key:key, indexorrange:index};
rl.singleIndexQuery(arg, function(err, result){
	console.log('[4]');
	if(err || result.code != 0){
		console.log('Oops! '+err);
	}else{
		console.log(result.data);
	}
});

	// search across field-branches for non-existent entry
index = { userid: 12300,
	 lastname: 'lastname2',
	 firstname: 'firstname1'};
var arg = {cmd:cmd, key:key, indexorrange:index};
rl.singleIndexQuery(arg, function(err, result){
	console.log('[5]');
	if(err || result.code != 0){
		console.log('Oops! '+err);
	}else{
		console.log(result.data);
	}
});


// TODO
// rangers
cmd = key.getCommand().rangeasc;		// range in ascending order
// rl.singleIndexQuery()

// mergers
// rl.mergeStreams()

// migrants
// rl.migrate()

//cmd = key.getCommand().mget;
var arg = {cmd:cmd, key:key, indexlist:indexList};
//rl.indexListQuery(arg, function(err, result){
//	if(err || result.code != 0){
//		console.log('Oops!');
//	}else{
//		console.log(result.data);
//	}
//});


