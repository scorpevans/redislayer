var rl = require('./redislayer');
var zset = rl.datatype.getStruct().zset.getId();


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
			index:  { fields: ['age', 'userid', 'username', 'fullname']
				, fieldprependskey: [null, null, true, true]		// username & fullname are now so-called field-branches
				, offsets: [null, 52, 30, 30]
				, offsetprependsuid: [false, true, true, true]		// userid=true is crucial; see offsetprependsuid in dtree.js
				, partitions: [true]
				, factors: [10000000000000, 1]			
				, types: ['integer', 'integer', 'text', 'text']},
			keys:	[{
				id:	'key1',
				label:	'example:employee:detail'}]}]}],
};

rl.datatype.loadtree(dtree);		// load dtree into redislayer


// 2. start querying your data-layer via redislayer; see redislayer.js for API
// Note that there's no reference to where/how to fetch/save the data
// this means the storage config can be changed without changing your code
// and migration is just a function call

// setters
var key = rl.datatype.getKey().key1;
var cmd = key.getCommand().add;
var index = {age: 12, userid: 12345, username: 'scorpevans', fullname: 'Tom Foo'};
var args = null;
var attr = {nx: true};
rl.singleIndexQuery(cmd, [key], index, args, attr, function(err, result){
	if(err || result.code != 0){
		console.log('Oops!');
	}else{
		console.log('Hurray!');
	}
});

var idx1 = {age: 13, userid: 23456, username: 'scorpevans2', fullname: 'Dick Bar'};
var idx2 = {age: 14, userid: 12346, username: 'scorpevans3', fullname: 'Harry Baz'};
var indexList = [{index:idx1, args:args, attribute:attr}, {index:idx2, args:args, attribute:attr}];
rl.indexListQuery(cmd, [key], indexList, function(err, result){
	if(err || result.code != 0){
		console.log('Oops!');
	}else{
		console.log('Hurray!');
	}
});

// getters
cmd = key.getCommand().get;
index = {//age: 12,				// not required since field was not configured for KEYTEXT or UID
	 //fullname: 'Tom Foo',			// only field-branches which explicitly exist in Object.keys() are searched
	 userid: 12345,
	 username: 'scorpevans'};
rl.singleIndexQuery(cmd, [key], index, null, null, function(err, result){
	if(err || result.code != 0){
		console.log('Oops!');
	}else{
		console.log(result.data);
	}
});


// TODO
// rangers
cmd = key.getCommand().rangeasc;		// range in ascending order
// rl.singleIndexQuery()

// mergers
// rl.mergeRanges()

// migrants
// rl.migrate()

//cmd = key.getCommand().mget;
//rl.indexListQuery(cmd, [key], indexList, function(err, result){
//	if(err || result.code != 0){
//		console.log('Oops!');
//	}else{
//		console.log(result.data);
//	}
//});


