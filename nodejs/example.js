
// NB: compliment this examples here with the documentation in redislayer.js

var rl = require('./redislayer');
var dtree = require('./dtree');
var clist = require('./clist');			// requires redis running on ports 6379 and 6380


// SETUP
//rl.loadClusterList({clist:clist});		// already done by dtree
rl.loadDatatypeTree({dtree:dtree});		// load datatype tree
rl.setDefaultClusterId(1000);			// set default cluster for this instance of redislayer


// QUERYING
// Note that there's no reference to where/how to fetch/save the data
// this means that, there's no need to refactor code when storage is changed to equivalent configs
// and migration is just a function call

// setters
/*
var key = rl.getKey().key1;
var cmd = key.getCommand().add;
var index = {gender: 1, userid: 12300, firstname: 'firstname1', lastname: 'lastname1'};
var attr = {nx: true};
var arg = {cmd:cmd, key:key, indexorrange:index, attribute:attr};
rl.singleIndexQuery(arg, function(err, result){
	console.log('[set index]');
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
	console.log('[set multiple indexes]');
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
*/

