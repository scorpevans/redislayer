
// USAGE: run this script for the first time to insert sample data, then change the value mycase variable to perform search
// WARNING: this example would create keys of the form "redislayer:example:*" in your redis database
//      of course you can easily delete these keys with the linux commands:
//      - redis-cli -p 6379 keys "redislayer:example:*" | xargs redis-cli -p 6379 del 
var mycase = 'insert';	// NB: change this value after first run!!!


var async = require('async');
var redisDB = require('redis');
var rl = require('./redislayer');


// redislayer configurations

// cluster info
var clist = [{
	id: 1000,
	label: 'redis6379',
	type: 'redis',
	role:{
		master: {proxy: function(){return redisDB.createClient(port=6379)}},
	}
}];

rl.loadClusterList({clist:clist});
rl.setDefaultClusterByLabel('redis6379');

// storage info
var hash = rl.getStruct().hash.getId();
var zset = rl.getStruct().zset.getId();

var dtree = {
	defaultgetter: {
		clusterinstance: function(arg){return rl.getDefaultCluster().master;},
	},
	structs: [{
		id: hash,
		configs: [{
			id: 'contactdetail',
			index: {
				fields: ['id', 'name', 'city', 'phone'],
				types: ['integer', 'text', 'text', 'integer'],
				offsets: [300],
				offsetprependsuid: [true],
			},
			keys: [{
				id: 'contact',
				label: 'redislayer:example:contact:detail',
			}]
		}]},{
		id: zset,
		configs: [{
			id: 'fieldindex',
			index: {
				fields: ['prefix', 'id'],
				types: ['text', 'integer'],		// (NB: phone will be ordered as a string!)
				offsetprependsuid: [true, true],
			},
			keys: [{
				id: 'name',
				label: 'redislayer:example:name',
				},{
				id: 'city',
				label: 'redislayer:example:city',
				},{
				id: 'phone',
				label: 'redislayer:example:phone'
				}
			]}
		]}
	], 
};

rl.loadDatatypeTree({dtree:dtree});


// store and index contact data

var generatePrefixes = function(word){
	prefixes = [];
	for(var i=1; i <= (word || []).length; i++){
		prefixes.push(word.slice(0,i).toLowerCase());
	}
	return prefixes;
};


var createContact = function(data_object, then){
	var key = rl.getKey().contact;
	var arg = {	cmd: key.getCommand().add,
			key: key,
			indexorrange: data_object,
	};
	// store contact details
	rl.singleIndexQuery(arg, function(err, result){
		if(err || result.code != 0){
			console.error('Oops! '+err);
		}
		// create index for fields
		var fields = ['name','city','phone'];
		async.each(Object.keys(data_object), function(fld, callback){
			if(fields.indexOf(fld) < 0){
				callback(null);
			}else{
				var indexList = [];
				var prefixes = generatePrefixes(data_object[fld]);
				for(var j=0; j < prefixes.length; j++){
					indexList.push({index:{prefix:prefixes[j], id:data_object.id}});
				}
				key = rl.getKey()[fld];
				arg = {	cmd: key.getCommand().add,
					key: key,
					indexlist: indexList
				};
				rl.indexListQuery(arg, callback);
			}
		}, then);
	});
};


if(mycase == 'insert'){
	// insert some data for testing
	var names = ['foOd', 'foobar', 'bar', 'bARtender', 'fOOnami', 'tom', 'dick', 'harry', 'john', 'paul'];
	var cities =  ['london', 'newyork', 'mars', 'berlin', 'moscow', 'paris', 'accra', 'rio', 'beijing', 'islamabad'];
	var phones =  ['111222', '222111', '112211', '1111111', '222222', '121212', '123123', '12345', '123333', '22211'];
	
	var contacts = [];
	for(var i=0; i < 2000; i++){
		var idx = Math.round(Math.random()*9);
		var id = i;
		var name = names[idx];
		idx = Math.round(Math.random()*9);
		var city = cities[idx];
		idx = Math.round(Math.random()*9);
		var phone = phones[idx];
		var index = {id:id, name:name, city:city, phone:phone};
		contacts.push(index);
	}

	async.each(contacts, function(index, callback){
		createContact(index, callback);
	}, function(err){
		console.log(err);
		console.log('DONE!');
	});
}else{	// search
	// rangeConfigs queries to fetch name, city, and phone
	var index = {prefix:'foo'};					// name search
	var nameRange = new rl.rangeConfig(index);
	nameRange.stopProp = 'prefix';

	index = {prefix:'rio'};						// city search
	var cityRange = new rl.rangeConfig(index);
	cityRange.stopProp = 'prefix';

	index = {prefix:'123'};						// phone search
	var phoneRange = new rl.rangeConfig(index);
	phoneRange.stopProp = 'prefix';
	
	// streamConfigs to be used in inner-join
	var singleIndexQuery = function(cmd, key, indexorrange, attribute, then){
		var arg = {	cmd: cmd,
				key: key,
				indexorrange: indexorrange,
				attribute:attribute,
		};
		rl.singleIndexQuery(arg, then);
	};
	var key = rl.getKey().name;
	var attr = {limit: 50};
	
	var nameStream = new rl.streamConfig();
	nameStream.func = singleIndexQuery;
	nameStream.args = [key.getCommand().rangeasc, key, nameRange, attr];
	nameStream.cursorIndex = 2;
	nameStream.attributeIndex = 3;

	key = rl.getKey().city;
	var cityStream = new rl.streamConfig();
	cityStream.func = singleIndexQuery;
	cityStream.args = [key.getCommand().rangeasc, key, cityRange, attr];
	cityStream.cursorIndex = 2;
	cityStream.attributeIndex = 3;

	key = rl.getKey().phone;
	var phoneStream = new rl.streamConfig();
	phoneStream.func = singleIndexQuery;
	phoneStream.args = [key.getCommand().rangeasc, key, phoneRange, attr];
	phoneStream.cursorIndex = 2;
	phoneStream.attributeIndex = 3;

	var joinConfig = new rl.joinConfig();
	joinConfig.setInnerjoin();
	joinConfig.setOrderAsc();
	joinConfig.setModeList();
	joinConfig.streamConfigs = [nameStream, cityStream, phoneStream];
	joinConfig.joint = new rl.joint('id', 'id');
	joinConfig.limit = 20;
	rl.mergeStreams({joinconfig:joinConfig}, function(err, result){
		if(err || result.code != 0){
			console.error(err);
		}else{
			var indexList = [];
			for(var i=0; i < (result || {data:[]}).data.length; i++){
				indexList.push({index: {id:result.data[i].id}});
			}
			var key = rl.getKey().contact;
			var arg = {	cmd: key.getCommand().mget,
					key: key,
					indexlist: indexList,
			};
			rl.indexListQuery(arg, function(err, result){
				if(err || result.code != 0){
					console.error('Oops! '+err);
				}else{
					console.log(result.data);
				}
				console.log('DONE!');
			});
			// in order to fetch next set of results,
			// construct a range with last element of results,
			// then set rangeConfig.excludeCursor=true
		}
	});
}




