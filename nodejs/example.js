
// NB: compliment these examples with the documentation in redislayer.js
// WARNING: this example would create keys of the form "redislayer:example:*" in your redis database
//	of course you can easily delete these keys with the linux commands:
//	- redis-cli -p 6379 keys "redislayer:*" | xargs redis-cli -p 6379 del
//	- redis-cli -p 6380 keys "redislayer:*" | xargs redis-cli -p 6380 del

var rl = require('./redislayer');
var dtree = require('./dtree');
var clist = require('./clist');			// requires redis running on ports 6379 and 6380
var async = require('async');


// SETUP: check dtree.js for info on storage configuration used

//rl.loadClusterList({clist:clist});		// load list of clusters; already done in dtree.js
//rl.loadDatatypeTree({dtree:dtree});		// load datatype tree; already done in dtree.js
rl.setDefaultClusterLabel('redis6379');		// set default cluster for this instance of redislayer


// QUERYING
// Note that there's no reference to where/how to fetch/save the data
// this means that, there's no need to refactor code when storage is changed to equivalent configs
// and migration is just a function call


// helper functions and objects to insert sample data

// create an unbounded (it would NOT be loaded into Redislayer) utility Config/Key to help stow IDs into a Redis set
// this collection would help to easily create memberships 
var indexConfig = {	fields:['uid', 'xid'],
			offsetprependsuid:[true],
			types:['integer', 'integer']};
var myConfig = rl.createConfig({id: 'unbounded',
				struct: rl.getStruct().set,
				indexconfig: indexConfig,
				ontree: false});
var myUserSetKey = rl.createKey({	id: 'users',
					label: 'redislayer:example:userset',
					config: myConfig,
					ontree: false});
var myGroupSetKey = rl.createKey({	id: 'groupset',
					label: 'redislayer:example:groupset',
					config: myConfig,
					ontree: false});


// helper function to create entities i.e. users and groups
createEntities = function(entity_list, entity_type, id_arg, entity_arg, then){
	async.each(entity_list, function(entityIndex, callback){
		rl.singleIndexQuery(id_arg, function(err, result){
			if(err || result.code != 0){
				err = '1: '+err;
				callback(err);
			}else{
				var entityId = '999'+result.data;	// '999' is the 3-digit idPrefixInfo specified in dtree.js
				entityIndex.entityid = entityId;
				entityIndex.firstnames = entityIndex.firstnames+' '+entityId;
				entityIndex.comment = entityIndex.comment+'_'+entityId;
				if(entityIndex.lastnames != null){
					entityIndex.lastnames = entityIndex.lastnames+' '+entityId;
				}
				entity_arg.indexorrange = entityIndex;
				rl.singleIndexQuery(entity_arg, function(err, result){
					if(err || result.code != 0){
						err = '2: '+err;
						callback(err)
					}else{	// stow the ID into a redis-set; would be used for creating membership
						var key = (entity_type == 'users' ? myUserSetKey : myGroupSetKey);
						var arg = {	cmd: key.getCommand().add,
								key: key,
								indexorrange: {uid: entityId}};
						rl.singleIndexQuery(arg, function(err, result){
							if(err || result.code != 0){
								err = '3: '+err;
							}
							callback(err)
						});
					}
				});
			}
		});
	},function(err){
		if(err){
			err = 'Oops! createEntities '+err;
		}else{
			console.log('#### '+entity_type+' added ####');
		}
		then(err);
	});
};

// helper for printing errors and results
oopsCaseErrorResult = function(mycase, err, result, logResult){
	if(err || result.code != 0){
		err = 'Oops! '+mycase+': '+err;
	}else if(logResult){
		console.log(result.data);
	}
	return err;
};




var numberOfUsers = 1000;
var numberOfGroups = 1000;
var membershipCycles = 100;
var 	range7 = null,
	range8 = null,
	attr7 = null,
	attr8 = null,
	rangePartitions = null,
	joinConfig = null;

// aside: using this method with async.whilst and switch statement in order to sequence the next operations
var sequence = -1;
//comment out unwanted cases
var cases = [];
cases.push(1);
cases.push(2);
cases.push(3);
cases.push(4);
cases.push(5);
cases.push(6);
cases.push(7);
cases.push(8);
cases.push(9);
cases.push(10);
cases.push(11);

var isNotComplete = true;

async.whilst(
function(){return isNotComplete;},
function(callback){
sequence++;
switch(cases[sequence]){

// SETTERS

case 1: // add some users
	
	// EXERCISE: observe how the user objects are stored in Redis exactly as spelled-out by the config in dtree.js
	//	- note that user id/details are made on redis6380
	//	- note how the KEYTEXT, UID and XID components are composed based on the config specification
	//	- note the keysuffixes and the keychain formed by suffixing the key
	
	console.log('\n#### CASE 1: add users ####');
	var users = [];
	for(var i=1; i <= numberOfUsers; i++){
		users.push({
			firstnames: 'first name',
			lastnames: 'last name',
			comment: 'user_comment',
		});
	};

	var idKey = rl.getKey().skey_userid;
	var idIndex = {increment: 2000};		// the fields of Indexes aligns with the defined configuration
	var idArg = {
		cmd: idKey.getCommand().incrby,		// using the Key is the recommended route to an assigned commandset
		key: idKey,
		indexorrange: idIndex,
		attribute: null};
	var detailKey = rl.getKey().hkey_user;
	var detailArg = {
		cmd: detailKey.getCommand().add,
		key: detailKey};
	createEntities(users, 'users', idArg, detailArg, callback);
	break;
case 2: // add some groups

	// EXERCISE: observe how the group objects are stored in Redis exactly as spelled-out by the config in dtree.js
	//	- note that group id/details are made on redis6379; different cluster from the user id/details
	//	- note how the KEYTEXT, UID and XID components are composed based on the config specification
	//	- note the keysuffixes and the keychain formed by suffixing the key
	
	console.log('\n#### CASE 2: add groups ####');
	var groups = [];
	for(var i=1; i <= numberOfGroups; i++){
		groups.push({
			firstnames: 'group name',
			comment: 'group_comment',
		});
	};
	
	var idKey = rl.getKey().hkey_entityid;
	var idIndex = {entitytype:'groupid' ,increment: 2000};
	var idArg = {
		cmd: idKey.getCommand().incrby,
		key: idKey,
		indexorrange: idIndex,
		attribute: null};
	var detailKey = rl.getKey().hkey_group;
	var detailArg = {
		cmd: detailKey.getCommand().add,
		key: detailKey};
	createEntities(groups, 'groups', idArg, detailArg, callback);
	break;
case 3: // add some memberships

	// EXERCISE: observe how the membership objects are stored in Redis exactly as spelled-out by the config in dtree.js
	//	- note how the KEYTEXT, UID and XID components are composed based on the config specification
	//	- (NB: for UID of struct=zset, a padding is made for numeric values inorder to maintain the numeric sorting)
	//	- note the keysuffixes and the keychain formed by suffixing the key
	//	#### note that different keychains have been routed into different clusters 
	//	- note how the Redis-Score is built from the values of the [factors] prop of the config

	// user-ids and group-ids have been stowed into redis sets during creation
	// use srandmember command to select a random number of groups and users and create membership between them
	// repeat till enough memberships are created	
	console.log('\n#### CASE 3: add memberships ####');
	var cycles = Array(membershipCycles);
	async.each(cycles, function(cyc, cb){
		var arg = {	cmd: myUserSetKey.getCommand().randmember,
				key: myUserSetKey,
				indexorrange: {xid: 10}};
		rl.singleIndexQuery(arg, function(err, result){
			if(err || result.code != 0){
				err = '1: '+err;
				callback(err);
			}else{
				var members = result.data;
				arg.key = myGroupSetKey;
				arg.cmd = myGroupSetKey.getCommand().randmember;
				rl.singleIndexQuery(arg, function(err, result){
					if(err || result.code != 0){
						err = '2: '+err;
						callback(err);
					}else{
						var entities = result.data;
						var indexList = [];
						for(var j=0; j < entities.length; j++){
							for(var k=0; k < members.length; k++){
								indexList.push({index: {isadmin: Math.round(Math.random()),
											entityid: entities[j].uid,
											memberid: members[k].uid},
										attribute: null});
							}
						}
						var key = rl.getKey().zkey_membership;
						arg = {	cmd: key.getCommand().add,
							key: key,
							indexlist: indexList};
						rl.indexListQuery(arg, function(err, result){
							if(err || result.code != 0){
								err = '3: '+err;
							}
							cb(err);
						});
					}
				});
			}
		});
	},function(err){
		if(err){
			err = 'Oops! case3 '+err;
		}else{
			console.log('#### memberships added ####');
		}
		callback(err);
	});
	break;

// GETTERS

// EXERCISE: test if redislayer is able to use the config info to return the stored objects

case 4:
	console.log('\n#### CASE 4: query for an existing object ####');
	var key = rl.getKey().hkey_user;
	var arg = {	cmd: key.getCommand().get,
			key: key,
			indexorrange: {entityid:999188000, firstnames:null}};	// field-branches are searched only if their property exist
	rl.singleIndexQuery(arg, function(err, result){
		err = oopsCaseErrorResult('case4', err, result, true);
		callback(err);
	});
	break;
case 5:
	console.log('\n#### CASE 5: query for a non-existing object ####');
	var key = rl.getKey().hkey_user;
	var arg = {	cmd: key.getCommand().get,
			key: key,
			indexorrange: {entityid:555188000, firstnames:null, lastnames:null}};
	rl.singleIndexQuery(arg, function(err, result){
		err = oopsCaseErrorResult('case5', err, result, true);
		callback(err);
	});
	break;
case 6:
	console.log('\n#### CASE 6: query for existing and non-existing objects ####');
	var key = rl.getKey().hkey_user;
				// non-999 prefixes don't exist; non-000 suffixes don't exist
	var indexList = [	{index:{entityid: 999188000, firstnames:null, lastnames:null}},
				{index:{entityid: 999118000, firstnames:null, lastnames:null}},
				{index:{entityid: 99952000, firstnames:null, lastnames:null}},
				{index:{entityid: 99978099, firstnames:null, lastnames:null}},
				{index:{entityid: 99996000, firstnames:null, lastnames:null}},
				{index:{entityid: 99918000, firstnames:null, lastnames:null}},
				{index:{entityid: 999126999, firstnames:null, lastnames:null}},
				{index:{entityid: 99922000, firstnames:null, lastnames:null}},
				{index:{entityid: 77758000, firstnames:null, lastnames:null}},
				{index:{entityid: 99972000, firstnames:null, lastnames:null}}];
	var arg = {	cmd: key.getCommand().mget,
			key: key,
			indexlist: indexList};
	rl.indexListQuery(arg, function(err, result){
		err = oopsCaseErrorResult('case6', err, result, true);
		callback(err);
	});
	break;

// RANGERS

// EXERCISE: note that ranging can potentially be across partitions, key-chains and clusters
//	- note how the range-properties are used to configure the range-index
//	- note how range partitions are specified
//	- note how to provide cursor and attribute arguments to range functions so they can be used in joins later
//	- note the possible inclusions of the [keys] and [jointMap] fields to resultsets for later joins

case 7:
case 8:
case 9:
case 10:
case 11:
	rangePartitions = function rangePartitions(type, cursor, attribute, cb){
		var key = rl.getKey().zkey_membership;
		// redislayer will figure out the variant of range to use (rangebyscore/rangebylex/etc)
		// or we can specify with .bylex/ or .byscore etc; but results will be wrong if this doesn't match the key-config
		// 	e.g. the config of zkey_membership requires a special type of ranging done with lua-scripting!!
		var cmd = key.getCommand()[type];	// e.g. key.getCommand().rangeasc
		var arg = {	cmd: cmd,
				key: key,
				indexorrange: cursor,
				attribute:attribute,
				};
		rl.singleIndexQuery(arg, function(err, result){
			// we will use the results of this function for joins
			// joins require info on the sorting order of resultset
			// hence all resultsets used in joins must have [keys] field of resultset keys
			// see resultsetCallback in redislayer.js for info in case the resultset doesn't come from redislayer
			//result.keys = [key];		// already included with redislayer queries
			// let's add a jointmap to the resultset
			// this is the way to standardize fields for callers who use this function as a join-stream
			// the other way is that the callers themselves provide a jointmap when joining
			var jointMap = new rl.jointMap();
			jointMap.addMaskToField('user', 'memberid');
			result.jointMap = jointMap;
			cb(err, result);
		});
	};
	var index = {	isadmin: 0,			// range single partition
			entityid: 9991952000,
			memberid: 99918000};
	range7 = new rl.rangeConfig(index);
	// the startProp and stopProp basically says we want members of the entityid=9991952000
	range7.startProp = 'entityid';
	range7.stopProp = 'entityid';
	range7.boundValue = null;
	range7.excludeCursor = false;
	attr7 = {limit:10, withscores:true};		// withscores since the isadmin property is in the score
	if(cases[sequence] == 7){
		console.log('\n#### CASE 7: range on just a single partition ####');
		rangePartitions('rangeasc', range7, attr7, function(err, result){
			err = oopsCaseErrorResult('case7', err, result, true);
			callback(err);
		});
		break;
	}

case 8: 	
	var index = {	isadmin: 1,			// range single partition
			entityid: 9991952000,
			memberid: 99918000};
	range7.index = index;
	if(cases[sequence] == 8){
		console.log('\n#### CASE 8: range on just a single partition ####');
		rangePartitions('rangeasc', range7, attr7, function(err, result){
			err = oopsCaseErrorResult('case8', err, result, true);
			callback(err);
		});
		break;
	}

case 9:

	var index = {	isadmin: [0,1],			// merge multiple partitions
			entityid: 9991952000,
			memberid: 99918000};
	range9 = new rl.rangeConfig(index);
	// the startProp, stopProp and boundValue, imply we want members of the entityids from 9991952000 to 9991990000
	range9.startProp = 'entityid';
	range9.stopProp = 'entityid';
	range9.boundValue = 9991990000;
	range9.excludeCursor = false;
	attr9 = {limit:10, withscores:true};
	if(cases[sequence] == 9){
		console.log('\n#### CASE 9: range across high-order partition-field [isadmin];'
				+' result is nonetheless ordered by the non-partition fields only ####');
		rangePartitions('rangeasc', range9, attr9, function(err, result){
			err = oopsCaseErrorResult('case9', err, result, true);
			callback(err);
		});
		break;
	}

// MERGERS

// EXERCISE: note that joins can be made over joins over joins ...
//	- note the use of the jointMap; note it can be provided along with the stream's resultset or when joining
//	- note the use of namespacing
//	-     without namespaces conflicting field-names are mangled; joint-fields are always merged under the name of the joints
//	-     you don't want automatic mangling of field-names because you can't rely any names for further joins

case 10:

	var case8Stream = new rl.streamConfig();
	case8Stream.func = rangePartitions;
	case8Stream.args = ['rangeasc', range7, attr7];			// NB: streams normally range (not count); else join doesn't make much sense
	case8Stream.cursorIndex = 1;
	case8Stream.attributeIndex = 2;
	case8Stream.namespace = null;
	case8Stream.jointMap = null;					// aliasing not required since both join-streams have the same field-names
	var case9Stream = new rl.streamConfig();
	case9Stream.func = rangePartitions;
	case9Stream.args = ['rangeasc', range9, attr9];
	case9Stream.cursorIndex = 1;
	case9Stream.attributeIndex = 2;
	case9Stream.namespace = null;
	case9Stream.jointMap = null;
	joinConfig = new rl.joinConfig();
	joinConfig.setInnerjoin();
	joinConfig.setOrderAsc();					// NB: this must match the range direction of the streams
	joinConfig.setModeList();
	joinConfig.streamConfigs = [case8Stream, case9Stream];
	joinConfig.joint = new rl.joint(null, 'memberid');		// the from-to range that the join focuses on
	joinConfig.limit = 20;						// NB: this is useful even in case of modeCount()
	if(cases[sequence] == 10){
		console.log('\n#### CASE 10: inner-join the ranges from cases 8 and 9 ####');
		rl.mergeStreams({joinconfig:joinConfig}, function(err, result){
			err = oopsCaseErrorResult('case10', err, result, true);
			callback(err);
		});
		break;
	}

case 11:
	
	console.log('\n#### CASE 11: full-join the ranges from cases 8 and 9  ####');
	joinConfig.setFulljoin();
	// let's try namespacing
	joinConfig.streamConfigs[0].namespace = 'case8';
	joinConfig.streamConfigs[1].namespace = 'case9';
	// try [user] (instead of memberid) to test the jointmap from the resultset of the stream rangePartitions
	joinConfig.joint = new rl.joint('entityid', 'user');		// NB: not recommended to set @from parameter of joints; see docs
	// let's provide a redundant jointMap for one of the streams; just as a showcase of jointMap-creation
	//	this is not necessary because the resultsets of the streams provide one
	joinConfig.streamConfigs[0].jointMap = new rl.jointMap();
	joinConfig.streamConfigs[0].jointMap.addMaskToField('user', 'memberid');
	rl.mergeStreams({joinconfig:joinConfig}, function(err, result){
		err = oopsCaseErrorResult('case11', err, result, true);
		callback(err);
	});
	break;

// TODO MIGRANTS

default:
	isNotComplete = false;
	callback(null);		// sequence complete
}
},
function(err){
	if(!err){
		console.log("\n#### That's all folks! ####");
	}else{
		console.log(err);
		console.log("\n#### please submit a pull-request to fix the error ####");
	}
});	


