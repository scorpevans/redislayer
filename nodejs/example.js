
// NB: compliment this examples here with the documentation in redislayer.js
// WARNING: this example would create keys of the form "redislayer:example:*" in your redis database
//	of course you can easily delete these keys with the linux commands:
//	- redis-cli -p 6379 keys "redislayer:*" | xargs redis-cli -p 6379 del
//	- redis-cli -p 6380 keys "redislayer:*" | xargs redis-cli -p 6380 del

var rl = require('./redislayer');
var dtree = require('./dtree');
var clist = require('./clist');			// requires redis running on ports 6379 and 6380
var async = require('async');


// SETUP: check dtree.js for info on configuration

//rl.loadClusterList({clist:clist});		// load list of clusters; already done in dtree.js
//rl.loadDatatypeTree({dtree:dtree});		// load datatype tree; already done in dtree.js
rl.setDefaultClusterId(1000);			// set default cluster for this instance of redislayer


// QUERYING
// Note that there's no reference to where/how to fetch/save the data
// this means that, there's no need to refactor code when storage is changed to equivalent configs
// and migration is just a function call


// create an unbounded (it would not be loaded into Redislayer) utility Config/Key to help stow IDs into a Redis set
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
var sequence = 3;			// (case number to start from) - 1
var isNotComplete = true;
async.whilst(
function(){return isNotComplete;},
function(callback){
sequence++;
switch(sequence){

// setters

case 1: // add some users
	
	// EXERCISE: observe how the user objects are stored in Redis exactly as spelled-out by the config in dtree.js
	//	- note that user id/details are made on redis6380
	//	- note how the KEYTEXT, UID and XID components are composed based on the config specification
	//	- note the keysuffixes and the keychain formed by suffixing the key
	var users = [];
	for(var i=1; i <= numberOfUsers; i++){
		users.push({
			firstnames: 'first name',
			lastnames: 'last name',
			comment: 'user_comment',
		});
	};

	var idKey = rl.getKey().skey_userid;
	var idIndex = {increment: 2000};		// this aligns with the index defined for skey_userid
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
	var groups = [];
	for(var i=1; i <= numberOfGroups; i++){
		groups.push({
			firstnames: 'group name',
			comment: 'group_comment',
		});
	};
	
	var idKey = rl.getKey().hkey_entityid;
	var idIndex = {entitytype:'groupid' ,increment: 2000};	// this aligns with the index defined for skey_userid
	var idArg = {
		cmd: idKey.getCommand().incrby,			// using the Key is the recommended route to an assigned commandset
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
	// user srandmember command to select a random number of groups and users and create membership between them
	// repeat till enough memberships are created
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

// getters

// EXERCISE: test if redislayer the config info to return the stored objects

case 4:
	console.log('#### query for an existing object ####');
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
	console.log('#### query for a non-existing object ####');
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
	console.log('#### query for existing and non-existing objects ####');
	var key = rl.getKey().hkey_user;
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

// rangers

// EXERCISE: note that ranging can potentially be across partitions, key-chains and clusters
//	- note how the range-properties are used to configure the range-index
//	- note how range partitions are specified
//	- note how to provide cursor and attribute arguments to range functions so they can be used in joins later

case 7:
	console.log('#### range across high-order partition-field [isadmin]; result is nonetheless ordered by memberid ####');
	rangePartitions = function rangePartitions(mycase, type, cursor, attribute, cb){
		var key = rl.getKey().zkey_membership;
		// redislayer will figure out variant of range to use (rangebyscore/rangebylex/etc)
		// or we can specify i.e. .bylex/.byscore/etc; but results will be wrong if this doesn't match the key-config
		var cmd = key.getCommand()[type];
		var arg = {	cmd: cmd,
				key: key,
				indexorrange: cursor,
				attribute:attribute,
				};
		rl.singleIndexQuery(arg, function(err, result){
			var logResult = mycase == 'case7' || mycase == 'case8';
			err = oopsCaseErrorResult(mycase, err, result, logResult);
			cb(err, result);
		});
	};
	var index = {	isadmin: [0,1],			// range across partitions
			entityid: 9991952000,
			memberid: 99918000};
	range7 = new rl.rangeConfig(index);
	// the startProp and stopProp basically says we want members of the entityid=9991952000
	range7.startProp = 'entityid';
	range7.stopProp = 'entityid';
	range7.boundValue = null;
	range7.excludeCursor = false;
	attr7 = {limit:30, withscores:true};		// the isadmin property is in the score
	rangePartitions('case7', 'rangeasc', range7, attr7, callback);
	break;
case 8:
	console.log('#### range on just a single partition ####');
	var index = {	isadmin: 0,			// range single partition
			entityid: 9991952000,
			memberid: 99918000};
	range8 = new rl.rangeConfig(index);
	// the startProp and stopProp basically says we want members of the entityids from 9991952000 to 9991990000
	range8.startProp = 'entityid';
	range8.stopProp = 'entityid';
	range8.boundValue = 9991990000;
	range8.excludeCursor = false;
	attr8 = {limit:30, withscores:true};
	rangePartitions('case8', 'rangeasc', range8, attr8, callback);
	break;

// mergers

case 9:
	console.log('#### inner-join the ranges from case 7 and 8 ####');
	var case7Stream = new rl.streamConfig();
	case7Stream.func = rangePartitions;
	case7Stream.args = ['case{9,10}', 'rangeasc', range7, attr7];	// NB: streams normally range (not count); else join doesn't make much sense
	case7Stream.namespace = 'case7';				// namespacing no necessary here since streams have equivalent fields
	case7Stream.jointMap = null;
	case7Stream.cursorIndex = 2;
	case7Stream.attributeIndex = 3;
	var case8Stream = new rl.streamConfig();
	case8Stream.func = rangePartitions;
	case8Stream.args = ['case{9,10}', 'rangeasc', range8, attr8];
	case8Stream.namespace = 'case8';
	case8Stream.jointMap = null;
	case8Stream.cursorIndex = 2;
	case8Stream.attributeIndex = 3;
	joinConfig = new rl.joinConfig();
	joinConfig.setInnerjoin();
	joinConfig.setOrderAsc();					// NB: this matches the range direction of the streams
	joinConfig.setModeList();
	joinConfig.streamConfigs = [case7Stream, case8Stream];
	joinConfig.joints = ['memberid', 'entityid'];			// order is irrelevant; redislayer knows better from key-configs
	joinConfig.limit = 10;						// NB: this is even useful in case of modeCount()
	rl.mergeStreams({joinconfig:joinConfig}, function(err, result){
		err = oopsCaseErrorResult('case9', err, result, true);
		callback(err);
	});
	break;
case 10:
	console.log('#### full-join the ranges from case 7 and 8 ####');
	joinConfig.setFulljoin();
	rl.mergeStreams({joinconfig:joinConfig}, function(err, result){
		err = oopsCaseErrorResult('case10', err, result, true);
		callback(err);
	});
	break;

// TODO migrants

default:
	isNotComplete = false;
	callback(null);		// sequence complete
}
},
function(err){
	if(!err){
		console.log("#### That's all folks! ####");
	}else{
		console.log(err);
		console.log("#### please submit a pull-request to fix the error ####");
	}
});	


