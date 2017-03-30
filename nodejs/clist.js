
// the list of clusters

clist = [
		{
			id: 1000,			// an integer identifier for a certain cluster-instance in the real world
			label: 'redis6379',		// a human-friendly identifier of a cluster-instance within redislayer
			type: 'redis',			// the type of database i.e. redis, sql, ...
			role:{	// different subsets of the cluster-instances
				// proxy is a nullary function to take care of all gymnastics concerning connections
				// the connection returned by this function would be used in the common ways
				master: {proxy: function(){return getRedis('6379');}},
				slaves: {proxy:null},
				//...
			}
		},
		{
			id: 2000,
			label: 'redis6380',
			type: 'redis',
			role:{
				master: {proxy: function(){return getRedis('6380');}},
			}
		},
		//...
	];


// do some fancy stuff with proxies/connection-pools/etc
var redisDB = require('redis');
var redis = {};
function getRedis(port){
	if(redis[port] && redis[port].connected){
		return redis[port];
	}
	redis[port] = redisDB.createClient({port:port, max_attempts:1});	// hint: a better proxy here would be a redis-sentinel
	redis[port].on('error', function(){});
	redis[port].on('end', function(){redis[port]=null;});
	return redis[port];
};


module.exports = clist;
