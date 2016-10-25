var redisDB = require('redis');

clist = [
		{
			id: 1000,			// an integer identifier for a certain cluster-instance in the real world
			label: 'redis6379',		// a human-friendly identifier of a cluster-instance within redislayer
			type: 'redis',			// the type of database i.e. redis, sql, ...
			role:{				// different subsets of the cluster-instances
				master: {proxy: redisDB.createClient(port=6379)},	// hint: a better proxy here would be a redis-sentinel
				slaves: {proxy:null},
				//...
			}
		},
		{
			id: 2000,
			label: 'redis6380',
			type: 'redis',
			role:{
				master: {proxy: redisDB.createClient(port=6380)},
			}
		},
		//...
	];


module.exports = clist;
