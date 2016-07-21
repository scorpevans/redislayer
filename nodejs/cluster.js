var redisDB = require('redis');

// NB: the term cluster should be understood as a master and accompanying slaves
// TODO develop cluster theme
// 	e.g. write goes into master instance, whereas read goes into cluster pool
var cluster = {};

	// CONFIGURATIONS
var	clusterid = '0000',
	instance = {
		'0000': {label: 'redis_6379', val: redisDB.createClient(), type: 'redis'},
	},
	default_instance = instance[clusterid];

cluster.getDefaultInstance = function(){
	return default_instance;
};

cluster.getId = function(){
	return clusterid;
};

cluster.getClusterInstanceType = function(cluster_instance){
	return cluster_instance.type;
};

cluster.getClusterInstanceByLabel = function(cluster_label){
	return instance[cluster_label];
};

module.exports = cluster;
