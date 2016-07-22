var redisDB = require('redis');

// NB: the term cluster should be understood as a master and accompanying slaves
// TODO develop cluster theme
// 	e.g. write goes into master instance, whereas read goes into cluster pool
var cluster = {};

	// CONFIGURATIONS
var	clusterid = '0000',
	instance = {
		'0000': {id: '0000', label: 'redis_6379', val: redisDB.createClient(), type: 'redis'},
	},
	default_instance = instance[clusterid];

cluster.getInstance = function(){
	return instance;
};

cluster.getDefaultInstance = function(){
	return default_instance;
};

cluster.getId = function(){
	return clusterid;
};

cluster.getInstanceType = function(cluster_instance){
	return cluster_instance.type;
};

cluster.getInstanceId = function(cluster_instance){
	return cluster_instance.id;
};

cluster.getInstanceLabel = function(cluster_instance){
	return cluster_instance.label;
};

module.exports = cluster;
