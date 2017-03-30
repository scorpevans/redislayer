var redisDB = require('redis');

// NB: the term cluster-instance should be understood as a master and accompanying slaves
//	the redislayer cluster is composed of several cluster-instances

var cluster = {
	defaultinstance: null,	// every redislayer-instance should have a default real-world cluster-instance
};	

var access_code = 'cluster._do_not_access_fields_with_this';


var clusterList = {};

// the default-instance could be used for e.g. ID-generation
// or similar queries which don't take any Index, hence cannot be routed otherwise
// see clusterinstance in dtree.js 
cluster.getDefault = function getDefaultCluster(){
	return cluster.defaultinstance;
};

cluster.getDefaultId = function getDefaultClusterId(){
	var instance = cluster.getDefault();
	return (instance != null ? instance.getId() : null);
};

cluster.getDefaultLabel = function getDefaultClusterLabel(){
	var instance = cluster.getDefault();
	return (instance != null ? instance.getLabel() : null);
};

cluster.getList = function getClusterList(cluster_label){
	return (cluster_label == null ? clusterList : clusterList[cluster_label]);
};

cluster.setDefaultByLabel = function setDefaultClusterByLabel(cluster_label){
	var instance = cluster.getList(cluster_label);
	cluster.defaultinstance = instance;
	return instance;
};

cluster.remove = function removeCluster(cluster_label){
	var instance = cluster.getList(cluster_label);
	delete clusterList[cluster_label];
	return instance;
};


cluster.getInstanceType = function getClusterType(cluster_instance){
	return cluster_instance.getType();
};

cluster.getInstanceId = function getClusterId(cluster_instance){
	return cluster_instance.getId();
};

cluster.getInstanceLabel = function getClusterLabel(cluster_instance){
	return cluster_instance.getLabel();
};

cluster.getInstanceProxy = function getClusterProxy(cluster_instance){
	return cluster_instance.getProxy();
};

cluster.loadList = function loadClusterList(clist){
	for(var i=0; i < clist.length; i++){
		var inst = clist[i];
		var getId = eval('(function(){return '+inst.id+';})');
		var getType = eval('(function(){return "'+inst.type+'";})');
		var getLabel = eval('(function(){return "'+inst.label+'";})');
		var myInstance = {};
		myInstance.getId = getId;
		myInstance.getType = getType;
		myInstance.getLabel = getLabel;
		for(var r in inst.role){
			myInstance[r] = function(){};
			myInstance[r].getProxy = eval('(function(){return clist['+i+'].role["'+r+'"].proxy();})');
			myInstance[r].getId = getId;
			myInstance[r].getType = getType;
			myInstance[r].getLabel = getLabel;
		}
		if(myInstance != {}){
			clusterList[inst.label] = myInstance;
		}
	}
};


module.exports = cluster;
