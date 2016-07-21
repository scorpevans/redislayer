var cluster = require('./cluster');
var datatype = require('./datatype');
var query = require('./query');
var join = require('./join');
var migrate = require('./migrate');

var redislayer = {};

// CLUSTER

redislayer.cluster = {
	/*
	 * @return	{string}	the id of the cluster to which this instance is bound
	 */
	getId: cluster.getId,
};

// DATA

redislayer.datatype = {
	// see dtree.js for examples of the [datatype ->- struct ->- config ->- key] hierarchy
	// and functions associated to them
	/**
	 * get the different types of data structures available
	 */
	getStruct: datatype.getStruct,
	/**
	 * recommended approach to creating multiple configs and keys
         * @param	{object}	dtree	see dtree.js for sample
	 */
	loadtree: datatype.loadtree,
	/**
	 * @param	{string}	id
	 * @param	{function}	struct
	 * @param	{object}	index_config
	 * @return 	{function}	newly created config
	 */
	createConfig: datatype.createConfig,
	/**
	 * @param 	{string}	id
	 * @param 	{string}	label
	 * @param 	{function}	key_config
	 * @return 	{function}	newly created key
	 */
	createKey: datatype.createKey,
	/*
	 * @return	{object}	available configs
	 */
	getConfig: datatype.getConfig,
	/*
	 * @return	{object}	available keys	
	 */
	getKey: datatype.getKey,
	// configuration getters/setters; see datatype.js
	getKeySeparator: datatype.getKeySeparator,
	getDetailSeparator: datatype.getDetailSeparator,
	getCollisionBreaker: datatype.getCollisionBreaker,
	getIdIncrement: datatype.getIdIncrement,
	getRedisMaxScoreFactor: datatype.getRedisMaxScoreFactor,
	getIdTrailInfoOffset: datatype.getIdTrailInfoOffset,
	setIdTrailInfoOffset: datatype.setIdTrailInfoOffset,
};

// QUERY

/**
 *
 */
redislayer.singleIndexQuery = function(cmd, keys, index, args, attribute, then){
	query.singleIndexQuery(cmd, keys, index, args, attribute, then);
};

/**
 *
 */
redislayer.indexListQuery = function(cmd, keys, index_list, then){
	query.indexListQuery(cmd, keys, index_list, then);
};

// JOIN

/**
 *
 */
// TODO change function signature
redislayer.mergeRanges = function(command_mode, ranges, comparator, join_type, limit, then){
	join.mergeRanges(command_mode, ranges, comparator, join_type, limit, then);
};

// MIGRATE

/**
 *
 */
redislayer.migrate = function(key_origin, key_destination, index_start, index_end, batch_size, nap_duration){
	return migrate(key_origin, key_destination, index_start, index_end, batch_size, nap_duration);
};


module.exports = redislayer;
