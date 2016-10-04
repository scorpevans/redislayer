var cluster = require('./cluster');
var datatype = require('./datatype');
var query = require('./query');
var join = require('./join');
var migrate = require('./migrate');



redislayer = {
	// CLUSTER
	
	/*
	 * @return	{string}	the id of the cluster to which this instance is bound
	 */
	getId: cluster.getId,

	// DATATYPE

	/**
	 * @return	
	 */
	jointMap: datatype.jointMap,

	// see dtree.js for examples of the [datatype ->- struct ->- config ->- key] hierarchy
	// and functions associated to them
	/**
	 * @return	{object}	get the different types of data structures available
	 */
	getStruct: datatype.getStruct,
	
	/**
	 * recommended approach to creating multiple configs and keys
	 * @param	{object}	dtree	see dtree.js for sample
	 */
	loadtree: function(arg){
		datatype.loadtree(arg.dtree);
	},
	
	/**
	 * @param	{string}	id
	 * @param	{function}	struct
	 * @param	{object}	index_config
	 * @return 	{function}	newly created config
	 */
	createConfig: function(arg){
		return datatype.createConfig(arg.id, arg.struct, arg.indexconfig);
	},
	
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
	// reasonable defaults have been set for all of these
	getKeySeparator: datatype.getKeySeparator,
	getDetailSeparator: datatype.getDetailSeparator,
	getCollisionBreaker: datatype.getCollisionBreaker,
	getIdIncrement: datatype.getIdIncrement,
	getRedisMaxScoreFactor: datatype.getRedisMaxScoreFactor,
	getIdPrefixInfoOffset: datatype.getIdPrefixInfoOffset,
	setIdPrefixInfoOffset: datatype.setIdPrefixInfoOffset,

	// QUERY
	
	/**
	 *
	 */
	singleIndexQuery: function(arg, then){
	        var cmd = arg.cmd
	        	, key = arg.key
	        	, index = arg.index
	        	, attribute = arg.attribute;
		query.singleIndexQuery(cmd, key, index, attribute, then);
	},
	
	/**
	 *
	 */
	indexListQuery: function(arg, then){
	        var cmd = arg.cmd
	        	, key = arg.key
	        	, indexList = arg.indexlist;
		query.indexListQuery(cmd, key, indexList, then);
	},
	
	// JOIN
	
	/**
	 *
	 */
	createResultsetOrd: function(arg){
		return join.createResultsetOrd(arg.id, arg.indexconfig);
	},
	
	/**
	 *
	 */
	mergeRanges: function(arg, then){
		var rangeMode = arg.rangemode
			, rangeOrder = arg.rangeorder
			, ranges = arg.ranges
			, comparator = arg.comparator
			, joinType = arg.jointype
			, limit = arg.limit;
		join.mergeRanges(rangeMode, rangeOrder, ranges, comparator, joinType, limit, then);
	},
	
	// MIGRATE
	
	/**
	 *
	 */
	migrate: function(arg, then){
		var keyOrigin = arg.keyorigin
	        	, keyDestination = arg.keydestination
	        	, indexStart = arg.indexstart
	        	, indexEnd = arg.indexend
	        	, batchSize = arg.batchsize
	        	, napDuration = arg.napduration;
		migrate(keyOrigin, keyDestination, indexStart, indexEnd, batchSize, napDuration, then);
	},

};


module.exports = redislayer;
