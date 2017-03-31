
// NB: compliment the documentation here with the demo in example.js

var cluster = require('./cluster');
var datatype = require('./datatype');
var query = require('./query');
var comparison = require('./comparison');
var join = require('./join');
var migrate = require('./migrate');
var command = datatype.command;


redislayer = {


/* CLUSTER
Database-cluster connections are loaded into redislayer so that they can be used for storage configurations.
One of these clusters should be designated the default-cluster of this redislayer instance;
ID generations, and other such primitive queries, can be made on the default-cluster

See clist.js for the input format of clusters.
Redislayer returns cluster objects with fields corresponding to the Roles the cluster was configured with;
	the cluster should be instantiated further by selecting one of these Roles
	each cluster-instance has methods to return properties it was configured with
*/
	
	/*
	 * @param	{string}	the label of the cluster to be designated as the default-cluster
	 * @return	{object}	the default cluster
	 */
	setDefaultClusterByLabel: cluster.setDefaultByLabel,

	/*
	 * @return	{string}	the label of the cluster designated as the default-cluster
	 */
	getDefaultClusterLabel: cluster.getDefaultLabel,

	/*
	 * @return	{number}	the ID of the cluster designated as the default-cluster
	 */
	getDefaultClusterId: cluster.getDefaultId,

	/**
	 * @return	{object}	the default-cluster along with the Roles with which it was configured
	 *				NB: instantiate the cluster by selecting a Role of the returned object
	 */
	getDefaultCluster: cluster.getDefault,

	/**
	 * @param	{string}	the label of the cluster to fetch; NULL value returns all clusters
	 * @return	{object}	the cluster along with the Roles with which it was configured
	 *				NB: instantiate the cluster by selecting a Role of the returned object
	 */
	getCluster: cluster.getList,


	/**
	 * @param	{string}	the label of the cluster to remove from redislayer
	 * @return	{object}	the removed cluster
	 */
	removeCluster: cluster.remove,

	/**
	 * @param	{object}	arg - a dict
	 * @param	{object}	arg.clist - the list of clusters to load into redislayer; see clist.js for format
	 */
	loadClusterList: function loadClusterList(arg){
		cluster.loadList(arg.clist);
	},


/* DATATYPE
The core of Redislayer is the configuration of how data objects should be stored/retrieved, and on/from which cluster.
See dtree.js for a sample configuration illustrating the [datatype ->- struct ->- config ->- key] hierarchy
At the top of the Configuration tree is a Struct/ure (which specifies which storage structure should be used),
	then comes a number of child Configs (which describe the expected data object and how it should be decomposed for storage),
	within each config comes a number of Keys (which specify the storage namespace to be used).
Key/Config/Struct objects have methods to set or get properties they were configured with.
	NB: obviously, inner property settings take precedence over ones set higher up the hierarchy
Key objects also have a command-set depending on the Struct under which they are configured.
*/

	// reasonable defaults have been set for the following configurations
	/**
	 * @param	{string}	the string used to separate key-parts e.g. ':' as in user:name:first
	 */
	setKeySeparator: datatype.setKeySeparator,
	/** redislayer may need to make it's own probing insertions/deletions; it must be ensured data is not overwritten
	 * @param	{string}	a messy string which is guaranteed not to be found in any stored data
	 */
	setCollisionBreaker: datatype.setCollisionBreaker,

	/**
	 * @return	{string}	see setKeySeparator
	 */
	getKeySeparator: datatype.getKeySeparator,
	/**
	 * @return	{string}	see setCollisionBreaker
	 */
	getCollisionBreaker: datatype.getCollisionBreaker,
	/**
	 * @return	{string}	see getDetailSeparator
	 */
	getDetailSeparator: datatype.getDetailSeparator,

	/**
	 * @return	{number}	the maximum 
	 */
	getRedisMaxScoreFactor: datatype.getRedisMaxScoreFactor,

	/**
	 * @return	{string}	the string constant used to represent ascending-order
	 */
	getAscendingOrderLabel: command.getAscendingOrderLabel,

	/**
	 * @return	{string}	the string constant used to represent descending-order
	 */
	getDescendingOrderLabel: command.getDescendingOrderLabel,

	/**
	 * recommended approach to creating configs, keys and getters
	 * @param	{object}	arg - a dict
	 * @param	{object}	arg.dtree - see dtree.js for sample
	 */
	loadDatatypeTree: function loadDatatypeTree(arg){
		datatype.loadTree(arg.dtree);
	},

	/**
	 * @param	{object}	arg - a dict
	 * @param	{string}	arg.id - unique identifier across all Configs
	 * @param	{function}	arg.struct - struct; select one from getStruct()
	 * @param	{object}	arg.indexconfig - index_config; see dtree.js for sample
	 * @param	{boolean}	arg.ontree - specify if the config should be loaded into Redislayer
	 * @return 	{function}	newly created config
	 * @throws	an error describing a malformed config
	 */
	createConfig: function createConfig(arg){
		return datatype.createConfig(arg.id, arg.struct, arg.indexconfig, arg.ontree);
	},
	
	/**
	 * @param 	{object}	arg - a dict
	 * @param 	{string}	arg.id - unique identifier across all Keys
	 * @param 	{string}	arg.label - a label which would be used as the table/key name
	 * @param 	{function}	arg.config - select one from getConfig() or createConfig()
	 * @param	{boolean}	arg.ontree - specify if the key should be loaded into Redislayer
	 * @return 	{function}	newly created key
	 */
	createKey: function createKey(arg){
		return datatype.createKey(arg.id, arg.label, arg.config, arg.ontree)
	},

	// the following section is API offer getters and setters to dtree
	// they are best understood by first taking a look at dtree.js
	
	/**
	 * @return	{object}	get the different types of data structures available
	 */
	getStruct: datatype.getStruct,
	
	/**
	 * @return	{object}	available configs
	 */
	getConfig: datatype.getConfig,
	
	/**
	 * @return	{object}	available keys
	 */
	getKey: datatype.getKey,

	/**
	 * @return	{function}	see defaultgetter dict in dtree.js
	 */
	getDefaultClusterInstanceGetter: function(){return datatype.getClusterInstanceGetter();},

	/**
	 * @return	{function}	see defaultgetter dict in dtree.js
	 */
	getDefaultFieldMinValueGetter: function(){return datatype.getFieldMinValueGetter();},
	/**
	 * @return	{function}	see defaultgetter dict in dtree.js
	 */
	getDefaultFieldMaxValueGetter: function(){return datatype.getFieldMaxValueGetter();},
	/**
	 * @return	{function}	see defaultgetter dict in dtree.js
	 */
	getDefaultFieldNextKeySuffixGetter: function(){return datatype.getFieldNextChainGetter();},

	/**
	 * @param	{function}	see getDefaultClusterInstanceGetter
	 */
	setDefaultClusterInstanceGetter: function(func){datatype.setClusterInstanceGetter(func);},

	/**
	 * @param	{function}	see getDefaultFieldNextKeySuffixGetter
	 */
	setDefaultFieldMinValueGetter: function(func){datatype.setFieldMinValueGetter(func);},
	/**
	 * @param	{function}	see getDefaultFieldNextKeySuffixGetter
	 */
	setDefaultFieldMaxValueGetter: function(func){datatype.setFieldMaxValueGetter(func);},
	/**
	 * @param	{function}	see getDefaultFieldNextKeySuffixGetter
	 */
	setDefaultFieldNextKeySuffixGetter: function(func){datatype.setFieldNextChainGetter(func);},

	/** 
	 * get a keysuffixindex from an index; see dtree.js
	 * keysuffixindex shows the values which would be used to suffix the main key-label
	 * @param	{function}	key
	 * @param	{string}	field-branch to consider or NULL if index is configured with no field-branches
	 * @param	{object}	index
	 * @return	{object}	keysuffixindex
	 */
	getKeyFieldSuffixIndex: datatype.getKeyFieldSuffixIndex,
	

/* QUERY
See examples.js for examples.
For a given Key, the commands available for querying are exposed by calling the getCommand() method.
A query Index and Attribute provide additional arguments for the command.
A query Index is simply a data object with fields/values corresponding to what was configured for the Key
	in the case of range-queries, a slight variation of an Index, called rangeConfig, is used instead
A query Attributes is an object with a subset of the following redis attributes (see redis documentation):
	- nx (boolean)
	- limit (integer)
*/

	/**
	 * A query Range or rangeConfig is an object with the following fields:
	 * 	- index:	an Index on which the Range is based
	 * 	- startProp:	the field in the Index from where ranging is to begin; fields in the index with lower
	 * 			ordering are ignored; e.g. $f1:$startProp:$f3 would begin ranging from $f1:$startProp:, unless there's a startValue
	 * 			NULL value indicates that the entire index values should be used, unless there's a startValue
	 * 	- stopProp:	the field in the index whose value should terminate ranging
	 * 			e.g. $f1:$stopProp:$f3 terminates at $f1:($stopProp+1): exclusively, unless there's a stopValue
	 * 			NULL value indicates that ranging terminates as soon as the scope of the startProp changes
	 * 	- startValue:	the value of startProp; if startProp is NULL, it is simply the start value of the range command e.g. for zrange
	 * 			e.g. $f1:$startProp:$f3 begins ranging at $f1:startValue: inclusively
	 * 	- stopValue:	the end value of stopProp; if stopProp is NULL, it is simply the stop value of the range command e.g. for zrange
	 * 			e.g. $f1:$stopProp:$f3 terminates at $f1:stopValue: inclusively
	 * 	- excludeCursor:	a boolean to indicate whether the start point of the Range should be included in resultset
	 * 	- cursorMatchOffset:	redislayer sets this in case of joins, to specify amount of already returned
	 * 				values which match the cursor position; it should be returned for re-fetches
	 * @constructor
	 * @param	{object}	an index
	 * @return	{object}	rangeConfig 
	 */
	rangeConfig: query.rangeConfig,

	/**
	 * @param	{object}	arg - a dict
	 * @param	{object}	arg.cmd - select a command from $key.getCommand()
	 * @param	{object}	arg.key - a key
	 * @param	{object}	arg.indexorrange - a Range in case of range-query or a simple Index otherwise
	 * @param	{object}	arg.attribute - query attributes
	 * @param	{resultsetCallback}	callback handler
	 */
	singleIndexQuery: function singleIndexQuery(arg, then){
		query.singleIndexQuery(arg.cmd, arg.key, arg.indexorrange, arg.attribute, then);
	},
	
	/**
	 * Make queries in bulk i.e. same command on multiple Indexes-Attribute pairs
	 * @param	{object}	arg - a dict
	 * @param	{object}	arg.cmd - select a command from $key.getCommand()
	 * @param	{object}	arg.key - a key
	 * @param	{object}	arg.indexList - a list of objects
	 * @param	{object}	arg.indexList.index - an index
	 * @param	{object}	arg.indexList.attribute - an attribute
	 * @param	{resultsetCallback}	callback handler
	 */
	indexListQuery: function indexListQuery(arg, then){
		query.indexListQuery(arg.cmd, arg.key, arg.indexlist, then);
	},

	
/* JOIN
Redislayer can merge-join multiple input streams provided the join-fields (or joints) are ordered similarly.
The process of join involves:
	- creating streamConfigs to represent each of the data-streams involved in the join
	- possible creating jointMaps as part of each streamConfig to map join-fields with the stream's field-names
	- creating a joinConfig for the streamConfigs to put the join parameters together

Joining involves repeated calls to the join-streams, in-between, tweaking their attribute.limit and cursors.
For join to be able to order resultsets, streams have to return the list of keys associated with their resultset;
	this is why singleIndexQuery and indexListQuery both return a [keys] field.
In case a resultset doesn't have the [keys] property, a so-called [ord] property can be provided as follows:
	- if a Config is not available generate one with redislayer.createConfig; if it is not required that this config
	is added to redislayer's store of configs, set arg.ontree=false; arg.struct may also be set to NULL.
	- then call redislayer.getConfigFieldOrdering(config, jointMap, joints) method to generate an ord.
*/

	/**
	 * @param	{string}	the namespace separator to be used for stream aliasing e.g. '.'
	 */
	getNamespaceSeparator: join.getNamespaceSeparator,
	
	/**
	 * @return	{string}	see getNamespaceSeparator(string)
	 */
	setNamespaceSeparator: join.setNamespaceSeparator,

	/**
	 * A join stream may use a jointMap to map to possibly different names it has of the join-fields
	 * Once an instance of jointMap is created, the method addMaskToField(join-field-name, local-field-name)
	 * is called several times to create the mappings
	 * @constructor
	 * @return	{object}	a jointMap	
	 */
	jointMap: comparison.fieldMask,

	/**
	 * The joint object determines the range of fields to be used in checking the relative storage/retrieval ordering between objects.
	 * WARNING: The @from param allows to ignore higher-order fields which the caller assures to be ordered
	 *	This reduces administration of joinmaps (especially when out-of-focus higher-order fields are added later).
	 *	However when the ordering of such ignored fields is broken, cursoring may go wrong, hence infinite loops may result
         *	=> set @from IFF you can assure ordering of higher-order fields
	 * Since partitioned fields are by default excluded from comparisons, required inclusions should be made with the setPartitions method.
	 * The setPartitions method take a list of partitions which should be included in the ordering comparison.
	 * @constructor
	 * @param	{string}	from - the higher-ordering field from where the join is focused; NULL value recommended.
	 * @param	{string}	to - the name of the least-ordering field; ordering would be checked until and including this field
	 */
	joint: comparison.joint,
		
	/**
	 * @param	{object}	arg -
	 * @param	{object}	arg.config - the config on which the ordering should be based
	 * @param	{object}	arg.jointmap - possible jointmap
	 * @param	{object}	arg.joint - a joint object to demarcate the ordering comparison
	 * @return	{object}	ord object
	 */
	getConfigFieldOrdering: function getConfigFieldOrdering(arg){
		return comparison.getConfigFieldOrdering(arg.config, arg.jointmap, arg.joint);
	},

	/**
	 * A streamConfig is created to express info about a stream used in a join.
	 * The following fields have to be provided for streamConfigs:
	 *	- func:	the fullname of the function to be called for the stream's data
	 * 	- args:	the list of args to be provided to [func]
	 * 	- namespace:	a unique label across all streamConfigs of a single join;
	 *			note that streams may have totally unrelated fields with the same name.
	 *			this label should persist across calls e.g. array-indexes are not good for this
	 * 	- attributeIndex:	the index of [args] which takes query-attribute object
	 * 	- cursorIndex:	the index of [args] which takes the cursor object
	 *			NB: the cursor itself should have field-names corresponding to the namespace
	 *			cursor field-names without getNamespaceSeparator() are always considered part of the current stream
	 * @constructor
	 * @return	{object}	a streamConfig
	 */
	streamConfig: join.streamConfig,

	/**
	 * A joinConfig puts together all the specifications of the join to be made.
	 * the following methods and fields are available:
	 * 	- setInnerJoin():	specify that the join is an inner-join	
	 *	- setFullJoin():	specify that the join is a full-join
	 *	- setOrderAsc():	specify that the join is in ascending order
	 *	- setOrderDesc():	specify that the join is in descending order
	 * 	- setModeList():	specify that the joined records should be listed
	 * 	- setModeCount():	specify that only the counts of the joined records should be returned
	 * 	- streamConfigs:	the list of streamConfigs involved in the join
	 * 	- joint:	joint object
	 * 	- limit:	the number of results to return; useful even for setModeCount() for smart termination
	 * @constructor
	 * @return	{object}	a joinConfig
	 */
	joinConfig: join.joinConfig,

	/**
	 * merge-join a list of resultset streams ordered in the same direction.
	 * This is currently valid only for one-to-one joins; behaviour on other joins is unspecified.
	 * @param	{object}	arg - a dict
	 * @param	{object}	arg.joinconfig - the joinConfig
	 * @param	{resultsetCallback}	callback handler
	 */
	mergeStreams: function mergeStreams(arg, then){
		join.mergeStreams(arg.joinconfig, then);
	},

	
/* MIGRATE
Migrating is simply the call to a function.
*/
	/**
	 * @todo
	 */
	migrate: function migrate(arg, then){
		migrate(arg.keyorigin, arg.keydestination, arg.indexstart, arg.indexend, arg.batchsize, arg.napduration, then);
	},

// CALLBACK
	/**
	 * @callback	resultsetCallback
	 * @param	{object}	result - a dict
	 * @param	{number}	result.code - 0 for success, else it depends
	 * @param	{object}	result.data - integer, string or list depending on query
	 * @param	{object}	result.keys - a list of the query keys NB: some commands take multiple keys
	 * @param	{object}	result.ord - an ord object; encoding about the ordering of the resultset
	 *						either results.keys or results.ord must be present for Joins to work with the resultset
	 *						for resultsets which do not come from redislayer, ord can be generated as follows:
	 *						- if a Config is not available generate one with redislayer.createConfig
	 *						---- if it is not required that this config is added to redislayer's store of configs,
	 *						     set arg.ontree=false; arg.struct may also be set to NULL.
	 *						- then call redislayer.getConfigFieldOrdering(config, jointMap, joints) method to generate an ord
	 */

};


module.exports = redislayer;
