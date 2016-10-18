var rl = require('./redislayer');
var clist = require('./clist');
var redisMaxScoreFactor = rl.getRedisMaxScoreFactor();
var string = rl.getStruct().string.getId();
var set = rl.getStruct().set.getId();
var hash = rl.getStruct().hash.getId();
var zset = rl.getStruct().zset.getId();

/**
 * The configuration of how and where to store data is made in a hierarchy of structs > configs > keys as follows:
 *	dtree = {
 *		id: #,
 *		defaultgetter: {#},
 *		structs:[
 *			{
 *				id: #,
 *				structgetter: {#},
 *				configs:[
 *					{
 *						id: #,
 *						configgetter: {#},
 *						index: {#},
 *						keys:[
 *							{
 *								id: #,
 *								label: #,
 *								keygetter: {#}
 *							},
 *							... more keys
 *						]
 *					},
 *					... more configs
 *				],
 *			},
 *			... other structs
 *		],
 *	};
*/


// load clusters so we can reference them in the configs
rl.loadClusterList({clist:clist});


// this dtree is designed to demonstrate all the juicy aspects of configuring storages
// NB see example.js for a demo using this dtree
var idPrefixInfoOffset = 4;
var dtree = {
	id:	null,	// whatever
	defaultgetter:	{
		// see configurations in datatype.js for defaults and examples
		/**
		 * decide on which cluster a query runs based on query arguments
		 * provide a deterministic function which expects the following arguments
		 * @param	{object}	arg - a dict
		 * @param	{object}	arg.metacmd - generic command e.g. get (specific commands are made after cluster is known)
		 * @param	{object}	arg.key - query Key
		 * @param	{object}	keysuffixindex - query Index with field values replaced with their keySuffixes
		 *				keySuffixes group indexes into equivalent classes of their keyText; this input is necessary
		 *				to prevent clusterinstance function from making non-contiguous distribution of index
		 *				as could have been the case with full index (as opposed to suffixes)
		 * @param	{object}	arg.keyfield - possible field-branch; see fieldprependskey
		 * @return	{object}	the cluster-instance on which to perform a query
		 */
		clusterinstance: function(arg){return rl.getDefaultCluster().getProxy().master;},
		/**
		 * provide a function returning minimum bound to help terminate iterating across key-chains; see offsets
		 * such a function could infer this by e.g. querying the minimum values of fields
		 * since different fields may have dramatically different bounds, this function is most useful on the field-level
		 * @return	{string}	the minimum value
		 */
		minvalue: null,
		/**
		 * provide a function returning maximum bound to help terminate iterating across key-chains; see offsets
		 * such a function could infer this by e.g. querying the maximum values of fields
		 * since different fields may have dramatically different bounds, this function is most useful on the field-level
		 * @return	{string}	the maximum value
		 */
		maxvalue: null,
		/**
		 * expedite iterating through key-chains by making optimal increments
		 * provide a function which returns the next value after given key-suffix
		 * @param	{string}	the key-suffix whose next value is to be computed
		 * @return	{string}	the next key-suffix
		 */
		nextchain: null,
		/**
		 * expedite iterating through key-chains by making optimal decrements
		 * provide a function which returns the previous value after given key-suffix
		 * @param	{string}	the key-suffix whose previous value is to be computed
		 * @return	{string}	the previous key-suffix
		 */
		previouschain: null,
	},
	structs:[{						// this structure ([{},...]) helps distinguish lists from values
		id:	zset,	// redis sorted-set struct
		structgetter: {
			clusterinstance: null,
			minvalue: null,
			maxvalue: null,
			nextchain: null,
			previouschain: null
		},
		configs: [{
			id:	'zset_membership',
				/** DEFINITIONS
				 * KEYTEXT, UID and XID are the storage blocks of redislayer
				 * KEYTEXT is the table/namespace/key/etc which serves as the main storage location
				 * UID is the unique identifier stored (e.g. with struct=sets), or used to access other stored values
				 * XID is the secondary value stored on a UID, or non-unique value of a KEYTEXT
				 * the index of a config specifies fields and how they should be decomposed to form the above
				 */
			index:  {
				/**
				 * the fields-prop lists the complete set of fields associated with this object
				 * the list-values of the subsequent props correspond with the order of the fields-prop
				 */
				fields: ['isadmin', 'entityid', 'memberid'],
				/**
				 * specify if the name of the field itself (as opposed to the value) should be appended to the key-label
				 * this cosmetic helps label keys nicely, and also serves for vertical partitioning
				 * WARNING! UID should be a unique-identifier without field-branch inclusions; see offsetprependsuid prop
				 *	in fact, field-branches should not be put into UID; this is a sign of a bad configuration
				 *	otherwise there's no reference to use to rebuild the object after it is branched for storage
				 *	see offsetprependsuid
				 */
				fieldprependskey: [],
				/**
				 * It is usually useful to ensure that only certain number of IDs end up in a key
				 * 	split the values of a fields to suffix the key
				 * also some field values may be prefixed/suffixed with namespace info making them too long
				 *	such info can also be split off to be suffixed to the key
				 * such splits, and subsequence suffixing of keys leads to horizontal partitioning
				 *	the entire chain of keys resulting such partitioning is know as key-chain
				 * <offset> % 100 specifies the number of chars to remove from the front of the value i.e. prefix-info
				 * <offset> / 100 specifies the number of trailing chars of the remaining value to retain
				 * e.g. offset=42 on value='10004567' would suffix the key with '100045' and retain '67'
				 * 	whereas offset=49 on value='10004567' would suffix the key with '1000' and retain '4567'
				 * 	offset=null means don't make any suffixing
				 *	offset=0 means suffix the key with the entire value of the field
				 *		if offset!=0 but (offset/100)=0, it implies keep everything after doing the offset%100 part
				 *		hence offset=05 suffices when offset=9999999999905 is intended
				 * NB: offsetted fields are required in all queries since they help define the key to query
				 * 	hence it's not advisable to offset fields which change frequently
				 *	since updating requires knowledge of the existing value, then a delete, followed by an insert
				 */
				offsets: [null, 500+idPrefixInfoOffset, 500+idPrefixInfoOffset],
				/**
				 * if the design is such that several fields would produce the same keysuffix,
				 * designate the index of only one of them to take care of this; instead of repeating the same suffix in the key
				 * with offsetgroups, several fields can be tried to find keysuffixes in cases of NULL values
				 * 	this is useful for zset.zscore and zset.range, which miss either a value in the Member or Score
				 *	in case keysuffixing is made, the key can be made out in both commands only if there are offsetgroups
				 *	between the Member and Score i.e. the keysuffix can be picked from either the Member or Score
				 */
				offsetgroups: [null, 1, 1],
				/**
				 * specify whether the value (possibly offsetted) should contribute towards the <unique field> stored
				 * this prop composes the unique-id (UID) of a given object
				 * NB: uid-prepended fields are required in all queries since they help define the uid to query
				 * 	hence it's not advisable to uid-prepend fields which change frequently
				 *	since updating require knowledge of the existing value, then a delete, followed by an insert
				 */
				offsetprependsuid: [null, true, true],	// for demo purposes, we won't store everything in the UID
				/**
				 * mark fields as being partitions, if they should not affect the retrieval ordering
				 * e.g. in this case, userid's would be ordered despite the preceding gender field
				 * it is recommended that partition fields have relatively few possible values
				 * NB: this prop is not used for physical partitioning; see instead [offsets] or [fieldprependskey] 
				 */
				partitions: [true],			// applies only to sorted-sets
				/**
				 * specify the different factors by which field values should be multiplied, before their summation
				 * this is used for the scores of redis sorted-sets
				 * NULL or 0 value indicates field should not be included in summation
				 */
				factors: [redisMaxScoreFactor, 1],	// applies only to sorted-sets
				/**
				 * mandatory: specify whether the field is a text or integer field
				 * this is very useful for SQL storages, and also for keyChains to determine the type to use for comparisons
				 */
				types: ['integer', 'integer', 'integer']},
			// each field may have it's own getter except with clusterinstance, which takes a single function
			// obviously, inner getters/settings take precedence over outter ones
			configgetter: {
				clusterinstance: null,		// not defined per field
				minvalue: [],
				maxvalue: [],
				nextchain: [],
				previouschain: [],
			},
			keys:	[{
				id:	'zkey_membership',		// id reference to this key
				label:	'entity:members',		// the name of the key within the database
				// inner definitions of getters take precedence over outer ones
				keygetter: {
					clusterinstance: null,
					minvalue: [],
					maxvalue: [],
					nextchain: [],
					previouschain: [],
				}
			}]
		}]
		},{
		id:	hash,	// redis hash struct
		configs:[{
			id:	'hash_entitydetails',
			index:	{
				fields: ['entityid', 'firstnames', 'lastnames', 'comments'],
				fieldprependskey: [null, true, true, true],	// 3 field-branches
				offsets: [500+idPrefixInfoOffset, idPrefixInfoOffset, idPrefixInfoOffset, idPrefixInfoOffset],
				offsetgroups: [0, 0, 0, 0],			// entityid's key-suffix subsumes those of the other fields
				offsetprependsuid: [true],
				types:	['integer', 'text', 'text', 'text']
			},
			keys:	[{
				id:'hkey_groups',
				label:'group:detail',
				keygetter:	{
					clusterinstance: function(arg){return rl.getClusterList()['1000'].getProxy().master;},
				}
				},{
				id:'hkey_user',
				label:'user:detail',
				keygetter:	{
					clusterinstance: function(arg){return rl.getClusterList()['2000'].getProxy().master;},
				}
			}]
			},{
			id:	'hash_entityid',
			index:	{
				fields: ['entitytype', 'increment'],
				offsetprependsuid: [true],
				types:	['text', 'integer'],
			},
			keys:	[{id:'hkey_entityid', label:'entity:id'}],
		}]
		},{
		id:	string,	// redis string struct
		configs:[{
			id:	'string_entityid',
			index:	{
				fields:	['increment'],
				types:	['integer'],
			},
			keys:	[{
				id:'skey_userid',
				label:'user:id',
				keygetter:	{
					clusterinstance: function(arg){return rl.getClusterList()['2000'].getProxy().master;},
				}
			}],
		}]
	}],
};


module.exports = dtree;
