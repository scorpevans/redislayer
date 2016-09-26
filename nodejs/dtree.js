var redislayer = require('./redislayer');
var idTrailInfoOffset = redislayer.datatype.getIdTrailInfoOffset();
var redisMaxScoreFactor = redislayer.datatype.getRedisMaxScoreFactor();
var string = redislayer.datatype.getStruct().string.getId();
var set = redislayer.datatype.getStruct().set.getId();
var hash = redislayer.datatype.getStruct().hash.getId();
var zset = redislayer.datatype.getStruct().zset.getId();

var dtree = {
	id:	'datatype',
	defaultgetter:	{ // see configurations in datatype.js for defaults and examples
			  /**
			   * provide a function(arg)
			   * 	@arg is a dict with the fields {metacmd:, key:, keysuffixindex:, keyfield:)
			   * 	@return the cluster-instance on which to perform a query
			   * NB: metacmd is the generic command e.g. get (before deciding on a cluster specific commands can't be known)
			   * NB: keysuffixindex is exactly like the index but with field values replaced with their keySuffix 
			   *	keySuffixes group indexes into equivalent classes of their keyText
        		   * 	this input is necessary to prevent clusterinstancefunction from making non-contiguous distribution of index
			   *    as could have been the case with full index (as opposed to suffixes)
			   */
			  clusterinstance: null,
			  /**
			   * provide a function()
			   * which returns the current minimum value of a field
			   * this is used as a lower bound for keyChain computations
			   */
			  minvalue: null,
			  /**
			   * provide a function()
			   * which returns the current maximum value of a field
			   * this is used as an upper bound for keyChain computations
			   */
			  maxvalue: null,
			  /**
			   * provide a function(suffix)
			   * which returns the key-chain after given suffix
			   */
			  nextchain: null,
			  /**
			   * provide a function(suffix)
			   * which returns the key-chain before given suffix
			   */
			  previouschain: null,
			  /**
			   * provide a function(key, field, suffix1, suffix2)
			   * which decides precedence of key-chains i.e. ?suffix1 < suffix2?
			   */
			  islessthancomparator: null,
			},
	structs:[{
		id:	zset,
		structgetter: {	clusterinstance: null,
				minvalue: null,
				maxvalue: null,
				nextchain: null,
				previouschain: null,
				islesscomparator: null},
		configs: [{
			id:	'zkey_user',
				// KEYTEXT, UID and XID are the storage components of redislayer
				// KEYTEXT is the table/key/etc which serves as the main storage object
				// UID is the unique identifier stored, or used to access other stored values
				// XID is the secondary value stored along with a UID, or non-unique value of a KEYTEXT
				// the index of a config specifies fields and how they should be used to compose the above
				/**
				 * list the complete set of fields associated with this object
				 * the value remaining props correspond with the order of the field prop
				 */
			index:  { fields: ['gender', 'userid', 'username', 'fullname']
				/**
				 * specify if the name of the field itself (as opposed to the value) should be appended to the key-label
				 * this cosmetic helps label keys nicely, and also serves for vertical partitioning
				 * WARNING! UID should be a unique-identifier without field-branches inclusions
				 *	in fact, field-branches should not be put into UID; this is a sign of a bad configuration
				 *	otherwise there's no reference to use to rebuild the object once it is branched for storage
				 *	see offsetprependsuid
				 */
				, fieldprependskey: [null, null, true, true]
				/**
				 * specify which mid-part of the value of a field should be retained
				 * the remaining is cut off to suffix keys
				 * the idea is to shorten values to meet storage restrictions e.g. in redis
				 * 	or for horizontal partitioning, since keysuffixes lead to new keys
				 * <offset> % 100 specifies the chars to skip from the right of the value i.e. trailing-info
				 *	trailing-info is info appended to the end of the actual value
				 * <offset> / 100 specifies the number of chars to retain as value after the skip
				 * e.g. offset=52 on value='123456789' would suffix the key with '1289'
				 * 	and use a value of '34567'
				 * NB: for the sake of merge-joins, trailing-info offset value should be globally consistent for a field
				 *	even across configs, in order to retain the same storage ordering of values across keys
				 * NB: offsetted fields are required in all queries since they help define the key to query
				 */
				, offsets: [null, 500+idTrailInfoOffset]
				/**
				 * if the design is such that several fields would produce the same keysuffix,
				 * nominate an index to represent their keysuffix, instead of repeating the same suffix in the key
				 * 	of course the designated index should be the field-index of one of the participating fields
				 * with offsetgroups, several fields can be tried to find keysuffixes in cases of NULL values
				 * 	this is essential for zset.zscore and zset.range, in which case keysuffix comes from either member/score
				 */
				, offsetgroups: []
				/**
				 * specify whether the offsetted value should contribute towards the <unique field> stored
				 * this prop composes the unique-id (UID) of a given object
				 * WARNING: for this specific config, it is crucial that userid=true; see fieldprependskey
				 *	this index-config is a bad one; better would be if the struct=hash and field-branches don't prepend UID
				 *	with struct=zset, the XID must be a float so we are forced to stow strings in UID
				 */
				, offsetprependsuid: [false, true, true, true]
				/**
				 * mark fields as being partitions, if they should not affect the retrieval ordering
				 * e.g. in this case, userid's would be ordered despite the preceding gender field
				 * it is recommended that partition fields have relatively few possible values
				 * NB: this prop is not used for physical partitioning; see instead [offsets] or [fieldprependskey] 
				 */
				, partitions: [true]			// applies only to sorted-sets
				/**
				 * specify the different factors by which field values should be multiplied, before their summation
				 * this is used for the scores of redis sorted-sets
				 * NULL or 0 value indicates field should not be included in summation
				 */
				, factors: [redisMaxScoreFactor, 1]	// applies only to sorted-sets
				/**
				 * specify whether the field is a text or integer field
				 * this is very useful for SQL storages, and also for keyChains to determine the type to use for comparisons
				 */
				, types: ['integer', 'integer', 'text', 'text']},
			// each field may have it's own getter except with clusterinstance
			fieldgetter: {	clusterinstance: null,		// not defined per field
					minvalue: [],
					maxvalue: [],
					nextchain: [],
					previouschain: [],
					islesscomparator: []},
			keys:	[{
				id:	'key1',				// id reference to this key
				label:	'employee:detail',		// the name of the key within the database
				keygetter: {clusterinstance: null,
						minvalue: [],
						maxvalue: [],
						nextchain: [],
						previouschain: [],
						islesscomparator: []},
				},
				{
				id:	'key2',
				label:	'customer:detail',
				}]}]}],
};


module.exports = dtree;
