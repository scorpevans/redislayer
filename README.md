A redis-inspired query engine for interacting transparently with your heterogeneous data layer.

## Motivation - transparency atop heterogeneity
1. storage :- use a standard configuration to define how and where to store/retrieve data objects on heterogeneous databases
2. query :- use a standard directive to talk to heterogenous databases, and receive a standard result
3. cluster :- interact with just a single endpoint, instead of separately to tons of databases

## Framework
Due to it's primitive storages, Redis API is a suitable entry point to interact with other more abstract databases. With Redislayer you can just speak Redis, and trust that nearly loss-less translations are made to other databases.

Redislayer allows the user to list the available database locations and types, and to configure how/where data should be stored/retrieved. Having done this, the user just has to query redislayer with standardized commands; redislayer takes care of the processing and returns a standardized resultset to the user.

The standardization provided by redislayer eliminates the need for refactoring when storage changes between equivalent configurations, and reduces migration to a function call.

## [Example](https://github.com/scorpevans/redislayer/blob/master/nodejs/example.js)
The examples in the link are tuned to demonstrate almost all use-cases of Redislayer. Jumping between the examples.js and interface.js is all that you need to know in order to use Redislayer.

## [Interface](https://github.com/scorpevans/redislayer/blob/master/nodejs/redislayer.js)
This link documents all you need to know about the utilities of Redislayer.
