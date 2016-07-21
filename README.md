# redislayer
A redis-inspired framework for interacting transparently with your heterogeneous data layer

## Motivation
##### Do you have the following requirements?
1. define different storage techniques for your data
2. store data across hetergenous database types (e.g. redis, sql, etc)
3. spread your data across myriads of database locations

##### Then you may want transparency in the following areas:
1. storage :- define only once how and where to store/retrieve different data objects
2. query :- use a standard directive to talk to heterogenous databases, and receive a standard result
3. location :- interact with just a single endpoint, instead of separately to tons of databases

## Framework
Redislayer allows the user to list the available database locations, and to cofigure how/where data should be stored/retrieved. Having done this, the user just has to query redislayer with standardized commands; redislayer takes care of the processing and returns a standardized resultset to the user.

The standardization provided by redislayer eliminates the need for refactoring when storage configurations change, and reduces migration to a function call.

## [Example](https://github.com/scorpevans/redislayer/blob/master/nodejs/example.js)
