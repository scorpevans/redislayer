# redislayer  ... under construction
A redis-inspired query engine for interacting transparently with your heterogeneous data layer

## Motivation - transparency atop heterogeneity
1. storage :- use a standard configuration to define how and where to store/retrieve data objects on heterogeneous databases
2. query :- use a standard directive to talk to heterogenous databases, and receive a standard result
3. cluster :- interact with just a single endpoint, instead of separately to tons of databases

## Framework
Redislayer allows the user to list the available database locations and types, and to configure how/where data should be stored/retrieved. Having done this, the user just has to query redislayer with standardized commands; redislayer takes care of the processing and returns a standardized resultset to the user.

The standardization provided by redislayer eliminates the need for refactoring when storage configurations change, and reduces migration to a function call.

## [Example](https://github.com/scorpevans/redislayer/blob/master/nodejs/example.js)
## [Interface](https://github.com/scorpevans/redislayer/blob/master/nodejs/redislayer.js)
