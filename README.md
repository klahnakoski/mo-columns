# mo-columns

More Columns!  Experiments with columnar datastores

## Problem 

We want a way to cheaply query a Petabyte of data a 10K times a day.  

* Target price: $250/day
* Target latency: < 1 sec for full scan


## How fast can we go?

* Can Python/Sqlite process large array (>10billion elements) operations faster than Numpy? We assume distributed computing is allowed.
* Can Python/Sqlite implement the sharded, columnar storage like Elasticsearch, and be faster? - If not, what is the cause of the slowdown?
* Can the Python/Sqlite running on spot nodes, with data backed up to S3 be reliable and fast?  Can we do process queries for less $/gigibyte than BigQuery?


## Features

* Split JSON documents into columns - Columns are in their own table (?their own database?), indexed by document ID, with an additional index on the value. 
* Ingestion will involve splitting documents into columns and creating new databases. Each database is a "shard"
* All databases are read-only:  Databases are merged-and-replaced in atomic actions.
* Query results are in the form of a SQLite database
* Queries are broadcast to all shards, over all nodes, and aggregated by the query node (which can include the client, for maximum distribution of effort)

## Tests

1. Single node, multiple shards - how fast can it `SELECT SUM(A) FROM T WHERE Y GROUP BY A, B`
2. Single node, multiple shards - speed to load 10K x 1M grid
3. Single node, multiple shards - speed multiply grids
4. Multiple nodes - distribute query, return results, aggregate results

