# mo-columns

More Columns!  Experiments with columnar datastores

## Problem 

We want a way to cheaply query a Petabyte of data at 10K times a day.  

* Target price: $250/day
* Target latency: < 1 sec for full scan


## How fast can we go?

* Can Python/Sqlite process large array (>10billion elements) operations faster than Numpy? We assume distributed computing is allowed.
* Can Python/Sqlite implement the sharded, columnar storage like Elasticsearch, and be faster? - If not, what is the cause of the slowdown?
* Can the Python/Sqlite running on spot nodes, with data backed up to S3 be reliable and fast?  Can we process queries for less $/gigibyte than BigQuery?


## Desired Features

* Split JSON documents into columns - Columns are in their own table (?their own database?), indexed by document ID, with an additional index on the value. 
* Ingestion will involve splitting documents into columns and creating new databases. Each database is a "shard"
* All databases are read-only:  Databases are merged-and-replaced in atomic actions.
* Query results are in the form of a SQLite database
* Queries are broadcast to all shards, over all nodes, and aggregated by the query node (which can include the client, for maximum distribution of effort)

## Desired Tests

1. Single node, multiple shards - how fast can it perform `SELECT SUM(A) FROM T WHERE Y GROUP BY A, B`
2. Single node, multiple shards - speed to load 10K x 1M grid 
3. Single node, multiple shards - speed multiply grids
4. Multiple nodes - distribute query, return results, aggregate results

## Results

### Loading Sqlite from Python (8K/sec)

Can load simple JSON (6 properties) at 14K/sec (1M/69sec) by making a single column hold the JSON, and using the JSON1 library to split that JSON into columns.
* Insertion of records to table is limited by Python serialization: This can go faster if JSON strings are shunted directly into table.
* Using multiple threads to split the JSON into columns is capable of using 100% of CPU, and is a little faster than single threaded version.
* Columns are MUCH LARGER on disk than anticipated:
  * `_id` - 20char - 57M - 57bytes/row
  * `a` - 6char - 29M - 29bytes/row
  * `b` - float - 33M - 33bytes/row (float is 8bytes)
  * 2 bytes per byte makes sense with index
  * 17 bytes to store nothing (8 bytes for primary key + 4 bytes for indexed `rowid` + 5 ?control? )
  * 2x compression on zip, which makes sense given the high entropy values
  * 4x compression on cardinality=7 column 

### Dot Product

Storing each element of the matrix in a row `(row, col, value)` makes dot product slow.   
* Sqlite - 50K * 1K => multiply (took 43.481 seconds) 1.1Mops/sec
* Numpy - 1M * 1K (took 5.117 seconds)  196Mops/sec (200x faster than sqlite)
* Numpy - 1M * 10k (took 280.096 seconds)  35Mops/sec (32x faster than sqlite)

**Notes**
* Numpy - high speed comes from ?avoiding cache misses?
* Rows and columns as INTEGER (instead of string) made little difference
* CROSS JOIN is required to force query planner to iterate through table properly
* :memory: database made little difference


## Conclusion

Sqlite's data format is too voluminous for efficient big-data applications. The issues are:
* the 64bit integer types used in `rowid`.
* An index, which is required for fast filtering, doubles the database size - there is no internal compression that takes advantage of the index ordering.
* Low cardinality columns do not zip well - this prevents any hope of trading compute for reduced network time.

Sqlite with a custom file format plugin (like [parquet](https://github.com/cldellow/sqlite-parquet-vtable/)) may solve those problems  

