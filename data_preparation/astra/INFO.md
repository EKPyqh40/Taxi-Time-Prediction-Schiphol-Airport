# Astra Data Preparation

Astra data preparation differs from other preprocessing by consisting out of to steps. Astra data is namely first reduced before processing. During the reduction step the astra data records are compressed by creating a single record for each transition of a track from one polygon to the other. The subsequent processing step generates a single record for each track.

## astra_insert.py

Script for parallel insertion of astra data into PostgreSQL database.

Syntax:
astra_insert.py \[number of workers\] \[offset (number of the worker)\]

## astra_schema.pgsql

PostgreSQL schema of the astra table.

## astra reduce

To reduce the astra data, a query is used. The query links different entries together into a track. For each track the query considers every entry in astra. As astra is however extremly large, this would be very expensive. Therefore the query is applied only on section of the astra data that is near in time. Python is used to run each of these subqueries in parallel.

### astra_reduce.py

Executes PostgreSQL subqueries in parallel using the generic query found in generic_astra_reduce_query.pgsql

Syntax:
astra_reduce.py \[number of workers\] \[offset (number of the worker)\]

### astra_reduce_schema.pgsql

PostgreSQL schema of the astra_reduce table

### generic_astra_reduce_query.pgsql

Contains the generic reduction query

### utils.pgsql

Contains several functions used in the pgsql queries, and verification data for the queueing algorithm.

## astra_reduce_process_query.pgsql

Contains the query for the final materialized view (astra_processed). This materialized view contains a single record for each track.
