# Astra Data Preparation

## flt_insert.py

Script for parallel insertion of flt data into PostgreSQL database. Additonally the data is slightly compressed by only keeping the final result of a thread of reported instantanous changes. Furthermore several extra attributes are constructed (local, civ_reg_gnr,...).

Syntax:
flt_insert.py \[number of workers\] \[offset (number of the worker)\]

## flt_schema.pgsql

PostgreSQL schema of the flt table.

## astra_reduce_process_query.pgsql

Contains the query for the final materialized view (flt_processed). This materialized view contains a single record for each flight.

## flt_process_schema.pgsql

PostgreSQL schema of the flt_processed table.
