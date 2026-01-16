# DazzleDuck SQL Spark Integration

This guide explains how to set up and query DazzleDuck tables using Apache Spark SQL with Arrow Flight SQL.

## Overview
DazzleDuck leverages Apache Arrow Flight, a high-performance RPC framework designed specifically for transferring large amounts of columnar data over a network. 
Unlike traditional JDBC/ODBC protocols, Arrow Flight eliminates the need for row-based serialization,
significantly reducing transfer latency and increasing throughput.

## Prerequisites

- Apache Spark 3.5.7
- JDK 17
- Docker

## Getting Started

### 1. Start the DazzleDuck Server

Launch the DazzleDuck server with the `example/data` directory mounted at `/data`:

```bash
docker run -ti -v "$PWD/example/data":/local-data -p 59307:59307 -p 8080:8080 dazzleduck/dazzleduck --conf warehouse=/warehouse
```
For DazzleDuck server Check this repo(https://github.com/dazzleduck-web/dazzleduck-sql-server)
### 2. Launch Spark SQL

Start Spark SQL with the DazzleDuck package:

```bash
bin/spark-sql --packages io.dazzleduck.sql:dazzleduck-sql-spark:0.0.4
```

### 3. Create a Temporary View

At the Spark SQL prompt, create a temporary view to access your data:

```sql
CREATE TEMP VIEW t (key STRING, value STRING, p INT)
USING io.dazzleduck.sql.spark.ArrowRPCTableProvider
OPTIONS (
  url 'jdbc:arrow-flight-sql://localhost:59307?disableCertificateVerification=true&user=admin&password=admin',
  partition_columns 'p',
  path '/local-data/parquet/kv',
  connection_timeout 'PT60M'
);
```

### 4. Query the Table

```sql
SELECT * FROM t;
```

## Working with DuckLake

### 1. Start the DazzleDuck Server

Launch the DazzleDuck server :

```bash
docker run -ti -v "$PWD/StartUpScript.sql:/startup/StartUpScript.sql" -p 59307:59307 -p 8080:8080 dazzleduck/dazzleduck --conf warehouse=/warehouse --conf startup_script_provider.script_location=/startup/StartUpScript.sql

```




## Querying DuckLake Tables via Spark SQL

### 2. Start Spark SQL

```bash
bin/spark-sql --packages io.dazzleduck.sql:dazzleduck-sql-spark:0.0.4
```

### 3. Create a Temporary View for DuckLake

At the Spark SQL prompt:

```sql
CREATE TEMP VIEW t (key STRING, value STRING, partition INT)
USING io.dazzleduck.sql.spark.ArrowRPCTableProvider
OPTIONS (
   url 'jdbc:arrow-flight-sql://localhost:59307?useEncryption=true&disableCertificateVerification=true&user=admin&password=admin',
  database 'my_data',
  schema 'main',
  table 'demo',
  partition_columns 'partition',
  connection_timeout 'PT10M'
);
```

### 4. Query the Table

```sql
SELECT * FROM t;
```

## Notes

- Default credentials are `admin/admin` for both username and password
- Connection timeouts can be adjusted based on your data size and network conditions
- Replace `catalog_name`, `schema_name`, and `table_name` with your actual catalog, schema, and table identifiers

## Troubleshooting

- **Connection refused**: Verify that the DazzleDuck server is running and the ports are correctly exposed
- **Certificate verification errors**: Ensure `disableCertificateVerification=true` is included in the connection URL for development environments
- **Timeout issues**: Increase the `connection_timeout` value if working with large datasets
