# TPCC benchmark

This is a fork of OLTPBench that is used to run the TPCC benchmark. All the other benchmarks have been removed.


Just like the original benchmark this is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

## Documentation

Detailed documentation lives in [`docs/`](docs/README.md):

- [TPC-C workload](docs/TPCC_WORKLOAD.md): what each transaction does, the schema, and the metrics
- [YugabyteDB-specific changes](docs/YUGABYTEDB_CHANGES.md): what changed from OLTPBench, and why
- [Configuration reference](docs/CONFIGURATION.md) · [Running](docs/RUNNING.md) · [Metrics & results](docs/METRICS_AND_RESULTS.md)
- [Architecture](docs/ARCHITECTURE.md) · [Project structure](docs/PROJECT_STRUCTURE.md) · [Code review guide](docs/CODE_REVIEW_GUIDE.md)

AI coding agents: see [`AGENTS.md`](AGENTS.md) / [`CLAUDE.md`](CLAUDE.md).

## Dependencies

+ Java 8+
+ Apache Ant


## Environment Setup
+ Install Java, Ant and Ivy.
+ Download the source code.
  ```bash
  git clone https://github.com/yugabyte/tpcc.git
  ```
+ Run the following commands to build:
  ```bash
  ant bootstrap
  ant resolve
  ant build
  ```

## Setup of the Database
The DB connection details should be as follows:

````xml
<!-- config/sample_tpcc_config.xml -->
    <!-- Connection details -->
    <dbtype>postgres</dbtype>
    <driver>org.postgresql.Driver</driver>
    <DBUrl>jdbc:postgresql://<ip>:5433/yugabyte</DBUrl>
    <username>yugabyte</username>
    <password></password>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
````

The details of the workloads have already been populated in the sample config present in /config.
The workload descriptor works the same way as it does in the upstream branch and details can be found in the [on-line documentation](https://github.com/oltpbenchmark/oltpbench/wiki).


## Running the Benchmark
A utility script (./tpccbenchmark) is provided for running the benchmark. Run it from the repository root.
Run `./tpccbenchmark --help` for the full list of options. The most common ones are:

```
-c,--config <arg>              Workload configuration file [default: config/workload_all.xml]
   --create=true               Create the tables, indexes and SQL procedures (drops existing tables)
   --load=true                 Load data using the benchmark's data loader
   --execute=true              Execute the benchmark workload
   --clear=true                Drop all the TPC-C tables
   --enable-foreign-keys=true  Add the foreign keys (after a multi-client load)
   --nodes <arg>               Comma separated list of database nodes (default 127.0.0.1)
   --warehouses <arg>          Number of warehouses (default 10)
   --start-warehouse-id <arg>  First warehouse of this client's slice (multi-client runs)
   --total-warehouses <arg>    Total warehouses across all clients
   --loaderthreads <arg>       Number of loader threads
   --num-connections <arg>     Number of DB connections used during execute
   --warmup-time-secs <arg>    Warmup time before measurement starts
   --vv                        Output verbose execute results (retries, failure latencies)
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for every flag and XML option, with defaults.

## Example
The following commands initialize a tpcc database (--create=true --load=true) and a then run a workload (--execute=true) as described in config/workload_all.xml file. The results (latency, throughput) are summarized and written to stdout:

```
./tpccbenchmark -c config/workload_all.xml --create=true
./tpccbenchmark -c config/workload_all.xml --load=true
./tpccbenchmark -c config/workload_all.xml --execute=true
```
