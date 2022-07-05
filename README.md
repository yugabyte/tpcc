# TPCC benchmark

This is a fork of OLTPBench that is used to run the TPCC benchmark. All the other benchmarks have been removed.


Just like the original benchmark this is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

## Dependencies

+ Java (+1.7)
+ Apache Ant


## Environment Setup
+ Install Java, Ant and Ivy.
+ Download the source code.
  ```bash
  git clone https://github.com/yugabyte/tpcc.git
  ```
+ Run the following commands to build:
  ```bash
  mvn clean install
  ```
+ Copy and unpack `target/tpcc.tar.gz` file on client machine

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
A utility script `./tpccbenchmark` is provided for running the benchmark. The options are

```
 -c,--config <arg>                    Workload configuration file
                                      [default: config/workload_all.xml]
    --clear <arg>                     Clear all records in the database
                                      for this benchmark
    --create <arg>                    Initialize the database for this
                                      benchmark
    --create-sql-procedures <arg>     Creates the SQL procedures
    --dir <arg>                       Directory containing the csv files
    --enable-foreign-keys <arg>       Whether to enable foregin keys
    --execute <arg>                   Execute the benchmark workload
 -gpc,--geopartitioned-config <arg>   GeoPartitioning configuration file
                                      [default:
                                      config/geopartitioned_workload.xml]
 -h,--help                            Print this help
    --histograms                      Print txn histograms
 -im,--interval-monitor <arg>         Throughput Monitoring Interval in
                                      milliseconds
    --initial-delay-secs <arg>        Delay in seconds for starting the
                                      benchmark
    --load <arg>                      Load data using the benchmark's data
                                      loader
    --loaderthreads <arg>             Number of loader threads (default
                                      10)
    --merge-results <arg>             Merge results from various output
                                      files
    --nodes <arg>                     comma separated list of nodes
                                      (default 127.0.0.1)
    --num-connections <arg>           Number of connections used
    --output-raw <arg>                Output raw data
    --output-samples <arg>            Output sample data
    --start-warehouse-id <arg>        Start warehouse id
    --total-warehouses <arg>          Total number of warehouses across
                                      all executions
    --vv                              Output verbose execute results
    --warehouses <arg>                Number of warehouses (default 10)
    --warmup-time-secs <arg>          Warmup time in seconds for the
                                      benchmark
```

## Example
First step is to create tables and indexes, can be done by calling following command

```
./tpccbenchmark --nodes $COMMA_SEPARATED_IPS --create true --vv
```

Since data loading can be a lengthy process,one can be used to populate a database which can be reused for multiple experiments:

```
./tpccbenchmark --nodes $COMMA_SEPARATED_IPS --load true --warehouses $WAREHOUSES --loaderthreads $LOADER_THREADS --vv
```

Then running an experiment could be simply done with the following command on a fresh or used database.

```
./tpccbenchmark--nodes $COMMA_SEPARATED_IPS --execute true --warehouses $WAREHOUSES --warmup-time-secs 30 --vv
```
