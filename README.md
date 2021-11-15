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
A utility script (./tpccbenchmark) is provided for running the benchmark. The options are

```
-c,--config <arg>            [required] Workload configuration file
   --clear <arg>             Clear all records in the database for this
                             benchmark
   --create <arg>            Initialize the database for this benchmark
   --execute <arg>           Execute the benchmark workload
-h,--help                    Print this help
   --histograms              Print txn histograms
   --load <arg>              Load data using the benchmark's data loader
-o,--output <arg>            Output file (default System.out)
   --runscript <arg>         Run an SQL script
-s,--sample <arg>            Sampling window
-v,--verbose                 Display Messages
```

## Example
The following command for example initiates a tpcc database (--create=true --load=true) and a then run a workload as described in config/workload_all.xml file. The results (latency, throughput) are summarized and written into two files: outputfile.res (aggregated) and outputfile.raw (detailed):

```
./tpccbenchmark -c config/workload_all.xml --create=true --load=true --execute=true -s 300 -o outputfile
```

Since data loading can be a lengthy process, one could first create a and populate a database which can be reused for multiple experiments:

```
./tpccbenchmark -c config/workload_all.xml --create=true --load=true
```

Then running an experiment could be simply done with the following command on a fresh or used database.

```
./tpccbenchmark -c config/workload_all.xml --execute=true -s 300 -o outputfile
```
