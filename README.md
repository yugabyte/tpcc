# TPCC benchmark

This is a fork of Oltp bench that is used to run the TPCC benchmark. All the other benchmarks have been removed.


Just like the original bencmark this is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

## Dependencies

+ Java (+1.7)
+ Apache Ant


## Environment Setup
+ Install Java and Ant.
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
If you start from scratch, you should create an empty database (e.g., for TPC-C you can create a db named <b>tpcc</b>) and provide login credential to the benchmark, by modifying  <b>the workload descriptor file</b>. The ./config directory provides several examples, we now use the sample_tpcc_config.xml. You should edit the following portion:

````xml
<!-- config/sample_tpcc_config.xml -->
    <!-- Connection details -->
    <dbtype>mysql</dbtype>
    <driver>com.mysql.jdbc.Driver</driver>
    <DBUrl>jdbc:mysql://localhost:3306/tpcc</DBUrl>
    <username>root</username>
    <password>mysecretpassword</password>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
````

## Running the Benchmark
A utility script (./tpccbenchmark) is provided for running the benchmark. The options are

```
-c,--config &lt;arg&gt;            [required] Workload configuration file
   --clear &lt;arg&gt;             Clear all records in the database for this
                             benchmark
   --create &lt;arg&gt;            Initialize the database for this benchmark
   --dialects-export &lt;arg&gt;   Export benchmark SQL to a dialects file
   --execute &lt;arg&gt;           Execute the benchmark workload
-h,--help                    Print this help
   --histograms              Print txn histograms
   --load &lt;arg&gt;              Load data using the benchmark's data loader
-o,--output &lt;arg&gt;            Output file (default System.out)
   --runscript &lt;arg&gt;         Run an SQL script
-s,--sample &lt;arg&gt;            Sampling window
-v,--verbose                 Display Messages
```

## Example
The following command for example initiate a tpcc database (--create=true --load=true) and a then run a workload as described in config/sample_tpcc_config.xml file. The results (latency, throughput) are summarized into 5seconds buckets (-s 5) and the output is written into two file: outputfile.res (aggregated) and outputfile.raw (detailed):

```
./tpccbenchmark -c config/sample_tpcc_config.xml --create=true --load=true --execute=true -s 5 -o outputfile
```

Since data loading can be a lengthy process, one would first create a and populate a database which can be reused for multiple experiments:

```
./tpccbenchmark -c config/sample_tpcc_config.xml --create=true --load=true
```

Then running an experiment could be simply done with the following command on a fresh or used database.

```
./tpccbenchmark -c config/sample_tpcc_config.xml --execute=true -s 5 -o outputfile
```

## Workload Descriptor
The workload descriptor works the same way as it is in the upstream branch and details can be found in the [on-line documentation](https://github.com/oltpbenchmark/oltpbench/wiki).
