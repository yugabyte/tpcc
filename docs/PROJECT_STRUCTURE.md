# Project Structure

A map of every directory and source file, with one line on what each is for. Files marked
**(legacy)** are OLTPBench leftovers that are unused or unmaintained.

```
tpcc/
├── AGENTS.md / CLAUDE.md        Guidance for AI coding agents (and a fast orientation for humans)
├── README.md                    Quick start
├── docs/                        Detailed documentation (this folder)
├── tpccbenchmark                Launcher: java -Xmx8G -cp $(classpath.sh) DBWorkload $@
├── classpath.sh                 Prints build/ + lib/*.jar classpath (used by tpccbenchmark)
├── build.xml                    Ant build: bootstrap | resolve | build | clean
├── download-deps.sh             The real dependency resolver: curls jars from Maven Central into lib/
├── ivy.xml, ivysettings.xml     Dependency manifest (kept in sync; no longer used by `ant resolve`)
├── pom.xml                      Maven manifest, mainly for IDE import; not the official build
├── log4j.properties             Console logging config (INFO); passed via -Dlog4j.configuration
├── manifest.mf, src/manifest.mf (legacy) jar manifests
├── LICENSE                      Apache 2.0 (inherited from OLTPBench)
├── config/
│   ├── workload_all.xml             YugabyteDB workload config (smart driver, port 5433)
│   ├── workload_all_pg.xml          PostgreSQL workload config (port 5432)
│   └── geopartitioned_workload.xml  Geo-partitioning/tablespace config (read by all modes except help/merge; disabled by default)
├── run_scripts/                 Multi-client orchestration over SSH
│   ├── Readme.md                    Step-by-step guide
│   ├── setup_clients.sh             Installs java + release tarball + ulimits on each client
│   ├── limits.conf                  /etc/security/limits.conf for clients
│   └── run_tpcc_on_clients.cpp      create | create-procedures | load | enable-foreign-keys | execute | kill across clients
├── lib/                         (gitignored except ant-contrib.jar) downloaded dependency jars
├── build/                       (gitignored) compiled classes
├── results/                     (gitignored) run outputs: oltpbench*.csv, json/output.json
├── tools/                       (legacy) python2 plotting scripts, rs-sysmon/dstat monitoring
├── stand-alone-testing-of-alt-sql-from-proc-solns/
│                                2020 study: PL/pgSQL vs client-side NewOrder, set-based SQL; SQL + python + timing logs.
│                                Not part of the build. See its README.md.
└── src/
    ├── META-INF/persistence.xml (legacy) JPA config, copied to build/ but unused
    └── com/oltpbenchmark/       All Java sources (package root)
```

## `src/com/oltpbenchmark/`: core driver

| File | Responsibility |
|------|----------------|
| `DBWorkload.java` | **`main()`**. Parses options, builds `WorkloadConfiguration`, dispatches the mode (create/load/execute/...), prints all result tables (TPM-C, latencies, worker task latencies, retries), CSV merge. |
| `CommandLineOptions.java` | Commons-CLI flag definitions and getters; `Mode` enum and mode precedence. |
| `ConfigFileOptionsBase.java` | XPath-based XML reader helpers (`getStringOpt`, `getIntOpt`, ...). |
| `ConfigFileOptions.java` | Workload XML keys (`dbtype`, `driver`, `isolation`, `runtime`, `rate`, ...). |
| `GeoPartitionedConfigFileOptions.java` | Geo XML parsing and validation into a `GeoPartitionPolicy`. |
| `WorkloadConfiguration.java` | The resolved configuration object (plus code defaults) handed to everything else. Holds the `Phase` list and the `WorkloadState`. |
| `Phase.java` | One workload phase: duration, warmup, rate, weights; `chooseTransaction()` does the weighted pick. |
| `ThreadBench.java` | Execution driver: starts workers, runs the rate/phase clock loop, switches states, joins workers, builds `Results`. Includes `MonitorThread` and `WatchDogThread`. |
| `WorkloadState.java` | Shared work queue (capped at 10k) and phase pointer; `fetchWork()` blocks workers. |
| `BenchmarkState.java` | Global state machine (WARMUP → MEASURE → DONE → EXIT) and the start barrier. |
| `SubmittedProcedure.java` | A queued unit of work (just the transaction type id). |
| `Results.java` | Aggregated samples; CSV writers (`writeAllCSVAbsoluteTiming` is the one used). |
| `LatencyRecord.java` | Chunked (200 per chunk) sample store, base class. |
| `TransactionLatencyRecord.java` | Sample = (type, start, connection-acquire µs, operation µs). |
| `WorkerTaskLatencyRecord.java` | Sample = (type, fetch-work µs, keying µs, op-with-retry µs, think µs). |
| `DistributionStatistics.java` | Percentile/mean/stddev computation for CSV time buckets. |
| `TraceReader.java` | (legacy) trace-driven workloads; never configured. |

## `api/`: benchmark framework

| File | Responsibility |
|------|----------------|
| `BenchmarkModule.java` | The "tpcc" benchmark: Hikari pools per node (`createDataSource`), plain connections (`makeConnection`), schema create/clear, loader invocation, FK and procedure creation, **terminal creation** (`createTerminals`: 10 per warehouse, one district each). |
| `Worker.java` | One terminal thread: `run()` loop (fetch → keying → `doWork` → think → record), `doWork()` retry loop and error classification, `executeWork()` district selection, keying/think time tables. |
| `Loader.java` | Initial data generation: `ITEM` thread, per-warehouse threads, FK thread; batched inserts with retries; `unload()` (drop tables). |
| `Procedure.java` | Abstract transaction base; `getPreparedStatement`; `UserAbortException` (expected rollback). |
| `SQLStmt.java` | SQL text wrapper (with OLTPBench's `??` expansion, unused here). |
| `InstrumentedSQLStmt.java` | SQL plus a shared HdrHistogram for per-statement latency. |
| `TransactionType.java`, `TransactionTypes.java` | Id ↔ procedure class mapping (id = position in the XML, 1-based; 0 = INVALID). |

## `benchmarks/tpcc/`: TPC-C specifics

| File | Responsibility |
|------|----------------|
| `procedures/NewOrder.java` | NewOrder: set-based item/stock reads, `updatestockN` call, multi-row order lines. Also holds a NewOrder unit-test harness (`test()`) that the CLI cannot reach. |
| `procedures/Payment.java` | Payment: by-name/by-id customer lookup, warehouse/district `UPDATE ... RETURNING`, BC credit handling, history insert. |
| `procedures/OrderStatus.java` | OrderStatus: customer lookup, newest order, its order lines (read-only). |
| `procedures/Delivery.java` | Delivery: per-district oldest new order → delete → carrier → delivery date → customer balance; **commit per district**. |
| `procedures/StockLevel.java` | StockLevel: `d_next_o_id` plus the `getstockcounts` function call (autocommit). Also defines that function's DDL. |
| `TPCCConfig.java` | Scale constants: 100k items, 10 districts/warehouse, 3000 customers/district; `INVALID_ITEM_ID = -12345`. |
| `TPCCConstants.java` | Table names. |
| `TPCCUtil.java` | NURand and its constants, random strings, last-name syllables, `getRandomWarehouseId` (geo-aware). |
| `JsonMetricsHelper.java` | Builds and writes `results/json/output.json`; JSON merge. |
| `pojo/*.java` | Row holders used by the loader (`Customer`, `Stock`, ...) and `TpccRunResults` (the JSON schema). |

## `schema/`: DDL generation

| File | Responsibility |
|------|----------------|
| `TPCCTableSchemas.java` | **Single source of truth for table columns, primary keys (YB `HASH` vs PG) and partition keys.** Static cache built for the first dbType. |
| `TableSchema.java`, `Column.java` | Schema model and builder. |
| `Table.java` | `getCreateDdl` (abstract), `getDropDdl`, `getInsertDml` (`INSERT INTO t VALUES (?, ...)`, in column order). |
| `SchemaManager.java`, `SchemaManagerFactory.java` | Abstract create/indexes/FK/procedures API; factory picks default or geo. |
| `defaultschema/DefaultSchemaManager.java` | Tables, the 2 secondary indexes, 10 FKs (`NOT VALID`), `updatestock1..15` CTE procedures, `getstockcounts`. |
| `defaultschema/DefaultTable.java` | `CREATE TABLE ... (cols, PRIMARY KEY ...) [TABLESPACE t]`. |
| `geopartitioned/GeoPartitionedSchemaManager.java` | Tablespaces, partitioned tables, per-partition indexes/FKs/procedures plus the routing procedure. |
| `geopartitioned/PartitionedTable.java` | `PARTITION BY RANGE` parent (`SPLIT INTO 1 TABLETS`) and children `FOR VALUES FROM (a) TO (b) TABLESPACE ...`. |

## `jdbc/`, `types/`, `util/`, `test/`

| File | Responsibility |
|------|----------------|
| `jdbc/InstrumentedPreparedStatement.java` | Times `execute*` into a histogram; global on/off switch. |
| `types/State.java` | Benchmark states. |
| `types/TransactionStatus.java` | `SUCCESS`, `USER_ABORTED`, `RETRY`, `UNKNOWN`. |
| `types/SortDirectionType.java` | (legacy) |
| `util/GeoPartitionPolicy.java`, `PlacementPolicy.java`, `PlacementBlock.java` | Geo-partition model; the placement policy serializes (Gson) to YugabyteDB `replica_placement` JSON. |
| `util/RandomGenerator.java` | Random strings; `fastNumber` uses `ThreadLocalRandom`. |
| `util/LatencyMetricsUtil.java` | Average/p99 (µs → ms) used by every result table. |
| `util/ThreadUtil.java` | `runNewPool` (loader thread pool with error latch), `sleep`. |
| `util/FileUtil.java` | Next free filename (`oltpbench.N.csv`), mkdirs. |
| `util/Histogram.java`, `StringUtil.java`, `StringBoxUtil.java`, `Pair.java`, `LatchedExceptionHandler.java`, `InvalidUserConfiguration.java` | Small helpers. |
| `test/TestLoadedCluster.java` | (legacy) unfinished data checks; not runnable. |

## Where to make common changes

| I want to... | Edit |
|--------------|------|
| Change a transaction's SQL or logic | `benchmarks/tpcc/procedures/<Txn>.java` |
| Change table columns, keys, sharding | `schema/TPCCTableSchemas.java`. Update `Loader` (insert column order) and any procedure SQL that uses the columns. |
| Add or modify indexes, FKs, server-side procedures | `schema/defaultschema/DefaultSchemaManager.java` **and** `schema/geopartitioned/GeoPartitionedSchemaManager.java` |
| Change data generation | `api/Loader.java`, `benchmarks/tpcc/TPCCUtil.java` |
| Change retry or error handling | `api/Worker.doWork` |
| Change keying/think times or district choice | `api/Worker` (`getKeyingTimeInMillis`, `getThinkTimeInMillis`, `executeWork`) |
| Add a CLI flag | `CommandLineOptions` (definition and getter), then wire it into `DBWorkload.main` → `WorkloadConfiguration` |
| Add an XML option | `ConfigFileOptions` getter, `WorkloadConfiguration` field and default, `DBWorkload.main` wiring, `config/*.xml`, `docs/CONFIGURATION.md` |
| Add an output metric | `DBWorkload.Print*` (stdout), `JsonMetricsHelper` and `pojo/TpccRunResults` (JSON), and the merge logic if needed |
| Bump a dependency | `download-deps.sh` (used by the build), `ivy.xml`, `pom.xml` |
