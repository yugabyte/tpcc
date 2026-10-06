# YugabyteDB-Specific Changes

This repo started (Jan 2020) as a fork of [OLTPBench](https://github.com/oltpbenchmark/oltpbench).
Everything except TPC-C was removed, and the code was reworked to benchmark a **distributed SQL
database (YugabyteDB YSQL)** at scale: hundreds of thousands of warehouses, many client machines,
and multi-region clusters. PostgreSQL is still supported as a baseline (`<dbtype>postgres</dbtype>`).

This document explains **what changed relative to stock OLTPBench / a textbook TPC-C, and why**,
grouped by theme. Commit hashes and PR numbers are given so you can read the original diffs
(`git show <hash>`).

> **Rule of thumb for contributors:** most of these changes exist to cut **client↔server round
> trips** and **cross-node transaction conflicts**. In a distributed database each round trip may
> cross the network to another node, and conflicting writes cause retries instead of lock waits.
> A change that adds round trips to a hot transaction, or adds contention on a hot row, will show
> up directly in tpmC and p99 latency.

---

## 1. Schema & data placement

| Change | Where | Why |
|--------|-------|-----|
| **Hash-sharded composite primary keys** for YugabyteDB, e.g. `((ol_w_id, ol_d_id) HASH, ol_o_id, ol_number)`, `(s_w_id HASH, s_i_id ASC)` (993a156, then generated per dbtype in #150) | `schema/TPCCTableSchemas.java` | Rows for one `(warehouse, district)` live together in one tablet. Range scans on the remaining key columns (lowest new order, last 20 orders, order lines of one order) are single-tablet and ordered. Data spreads evenly as warehouses grow. |
| **Secondary indexes with hash prefixes**: `idx_customer_name ((c_w_id,c_d_id) HASH, c_last, c_first)`, `idx_order ((o_w_id,o_d_id) HASH, o_c_id, o_id DESC)` | `schema/defaultschema/DefaultSchemaManager.createIndexes` | Customer-by-last-name and newest-order-of-customer lookups become a single-tablet seek. `o_id DESC` makes `ORDER BY o_id DESC LIMIT 1` a forward scan. |
| **PostgreSQL keeps plain B-tree keys** (#150) | same | `HASH` is YugabyteDB-only syntax. `dbtype` chooses the variant. |
| **`NOT NULL` removed from column definitions** (#142) | `TPCCTableSchemas.java` | Part of the "conflict fix" change set. Primary key columns are still implicitly non-null. |
| **Foreign keys added after load, as `NOT VALID`** (0fbbed8 "Deferred foreign key checks", #55, #135) | `Loader`, `SchemaManager.enableForeignKeyConstraints` | Loading without FKs is much faster (no per-row parent lookups across nodes). `NOT VALID` skips re-validating billions of loaded rows, while new writes during the run are still checked. #135 split "drop" and "add" into separate steps so a failed drop doesn't block the add. |
| **FK on `DISTRICT` and `STOCK`** (#55) | same | Adds the remaining relationships (District→Warehouse, Stock→Warehouse/Item). |

## 2. Fewer round trips per transaction

| Change | Where | Why |
|--------|-------|-----|
| **`UPDATE ... RETURNING`** for `district` (NewOrder, Payment), `warehouse` (Payment), `oorder` (Delivery) (#117) | `procedures/*.java` | Replaces a `SELECT` + `UPDATE` pair with one statement, one round trip. |
| **Set-based NewOrder reads**: `SELECT ... FROM item WHERE i_id IN (...)` and `SELECT ... FROM stock WHERE s_w_id=? AND s_i_id IN (...)`, one pair per supplying warehouse (#45) | `NewOrder.getItemsAndStock` | 5–15 point reads become 2 statements. Each worker keeps 15 SQL text variants (1..15 placeholders) and prepares the right one per call; the driver's statement cache handles server-side reuse. |
| **Multi-row `INSERT` into `ORDER_LINE`** (#45) | `NewOrder.insertOrderLines` | 5–15 inserts become 1 statement. 11 variants (5..15 rows). |
| **Stock updates via stored procedures** `updatestock1..15` (#50) | `DefaultSchemaManager.createSqlProcedures`, `NewOrder.updateStockUsingProcedures` | N `UPDATE`s on one warehouse's stock become one `CALL`. Toggle: `<useStoredProcedures>`. |
| **Procedures rewritten as one CTE chain** `WITH update_cte1 AS (UPDATE ...), ... SELECT 1` (#142, issue #125) | same | One statement instead of N separate statements inside the procedure. Fewer RPCs inside the server and fewer intra-transaction conflicts. |
| **StockLevel as a server-side function** `getstockcounts` (#75, #76) | `StockLevel.InitializeGetStockCountProc` | The join of `ORDER_LINE` (last 20 orders) and `STOCK` runs inside the database in one call. #76 made StockLevel run in autocommit, so each statement gets its own snapshot (spec 2.8.2.3). |
| **Removed `FOR UPDATE`** from NewOrder selects on `district`/`stock` (#60, #102) | `NewOrder` | The rows are written later in the same transaction anyway. Under snapshot (`REPEATABLE READ`) isolation the write conflict is detected at update time, so explicit row locks only added cost. |
| **`FOR KEY SHARE` added (#112) and reverted (#131)** on NewOrder's customer and stock reads | `NewOrder` | #112 tried to avoid redundant FK-check reads by taking key-share locks up front. It slowed the `SELECT`s down and was reverted. Don't re-add it without measurements. |
| **`SET yb_enable_expression_pushdown to on`** on every connection checkout (#142; guarded by dbtype in #150) | `Worker.doWork` | Lets YugabyteDB evaluate filter expressions in DocDB (the storage layer) instead of the PostgreSQL layer, which reduces data shipped between layers. Errors from this statement are swallowed (since #148). It shares one `try` block with `setAutoCommit(false)`, so a failing `SET` also leaves the transaction in autocommit (K12). |

## 3. Contention, conflicts and retries

In YugabyteDB, concurrent conflicting transactions fail fast with serialization errors
(SQLSTATE `40001`). In PostgreSQL they would mostly wait on locks instead. The tool was changed to
treat these errors as a normal part of the workload.

| Change | Where | Why |
|--------|-------|-----|
| **Transaction retries** (#72, then #134, #148) | `Worker.doWork` | Failed transactions used to be counted as successes (#72 fixed this). Now an `SQLException` with a SQLState, and any other `Exception` or `Error`, is classified `RETRY`, and NewOrder is re-attempted up to `<maxRetriesPerTransaction>` times (only the `SQLException` path rolls back first, see K20). #134: rollback failures are ignored (the node may be down during smart-driver failover tests), and internal errors (`XX000`) are retried too. #148: `Error`/`Exception` became retryable instead of fatal, the `SET`/`setAutoCommit` block was wrapped in `catch (Throwable)`, and intermediate attempts log only the message. The final-attempt log concatenates `ex.getStackTrace()`, which prints an array reference rather than a trace (K22). |
| **Only NewOrder is retried** (e944a84, #111) | `Worker.doWork` | The other transactions record a failure and move on. |
| **Warn vs error logging for retries** (#154) | `Worker.doWork` | Expected contention errors (`40001`, `53200`, `XX000`) are logged only at DEBUG. Other errors log `WARN` until attempt == `maxRetriesPerTransaction + 1`, then `ERROR`. The check doesn't look at the transaction type, so a failed non-NewOrder transaction logs a misleading "Retrying..." WARN even though it isn't retried. |
| **Delivery split into one transaction per district** (#106) | `Delivery.run` | Committing per district keeps each database transaction short, which shrinks the window for conflicts. |
| **Isolation configured on the Hikari pool** (#98) | `BenchmarkModule.createDataSource` | Avoids a `SET TRANSACTION ISOLATION` round trip at the start of every transaction. |
| **Default isolation `TRANSACTION_REPEATABLE_READ`** in the sample config | `config/workload_all.xml` | Snapshot isolation in YugabyteDB. If the XML omits `<isolation>`, the code falls back to `SERIALIZABLE`. |
| **`ThreadLocalRandom` for string generation** (#149) | `util/RandomGenerator.fastNumber` | The shared `Random` used by `TPCCUtil.randomStr` was a contention point across loader threads. |

## 4. Connectivity

| Change | Where | Why |
|--------|-------|-----|
| **Hikari connection pooling** (#25, #97) | `BenchmarkModule.createDataSource` | 10 terminals per warehouse would need, e.g., 1M connections for 100k warehouses. Terminals spend most of their time in keying/think sleeps, so they now borrow a pooled connection only to run a transaction. `maxLifetime=0` (never recycle). |
| **One pool per `--nodes` entry, round-robin** (#25; 5 s pause between pools from #36) | `BenchmarkModule.createDataSource` / `getDataSource` | Each worker is bound to one pool when it is created, spreading terminals across the listed endpoints. The 5 s pause between pools lets the server cache system catalog info. With `dbtype=yugabyte` the pool's `YBClusterAwareDataSource` load-balances by default, so a pool's `serverName` is only the seed and its physical connections may land on any tserver. |
| **≤ 200 connections per node by default** (#35) | `DBWorkload` (`min(warehouses, nodes*200)`) | Avoids exhausting server connection slots. Override with `--num-connections`. |
| **YugabyteDB JDBC smart driver** `com.yugabyte:jdbc-yugabytedb` (#133, #143, #144, now `42.3.5-yb-4` in #156) | `BenchmarkModule` | With `dbtype=yugabyte`, pools use `com.yugabyte.ysql.YBClusterAwareDataSource`, and load/DDL connections use `jdbc:yugabytedb://` URLs. The cluster-aware data source enables load balancing by default (topology is learned from the seed node) and is used for failover testing. |
| **Multiple drivers** (#150) | `BenchmarkModule`, `config/workload_all_pg.xml` | `dbtype=postgres` uses `org.postgresql.ds.PGSimpleDataSource` / `jdbc:postgresql://`. |
| **`reWriteBatchedInserts=true`** (0338a61) | both connection paths | Loader batches become multi-row `INSERT`s on the wire. |
| **TLS** (#66, #68) | `BenchmarkModule` | `<sslCert>`/`<sslKey>` mean `sslmode=require` plus the client cert and key. |
| **Full JDBC URL override** (#67) | `<jdbcURL>` | For URL-only options. Today it applies only to the un-pooled create/load/FK/procedure connections. The execute pools ignore it (K19). |
| **Removed `conn.getTransactionIsolation()` call** (#138) | `Worker` | It threw with the driver in debug logging mode and had no value. |

## 5. Scale-out benchmarking (multiple client machines)

A single JVM cannot drive hundreds of thousands of warehouses, so the warehouse range is split
across clients.

| Change | Where | Why |
|--------|-------|-----|
| **Sharded TPC-C** (#36): `--start-warehouse-id`, `--total-warehouses` | `DBWorkload`, `Loader`, `BenchmarkModule.createTerminals`, `TPCCUtil.getRandomWarehouseId` | Each client loads and drives warehouses `[start, start+warehouses)`. Remote warehouses (1% of NewOrder items, 15% of Payment customers) are drawn from **all** `total-warehouses`, so cross-client remote transactions still happen. Load/DDL use plain (non-pooled) connections. |
| **`ITEM` is loaded only by the client whose slice starts at warehouse 1** | `Loader.createLoaderThreads` | Avoids duplicate-key failures on the shared table. |
| **FKs are skipped when `--start-warehouse-id` is given** | `DBWorkload` (`setShouldEnableForeignKeys(false)`) | One client runs `--enable-foreign-keys=true` after **all** loaders finish. |
| **`--initial-delay-secs`** | `DBWorkload` | Staggers client start-up so connection storms and warmup overlap less. |
| **Orchestration scripts** (#103) | `run_scripts/` | `setup_clients.sh` installs the release tarball and raises ulimits. `run_tpcc_on_clients.cpp` fans out create/load/FK/execute/kill over SSH. |
| **Result merging** (#40, #113, #130, #132) | `DBWorkload.mergeResults`, `JsonMetricsHelper.mergeJsonResults` | Combine per-client CSV or JSON outputs into one tpmC/latency summary. |

## 6. Loading at scale

| Change | Where | Why |
|--------|-------|-----|
| **Batched load**, `<batchSize>` (#19, #39) | `Loader` | One commit per batch, with multi-row inserts. |
| **Loader retries**, `<maxLoaderRetries>` (#78) | `Loader.performInsertsWithRetries` | Retries transient failures (leader moves, tablet splits, timeouts) during multi-hour loads. |
| **One `PreparedStatement` per retry attempt** (#122) | `Loader.getInsertStatement` | Re-executing a statement after a failed batch can fail. Every row is added to all N statements, and the next statement is used on retry. This multiplies loader memory by `maxLoaderRetries + 1`. |
| **Lower memory** (#34) | `LatencyRecord` (`ALLOC_SIZE = 200`) | OLTPBench pre-allocated large per-thread latency arrays. With 10 terminals per warehouse that blew up the heap. |
| **Load of `NEW_ORDER` fixed** (7c3f1c7) | `Loader.loadOrders` | A batch carry-over bug lost the final district's `NEW_ORDER` rows. Now all 900 open orders per district are loaded, per spec. |

## 7. Geo-partitioned TPC-C (#114)

An optional mode (`config/geopartitioned_workload.xml`, `enableGeoPartitionedWorkload=true`) for
multi-region clusters:

- Every warehouse-scoped table becomes a `PARTITION BY RANGE (<w_id column>)` parent (stored in a
  single tablet via `SPLIT INTO 1 TABLETS`) with `numberOfPartitions` child tables
  (`STOCK1`, `STOCK2`, ...). Each child is placed in its own **tablespace** with a YugabyteDB
  `replica_placement` policy (cloud/region/zone/min replicas), so a warehouse range lives in one
  region.
- `ITEM` is not partitioned and lives in a designated tablespace.
- Indexes, FKs and `updatestockN_<partition>` procedures are created per partition. A routing
  `updatestockN` procedure dispatches on `wid`.
- Remote warehouses are drawn **from the same partition** (`TPCCUtil.getRandomWarehouseId`), so
  transactions stay region-local.
- Each client must drive warehouses from exactly one partition (checked at start-up).

See [CONFIGURATION.md](CONFIGURATION.md#geo-partitioning-config) and the known issue about
`updatestock` routing in [CODE_REVIEW_GUIDE.md](CODE_REVIEW_GUIDE.md#known-issues--tech-debt).

## 8. Measurement & reporting

| Change | Where | Why |
|--------|-------|-----|
| Keying and think times per TPC-C 5.2.5 (#7) | `Worker` | OLTPBench had none. Required for a spec-shaped closed loop and for the efficiency metric. |
| District selection per spec (#10) | `Worker.executeWork` | Random district for NewOrder/Payment/OrderStatus; fixed terminal district for StockLevel (and Delivery's loop bound). |
| tpmC printed at the end (#23); trailing transactions after `runtime` excluded (#26) | `DBWorkload.PrintToplineResults` | |
| Latencies without wait times, connection-acquire latency (#37, #59) | `TransactionLatencyRecord` | Separate DB time from Hikari queueing. |
| Per-SQL-statement HdrHistograms, `<trackPerSQLStmtLatencies>` (#79) | `InstrumentedPreparedStatement` | Find which statement regressed. |
| Retry/failure tables only with `--vv` (#105) | `DBWorkload` | |
| Expected NewOrder rollbacks count toward tpmC (#107) | `Worker` (`USER_ABORTED`) | Spec-correct tpmC. |
| Worker task latency breakdown: fetch/keying/op/think (#110) | `WorkerTaskLatencyRecord` | Detects a client-side bottleneck (e.g. late keying wake-ups). |
| Only count work whose terminal cycle ends in MEASURE (#110); retry/failure counters measure-only too (#111) | `Worker.run` | The state is checked after the think sleep. |
| JSON output `results/json/output.json` and JSON merge (#113, #130) | `JsonMetricsHelper` | Machine-readable results for perf pipelines. |
| Number formatting fix (#120) | `DBWorkload` | `DecimalFormat` grouping separators (`1,234.5`) broke `Double.parseDouble`. |
| LatencyRecord chunk-boundary fix (4440767) | `LatencyRecord` | Fixed a crash when the sample count was a multiple of 200. |

## 9. Build & dependencies

| Change | Where | Why |
|--------|-------|-----|
| Ivy-based dependency resolution (#2), later replaced by **`download-deps.sh`** with sequential downloads and retries (#160) | `build.xml` (`ant resolve`), `download-deps.sh` | Ivy's parallel resolution kept failing on Maven Central rate limits. `ivy.xml` and `pom.xml` remain as dependency manifests (and for IDE import), but `ant resolve` now only runs the script. |
| `jdbc-yugabytedb` pinned to `42.3.5-yb-4`, `postgresql` `42.4.5`, `HikariCP` `3.4.5` | `download-deps.sh`, `ivy.xml`, `pom.xml` | Keep these three files in sync when bumping a version. |

---

## Practical notes for testing YugabyteDB with this tool

- Point `--nodes` at **YB-TServer** YSQL endpoints (default port 5433), not masters
  (`run_tpcc_on_clients.cpp` drops the first 3 IPs as masters).
- Expect `40001` serialization failures under load. They are normal. Watch the retry table
  (`--vv`): a growing first-attempt failure rate usually means hot-row contention (district,
  warehouse) or too many connections per warehouse.
- The tablet count, `ysql_max_connections`, and pre-splitting decide whether the cluster or the
  client is the bottleneck. Check worker task latencies: if "Keying"/"Thinking" are far above the
  configured means, the client JVM is overloaded.
- Server-side routines must exist before `--execute`: run `--create=true` (which creates them) or
  `--create-sql-procedures=true`.
