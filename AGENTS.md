# AGENTS.md

Guidance for AI coding agents (Claude Code, Codex, Cursor, Copilot, ...) and a quick orientation
for humans. Detailed docs live in [`docs/`](docs/README.md).

## What this repo is

A **TPC-C load generator for YugabyteDB** (and PostgreSQL as a baseline). It is a 2020 fork of
OLTPBench with every other benchmark removed, rewritten for distributed SQL: hash-sharded schema,
fewer round trips per transaction, retries for serialization conflicts, the YugabyteDB smart JDBC
driver, multi-client sharded runs and geo-partitioning. Its output (tpmC, efficiency, latency
percentiles, retry rates) feeds performance regression pipelines. **Changes that silently shift
those numbers are the worst bugs here.**

- Language: Java (single module, package root `src/com/oltpbenchmark`), about 11k lines.
- Build: Ant (`build.xml`); dependencies come from `download-deps.sh`.
- No automated tests and no CI. Validation is manual (see below).

## Commands

```bash
ant resolve                 # = ./download-deps.sh → jars into lib/ (sequential, retried)
ant build                   # compile src/ → build/
# no-Ant fallback:
mkdir -p build && javac -nowarn -d build -cp "lib/*" $(find src -name '*.java')

# run from the repo root (relative classpath and log4j path); mode flags need "=true"
./tpccbenchmark --help
./tpccbenchmark -c config/workload_all.xml --create=true  --nodes=127.0.0.1
./tpccbenchmark -c config/workload_all.xml --load=true    --nodes=127.0.0.1 --warehouses=2
./tpccbenchmark -c config/workload_all.xml --execute=true --nodes=127.0.0.1 --warehouses=2 --vv
./tpccbenchmark --merge-json-results=true --dir=<dir_with_json_files>
```

Other modes: `--clear=true` (drops the tables), `--enable-foreign-keys=true`,
`--create-sql-procedures=true`, `--merge-results=true --dir=...`. Multi-client runs use
`--start-warehouse-id`, `--total-warehouses` and `--initial-delay-secs`
([docs/RUNNING.md](docs/RUNNING.md)).

## Code map

| Area | Files |
|------|-------|
| Entry point, modes, result printing | `src/com/oltpbenchmark/DBWorkload.java` |
| CLI flags / XML options | `CommandLineOptions.java`, `ConfigFileOptions.java`, `GeoPartitionedConfigFileOptions.java` → `WorkloadConfiguration.java` |
| Connections, terminal creation | `api/BenchmarkModule.java` |
| Terminal loop, retries, keying/think | `api/Worker.java` |
| Rate/phase clock, states | `ThreadBench.java`, `WorkloadState.java`, `BenchmarkState.java`, `Phase.java` |
| The 5 transactions | `benchmarks/tpcc/procedures/{NewOrder,Payment,OrderStatus,Delivery,StockLevel}.java` |
| Data loading | `api/Loader.java`, `benchmarks/tpcc/TPCCUtil.java` |
| Schema, keys, indexes, FKs, server procedures | `schema/TPCCTableSchemas.java`, `schema/defaultschema/*`, `schema/geopartitioned/*` |
| JSON results | `benchmarks/tpcc/JsonMetricsHelper.java`, `pojo/TpccRunResults.java` |
| Configs | `config/workload_all.xml` (YB), `config/workload_all_pg.xml` (PG), `config/geopartitioned_workload.xml` |

Full file-by-file map: [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md).

## Rules for making changes

1. **Hot path = NewOrder + Payment (88% of the mix).** Don't add round trips (extra `SELECT`s,
   splitting multi-row statements), row locks (`FOR UPDATE`/`FOR KEY SHARE` were removed or
   reverted on purpose), or work on the hot rows (`WAREHOUSE`, `DISTRICT`) without before/after
   benchmark numbers.
2. **Schema changes go in both places:** both dbtypes (`yugabyte` HASH keys vs `postgres` in
   `TPCCTableSchemas`) and both managers (`DefaultSchemaManager` *and*
   `GeoPartitionedSchemaManager`). Loader inserts are **positional**, so keep `Loader` setters in
   `TPCCTableSchemas` column order.
3. **Preserve measurement semantics:** a terminal cycle is recorded only if the state is `MEASURE` after its think sleep; expected
   NewOrder rollbacks (`UserAbortException` → `USER_ABORTED`) count as successes; failures never
   do; only NewOrder retries; samples are µs and reports are ms.
4. **Transaction order is fixed:** NewOrder, Payment, OrderStatus, Delivery, StockLevel.
   `DBWorkload` hard-codes ids 1..5.
5. **JSON output field names are a public contract** (downstream pipelines parse
   `results/json/output.json`). Add fields; don't rename or remove them.
6. **Procedures are per-worker objects** with mutable `PreparedStatement` fields. Base SQL and all
   histograms are `static final` and shared. NewOrder's per-size SQL variants are per instance but
   reuse the shared histograms. Keep it that way.
7. **New options:** XML key → `ConfigFileOptions` getter + `WorkloadConfiguration` default +
   `DBWorkload` wiring + both sample XMLs + `docs/CONFIGURATION.md`. Don't change existing
   defaults silently: perf pipelines depend on them.
8. **Dependencies:** bump `download-deps.sh` (what the build uses), `ivy.xml` and `pom.xml`
   together.
9. **Logging:** nothing per-transaction above DEBUG on the success path. Expected contention errors
   (`40001` etc.) stay at DEBUG; other failures log WARN while attempts remain and ERROR on the
   final one.
10. Match the surrounding style: 2- or 4-space indentation as in the file you edit, the existing
    OLTPBench license headers, and short comments that explain *why*. Don't reformat files you
    aren't changing.

## Gotchas

- `config/geopartitioned_workload.xml` is parsed in every mode except `--help` and merge, even
  when geo is disabled. The
  workload XML is parsed even for `--help`.
- If no mode flag is given, the parser falls into merge-results and fails asking for `--dir`.
- Setting `--start-warehouse-id` at all disables FK creation during load, and `ITEM` is loaded
  only when it equals 1.
- Code defaults differ from the sample XML (isolation `SERIALIZABLE` vs `REPEATABLE_READ`; retries
  0 vs 2; `trackPerSQLStmtLatencies` true vs false). See [docs/CONFIGURATION.md](docs/CONFIGURATION.md).
- Loader exceptions are swallowed, so check row counts. A connection-acquire failure during
  execute calls `System.exit(-1)`.
- JVM assertions must stay **off**. Some asserts are stale (e.g. in `Procedure.getPreparedStatement`).
- `useStoredProcedures=false` uses a buggy stock-update path (swapped binds).
- Known bugs and tech debt are listed in
  [docs/CODE_REVIEW_GUIDE.md#known-issues--tech-debt](docs/CODE_REVIEW_GUIDE.md#known-issues--tech-debt).
  Don't "fix" them as a side effect of an unrelated change: they change the numbers.

## Validating a change

1. It compiles (`ant build` or the `javac` fallback).
2. Small smoke run on a local YugabyteDB (`yugabyted start`) and, for dbtype-dependent code,
   PostgreSQL: `--create=true`, `--load=true --warehouses=2`, then `--execute=true` with a copy of the config
   that sets `useKeyingTime`/`useThinkTime` to false and `runtime` to 60. Check that all 5
   transaction types have counts and there are no unexpected ERRORs (`--vv`).
3. Run the TPC-C consistency SQL in [docs/CODE_REVIEW_GUIDE.md](docs/CODE_REVIEW_GUIDE.md#3-how-to-validate-a-change).
4. For performance-affecting changes, compare baseline vs change on the same cluster and flags
   (tpmC, efficiency, NewOrder p99, Retry #0 %).

## Reviewing a change

Follow [docs/CODE_REVIEW_GUIDE.md](docs/CODE_REVIEW_GUIDE.md): its invariants and checklist are
written for AI and human reviewers. Prioritize: wrong numbers > added round trips/contention >
broken schema parity (YB/PG, default/geo) > everything else.

## Further reading

- [docs/TPCC_WORKLOAD.md](docs/TPCC_WORKLOAD.md): the workload, per-transaction SQL, spec deviations
- [docs/YUGABYTEDB_CHANGES.md](docs/YUGABYTEDB_CHANGES.md): what changed for YugabyteDB, and why
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the runtime design
- [docs/METRICS_AND_RESULTS.md](docs/METRICS_AND_RESULTS.md): interpreting output
