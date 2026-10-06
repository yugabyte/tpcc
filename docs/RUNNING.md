# Building & Running

- [Prerequisites](#prerequisites)
- [Build](#build)
- [Single-client run](#single-client-run)
- [Multi-client (sharded) run](#multi-client-sharded-run)
- [Geo-partitioned run](#geo-partitioned-run)
- [PostgreSQL baseline](#postgresql-baseline)
- [Sizing guidance](#sizing-guidance)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- **JDK 8+** (the code uses `java.util.Optional` and lambdas; it is commonly run on JDK 11/17).
- **Apache Ant** for the standard build (Ivy is no longer needed to resolve dependencies).
- `bash` and `curl` (used by `download-deps.sh`).
- A reachable **YugabyteDB** cluster (YSQL, default port 5433) or **PostgreSQL**.

## Build

```bash
ant bootstrap   # optional now: installs ivy.jar into ~/.ant/lib (no longer used by 'resolve')
ant resolve     # runs ./download-deps.sh: fetches every jar from Maven Central into lib/
ant build       # compiles src/ into build/
ant clean       # removes build/
```

`download-deps.sh` downloads one jar at a time, with up to 3 attempts per jar (5 s apart). It skips
jars already in `lib/` and stops at the first jar that still fails.
To add or bump a dependency, edit **`download-deps.sh`** (what the build actually uses), and keep
`ivy.xml` and `pom.xml` in step.

Without Ant:

```bash
./download-deps.sh
mkdir -p build/META-INF && cp src/META-INF/persistence.xml build/META-INF/
javac -nowarn -d build -cp "lib/*" $(find src -name '*.java')
```

`./tpccbenchmark` runs `java -Xmx8G -cp <build + lib/*.jar> -Dlog4j.configuration=log4j.properties
com.oltpbenchmark.DBWorkload $@`. `$@` is unquoted, so arguments containing spaces (e.g. a config
path) get split. **Run it from the repo root**: the classpath and the log4j
path are relative. To change the heap, edit `memory=` in the script.

Don't use the `ant execute` or `ant dialects-export` targets. They are left over from OLTPBench
and pass a `-b` flag that no longer exists.

## Single-client run

```bash
# 1. Schema + indexes + stored procedures (drops existing tables!)
./tpccbenchmark -c config/workload_all.xml --create=true  --nodes=10.0.0.1,10.0.0.2,10.0.0.3

# 2. Initial data (FKs are added at the end)
./tpccbenchmark -c config/workload_all.xml --load=true    --nodes=... --warehouses=100 --loaderthreads=48

# 3. Benchmark
./tpccbenchmark -c config/workload_all.xml --execute=true --nodes=... --warehouses=100 \
                --warmup-time-secs=300 --num-connections=100
```

Notes:

- **`--warehouses` must be the same in load and execute.** Execute does not check it against the
  data.
- **Execute changes the data.** Orders, order lines and history grow; `NEW_ORDER` shrinks and
  grows. For comparable runs, `--create` and `--load` again (or restore a snapshot).
- Results go to `results/oltpbench[.N].csv` and `results/json/output.json` (overwritten each run),
  plus the summary on stdout. See [METRICS_AND_RESULTS.md](METRICS_AND_RESULTS.md).
- `--clear=true` drops all TPC-C tables.

## Multi-client (sharded) run

Split the warehouse range across N client machines. Each client loads and drives its own slice:

```bash
# once, from any client
./tpccbenchmark --create=true --nodes=<all tservers>

# on client i (0-based), each with its own slice:
./tpccbenchmark --load=true --nodes=<subset> \
    --total-warehouses=10000 --warehouses=2500 --start-warehouse-id=$((i*2500+1)) \
    --loaderthreads=32 --initial-delay-secs=$((i*20))

# once, after ALL loads finish (setting --start-warehouse-id disabled automatic FKs)
./tpccbenchmark --enable-foreign-keys=true --nodes=<any>

# on client i, run concurrently:
./tpccbenchmark --execute=true --nodes=<subset> \
    --total-warehouses=10000 --warehouses=2500 --start-warehouse-id=$((i*2500+1)) \
    --num-connections=200 --warmup-time-secs=$((1320 - i*30)) --initial-delay-secs=$((i*30))
```

- Only the client with `--start-warehouse-id=1` loads `ITEM`.
- Staggering `--initial-delay-secs` while **shrinking** `--warmup-time-secs` by the same amount
  makes every client start *measuring* at the same moment. This is what
  `run_scripts/run_tpcc_on_clients.cpp` does (22 min warmup, 30 s stagger).
- Aggregate: sum the per-client `TPM-C` lines, e.g.
  `grep -i "tpm-c" /tmp/*execute*txt | awk '{print $4}' | paste -s -d+ - | bc`,
  or copy every client's `results/json/output.json` (renamed) into one directory and run
  `./tpccbenchmark --merge-json-results=true --dir=<dir>`.

### `run_scripts/` automation

See [`run_scripts/Readme.md`](../run_scripts/Readme.md). In short:

1. `clients.txt` holds client IPs. `yb_nodes.txt` holds YB node IPs, **masters first** (the first 3
   are skipped when `ignore_masters=true`).
2. `SSH_USER`, `SSH_ARGS`, `SCP_ARGS` are exported, then `./setup_clients.sh` runs. It installs
   java, wget and tmux with `yum`, downloads the **release 1.9** tarball, and installs
   `limits.conf` (nofile 1048576, nproc 40000).
3. Edit the constants at the top of `run_tpcc_on_clients.cpp` (warehouses, IPs per client,
   connections, delays, SSH user and key), then compile with
   `g++ --std=c++11 run_tpcc_on_clients.cpp -o run_tpcc_on_clients`.
4. `./run_tpcc_on_clients create|create-procedures|load|enable-foreign-keys|execute|kill [suffix]`.
   Output goes to `/tmp/<client-ip>_<stage>_<suffix>.txt` on the machine you run it from. `<stage>`
   is one of `create`, `create-procedures`, `loader`, `enable-foreign-keys`, `execute`, `kill`.

The script downloads a pinned **release tarball (1.9)**, not your working tree. To benchmark local
changes, build and copy your own `tpcc.tar.gz` (the `scp` line is commented out in
`setup_clients.sh`).

## Geo-partitioned run

1. Set `enableGeoPartitionedWorkload=true`, `numberOfPartitions`, and one tablespace per partition
   (plus the ITEM and parent-table tablespaces) in a copy of `config/geopartitioned_workload.xml`.
   Zone names must match the cluster's placement info.
2. Pass `-gpc <file>` and the **same** `--total-warehouses` to every command (create, load,
   execute). The partition math depends on it.
3. Give each client a slice inside one partition (`--start-warehouse-id`, `--warehouses`), and
   point its `--nodes` at that partition's region.

See [CONFIGURATION.md](CONFIGURATION.md#geo-partitioning-config) for the rules, and the known
issue about the last partition's `updatestock` routing in
[CODE_REVIEW_GUIDE.md](CODE_REVIEW_GUIDE.md#known-issues--tech-debt).

## PostgreSQL baseline

```bash
./tpccbenchmark -c config/workload_all_pg.xml --create=true --nodes=<pg-host>
./tpccbenchmark -c config/workload_all_pg.xml --load=true   --nodes=<pg-host> --warehouses=100
./tpccbenchmark -c config/workload_all_pg.xml --execute=true --nodes=<pg-host> --warehouses=100
```

With `dbtype=postgres`: non-hash primary keys, `org.postgresql` data source, and no
`yb_enable_expression_pushdown`.

## Sizing guidance

- **Terminals** = 10 × warehouses per client. Each is a Java thread, so thousands of threads per
  JVM are normal. Raise `nproc`/`nofile` limits (see `run_scripts/limits.conf`).
- **Connections**: the default is `min(warehouses, 200 × nodes)`. With spec keying and think
  times, about 1 connection per warehouse is typically enough. Too few connections show up as
  high "Connection Acq Latency". Too many cause contention and server memory pressure.
- **Heap**: 8 GB by default (`tpccbenchmark`). Latency samples are kept in memory for the whole
  run (one object per transaction), so long, high-throughput runs need more.
- **Loader threads**: one warehouse per thread. Balance against cluster write capacity. Loader
  memory grows with `batchSize × (maxLoaderRetries + 1)` per thread.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `Must specify directory with results to merge` | No mode flag given, or a mode flag whose value isn't `true` (e.g. `--create=yes`). |
| `MissingArgumentException: Missing argument for option: create` | A mode flag was given without a value. Write `--create=true`, not `--create`. |
| `Failed to initialize JDBC driver` | `<driver>` class isn't on the classpath. Run `ant resolve` or `./download-deps.sh`. |
| `procedure updatestockN(...) does not exist` / `function getstockcounts` missing | `--create-sql-procedures=true` (or `--create=true`) wasn't run, e.g. after a manual schema restore. |
| Load ends "Finished!" but row counts are low | Loader exceptions are logged (`Failed to load data for TPC-C`) and **swallowed**. Grep the log; check `SELECT count(*)` per table. |
| Efficiency far below 100% with few errors | Not enough connections (check conn-acquire latency), an overloaded client (check Keying/Thinking worker task latencies against the spec means), or slow DB latency. |
| Many `40001` retries | Hot-row contention (district/warehouse). Normal to a point. Compare the first-attempt failure % across builds. |
| `NewOrder delete failed. Not running with SERIALIZABLE isolation?` (Delivery) | Two Deliveries raced on the same district. The second one fails, which is expected under snapshot isolation. |
| Execute aborts with `Unexpected fatal, error in 'Worker<N>' when executing ...` and the JVM exits | A worker could not *get or close* a connection (Hikari timeout after `hikariConnectionTimeoutMs`, or the node is unreachable). This error is not retried: it goes to the uncaught-exception handler, which calls `System.exit(-1)`. Check `--num-connections`, server connection limits and node health. |
| Running with `-ea` throws `AssertionError`s | JVM assertions are off by design. Several asserts are stale (e.g. `Procedure.getPreparedStatement`). |
