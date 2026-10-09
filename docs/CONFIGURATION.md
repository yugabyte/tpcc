# Configuration Reference

There are three sources of settings:

1. **Command-line flags**: the mode (create/load/execute/...), scale, nodes, connections, warmup.
   Parsed in `src/com/oltpbenchmark/CommandLineOptions.java`.
2. **Workload XML** (`-c`, default `config/workload_all.xml`): database connection, mix, runtime,
   retries. Parsed in `ConfigFileOptions.java` (XPath keys).
3. **Geo-partitioning XML** (`-gpc`, default `config/geopartitioned_workload.xml`). Read in every
   mode except `--help` and the merge modes, so the file must exist even when geo-partitioning is off. Parsed in
   `GeoPartitionedConfigFileOptions.java`.

All three are combined into a single `WorkloadConfiguration` in `DBWorkload.main`.

> "Code default" means the value used when the key or flag is absent. It can differ from the
> value in the sample XML, so always check both.

---

## Command-line flags

Mode flags take an explicit `=true`. One mode runs per invocation. If several are set, the first
match in this order wins: `help > clear > create > load > execute > enable-foreign-keys >
create-sql-procedures > merge-json-results > merge-results`.

| Flag | Meaning |
|------|---------|
| `-h`, `--help` | Print help. The workload XML is still parsed first, so it must be valid. |
| `--create=true` | Drop and re-create all tables and indexes, then create the SQL procedures and functions. |
| `--load=true` | Load the initial data (and add FKs afterwards, see `enableForeignKeysAfterLoad`). |
| `--execute=true` | Run the benchmark. |
| `--clear=true` | `DROP TABLE ... CASCADE` all 9 tables (it does not truncate them). |
| `--enable-foreign-keys=true` | Drop, then add, all FK constraints (`NOT VALID`). Use after a multi-client load. |
| `--create-sql-procedures=true` | (Re)create `updatestock1..15` and `getstockcounts`. Also honored *alongside* another mode flag. |
| `--merge-results=true --dir=<d>` | Merge per-client `results/*.csv` files in `<d>`. See [METRICS_AND_RESULTS.md](METRICS_AND_RESULTS.md). |
| `--merge-json-results=true --dir=<d>` | Merge per-client JSON results in `<d>` into `results/json/output.json`. |

> If no mode flag is given, the parser falls through to merge-results and fails with
> "Must specify directory with results to merge". Always pass a mode.

Scale and topology:

| Flag | Code default | Meaning |
|------|--------------|---------|
| `-c`, `--config <file>` | `config/workload_all.xml` | Workload XML |
| `-gpc`, `--geopartitioned-config <file>` | `config/geopartitioned_workload.xml` | Geo-partitioning XML |
| `--nodes=<ip1,ip2,...>` | `127.0.0.1` | YSQL/PostgreSQL endpoints. One Hikari pool per node; workers are assigned round-robin. |
| `--warehouses=<n>` | `10` | Warehouses **this client** loads/drives. Terminals = `10 * n`. |
| `--start-warehouse-id=<id>` | `1` | First warehouse of this client's slice. **Setting it at all disables automatic FK creation during load.** `ITEM` is loaded only when it is 1. |
| `--total-warehouses=<n>` | `= --warehouses` | Warehouses across all clients. Used to pick remote warehouses and for geo-partition math. |
| `--loaderthreads=<n>` | `min(10, warehouses)` | Concurrent loader threads (one warehouse per thread). |
| `--num-connections=<n>` | `min(warehouses, 200 * nodes)` | Total pooled connections for execute, split evenly across nodes (`ceil(n / nodes)` per pool). Raised to at least `loaderthreads`. |
| `--warmup-time-secs=<s>` | `0` | Warmup before measurement starts. A terminal cycle is recorded only if the state is MEASURE at the end of its think-time sleep (see [METRICS_AND_RESULTS.md](METRICS_AND_RESULTS.md#what-gets-recorded)). |
| `--initial-delay-secs=<s>` | none | Sleep before starting (staggers multi-client runs). |
| `-im`, `--interval-monitor=<ms>` | `0` (off) | Print instantaneous throughput every `<ms>` milliseconds. |
| `--vv` | off | Also print the FAILURE LATENCIES and RETRY ATTEMPTS tables. |
| `--histograms` | — | Currently a no-op (see known issues). |
| `--output-raw`, `--output-samples` | — | Declared but unused. |

The flags `-o`, `-s`, `-v` and `--runscript` that older README text mentions **do not exist**.

---

## Workload XML (`config/workload_all.xml`)

```xml
<parameters>
    <dbtype>yugabyte</dbtype>              <!-- yugabyte | postgres -->
    <driver>com.yugabyte.Driver</driver>
    <port>5433</port>
    <username>yugabyte</username>
    <DBName>yugabyte</DBName>
    <password></password>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    ...
    <transactiontypes> ... </transactiontypes>
    <runtime>1800</runtime>
    <rate>10000</rate>
    <maxRetriesPerTransaction>2</maxRetriesPerTransaction>
    <maxLoaderRetries>2</maxLoaderRetries>
</parameters>
```

### Connection

| Key | Code default | Sample | Notes |
|-----|--------------|--------|-------|
| `dbtype` | *(required)* | `yugabyte` | `yugabyte` selects the smart-driver data source, `HASH` primary keys and `SET yb_enable_expression_pushdown`. **Any other value** is treated as PostgreSQL. |
| `driver` | *(required)* | `com.yugabyte.Driver` | Loaded with `Class.forName` as a sanity check. Use `org.postgresql.Driver` for PostgreSQL. |
| `port` | `5433` | `5433` | Use `5432` for stock PostgreSQL. |
| `username` / `password` / `DBName` | — | `yugabyte` / empty / `yugabyte` | |
| `isolation` | `TRANSACTION_SERIALIZABLE` | `TRANSACTION_REPEATABLE_READ` | Also `TRANSACTION_READ_COMMITTED` and `TRANSACTION_READ_UNCOMMITTED`. Set on the Hikari pool. Unknown values print a warning and keep the default. |
| `sslCert` / `sslKey` | empty | empty | A non-empty `sslCert` means `sslmode=require` plus the cert and key. |
| `jdbcURL` | empty | empty | Replaces the generated URL **only for the un-pooled connections** used by create, load, FK and procedure steps. Every such connection then uses this URL, with no round-robin over `--nodes`. **The execute/clear Hikari pools ignore it**: they set `dataSourceClassName`, and Hikari logs "using dataSourceClassName and ignoring jdbcUrl". They always connect to each `--nodes` host. See K19. |
| `hikariConnectionTimeoutMs` | `60000` | `180000` | How long a terminal waits for a pooled connection. |

### Workload shape

| Key | Code default | Sample | Notes |
|-----|--------------|--------|-------|
| `transactiontypes/transaction/{name,weight}` | *(required)* | 45/43/4/4/4 | **Keep the order** NewOrder, Payment, OrderStatus, Delivery, StockLevel. Reporting code assumes it. |
| `runtime` | *(required)* | `1800` | Measurement seconds (after warmup). Also the tpmC denominator. |
| `rate` | *(required)* | `10000` | Requests per second offered to the shared queue. Leave it high: with keying/think on, terminals limit throughput. |
| `useKeyingTime` | `true` | `true` | Spec keying delays (18/3/2/2/2 s). |
| `useThinkTime` | `true` | `true` | Spec think delays (mean 12/12/10/5/5 s). |
| `maxRetriesPerTransaction` | `0` | `2` | Extra attempts for **NewOrder only**. (The XML comment above it says "set to 0", but the value is 2.) |
| `useStoredProcedures` | `true` | `true` | NewOrder stock update via `CALL updatestockN`. `false` uses a batched `UPDATE` path that has a known bug, see [CODE_REVIEW_GUIDE.md](CODE_REVIEW_GUIDE.md). |
| `trackPerSQLStmtLatencies` | **`true`** | `false` | Per-statement HdrHistograms, printed after the run. Note that the code default is *on*. |
| `displayEnhancedLatencyMetrics` | — | `false` | Not read by the code. |

### Loading

| Key | Code default | Sample | Notes |
|-----|--------------|--------|-------|
| `batchSize` | `128` | `128` | Rows per JDBC batch and per commit. |
| `maxLoaderRetries` | `0` | `2` | Retries per failed batch. Memory used by loader batches scales with `maxLoaderRetries + 1`. |
| `enableForeignKeysAfterLoad` | `true` | `true` | `true`: add FKs after all warehouses are loaded. `false`: the ITEM loader thread adds the FKs at the start of the load, running concurrently with the warehouse loader threads (slower). Either way, nothing happens if `--start-warehouse-id` was given. |

### Variants shipped

- `config/workload_all.xml`: YugabyteDB (smart driver, port 5433).
- `config/workload_all_pg.xml`: PostgreSQL (`org.postgresql.Driver`, port 5432, user `postgres`).
  It contains a placeholder password. Don't commit real credentials.

---

## Geo-partitioning config

`config/geopartitioned_workload.xml` (shipped with `enableGeoPartitionedWorkload=false`):

```xml
<parameters>
    <enableGeoPartitionedWorkload>true</enableGeoPartitionedWorkload>
    <numberOfPartitions>2</numberOfPartitions>
    <tablespaces>
        <tablespace>
            <name>tablespace0</name>
            <storeItemTable>true</storeItemTable>          <!-- ITEM lives here -->
            <storePartitionedTables>true</storePartitionedTables> <!-- parent tables -->
            <storePartitions>false</storePartitions>
            <replicationFactor>1</replicationFactor>
            <placementPolicy>
                <placementBlock>
                    <cloud>aws</cloud><region>us-west-2</region><zone>us-west-2a0</zone>
                    <minReplicationFactor>1</minReplicationFactor>
                </placementBlock>
            </placementPolicy>
        </tablespace>
        <tablespace>  <!-- one per partition, in partition order -->
            <name>tablespace1</name>
            <storePartitions>true</storePartitions>
            ...
        </tablespace>
    </tablespaces>
</parameters>
```

Validation (all enforced at start-up, failures throw `InvalidUserConfiguration`):

- `total-warehouses % numberOfPartitions == 0`.
- This client's slice `[start, start + warehouses)` must fall within **one** partition.
- At least one tablespace each with `storePartitionedTables`, `storeItemTable` and
  `storePartitions`.
- The number of `storePartitions` tablespaces must be at least `numberOfPartitions`. Partition `i`
  (1-based) goes in the i-th such tablespace.
- For each tablespace, the sum of `minReplicationFactor` must not exceed `replicationFactor`.

Each tablespace is created as
`CREATE TABLESPACE <name> WITH (replica_placement='{"num_replicas":N,"placement_blocks":[...]}')`.
