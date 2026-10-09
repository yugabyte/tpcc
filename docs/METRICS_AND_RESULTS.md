# Metrics & Results

`--execute=true` produces three outputs:

1. **Summary tables on stdout** (log4j, INFO).
2. **`results/oltpbench.csv`**: one row per successful transaction. If the file exists,
   `oltpbench.2.csv`, `oltpbench.3.csv`, ... are used instead.
3. **`results/json/output.json`**: machine-readable summary. **Overwritten** on every run.

All latencies in the summary tables and the JSON are in **milliseconds**. Raw CSV values are in
microseconds (latencies) or nanoseconds (timestamps).

## What gets recorded

`Worker.run()` decides once per terminal cycle (fetch → keying → transaction → think). The cycle
is recorded only if the benchmark state is `MEASURE` **at the end of its think-time sleep** and was `WARMUP` or `MEASURE` when the work was fetched (same phase). The
decision is made *after* the think sleep, which has two effects:

- A transaction that ran late in warmup **is** recorded if its think time ends after measurement starts.
- A transaction that finished during measurement is **dropped** if its think time runs past the end.

With think times of 5–12 s, this shifts the effective window by a few seconds at each end. Each
piece of work yields one or more *attempts*:

| Attempt status | Recorded in | Counts toward tpmC |
|----------------|-------------|--------------------|
| `SUCCESS` | latencies (success) | yes, if NewOrder |
| `USER_ABORTED` (NewOrder's expected 1% rollback) | latencies (success) | **yes** |
| `RETRY` / `UNKNOWN` (any failure) | failure latencies, and the retry counter `[txnType][attemptNo]` | no |

## stdout summary

### RESULTS

The example numbers on this page are illustrative: 100 warehouses, 1800 s.

```
================RESULTS================
             TPM-C |            1283.11
        Efficiency |             99.78%
Throughput (req/s) |              47.53
```

- **TPM-C** = recorded NewOrder successes × 60 / `<runtime>`.
- **Efficiency** = TPM-C / (12.86 × `--warehouses`) × 100. This uses the client's **own**
  warehouse count, so each client in a sharded run reports efficiency for its own slice.
- **Throughput** = all recorded successful transactions / measured wall time.

### LATENCIES (INCLUDE RETRY ATTEMPTS)

Per transaction type: count, average, p99 and average **connection acquisition** latency. Despite
the header, the latency here is the duration of the **successful attempt only** (from
`startOperation` to `endOperation`). It does not include time spent in earlier failed attempts or
waiting for a connection. Connection acquire time (Hikari `getConnection` plus the
`SET yb_enable_expression_pushdown` and `setAutoCommit` calls) is shown separately.

### WORKER TASK LATENCIES

The whole terminal cycle, per transaction type:

| Task | Measures | Healthy value |
|------|----------|---------------|
| Fetch Work | Waiting for the next queued request | ≈ 0 |
| Keying | Keying sleep | ≈ spec keying time (18000 / 3000 / 2000 ms) |
| Op With Retry | Connection acquire plus **all** attempts plus commit | Your real end-to-end transaction latency |
| Thinking | Think sleep | ≈ spec mean think time |

If Keying or Thinking are much larger than configured, the **client** is overloaded (thread
scheduling, GC). Fix that before blaming the database.

### FAILURE LATENCIES and RETRY ATTEMPTS (with `--vv`)

```
=================== RETRY ATTEMPTS ====================
  Transaction  |    Count  | Retry #0 - Failure Count | Retry #1 - Failure Count | Retry #2 - Failure Count |
      NewOrder |     38493 |              512 ( 1.33%) |               27 ( 0.07%) |                1 ( 0.00%) |
```

- **Count** = number of terminal cycles recorded for the type.
- **Retry #k** = how many times attempt *k* failed (attempt 0 is the first try). Only NewOrder has
  columns beyond #0 populated, because only NewOrder retries.
- A NewOrder that fails every attempt is counted in all columns and contributes nothing to tpmC.

### Per-SQL-statement latencies (`trackPerSQLStmtLatencies`)

When enabled (the code default is **on**; the sample XML turns it off), each procedure prints
HdrHistogram stats per statement after the run, e.g.

```
NewOrder :
latency GetCust Count : 38493 Avg Latency: 1.21 msecs, p99 Latency: 4.3 msecs
latency UpdateDist ...
```

Histograms are static and shared by all workers, and record execute time only (not result-set
iteration). They record **every** execution regardless of benchmark state (warmup, cool-down,
failed attempts and retries included), so their counts won't match the LATENCIES table.

## `results/oltpbench.csv`

```
Start,<nanoTime at execute start>
End,<nanoTime at start + warmup + runtime>
Transaction Name,Start Time (nanoseconds),Connection Latency (microseconds),OperationLatency (microseconds)
NewOrder,1234567890123,412,8312
...
```

Rows are sorted by start time. Timestamps come from `System.nanoTime()`, so they are only
comparable **within one JVM**.

## `results/json/output.json`

Schema (`benchmarks/tpcc/pojo/TpccRunResults.java`):

```jsonc
{
  "TestConfiguration": {
    "numNodes": 3, "totalWarehouses": 100, "numWarehouses": 100, "numDBConnections": 100,
    "warmupTimeInSecs": 300, "runTimeInSecs": 1800, "numRetries": 2,
    "testStartTime": "06-10-26_14:03:00"
  },
  "Results": { "tpmc": 1283.11, "efficiency": 99.78, "throughput": 47.53 },
  "Latencies": [ { "Transaction": "NewOrder", "Count": 38493, "avgLatency": 8.3,
                   "P99Latency": 31.2, "connectionAcqLatency": 0.41 }, ..., { "Transaction": "All", ... } ],
  "FailureLatencies": [ ... ],          // only with --vv
  "WorkerTaskLatency": { "NewOrder": [ { "Transaction": "Fetch Work", ... }, ... ], ..., "All": [...] },
  "RetryAttempts": { "NewOrder": { "count": 38493, "retriesFailureCount": [[512, 27, 1]] }, ... } // only with --vv
}
```

Fields only appear when set. `min*`/`max*` fields and `throughputMin`/`throughputMax` are filled in
only by a JSON merge.

## Merging multi-client results

### JSON (preferred)

Copy every client's `output.json` (renamed so the names don't collide) into one directory, then:

```bash
./tpccbenchmark --merge-json-results=true --dir=/path/to/jsons
```

This writes `results/json/output.json` on the machine that runs it:

- **tpmC** is recomputed from the **summed NewOrder counts**, using `runTimeInSecs` from the first
  file. **Efficiency** is recomputed against `totalWarehouses`.
- Per-transaction `Count` is summed. `avgLatency` and `connectionAcqLatency` are a running mean of
  the per-client averages (not weighted by count). `minLatency`/`maxLatency` are the min/max of the
  per-client averages. `P99Latency` is the **max** of the per-client p99s.
- Throughput: running mean plus min/max across clients.
- All files must list transactions in the same order.

### CSV (legacy)

```bash
./tpccbenchmark --merge-results=true --dir=/path/to/csvs --warehouses=<TOTAL warehouses> -c <same config>
```

This counts NewOrder rows whose start time plus *connection-acquire* latency (not operation
latency, see K13) falls before each file's `End` marker, and uses `<runtime>` from
the config and `--warehouses` for efficiency. It prints tpmC, efficiency and per-type avg/p99 to
stdout.

## Reading results: a short checklist

1. **Efficiency ≈ 100%?** If not, look at the next items in order.
2. **Worker task Keying/Thinking ≈ spec?** If not, the client is overloaded.
3. **Connection Acq Latency** high? Raise `--num-connections` (or the pool is starved by slow
   transactions).
4. **Retry #0 %** for NewOrder: compare across builds. A jump means more contention or conflict
   aborts.
5. **NewOrder p99** and **Op With Retry** p99: the user-visible tail.
6. Per-SQL stats (if enabled) to find which statement moved.
