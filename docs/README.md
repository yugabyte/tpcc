# Documentation

| Document | Read it when you want to... |
|----------|-----------------------------|
| [TPCC_WORKLOAD.md](TPCC_WORKLOAD.md) | Understand TPC-C and exactly what SQL each transaction runs, the schema, data generation, tpmC/efficiency, and the deviations from the spec |
| [YUGABYTEDB_CHANGES.md](YUGABYTEDB_CHANGES.md) | Know what was changed from OLTPBench for YugabyteDB (sharding, round-trip reduction, retries, smart driver, multi-client, geo-partitioning) and why, with PR references |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Understand the runtime: modes, connections, threads, state machine, worker loop, metrics pipeline |
| [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) | Find which file does what, and where to make a given change |
| [CONFIGURATION.md](CONFIGURATION.md) | Look up a CLI flag or XML key and its *real* default |
| [RUNNING.md](RUNNING.md) | Build, run single-client, multi-client, geo-partitioned or PostgreSQL runs; troubleshoot |
| [METRICS_AND_RESULTS.md](METRICS_AND_RESULTS.md) | Interpret every number printed, plus the CSV/JSON formats and result merging |
| [CODE_REVIEW_GUIDE.md](CODE_REVIEW_GUIDE.md) | Review or validate a change: invariants, checklist, consistency SQL, known issues |

AI agents: start with [`../AGENTS.md`](../AGENTS.md).
