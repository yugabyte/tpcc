# CLAUDE.md

@AGENTS.md

## Claude Code–specific notes

- **Read the matching doc before editing.** For transaction or SQL changes, read
  `docs/TPCC_WORKLOAD.md` §6. For schema/DDL, read `docs/TPCC_WORKLOAD.md` §4 and
  `docs/YUGABYTEDB_CHANGES.md` §1. For retries and metrics, read `docs/ARCHITECTURE.md` §4 and
  `docs/METRICS_AND_RESULTS.md`.
- **`--create=true` and `--clear=true` drop every TPC-C table.** Only run them against a local or
  throwaway database, and ask first if `--nodes` points anywhere else.
- **Verify compilation after Java edits.** Use `ant build`. If `ant` isn't installed, use
  `./download-deps.sh && mkdir -p build && javac -nowarn -d build -cp "lib/*" $(find src -name '*.java')`. There are
  no unit tests to run. Say so when reporting, rather than implying the change was tested.
- **Keep the docs in sync.** If you change a default, flag, XML key, transaction SQL, schema or
  output field, update the matching file in `docs/` (and `config/*.xml` samples) in the same
  change.
- **For code reviews**, apply the invariants and checklist in `docs/CODE_REVIEW_GUIDE.md`, and
  check the known-issues table first so you don't re-report a known item as new (cite it by its
  K-number instead).
- `lib/`, `build/` and `results/` are gitignored, except `lib/ant-contrib.jar`, which is tracked
  and needed by `build.xml`.
