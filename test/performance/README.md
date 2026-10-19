# Performance baseline suite

Run the full suite with `make test-performance`; it detects Docker or Podman automatically, including Podman machines on macOS. Ensure Docker or Podman is installed and running properly with its API socket available before running the suite. Set `DOCKER_HOST` to select a specific endpoint. The wrapper disables Ryuk for Podman unless `TESTCONTAINERS_RYUK_DISABLED` is explicitly set. CI runs the standard target on pull requests and `main` using Ubuntu 24.04 and uploads the JSON results.

When both runtimes are installed, select one with `CONTAINER_RUNTIME=docker make test-performance` or `CONTAINER_RUNTIME=podman make test-performance`. The default is `auto`, which prefers a working Docker context. An explicitly selected runtime never falls back to the other runtime. An explicit `DOCKER_HOST` takes precedence over runtime detection.

The suite starts PostgreSQL 16.6 through Testcontainers and applies the real migrations. The gRPC workload measures resource create, update, delete, and recreate at 1 and 4 workers in fresh and history scenarios. The consumer workload calls `ProcessMessage` directly at 1, 4, and 8 clients for create, workspace-changing update, unchanged update, and delete events. Its Relations backend is stateless; Kafka polling and delivery are outside the timed calls.

## Fixture shape and tuning

Each scenario truncates and reseeds the tables before timing; rows from earlier scenarios do not accumulate. By default, `PERF_BACKGROUND=100000` inserts 100,000 background resources. Each has one common representation, one reporter resource, and one reporter representation. These rows add database volume but are not the resources receiving measured requests.

`PERF_HISTORY=227872` adds that many reporter representation versions to the history target resources. The gRPC history scenario divides the total among its workers: one target has all 227,872 versions with one worker; four targets have 56,968 each with four workers. The fresh scenario has no extra history. The consumer scenario puts all 227,872 versions under **one** target; its 1, 4, and 8 clients all repeatedly process the same event payloads for that target, not one target per client. This intentionally measures a warm, concentrated workload; varied resources and versions would require another scenario. It also adds two common representation versions needed by its event cases. All targets use the same reporter type and instance (`hbi`/`perf`); worker and client counts do not represent reporter counts.

`PERF_PAYLOAD_BYTES=1024` controls the reporter payload in seeded rows and measured gRPC requests. The payload is repeated `x` characters, not random data, so PostgreSQL compresses it well and increasing this setting may add less disk usage than expected. `PERF_BACKGROUND` changes the number of background resources; `PERF_HISTORY` changes the total extra history versions. `PERF_ROUNDS`, `PERF_UPDATES`, and `PERF_READ_ITERATIONS` change measured work rather than the initial seed. For example, `PERF_BACKGROUND=300000 PERF_HISTORY=500000 PERF_PAYLOAD_BYTES=2048 make test-performance` runs a larger fixture. Check the database size in the result summary when tuning toward production scale.

This branch's consumer reads common representations, so its measurements do not exercise the previous-reporter query, even though reporter history is present in the database.

`baseline.json` sets provisional p95 limits of 50 ms for gRPC lifecycle operations and 5 ms for consumer operations. The test fails on operation errors, missing consumer samples, missing or stale baseline entries for a measured case, and p95 values above those limits. Calibrate the limits from repeated runs on the CI runner before treating them as service objectives.

`PERF_PROFILE=small make test-performance` runs a quick harness and compatibility check with 100 background resources, 40 history versions, 128-byte payloads, and 3 lifecycle rounds. **The small profile skips latency limits for both gRPC and consumer operations** because its sparse lifecycle samples are unsuitable for performance comparisons. Correctness checks, request deadlines, baseline entry validation, cleanup, and timing measurements remain enabled. The command and result summary explicitly mark latency limits as skipped; saved JSON records `settings.profile` as `small`. The default full profile continues enforcing latency limits.

`PERF_BACKGROUND`, `PERF_HISTORY`, `PERF_PAYLOAD_BYTES`, `PERF_ROUNDS`, `PERF_UPDATES`, `PERF_READ_ITERATIONS`, `PERF_READ_PARALLEL_WORKERS`, and `PERF_REQUEST_TIMEOUT_SECONDS` override workload settings. `PERF_BASELINE` selects another baseline file. Results are written to `test/performance/results/` by default; `PERF_RESULTS_DIR` changes the output directory. The Make target prints sample counts, min/average/p50/p95/p99/max latency, database size, and throughput after a passing run.

Earlier runs of the reporter-only history fixture occupied about 0.53 GiB locally; the expanded fixture must be measured again, and its repeated payload compresses well. It demonstrates a history-heavy workload but does not match the roughly 10 GiB production database.

### Live history coverage (default ephemeral workload)

`PERF_SCENARIOS` selects `fresh`, legacy `history` (deleted history with
restoration), and `history_live`. Explicit `fresh` selection uses new identities without restoration.
Omission preserves existing runner behavior;
the Make target selects `history_live`.
`history_live` measures workspace updates, reporter changes with the workspace
retained, and one delete per target. It never restores a measured identity.

`baseline-ephemeral-history-live.json` uses **2 × the indexed revision's p95**
for each report’s consumer-completion timing and worker count. Delete uses
pipeline timing because it has no correlated consumer-completion record.
API and report graph-observation timings remain informational.
These are user-selected limits from one sparse indexed run;
no additional calibration was performed. Existing slow-run measurements exceed
all consumer-completion and delete pipeline limits at both worker counts.

Plain `make test-performance-ephemeral` enforces these limits with depths
227,872 (one worker) and 56,968 (four workers), 100 rounds, 100,000 background
resources, 1,024-byte payloads, three updates, and batches of three rounds.
The equivalent explicit command is:

```sh
PERF_PROFILE=full PERF_SCENARIOS=history_live \
PERF_BASELINE="$PWD/test/performance/baseline-ephemeral-history-live.json" \
PERF_HISTORY_DEPTH=227872 PERF_HISTORY_DEPTH_4=56968 \
PERF_BACKGROUND=100000 PERF_PAYLOAD_BYTES=1024 PERF_ROUNDS=100 PERF_UPDATES=3 \
PERF_HISTORY_BATCH_ROUNDS=3 make test-performance-ephemeral
```

This command is expensive: setup repeats across 100 rounds. The indexed evidence
used only three rounds; a complete 100-round deployment has not been validated.
Small runs continue to skip latency enforcement even when a baseline is supplied.

The launcher streams timestamped runner progress to the terminal. Live-history
messages include worker count, batch number, round range, elapsed time, and phase:
background setup, API target setup, history seeding and verification, VACUUM,
CDC drain, measurement, batch cleanup, final cleanup, and result validation.
`rounds_measured` means those rounds finished measurement; cleanup and baseline
validation can still be pending. Long SQL phases log when entered, not a percent
complete. These messages are outside the measured operation timings.
To reconnect to the stream, use the namespace recorded in the run directory:

```sh
oc -n "$(cat "$run_dir/namespace.txt")" logs -f job/performance-runner
```

For an informational correctness run (the small Make profile defaults to depth
40 at both worker counts and three rounds):

```sh
PERF_PROFILE=small PERF_SCENARIOS=history_live \
PERF_HISTORY_DEPTH=40 PERF_BACKGROUND=100 PERF_ROUNDS=3 PERF_BASELINE= \
make test-performance-ephemeral
```

`PERF_HISTORY_DEPTH` includes version zero and is **per target**, at both 1 and
4 workers. Each round prepares one replacement target per worker. Total seeded
history is `depth * workers * rounds` for each case. Background resources are
seeded once per case. For example, depth 227,872 and 100 rounds across both
worker counts requires 113,936,000 history rows, before API updates. This is
substantially more expensive than the old 227,872-row fixture. To reproduce the
old distribution initially, run cases at depths 227,872 (1 worker) and 56,968
(4 workers), using `PERF_HISTORY_DEPTH_4` for the latter. No full-profile
runtime or batch size is claimed validated yet.

Setup copies API-established live representations in one atomic transaction
with explicit reporter/resource parent locks. It checks current state and common
references, analyzes the tables, and drains setup WAL. Bulk setup avoids
serializable predicate-lock tracking, which can exhaust PostgreSQL shared memory
on large history indexes; application transaction isolation is unchanged.
API events then traverse the deployed pipeline. Reports record request-to-
correlated-consumer timing separately from graph verification. Delete rows lack
a persisted transaction ID, so deletes use graph removal and tombstone checks.
Batch measurement windows exclude SQL setup and cleanup gaps in log diagnostics.
Small profiles skip latency gates; full gates require at least 100 samples per
operation and worker count. History limits contain `consumer_completion` for
`workspace_update` and `reporter_update`, and `propagation` for `delete`. Existing
fresh-only baseline evidence remains fresh-only.

Run fixture correctness against a disposable PostgreSQL database:

```sh
PERF_FIXTURE_TEST_DSN='postgres://…' go test -tags=integration \
  ./test/performance/ephemeral -run 'TestFixturePostgres|TestLiveHistoryPostgres|TestCompletionNotifications'
python3 scripts/performance/preflight-test.py
python3 scripts/performance/summary-test.py
```

`PERF_HISTORY_BATCH_ROUNDS` (1..100, initially 1) prepares that many targets
per worker before each measurement window. Larger batches reduce WAL barriers
but increase peak history rows to `depth * workers * batch rounds`. Each worker
still processes its disjoint targets sequentially, and every target is deleted
once. The last batch uses only the remaining rounds. Choose batch size from
measured database footprint and runtime; increasing it does not reduce the
required per-target depth or total seeded rows.

For the initial 227,872-row comparison distribution, set
`PERF_HISTORY_DEPTH=227872 PERF_HISTORY_DEPTH_4=56968`. The optional four-worker
value fixes depth per target for that entire concurrency case, including every
replacement. With ten rounds per batch, both cases then seed 2,278,720 rows per
batch, and 100 rounds seed 45,574,400 rows across the two cases. This preserves
history depth across delete samples rather than dividing a fixed history budget
among all replacement targets.

The PostgreSQL fixture test also accepts `PERF_FIXTURE_TARGETS` (1..40),
`PERF_FIXTURE_DEPTH` (at least 12), and `PERF_FIXTURE_BACKGROUND` for bulk setup
reproduction. For the one-worker calibration batch, use 10 targets, depth
227872, and 100000 background rows; for four workers, use 40 targets and depth
56968. These options write millions of rows; use a disposable database.
