# Performance scripts

Run the ephemeral performance test from the repository root:

```sh
make test-performance-ephemeral
```

The target builds current checkout code by default, deploys an ephemeral
environment, runs a quick deep-history comparison, collects results, and
releases its reservation. The default is `small`: three update rounds with
30 updates of each type per round (`PERF_UPDATES=30`), three deletes per worker,
batches of three, 100,000 background resources, 1,024-byte
payloads, and history depths 227,872/56,968 for one/four workers. This preserves
the workload that exposed the regression and records 90 samples of each update
operation at one worker and 360 at four workers, plus 3/12 delete samples.
Latency is informational; correctness checks remain enabled. Image and workload
overrides remain available.

Use `PERF_PROFILE=full make test-performance-ephemeral` for 100 update rounds,
30 deletes per worker, batches of ten, and enforcement of the committed baseline.
See [the workload guide](../../test/performance/README.md) for configuration.

## Scripts used by the ephemeral test

| Script | Purpose | When used |
|---|---|---|
| `ephemeral-test.sh` | Coordinates configuration, reservation, image publishing, deployment, runner Job, artifact recovery, and reservation release. | Entry point called by Make. |
| `progress.sh` | Streams deployment and readiness output, prints phase boundaries and a heartbeat every 15 seconds during quiet waits. | Sourced by the launcher. |
| `preflight.py` | Resolves environment settings and validates local inputs, including baseline limits, before cluster operations. | Every run. |
| `build-image.sh` | Builds either Inventory or the runner and records checkout revision, workspace status, build log, and image ID. | When current-code builds are selected; explicit image overrides skip the relevant build. |
| `prepare-deployment.py` | Renders the Inventory template and standalone SpiceDB deployment; keeps generated secrets in the launcher's private temporary directory. | Every run. |
| `metadata.py` | Records nonsecret images, resource allocations, Kafka partitions, and consistency settings for diagnosis. | Every run; metadata does not gate p95 baseline compatibility. |
| `summary.py` | Produces the readable summary and measured throughput diagnostics from recovered artifacts. | After artifact collection; can also be run manually. |

Regenerate a summary without deploying:

```sh
python3 scripts/performance/summary.py test/performance/results/RUN_DIRECTORY
```

The workload itself is Go code in `test/performance/ephemeral/`, with its runner
entry point in `test/performance/cmd/ephemeral/`. The committed baseline is
`test/performance/baseline-ephemeral-history-live.json`.

## Helper checks

Check the shell entry points for syntax errors:

```sh
bash -n scripts/performance/ephemeral-test.sh scripts/performance/build-image.sh
```

Live history is the Make default. Other scenarios are selectable with `PERF_SCENARIOS=fresh` or
`fresh,history_live`, and an explicit
`PERF_HISTORY_DEPTH` (per target, including version zero). Use `PERF_PROFILE=small`
and `PERF_BASELINE=` for correctness verification. The history baseline `test/performance/baseline-ephemeral-history-live.json`
uses twice the indexed run p95, as selected by the user. Fresh-only runs need
`PERF_BASELINE=test/performance/baseline-ephemeral-create-update-delete.json`. See
`test/performance/README.md` for fixture cost, operations and evidence fields.

The performance overlay uses `inventory-api migrate` for its isolated database.
It omits the newer optional `--enabled` argument so older comparison revisions
can use the same rendered migration command. Production templates retain their
migration control.

The launcher streams runner phase messages while waiting for the job. Live-history
progress includes batches, round ranges, worker count and elapsed time; recovered
pod logs retain these messages. A closed log stream is reported and automatically reconnected while the job is
running; reconnection replays available logs, so repeated lines are possible.
A lost log stream does not change the job result.
Reconnect with `oc -n NAMESPACE logs -f job/performance-runner`.

Before the workload starts, the console names each image publishing, deployment,
and readiness phase, streams that command's output, and prints a heartbeat every
15 seconds when it is still running. The full output remains in the named log
file. In particular, after the image references, the launcher creates SpiceDB
and its database, waits for the database, and runs Bonfire deployment (including
Inventory database migrations and dependency readiness). This is environment
setup; measured workload operations have not started yet.

Verify console feedback locally with `python3 scripts/performance/progress-test.py`.

The default runner and Kubernetes Job deadline is eight hours
(`PERF_JOB_TIMEOUT_SECONDS=28800`). The overall launcher deadline is nine hours
(`PERF_OVERALL_TIMEOUT_SECONDS=32400`), including deployment. Both can be
overridden when starting a run; the runner does not reload timeouts mid-run.

Full Make runs keep 100 update rounds but default to 30 delete rounds per
worker (`PERF_HISTORY_DELETE_ROUNDS=30`). Each live target receives three or
four update rounds before its one measured delete. This retains 300 samples of
each update operation per worker while reducing deletes to 30 per worker
(120 at four workers). All operations still run against deep history.

`PERF_HISTORY_BATCH_ROUNDS=10` prepares ten delete targets per worker per batch,
so the default uses three batches per worker count. Peak seeded history stays
at 2,278,720 rows per batch, while total seeded history drops from 45,574,400 to
13,672,320 rows across both cases (70% fewer). Use
`PERF_HISTORY_DELETE_ROUNDS=100` to restore one fresh target per update round.
The setting must be 1..PERF_ROUNDS; full baseline runs require at least 30 delete
rounds. The small profile defaults to three. Thirty delete samples provide a
coarser p95 than 100, but the existing numeric limits remain unchanged.

Live-history cases retain and log all baseline violations and continue from one
to four workers after a failed case. The overall run still fails if either case
fails. Cancellation or exhaustion of the overall runner deadline stops new work.
Failed setup/measurement cases attempt scoped cleanup before the next case.
