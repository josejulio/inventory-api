# Performance scripts

Run the ephemeral performance test from the repository root:

```sh
make test-performance-ephemeral
```

The target builds current checkout code by default, deploys an ephemeral
environment, runs the workload against the committed p95 limits, collects
results, and releases its reservation. Image overrides remain available.
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
