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
`test/performance/baseline-ephemeral-create-update-delete.json`.

## Helper checks

Check the shell entry points for syntax errors:

```sh
bash -n scripts/performance/ephemeral-test.sh scripts/performance/build-image.sh
```
