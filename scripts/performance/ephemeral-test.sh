#!/usr/bin/env bash
# Deploy and collect an in-cluster lifecycle workload. Never select a
# namespace from current context, user reservations, or a cluster-wide listing.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$repo/scripts/performance/progress.sh"
bonfire=${BONFIRE:-bonfire}
if ! command -v "$bonfire" >/dev/null 2>&1; then
    echo "Bonfire executable not found: $bonfire. Install bonfire on PATH or set BONFIRE to its executable." >&2
    exit 2
fi
# Build current working-tree source unless the caller explicitly selects an image.
build_inventory=false
if [[ -z ${PERF_LOCAL_INVENTORY_IMAGE:-} && -z ${INVENTORY_IMAGE:-} && -z ${IMAGE_TAG:-} ]]; then
    build_inventory=true
    PERF_LOCAL_INVENTORY_IMAGE=localhost/inventory-api:performance-current
    export PERF_LOCAL_INVENTORY_IMAGE
fi
# "auto" publishes the local runner into this run's own namespace registry.
PERF_RUNNER_IMAGE=${PERF_RUNNER_IMAGE:-auto}
build_runner=false
if [[ "$PERF_RUNNER_IMAGE" == auto && -z ${PERF_LOCAL_RUNNER_IMAGE:-} ]]; then
    build_runner=true
fi
for tool in oc python3 timeout tar; do command -v "$tool" >/dev/null || { echo "Missing tool: $tool" >&2; exit 2; }; done
python3 -c 'import yaml' || { echo 'Python PyYAML is required' >&2; exit 2; }
results=${PERF_RESULTS_DIR:-$repo/test/performance/results/ephemeral-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$results"
results=$(cd "$results" && pwd)
python3 "$repo/scripts/performance/preflight.py" "$results/config.json"
if [[ -n ${PERF_BASELINE:-} ]]; then
    cp "$PERF_BASELINE" "$results/baseline-ephemeral.json"
fi
if [[ "$PERF_RUNNER_IMAGE" == auto || -n ${PERF_LOCAL_INVENTORY_IMAGE:-} ]]; then
    command -v podman >/dev/null || { echo 'Local image publishing requires Podman' >&2; exit 2; }
fi
if [[ "$PERF_RUNNER_IMAGE" == auto && "$build_runner" == false ]]; then
    podman image exists "${PERF_LOCAL_RUNNER_IMAGE:-localhost/inventory-perf:ephemeral}" || {
        echo "Local runner image not found: ${PERF_LOCAL_RUNNER_IMAGE:-localhost/inventory-perf:ephemeral}. Build the runner before launching." >&2
        exit 2
    }
fi
if [[ -n ${PERF_LOCAL_INVENTORY_IMAGE:-} && "$build_inventory" == false ]]; then
    podman image exists "$PERF_LOCAL_INVENTORY_IMAGE" || { echo "Local Inventory image not found: $PERF_LOCAL_INVENTORY_IMAGE" >&2; exit 2; }
fi
if [[ "$build_runner" == true ]]; then
    "$repo/scripts/performance/build-image.sh" runner localhost/inventory-perf:ephemeral "$results"
fi
if [[ "$build_inventory" == true ]]; then
    "$repo/scripts/performance/build-image.sh" inventory "$PERF_LOCAL_INVENTORY_IMAGE" "$results"
fi
# Show exactly which local content or explicit remote reference will be tested.
{
    if [[ "$PERF_RUNNER_IMAGE" == auto ]]; then
        runner_local=${PERF_LOCAL_RUNNER_IMAGE:-localhost/inventory-perf:ephemeral}
        runner_id=$(podman image inspect --format '{{.Id}}' "$runner_local")
        [[ -n "$runner_id" ]] || { echo 'Cannot identify local runner image' >&2; exit 2; }
        echo "Runner: $runner_local (local image ID: $runner_id)"
        echo "Runner selection: $(if [[ "$build_runner" == true ]]; then echo current checkout; else echo explicit local image; fi)"
    else
        echo "Runner: $PERF_RUNNER_IMAGE (explicit remote image)"
    fi
    if [[ -n ${PERF_LOCAL_INVENTORY_IMAGE:-} ]]; then
        inventory_id=$(podman image inspect --format '{{.Id}}' "$PERF_LOCAL_INVENTORY_IMAGE")
        [[ -n "$inventory_id" ]] || { echo 'Cannot identify local Inventory image' >&2; exit 2; }
        echo "Inventory: $PERF_LOCAL_INVENTORY_IMAGE (local image ID: $inventory_id)"
        echo "Inventory selection: $(if [[ "$build_inventory" == true ]]; then echo current checkout; else echo explicit local image; fi)"
    else
        python3 - "$repo/deploy/kessel-inventory-ephem.yaml" <<'PYIDENTITY'
import os,sys,yaml
parameters={p['name']:p.get('value','') for p in yaml.safe_load(open(sys.argv[1]))['parameters']}
print('Inventory: '+os.environ.get('INVENTORY_IMAGE',parameters['INVENTORY_IMAGE'])+':'+os.environ.get('IMAGE_TAG',parameters['IMAGE_TAG'])+' (explicit remote image)')
PYIDENTITY
    fi
} | tee "$results/tested-images.txt"
namespace=''
runner_log_pid=''
registry_auth_dir=''
deployment_dir=''
reservation_attempted=false
reservation="inventory-perf-$(date +%s)-$RANDOM"
# Bound each network command, plus an overall deadline watched by this process.
run_timeout=${PERF_OVERALL_TIMEOUT_SECONDS:-32400}
run_timeout=$((10#$run_timeout))
reservation_seconds=$((run_timeout + 1800))
started=$SECONDS
collecting=false
bounded() {
    local limit=600
    if [[ "$collecting" == false ]]; then
        local left=$((run_timeout - (SECONDS-started)))
        (( left > 0 )) || return 124
        (( left >= limit )) || limit=$left
    fi
    timeout --kill-after=10s "${limit}s" "$@"
}
remaining() { (( SECONDS-started < run_timeout )) || { echo 'Overall deadline exceeded' >&2; return 124; }; }
collect() {
    [[ -n "$namespace" ]] || return 0
    bounded oc -n "$namespace" get pods,deployments,statefulsets,jobs,pvc -o json > "$results/deployment.json" 2> "$results/diagnostics-errors.log" || true
    bounded oc -n "$namespace" get events -o json > "$results/events.json" 2>> "$results/diagnostics-errors.log" || true
    # Pod JSON records resolved image digests, replicas and resource allocations.
    while read -r pod; do
        [[ -n "$pod" ]] || continue
        bounded oc -n "$namespace" logs "$pod" --all-containers --prefix > "$results/${pod#pod/}.log" 2>> "$results/diagnostics-errors.log" || true
    done < <(bounded oc -n "$namespace" get pods -o name 2>/dev/null || true)
    # A separate running collector mounts the durable PVC after the Job stops.
    # Collection never execs in the completed workload container.
    if bounded oc -n "$namespace" get pvc performance-results >/dev/null 2>&1; then
        bounded oc -n "$namespace" delete job performance-runner --cascade=foreground --wait=true --timeout=60s >> "$results/collection.log" 2>&1 || true
        python3 - "$repo/test/performance/ephemeral/job.yaml" "$PERF_RUNNER_IMAGE" <<'PY' | bounded oc -n "$namespace" apply -f - >> "$results/collection.log" 2>&1
import sys,yaml,json
spec=yaml.safe_load(open(sys.argv[1]))['items'][1]['spec']['template']['spec']
spec['containers'][0]['image']=sys.argv[2]
spec['containers'][0]['args']=['hold']
spec['containers'][0]['volumeMounts']=[{'name':'results','mountPath':'/results','readOnly':True}]
spec['volumes']=[spec['volumes'][0]]
print(json.dumps({'apiVersion':'v1','kind':'Pod','metadata':{'name':'performance-collector'},'spec':spec}))
PY
        if bounded oc -n "$namespace" wait pod/performance-collector --for=condition=Ready --timeout=120s >> "$results/collection.log" 2>&1; then
            bounded oc -n "$namespace" exec performance-collector -- /runner export > "$results/artifacts.tar" 2>> "$results/collection.log" || return 1
            mkdir -p "$results/runner"
            tar -xf "$results/artifacts.tar" -C "$results/runner" >> "$results/collection.log" 2>&1 || return 1
            [[ -s "$results/runner/status.json" ]] || return 1
        else
            return 1
        fi
    fi
}
cleanup() {
    status=$?
    trap - EXIT INT TERM
    set +e
    collecting=true
    stop_phase_watch
    progress "Collecting results and diagnostics; artifacts: $results"
    if [[ -n "$runner_log_pid" ]]; then
        kill "$runner_log_pid" 2>/dev/null || true
        wait "$runner_log_pid" 2>/dev/null || true
    fi
    cleanup_outcome='no namespace allocated'
    collect
    collection_status=$?
    if [[ "$status" == 0 && "$collection_status" != 0 ]]; then status=$collection_status; fi
    if [[ "$status" == 0 && ! -s "$results/runner/status.json" ]]; then
        echo 'Job succeeded but runner artifacts were not recovered' >&2
        status=1
    fi
    if [[ -n "$namespace" ]]; then
        if [[ ${PERF_KEEP_NAMESPACE:-false} == true ]]; then
            cleanup_outcome='namespace retained'
            echo "Retained namespace $namespace; release with: $bonfire namespace release $namespace --force"
        else
            progress "Releasing namespace $namespace"
            bounded "$bonfire" namespace release "$namespace" --force >> "$results/release.log" 2>&1
            release_status=$?
            if [[ "$release_status" == 0 ]]; then cleanup_outcome='reservation released'; else cleanup_outcome='reservation release failed'; fi
            if [[ "$release_status" != 0 ]]; then echo "Release failed for owned namespace $namespace; see release.log" >&2; [[ "$status" != 0 ]] || status=$release_status; fi
        fi
    elif [[ "$reservation_attempted" == true && ${PERF_KEEP_NAMESPACE:-false} != true ]]; then
        # Reservation allocation can succeed before the command is interrupted.
        # Cancel only the exact reservation name generated by this invocation.
        bounded oc delete namespacereservation "$reservation" --ignore-not-found=true >> "$results/release.log" 2>&1
        if [[ "$?" == 0 ]]; then cleanup_outcome='reservation cancelled'; else cleanup_outcome='reservation cancellation failed'; fi
    fi
    [[ -z "$registry_auth_dir" ]] || rm -rf "$registry_auth_dir"
    [[ -z "$deployment_dir" ]] || rm -rf "$deployment_dir"
    echo "Artifacts: $results"
    python3 "$repo/scripts/performance/summary.py" "$results" --exit-status "$status" --cleanup "$cleanup_outcome" || echo 'Could not generate summary; inspect artifact files.' >&2
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
deployment_dir=$(mktemp -d /tmp/inventory-perf-deployment.XXXXXX)
python3 "$repo/scripts/performance/prepare-deployment.py" --template "$repo/deploy/kessel-inventory-ephem.yaml" --template-output "$deployment_dir/inventory.yaml" --backend-output "$deployment_dir/backend.json" --spicedb-image "${PERF_SPICEDB_IMAGE:-quay.io/redhat-services-prod/project-kessel-tenant/kessel-relations/spicedb:latest}"
python3 - "$deployment_dir" "$repo" <<'PYCONFIG'
import os,sys,yaml
base={}
if os.environ.get('BONFIRE_LOCAL_CONFIG'):
    base=yaml.safe_load(open(os.environ['BONFIRE_LOCAL_CONFIG'])) or {}
apps=base.setdefault('apps',[])
app=next((a for a in apps if a['name']=='kessel'),None)
if app is None: app={'name':'kessel','components':[]};apps.append(app)
components=app.setdefault('components',[])
component=next((c for c in components if c['name']=='kessel-inventory'),None)
if component is None: component={'name':'kessel-inventory'};components.append(component)
component.update(host='local',repo=sys.argv[2],path=os.path.relpath(os.path.join(sys.argv[1],'inventory.yaml'),sys.argv[2]))
if os.environ.get('PERF_BONFIRE_SOURCE','appsre')=='file':
    apps_path=os.path.join(sys.argv[1],'apps.yaml')
    yaml.safe_dump({'apps':apps},open(apps_path,'w'))
    base['appsFile']={'host':'local','repo':sys.argv[2],'path':os.path.relpath(apps_path,sys.argv[2])}
yaml.safe_dump(base,open(os.path.join(sys.argv[1],'bonfire.yaml'),'w'))
PYCONFIG
python3 - "$repo/test/performance/ephemeral/job.yaml" "$PERF_RUNNER_IMAGE" "$results/config.json" <<'PY' > "$deployment_dir/job.json"
import yaml,json,sys,os
m=yaml.safe_load(open(sys.argv[1]));job=m['items'][1]
job['spec']['activeDeadlineSeconds']=json.load(open(sys.argv[3]))['run_timeout']//10**9
container=job['spec']['template']['spec']['containers'][0]
container['image']=sys.argv[2]
if os.environ.get('PERF_DATABASE_URL_FILE')=='env':
    secret=os.environ.get('PERF_DATABASE_SECRET','kessel-inventory-db')
    keys={'PGHOST':'db.host','PGPORT':'db.port','PGDATABASE':'db.name','PGUSER':'db.user','PGPASSWORD':'db.password'}
    container['env']=[{'name':name,'valueFrom':{'secretKeyRef':{'name':secret,'key':key}}} for name,key in keys.items()]
    container['env'].append({'name':'PGSSLMODE','value':os.environ.get('PERF_DATABASE_SSLMODE','prefer')})
print(json.dumps(m))
PY
oc whoami >/dev/null
progress "Reserving an ephemeral namespace; logs: $results/reserve.log"
reservation_attempted=true
# --force suppresses the existing-reservation prompt; it does not release them.
bounded "$bonfire" namespace reserve --name "$reservation" --duration "${reservation_seconds}s" --timeout 600 --force > "$results/reserve.out" 2> "$results/reserve.log"
namespace=$(python3 - "$results/reserve.out" <<'PY'
import re,sys
lines=[line.strip() for line in open(sys.argv[1]) if line.strip()]
if len(lines)!=1 or not re.fullmatch(r'ephemeral-[a-z0-9-]+',lines[0]):
    raise SystemExit('Cannot identify the owned namespace from reservation output')
print(lines[0])
PY
)
printf '%s\n' "$namespace" > "$results/namespace.txt"
progress "Namespace: $namespace; artifacts: $results"
if [[ "$PERF_RUNNER_IMAGE" == auto || -n ${PERF_LOCAL_INVENTORY_IMAGE:-} ]]; then
    registry=$(bounded oc registry info --public)
    registry_auth_dir=$(mktemp -d /tmp/inventory-perf-registry.XXXXXX)
    bounded oc registry login --registry="$registry" --to="$registry_auth_dir/auth.json" > "$results/registry-login.log" 2>&1
fi
if [[ "$PERF_RUNNER_IMAGE" == auto ]]; then
    run_phase "Publishing runner image to $namespace" "$results/runner-push.log" bounded podman push --authfile "$registry_auth_dir/auth.json" --digestfile "$results/runner-digest.txt" "${PERF_LOCAL_RUNNER_IMAGE:-localhost/inventory-perf:ephemeral}" "docker://$registry/$namespace/performance-runner:run"
    PERF_RUNNER_IMAGE="image-registry.openshift-image-registry.svc:5000/$namespace/performance-runner@$(cat "$results/runner-digest.txt")"
    export PERF_RUNNER_IMAGE
fi
if [[ -n ${PERF_LOCAL_INVENTORY_IMAGE:-} ]]; then
    run_phase "Publishing Inventory image to $namespace" "$results/inventory-push.log" bounded podman push --authfile "$registry_auth_dir/auth.json" --digestfile "$results/inventory-digest.txt" "$PERF_LOCAL_INVENTORY_IMAGE" "docker://$registry/$namespace/inventory-api:run"
    INVENTORY_IMAGE="image-registry.openshift-image-registry.svc:5000/$namespace/inventory-api"
    IMAGE_TAG="run@$(cat "$results/inventory-digest.txt")"
    export INVENTORY_IMAGE IMAGE_TAG
    printf '%s:%s\n' "$INVENTORY_IMAGE" "$IMAGE_TAG" > "$results/inventory-image.txt"
fi
echo "Deploying runner: $PERF_RUNNER_IMAGE"
if [[ -s "$results/inventory-image.txt" ]]; then
    echo "Deploying Inventory: $(cat "$results/inventory-image.txt")"
fi
# Provision the authorization datastore directly; there is no Relations API app.
run_phase "Creating SpiceDB and its database" "$results/spicedb-deploy.log" bounded oc -n "$namespace" apply -f "$deployment_dir/backend.json"
run_phase "Waiting for SpiceDB database readiness" "$results/spicedb-deploy.log" bounded oc -n "$namespace" rollout status deployment/performance-spicedb-db --timeout=180s
params=(--local-config-path "$deployment_dir/bonfire.yaml")
case "${PERF_BONFIRE_SOURCE:-appsre}" in
    appsre) ;;
    file) params+=(--source file --ref-env "" --fallback-ref-env "" --target-env "") ;;
    *) echo 'PERF_BONFIRE_SOURCE must be appsre or file' >&2; exit 2 ;;
esac
params+=(-p "kessel-kafkaconnect/IMAGE_TAG=${PERF_KAFKA_CONNECT_TAG:-latest}")
[[ -z ${INVENTORY_IMAGE:-} ]] || params+=(-p "kessel-inventory/INVENTORY_IMAGE=$INVENTORY_IMAGE")
[[ -z ${IMAGE_TAG:-} ]] || params+=(-p "kessel-inventory/IMAGE_TAG=$IMAGE_TAG")
remaining
run_phase "Deploying Inventory, database, Kafka/CDC and SpiceDB in $namespace (includes migrations and readiness waits)" "$results/deploy.log" bounded "$bonfire" deploy kessel -C kessel-inventory --namespace "$namespace" --no-release-on-fail --timeout 600 "${params[@]}"
# Bonfire waits for deployment init containers (including migrations). Explicit
# Kafka conditions supplement workload readiness; API preflight proves CDC flow.
# The shared Kafka Connect template currently includes an unrelated HBI source.
# Remove it before measurements; HBI requests are produced by this runner only.
bounded oc -n "$namespace" delete kafkaconnector hbi-outbox-connector --ignore-not-found=true > "$results/hbi-connector-removal.log"
for kind in kafka kafkaconnect; do
    run_phase "Waiting for $kind readiness" "$results/readiness.log" bounded oc -n "$namespace" wait "$kind" --all --for=condition=Ready --timeout=300s
done
run_phase "Waiting for Inventory CDC connector readiness" "$results/readiness.log" bounded oc -n "$namespace" wait kafkaconnector "${PERF_CONNECTOR_NAME:-kessel-inventory-source-connector}" --for=condition=Ready --timeout=300s
run_phase "Waiting for Inventory API readiness" "$results/readiness.log" bounded oc -n "$namespace" rollout status deployment/kessel-inventory-api --timeout=300s
if [[ -n ${PERF_CREDENTIALS_MANIFEST:-} ]]; then
    bounded oc -n "$namespace" apply -f "$PERF_CREDENTIALS_MANIFEST" > /dev/null
fi
progress "Recording deployment settings and preparing the workload job"
# Capture stable deployment settings before adding the workload Job.
bounded oc -n "$namespace" get deployments,statefulsets,pods,kafkatopics -o json | python3 "$repo/scripts/performance/metadata.py" resources --runner-image "$PERF_RUNNER_IMAGE" --runner-job-file "$repo/test/performance/ephemeral/job.yaml" > "$results/resource-metadata.json"
bounded oc -n "$namespace" get configmap "${PERF_INVENTORY_CONFIGMAP:-inventory-api-config}" -o json | python3 "$repo/scripts/performance/metadata.py" consistency > "$results/consistency-metadata.json"
python3 "$repo/scripts/performance/metadata.py" merge --resources-file "$results/resource-metadata.json" --consistency-file "$results/consistency-metadata.json" > "$results/deployment-metadata.json"
git -C "$repo" rev-parse HEAD > "$results/launcher-revision.txt"
git -C "$repo" diff --quiet || printf 'tracked workspace changes present\n' > "$results/launcher-dirty.txt"
config_files=(--from-file="config.json=$results/config.json")
if [[ -n ${PERF_BASELINE:-} ]]; then
    config_files+=(--from-file="baseline-ephemeral.json=$results/baseline-ephemeral.json")
fi
if [[ -n ${PERF_DATABASE_URL_FILE:-} ]]; then
    # Retain only nonsecret connector settings. Never write credentials to artifacts.
    bounded oc -n "$namespace" get kafkaconnector "${PERF_CONNECTOR_NAME:-kessel-inventory-source-connector}" -o json | python3 -c '
import json,sys
c=json.load(sys.stdin)["spec"]["config"]
keys=["slot.name","table.exclude.list","table.include.list","snapshot.mode","plugin.name"]
json.dump({k:str(c[k]) for k in keys if k in c},sys.stdout)
' > "$results/connector-policy.json"
    config_files+=(--from-file="connector-policy.json=$results/connector-policy.json")
fi
bounded oc -n "$namespace" create configmap performance-config "${config_files[@]}" > /dev/null
python3 - "$deployment_dir/job.json" "$PERF_RUNNER_IMAGE" <<'PY' | bounded oc -n "$namespace" apply -f - > "$results/job-create.log"
import json,sys
job=json.load(open(sys.argv[1]))
job['items'][1]['spec']['template']['spec']['containers'][0]['image']=sys.argv[2]
print(json.dumps(job))
PY

# Stream phase messages into the invoking terminal. Job status and recovered
# artifacts remain authoritative if the log connection is interrupted.
echo "Following workload progress in namespace $namespace (oc logs -f job/performance-runner)."
start_runner_logs() {
    oc -n "$namespace" logs -f job/performance-runner -c runner --timestamps --pod-running-timeout=120s &
    runner_log_pid=$!
}
start_runner_logs

while true; do
    remaining
    bounded oc -n "$namespace" get job performance-runner -o json > "$results/job-status.json"
    state=$(python3 - "$results/job-status.json" <<'PY'
import json,sys
s=json.load(open(sys.argv[1])).get('status',{})
print('success' if s.get('succeeded',0) else 'failed' if s.get('failed',0) else 'running')
PY
)
    case "$state" in success) break;; failed) exit 1;; esac
    if ! kill -0 "$runner_log_pid" 2>/dev/null; then
        log_status=0
        wait "$runner_log_pid" || log_status=$?
        progress "Runner log stream closed (exit $log_status); reconnecting and replaying available logs. Job is still running."
        start_runner_logs
    fi
    sleep 5
done
