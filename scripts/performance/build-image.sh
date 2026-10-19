#!/usr/bin/env bash
# Build a performance image from the checkout and record its source identity.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
kind=${1:?Expected runner or inventory}
case "$kind" in
    runner) image=${2:-localhost/inventory-perf:ephemeral}; dockerfile=test/performance/Dockerfile.ephemeral; argument=SOURCE_REVISION ;;
    inventory) image=${2:-localhost/inventory-api:performance-current}; dockerfile=Dockerfile; argument=VERSION ;;
    *) echo 'Expected runner or inventory' >&2; exit 2 ;;
esac
command -v podman >/dev/null || { echo 'Image builds require Podman on PATH' >&2; exit 2; }
results=${3:-$repo/test/performance/results/build-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$results"
revision=$(git -C "$repo" rev-parse HEAD)
git -C "$repo" status --short > "$results/$kind-source-status.txt"
printf '%s\n' "$revision" > "$results/$kind-source-revision.txt"
dirty=false
[[ ! -s "$results/$kind-source-status.txt" ]] || dirty=true
echo "Building $kind from checkout $revision (workspace changes: $dirty) -> $image"
podman build --build-arg "$argument=$revision" \
    --label "org.opencontainers.image.revision=$revision" \
    --label "io.kessel.performance.workspace-dirty=$dirty" \
    -f "$repo/$dockerfile" -t "$image" "$repo" > "$results/$kind-build.log" 2>&1 || {
    echo "Image build failed; see $results/$kind-build.log" >&2
    exit 1
}
podman image inspect --format '{{.Id}}' "$image" > "$results/$kind-local-image-id.txt"
echo "Built $kind: $image ($(cat "$results/$kind-local-image-id.txt"))"
