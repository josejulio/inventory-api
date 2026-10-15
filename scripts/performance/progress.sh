#!/usr/bin/env bash
# Console feedback for launcher commands; workload measurements run separately.
phase_watch_pid=''

progress() {
    printf '[%s] %s\n' "$(date -u +%H:%M:%SZ)" "$*"
}

stop_phase_watch() {
    if [[ -n "$phase_watch_pid" ]]; then
        kill "$phase_watch_pid" 2>/dev/null || true
        wait "$phase_watch_pid" 2>/dev/null || true
        phase_watch_pid=''
    fi
}

# Stream command output while retaining the artifact log and original failure.
# A heartbeat makes quiet readiness waits visible without changing their timeout.
run_phase() {
    local label=$1 log_file=$2 began=$SECONDS status
    shift 2
    progress "$label; log: $log_file"
    (
        local sleep_pid=''
        trap 'exit 0' TERM INT
        trap 'if [[ -n "$sleep_pid" ]]; then kill "$sleep_pid" 2>/dev/null || true; wait "$sleep_pid" 2>/dev/null || true; fi' EXIT
        while true; do
            sleep 15 &
            sleep_pid=$!
            wait "$sleep_pid" || exit 0
            sleep_pid=''
            progress "$label: still waiting ($((SECONDS-began))s elapsed); log: $log_file"
        done
    ) &
    phase_watch_pid=$!
    if "$@" 2>&1 | tee -a "$log_file"; then
        status=0
    else
        status=$?
    fi
    stop_phase_watch
    if [[ "$status" == 0 ]]; then
        progress "$label: complete ($((SECONDS-began))s)"
    else
        progress "$label: failed (exit $status, $((SECONDS-began))s); see $log_file"
    fi
    return "$status"
}
