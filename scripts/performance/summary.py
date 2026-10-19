#!/usr/bin/env python3
"""Summarize recovered ephemeral artifacts without contacting the cluster."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import re


def consumer_throughput(path):
    """Diagnostic log rate scoped to timed scenarios; never a throughput gate."""
    unavailable = {"available": False, "eligible_for_gate": False,
                   "reason": "consumer completion logs or scenario measurement windows unavailable"}
    try:
        windows = []
        for file in sorted((path / "runner").glob("*.json")):
            result = json.loads(file.read_text())
            if "operations" not in result:
                continue
            intervals = result.get('measurement_windows') or [{'start': result.get('measurement_start'), 'end': result.get('measurement_end')}]
            for interval in intervals:
                start, end = (datetime.fromisoformat(interval[key].replace("Z", "+00:00")) for key in ('start', 'end'))
                seconds = (end - start).total_seconds()
                if seconds <= 0 or start.tzinfo is None or end.tzinfo is None:
                    return unavailable
                windows.append({"start": start, "end": end, "duration_seconds": seconds,
                                "protocol": result.get("protocol"), "scenario": result.get("scenario"),
                                "workers": result.get("workers"), "messages": 0})
        windows.sort(key=lambda window: window['start'])
        if not windows or any(left['end'] > right['start'] for left, right in zip(windows, windows[1:])):
            return unavailable
        messages, topics, files, seen = 0, {}, [], set()
        marker = re.compile(r"\bts=(\S+).*\bmsg=consumed event from topic ([^,\s]+), partition (\d+) at offset (\d+)")
        for file in sorted(path.glob("kessel-inventory-api-*.log")):
            files.append(file.name)
            for line in file.read_text().splitlines():
                match = marker.search(line)
                if not match:
                    continue
                timestamp = datetime.fromisoformat(match[1].replace("Z", "+00:00"))
                identity = tuple(match.group(i) for i in (2, 3, 4))
                if identity in seen:
                    continue
                for window in windows:
                    if window['start'] <= timestamp < window['end']:
                        seen.add(identity)
                        window['messages'] += 1
                        messages += 1
                        topics[match[2]] = topics.get(match[2], 0) + 1
                        break
        if not messages:
            return unavailable
        seconds = sum(window['duration_seconds'] for window in windows)
        for window in windows:
            window['start_time'] = window.pop('start').isoformat()
            window['end_time'] = window.pop('end').isoformat()
            window['messages_per_minute'] = window['messages'] * 60 / window['duration_seconds']
        return {"available": True, "eligible_for_gate": False, "messages": messages,
                "duration_seconds": seconds, "messages_per_minute": messages * 60 / seconds,
                "topics": topics, "windows": windows, "source_files": files,
                "log_coverage": "unknown; missing or rotated logs can undercount",
                "source": "deduplicated successful consumer completion logs",
                "scope": "scenario measurement windows only; excludes setup, warmup and gaps; diagnostic, not maximum capacity"}
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return unavailable


def summarize(path, exit_status=None, cleanup=None):
    """Use runner status, samples and launcher outcome to describe a run."""
    problems = []

    def read(file):
        if not file.exists():
            return {}
        try:
            return json.loads(file.read_text())
        except (ValueError, OSError) as error:
            problems.append(f"Cannot read {file.name}: {error}")
            return {}

    status = read(path / "runner/status.json")
    config = read(path / "config.json")
    results = []
    for file in sorted((path / "runner").glob("*.json")):
        if file.name != "status.json":
            result = read(file)
            if "operations" in result:
                results.append(result)
    failures = [failure for result in results for failure in (result.get("failures") or [])]
    if status.get("error"):
        failures.append(status["error"])
    rate_evidence = read(path / 'runner/rate-rounds.json')
    failures.extend(f"Rate round {index}: {failure}" for index, result in enumerate(rate_evidence.get('rounds', []), 1) for failure in result.get('failures', []))
    selected = config.get('scenarios', [])
    # Setup failures precede the runner and cannot establish scenario coverage.
    # Once a job or runner artifact exists, missing results remain a failure.
    setup_failed = (exit_status is not None and exit_status != 0 and not results
                    and not (path / 'runner/status.json').exists()
                    and not (path / 'job-status.json').exists()
                    and not (path / 'job-create.log').exists())
    if selected and not setup_failed:
        expected = {(config.get('protocol', 'grpc'), scenario, workers) for scenario in selected for workers in (1, 4)}
        actual = {(r.get('protocol'), r.get('scenario'), r.get('workers')) for r in results}
        for case in sorted(expected - actual):
            failures.append('Missing scenario coverage: ' + '/'.join(map(str, case)))
        for case in sorted(actual - expected):
            failures.append('Unexpected scenario coverage: ' + '/'.join(map(str, case)))
    run_ids = {result['run_id'] for result in results if result.get('run_id')}
    if len(run_ids) > 1:
        failures.append('Mixed runner IDs in recovered scenario artifacts')
    counts = [0] * 6
    rows = []
    for result in results:
        for operation, stats in sorted(result["operations"].items()):
            api, pipeline = stats.get("api", {}), stats.get("propagation", {})
            values = [api.get("count", 0), pipeline.get("count", 0)] + [stats.get(key, 0) for key in ("errors", "timeouts", "propagation_errors", "propagation_timeouts")]
            counts = [left + right for left, right in zip(counts, values)]
            scenario = f"{result.get('protocol', '?')}/{result.get('scenario', '?')}/{result.get('workers', '?')}"
            rows.append(f"{scenario:<25} {operation:<18} {values[0]:>6} {api.get('p95_ms', 0):>12.2f} {pipeline.get('p95_ms', 0):>17.2f}")
    failed = (exit_status is not None and exit_status != 0) or status.get("success") is False or failures or any(counts[2:])
    outcome = "FAILED" if failed else "PASSED" if status.get("success") is True and results and not problems else "INCOMPLETE"
    lines = [f"Performance run: {outcome}", f"Artifacts: {path}"]
    namespace_file = path / "namespace.txt"
    if namespace_file.exists():
        lines.append(f"Namespace: {namespace_file.read_text().strip()}")
    if cleanup:
        lines.append(f"Cleanup: {cleanup}")
    elif (path / "release.log").exists():
        lines.append("Cleanup: release attempted; see release.log")
    if exit_status is not None:
        lines.append(f"Launcher exit status: {exit_status}")
    if setup_failed:
        lines.append("Workload not started: launcher failed during setup; scenario coverage checks skipped.")
    profile = config.get("profile") or (results[0].get("profile") if results else "unknown")
    lines += [f"Profile: {profile}", f"Samples: {counts[0]} measured API calls; {counts[1]} pipeline observations", f"API errors/timeouts: {counts[2]}/{counts[3]}; propagation errors/timeouts: {counts[4]}/{counts[5]}"]
    if "background" in config:
        legacy_history = f"{config.get('history', 0):,} legacy history versions; " if not config.get('scenarios') or 'history' in config['scenarios'] else ''
        lines.append(f"Workload: {config['background']:,} background resources/scenario; {legacy_history}payload {config.get('payload_bytes', '?')} bytes; {config.get('rounds', '?')} rounds/worker.")
    if config.get('scenarios'):
        lines.append('Selected scenarios: ' + ', '.join(config['scenarios']))
    for result in results:
        if result.get('scenario') == 'history_live':
            fixture = result.get('fixture', {})
            if 'history_delete_rounds' in result:
                lines.append(f"  Planned per worker: {result.get('rounds', '?')} update rounds; {result['history_delete_rounds']} deletes.")
            lines.append(f"Live history/{result.get('workers')}: depth {fixture.get('history_depth', '?')} per target; {result.get('seeded_history_rows', 0):,} total seeded rows; setup {result.get('setup_seconds', 0):.2f}s; measurement {result.get('measurement_seconds', 0):.2f}s; cleanup {result.get('cleanup_seconds', 0):.2f}s.")
            for operation, stats in sorted(result['operations'].items()):
                if operation != 'delete':
                    timing = stats.get('consumer_completion', {})
                    lines.append(f"  {operation}: request-to-correlated-consumer p95 {timing.get('p95_ms', 0):.2f} ms ({timing.get('count', 0)} samples; includes transport and CDC).")
    for result in results:
        database = result.get("database", {})
        if "database_bytes" in database:
            label = "Largest sampled fixture database for" if result.get("scenario") == "history_live" else "Database before"
            lines.append(f"{label} {result.get('scenario', '?')}/{result.get('workers', '?')} workers: {database['database_bytes'] / 1024**3:.3f} GiB (physical footprint includes indexes and reusable pages).")
    for result in results:
        duration = result.get('measurement_seconds', 0)
        if duration > 0 and not result.get('failures') and all(not stats.get(key, 0) for stats in result['operations'].values() for key in ('errors', 'timeouts', 'propagation_errors', 'propagation_timeouts')):
            completed = sum(stats.get('propagation', {}).get('count', 0) for stats in result['operations'].values())
            lines.append(f"{result.get('protocol', '?')}/{result.get('scenario', '?')}/{result.get('workers', '?')}: {completed * 60 / duration:.2f} verified workload completions/minute ({completed} over {duration:.2f} measured seconds; closed loop includes pipeline verification).")
    rate_results = read(path / 'runner/rate-rounds.json')
    for index, result in enumerate(rate_results.get('rounds', []), 1):
        lines.append(f"Rate round {index}: target {result['target_per_minute']:,}/min; accepted {result['accepted_per_minute']:.2f}/min; correlated consumer completions {result['completed_per_minute']:.2f}/min; backlog peak/end {result.get('peak_backlog', result['backlog_at_end'])}/{result['backlog_at_end']}; drain {result['drain_seconds']:.2f}s; pending after drain {result['pending_after_drain']}; producer misses {result['producer_missed']}.")
        lines.append(f"  API p95 {result['api']['p95_ms']:.2f} ms; write-to-consumer p95 {result['consumer']['p95_ms']:.2f} ms; verified relationships {result['verified_relationships']}; failures {len(result.get('failures', []))}.")
    if rate_results:
        lines.append('Rate rounds use independent scheduled creates and transaction-correlated notifications; graph verification/cleanup are outside production. Short exploratory rounds do not establish maximum capacity.')
    consumer = consumer_throughput(path)
    if consumer["available"]:
        lines += [f"Consumer log diagnostic: {consumer['messages_per_minute']:.2f} messages/minute observed ({consumer['messages']} logged completions over {consumer['duration_seconds']:.0f} seconds).",
                  "Consumer log rate uses scenario measurement windows; excludes setup/warmup and gaps. Diagnostic only, not a throughput gate or maximum capacity; log loss can undercount."]
    else:
        lines.append("Consumer log rate: unavailable; requires completion logs and scenario measurement windows.")
    if profile == "small":
        lines.append("Latency: informational; small profile does not enforce p95 limits.")
    elif config.get("baseline_file"):
        lines.append("Latency: calibrated baseline configured; inspect scenario failures for violations.")
    else:
        lines.append("Latency: calibration only; no baseline limits configured.")
    excluded = sorted({item for result in results for item in result.get("excluded_coverage", [])})
    if excluded:
        lines.append("Excluded coverage: " + ", ".join(excluded))
    if results:
        failed_cases = sum(bool(result.get('failures')) for result in results)
        failure_count = sum(len(result.get('failures') or []) for result in results)
        lines.append(f"Workload cases: {len(results)} recorded; {failed_cases} failed; {failure_count} recorded failures (see details below).")
    if rows:
        lines += ["", f"{'Protocol/scenario/workers':<25} {'Operation':<18} {'Calls':>6} {'API p95 ms':>12} {'Pipeline p95 ms':>17}", *rows, "", "Pipeline timing includes API plus CDC/Kafka/consumer/SpiceDB completion; API timing ends at the response."]
    else:
        lines.append("No measured scenario results recovered. Inspect deploy.log, readiness.log and collection.log.")
    if failures or problems:
        lines += ["", "Failures:", *[f"- {failure}" for failure in dict.fromkeys(failures + problems)]]
    lines += ["", "Details: runner/*.txt for readable statistics; runner/*.json for full measurements; runner/status.json for workload outcome.", "Diagnostics: pod *.log, events.json, deployment.json and job-status.json. artifacts.tar contains the recovered runner files."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--exit-status", type=int)
    parser.add_argument("--cleanup")
    args = parser.parse_args()
    if not args.directory.is_dir():
        parser.error("artifact directory does not exist")
    output = summarize(args.directory.resolve(), args.exit_status, args.cleanup)
    (args.directory / "summary.txt").write_text(output)
    (args.directory / "consumer-throughput.json").write_text(json.dumps(consumer_throughput(args.directory), indent=2) + "\n")
    print(output, end="")


if __name__ == "__main__":
    main()
