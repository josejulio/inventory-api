#!/usr/bin/env python3
"""Resolve local workload inputs before building images or accessing the cluster."""
import json
import math
import os
from pathlib import Path
import re
import sys

import yaml


def resolve(env):
    """Validate locally knowable runner settings; deployment identity comes later."""
    def integer(key, default, minimum=1, scale=1):
        raw = str(env.get(key, default))
        if not re.fullmatch(r'[0-9]+', raw):
            raise ValueError(f'{key} must be an integer >= {minimum}')
        value = int(raw)
        if value < minimum or value * scale > 2**63 - 1:
            raise ValueError(f'{key} is outside the supported range')
        return value

    def boolean(key, default='false'):
        value = env.get(key, default)
        if value not in ('true', 'false'):
            raise ValueError(f'{key} must be true or false')
        return value == 'true'

    profile = env.get('PERF_PROFILE', 'small')
    protocol = env.get('PERF_PROTOCOL', 'grpc')
    if profile not in ('small', 'full') or protocol not in ('grpc', 'http'):
        raise ValueError('profile must be small/full and protocol grpc/http')
    if env.get('PERF_BONFIRE_SOURCE', 'appsre') not in ('appsre', 'file'):
        raise ValueError('PERF_BONFIRE_SOURCE must be appsre or file')
    boolean('PERF_KEEP_NAMESPACE')
    integer('PERF_OVERALL_TIMEOUT_SECONDS', 2400, minimum=1200, scale=10**9)
    full = profile == 'full'
    c = {'profile': profile, 'protocol': protocol,
         'endpoint': env.get('PERF_ENDPOINT', 'kessel-inventory-api:9000'),
         'health_url': env.get('PERF_HEALTH_URL', 'http://kessel-inventory-api:8000/api/kessel/v1/livez'),
         'plaintext': boolean('PERF_PLAINTEXT'),
         'rounds': integer('PERF_ROUNDS', 20 if full else 3),
         'payload_bytes': integer('PERF_PAYLOAD_BYTES', 1024 if full else 128),
         'background': integer('PERF_BACKGROUND', 100000 if full else 100, minimum=0),
         'updates': integer('PERF_UPDATES', 3), 'results_dir': '/results'}
    for field, key, default, scale in (
            ('request_timeout', 'PERF_REQUEST_TIMEOUT_SECONDS', 10, 10**9),
            ('propagation_timeout', 'PERF_PROPAGATION_TIMEOUT_SECONDS', 60, 10**9),
            ('poll_interval', 'PERF_POLL_MS', 100, 10**6),
            ('run_timeout', 'PERF_JOB_TIMEOUT_SECONDS', 1200, 10**9)):
        c[field] = integer(key, default, scale=scale) * scale
    c['history'] = integer('PERF_HISTORY', 0 if env.get('PERF_SCENARIOS') == 'fresh' else (227872 if full else 40), minimum=0)
    database = env.get('PERF_DATABASE_URL_FILE', '')
    default_scenarios = 'fresh,history'
    c['scenarios'] = env.get('PERF_SCENARIOS', default_scenarios).split(',')
    if len(set(c['scenarios'])) != len(c['scenarios']) or any(v not in ('fresh', 'history', 'history_live') for v in c['scenarios']):
        raise ValueError('PERF_SCENARIOS must select distinct fresh, history or history_live scenarios')
    c['history_batch_rounds'] = integer('PERF_HISTORY_BATCH_ROUNDS', 1)
    if c['history_batch_rounds'] > 100:
        raise ValueError('PERF_HISTORY_BATCH_ROUNDS must be 1..100')
    if env.get('PERF_HISTORY_DEPTH_4'):
        c['history_depth_four_workers'] = integer('PERF_HISTORY_DEPTH_4', 2, minimum=2)
    c['history_depth'] = integer('PERF_HISTORY_DEPTH', 227872 if full else 40, minimum=2)
    if 'PERF_SCENARIOS' in env and not database:
        raise ValueError('explicit scenario selection requires database fixtures')
    if 'history_live' in c['scenarios'] and not database:
        raise ValueError('history_live requires database fixtures')

    if full and not database:
        raise ValueError('full profile requires PERF_DATABASE_URL_FILE')
    if database and 'history' in c['scenarios'] and c['history'] < 8:
        raise ValueError('history must be >= 8 for the four-worker lifecycle workload')
    if c['poll_interval'] >= c['propagation_timeout'] or not c['endpoint'] or not c['health_url']:
        raise ValueError('endpoints must be set and polling must be below the propagation timeout')
    if env.get('PERF_MESSAGE_RATES'):
        rates = env['PERF_MESSAGE_RATES'].split(',')
        if any(not re.fullmatch(r'[0-9]+', value) or not 1 <= int(value) <= 60000 for value in rates):
            raise ValueError('PERF_MESSAGE_RATES must be comma-separated rates between 1 and 60000 writes/minute')
        seconds = integer('PERF_RATE_SECONDS', 60)
        if seconds > 600 or not database or not (c['scenarios'] == ['fresh'] and 'PERF_SCENARIOS' in env):
            raise ValueError('rate rounds require fresh database fixtures and 1..600 seconds per round')
        if any(int(rate) * seconds / 60 > 100000 for rate in rates):
            raise ValueError('rate rounds allow at most 100000 scheduled writes per round')
        c['message_rates'] = [int(rate) for rate in rates]
        c['rate_duration'] = seconds * 10**9
    for key in ('PERF_DATABASE_URL_FILE', 'PERF_HEADERS_FILE', 'PERF_CA_FILE'):
        value = env.get(key, '')
        if not value:
            continue
        if key == 'PERF_DATABASE_URL_FILE' and value == 'env':
            c['database_url_file'] = 'env'
        else:
            if not re.fullmatch(r'[A-Za-z0-9_.-]+', value) or value in ('.', '..'):
                raise ValueError(f'{key} must name a credentials Secret key')
            field = {'PERF_DATABASE_URL_FILE': 'database_url_file', 'PERF_HEADERS_FILE': 'headers_file', 'PERF_CA_FILE': 'ca_file'}[key]
            c[field] = '/credentials/' + value
    if database:
        c['connector_policy_file'] = '/config/connector-policy.json'
    for key in ('BONFIRE_LOCAL_CONFIG', 'PERF_CREDENTIALS_MANIFEST'):
        if env.get(key):
            try:
                documents = list(yaml.safe_load_all(Path(env[key]).read_text()))
                if not documents or any(not isinstance(d, dict) for d in documents):
                    raise ValueError()
                if key == 'BONFIRE_LOCAL_CONFIG' and not isinstance(documents[0].get('apps', []), list):
                    raise ValueError()
            except (OSError, ValueError, yaml.YAMLError):
                raise ValueError(f'{key} must be a readable YAML mapping') from None
    baseline_path = env.get('PERF_BASELINE', '')
    if baseline_path:
        if not database:
            raise ValueError('baseline enforcement requires database fixtures')
        runner = env.get('PERF_RUNNER_IMAGE', 'auto')
        if runner != 'auto' and not re.search(r'@sha256:[a-f0-9]{64}$', runner):
            raise ValueError('baseline enforcement requires a digest-pinned remote runner or auto')
        try:
            b = json.loads(Path(baseline_path).read_text())
            def operations(scenario):
                if scenario == 'history_live':
                    return ['workspace_update', 'reporter_update', 'delete']
                return ['create', 'workspace_update', 'unchanged_update', 'delete'] + ([] if scenario == 'fresh' and 'PERF_SCENARIOS' in env else ['recreate'])
            def kinds(scenario, op):
                if scenario == 'history_live':
                    return ['propagation'] if op == 'delete' else ['consumer_completion']
                return ['api', 'propagation']
            keys = {f'{protocol}/{scenario}/{workers}/{op}/{kind}' for scenario in c['scenarios'] for workers in (1, 4) for op in operations(scenario) for kind in kinds(scenario, op)}
            if set(b['p95_limits_ms']) != keys or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0 for v in b['p95_limits_ms'].values()):
                raise ValueError()
            if full and c['rounds'] < 100:
                raise ValueError()
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            raise ValueError('PERF_BASELINE must be readable JSON with sufficient samples and all positive finite p95 limits for the selected cases') from None
        c['baseline_file'] = '/config/baseline-ephemeral.json'
    if 'PERF_SCENARIOS' not in env:
        c.pop('scenarios')
    return c


def main():
    try:
        config = resolve(os.environ)
        Path(sys.argv[1]).write_text(json.dumps(config, indent=2) + '\n')
    except (ValueError, OSError) as error:
        sys.exit(f'Performance preflight failed: {error}')


if __name__ == '__main__':
    main()
