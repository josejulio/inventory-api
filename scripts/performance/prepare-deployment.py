#!/usr/bin/env python3
"""Prepare an Inventory template and standalone SpiceDB for performance runs.

Secret manifests are written only under a caller-owned private temporary directory,
never to result artifacts. Relations API is not deployed or used.
"""
import argparse
import json
import os
from pathlib import Path
import secrets

import yaml


def inventory_template(document):
    for obj in document.get("objects", []):
        if obj.get("kind") == "ClowdApp" and obj.get("metadata", {}).get("name") == "kessel-inventory":
            spec = obj["spec"]
            spec["dependencies"] = [name for name in spec.get("dependencies", []) if name != "kessel-relations"]
            for deployment in spec.get("deployments", []):
                # Owned performance namespaces always migrate their isolated DB.
                # Use the common CLI accepted by both comparison revisions;
                # older binaries predate the optional --enabled flag.
                for container in deployment.get("podSpec", {}).get("initContainers", []):
                    if container.get("name") == "migration" and container.get("args", [])[:1] == ["migrate"]:
                        container["args"] = ["migrate"]

                for env in deployment.get("podSpec", {}).get("env", []):
                    if env["name"] == "INVENTORY_API_AUTHZ_SPICEDB_ENDPOINT":
                        env["value"] = "performance-spicedb:50051"
                    elif env["name"] == "INVENTORY_API_AUTHZ_SPICEDB_TOKEN":
                        env["valueFrom"]["secretKeyRef"]["name"] = "performance-spicedb-config"
    for parameter in document.get("parameters", []):
        if parameter["name"] == "AUTHZ_IMPL":
            parameter["value"] = "spicedb"
    return document


def backend(image):
    password = secrets.token_hex(24)
    key = secrets.token_hex(24)
    labels = {"app": "performance-spicedb-db"}
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "performance-spicedb-config"},
              "stringData": {"password": password, "preshared_key": key,
                             "datastore_uri": f"postgres://spicedb:{password}@performance-spicedb-db:5432/spicedb?sslmode=disable"}}
    pvc = {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "performance-spicedb-data"},
           "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}}
    container = {"name": "postgres", "image": "registry.redhat.io/rhel9/postgresql-16:9.5-1744136555",
                 "env": [{"name": "POSTGRESQL_DATABASE", "value": "spicedb"},
                         {"name": "POSTGRESQL_USER", "value": "spicedb"},
                         {"name": "POSTGRESQL_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "performance-spicedb-config", "key": "password"}}},
                         {"name": "PGDATA", "value": "/var/lib/pgsql/data/pgdata"}],
                 "ports": [{"containerPort": 5432}],
                 "readinessProbe": {"exec": {"command": ["pg_isready", "-U", "spicedb", "-d", "spicedb"]}, "periodSeconds": 5},
                 "resources": {"requests": {"cpu": "200m", "memory": "128Mi"}, "limits": {"cpu": "500m", "memory": "128Mi"}},
                 "volumeMounts": [{"name": "data", "mountPath": "/var/lib/pgsql/data"}]}
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "performance-spicedb-db"},
                  "spec": {"replicas": 1, "selector": {"matchLabels": labels},
                           "template": {"metadata": {"labels": labels}, "spec": {
                               "containers": [container], "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "performance-spicedb-data"}}]}}}}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "performance-spicedb-db"},
               "spec": {"selector": labels, "ports": [{"port": 5432, "targetPort": 5432}]}}
    cluster = {"apiVersion": "authzed.com/v1alpha1", "kind": "SpiceDBCluster", "metadata": {"name": "performance-spicedb"},
               "spec": {"secretName": "performance-spicedb-config", "config": {"datastoreEngine": "postgres", "image": image, "replicas": 1, "logLevel": "info"}}}
    return {"apiVersion": "v1", "kind": "List", "items": [secret, pvc, deployment, service, cluster]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", required=True)
    parser.add_argument("--template-output", required=True)
    parser.add_argument("--backend-output", required=True)
    parser.add_argument("--spicedb-image", default="quay.io/redhat-services-prod/project-kessel-tenant/kessel-relations/spicedb:latest")
    args = parser.parse_args()
    with open(args.template) as file:
        template = inventory_template(yaml.safe_load(file))
    with open(args.template_output, "w") as file:
        yaml.safe_dump(template, file)
    descriptor = os.open(args.backend_output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as file:
        json.dump(backend(args.spicedb_image), file)


if __name__ == "__main__":
    main()
