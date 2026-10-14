#!/usr/bin/env python3
"""Project cluster metadata to nonsecret performance diagnostics."""
import argparse
import json
import sys

import yaml


def resources(document, runner_image):
    """Discard pod IDs/timestamps and retain images, capacity and partitions."""
    deployments, images, topics = [], {}, []
    for item in document.get("items", []):
        kind = item["kind"]
        name = item["metadata"]["name"]
        spec = item.get("spec", {})
        if kind in ("Deployment", "StatefulSet"):
            containers = spec.get("template", {}).get("spec", {}).get("containers", [])
            deployments.append({"kind": kind, "name": name, "replicas": spec.get("replicas", 1),
                                "containers": [{"name": c["name"], "image": c["image"],
                                                "resources": c.get("resources", {}),
                                                "settings": {e["name"]: e.get("value") for e in c.get("env", [])
                                                             if e["name"] in ("INVENTORY_API_AUTHZ_IMPL", "INVENTORY_API_AUTHZ_SPICEDB_ENDPOINT")
                                                             or e["name"].startswith("INVENTORY_API_CONSISTENCY_")}}
                                               for c in sorted(containers, key=lambda c: c["name"])]})
        elif kind == "Pod":
            # Keep all observed image digests during a rollout without repeating
            # them for each replica.
            for c in item.get("status", {}).get("containerStatuses", []):
                if c.get("imageID"):
                    images.setdefault(c.get("image", c["name"]), set()).add(c["imageID"])
        elif kind == "KafkaTopic":
            topics.append({"name": name, "partitions": spec.get("partitions"),
                           "replicas": spec.get("replicas")})
    return {"runner_image": runner_image,
            "deployments": sorted(deployments, key=lambda d: (d["kind"], d["name"])),
            "image_digests": {k: sorted(v) for k, v in sorted(images.items())},
            "kafka_topics": sorted(topics, key=lambda t: t["name"])}


def consistency(document):
    """Select consistency settings without preserving credentials/config blobs."""
    data = document.get("data", {})
    config = yaml.safe_load(data.get("inventory-api-config.yaml", "{}")) or {}
    consumer = config.get("consumer", {})
    settings = config.get("consistency", {})
    allowed = ("read-after-write-enabled", "read-after-write-allowlist", "default-to-at-least-as-acknowledged")
    return {"consistency": {key: settings[key] for key in allowed if key in settings},
            "outbox_mode": config.get("storage", {}).get("outbox-mode"),
            "consumer_enabled": consumer.get("enabled"),
            "consumer_topic": consumer.get("topic")}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("resources", "consistency", "merge"))
    parser.add_argument("--runner-image", default="unknown")
    parser.add_argument("--runner-job-file")
    parser.add_argument("--resources-file")
    parser.add_argument("--consistency-file")
    args = parser.parse_args()
    if args.mode == "resources":
        output = resources(json.load(sys.stdin), args.runner_image)
        if args.runner_job_file:
            with open(args.runner_job_file) as job_file:
                job = yaml.safe_load(job_file)["items"][1]
            output["runner_resources"] = job["spec"]["template"]["spec"]["containers"][0]["resources"]
    elif args.mode == "consistency":
        output = consistency(json.load(sys.stdin))
    else:
        with open(args.resources_file) as resource_file, open(args.consistency_file) as consistency_file:
            output = {**json.load(resource_file), **json.load(consistency_file)}
    json.dump(output, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
