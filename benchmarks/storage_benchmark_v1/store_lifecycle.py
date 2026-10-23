"""Cross-process key identity and explicit, manifest-based cleanup."""

import hashlib
import json
import os
import re
from dataclasses import replace
from pathlib import Path

from storage.mooncake_store import MooncakeStoreStorage, StoreConfig


def store_config(args):
    return StoreConfig(
        local_hostname=args.local_hostname,
        metadata_server=args.metadata_server,
        master_server_addr=args.master_server,
        protocol=args.protocol,
        device_name=args.device_name,
        global_segment_size=args.global_segment_size,
        local_buffer_size=args.local_buffer_size,
        io_api=args.io_api,
        verify=args.verify,
    )


def dataset_identity(model_config, trace_bytes):
    # Full file identity deliberately excludes max_requests: a reader can use
    # a subset of the same trace. Different files/layouts must not share IDs.
    trace_hash = hashlib.sha256(trace_bytes).hexdigest()
    descriptor = {
        "key_schema": 2,
        "payload_schema": 1,
        "model": model_config,
        "page_size_tokens": 512,
        "trace_sha256": trace_hash,
    }
    digest = hashlib.sha256(
        json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return digest


def save_key_manifest(path, namespace, keys, master_server, execution_id):
    """Persist planned keys before any remote writes, including uncertain RPCs.

    Refuse overwrite so a later run cannot discard the only cleanup inventory.
    A partial local write aborts before connecting; it must not be used to clean.
    """
    record = {
        "schema_version": 1,
        "namespace": namespace,
        "master_server": master_server,
        "execution_id": execution_id,
        "keys": sorted(set(keys)),
    }
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(record, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def load_cleanup_keys(paths, master_server):
    keys = set()
    namespaces = set()
    for path in paths:
        record = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(record, dict):
            raise ValueError(f"invalid benchmark manifest: {path}")
        namespace = record.get("namespace", "")
        if (
            record.get("schema_version") != 1
            or not isinstance(namespace, str)
            or not re.fullmatch(
                r"mcbench-[A-Za-z0-9_-]{1,80}:v2:[0-9a-f]{64}", namespace
            )
        ):
            raise ValueError(f"invalid benchmark manifest namespace: {path}")
        if record.get("master_server") != master_server:
            raise ValueError(f"manifest Master differs from --master-server: {path}")
        entries = record.get("keys")
        if not isinstance(entries, list) or any(
            not isinstance(key, str)
            or not key.startswith(namespace + ":")
            or len(key) <= len(namespace) + 1
            for key in entries
        ):
            raise ValueError(f"manifest contains keys outside its namespace: {path}")
        keys.update(entries)
        namespaces.add(namespace)
    return sorted(keys), sorted(namespaces)


def run_store_cleanup(args, client_factory=None):
    """Caller must stop ALL readers/writers before explicitly invoking this."""
    keys, namespaces = load_cleanup_keys(args.cleanup_manifest, args.master_server)
    result = {
        "schema_version": 1,
        "backend": "mooncake",
        "action": "cleanup",
        "status": "dry_run" if args.dry_run else "ok",
        "namespaces": namespaces,
        "planned_keys": len(keys),
        "deleted_keys": 0,
        "already_missing_keys": 0,
        "errors": [],
        "cleanup_errors": [],
    }
    if args.dry_run or not keys:
        return result
    store = None
    try:
        # Cleanup needs no large KV buffers or contributed capacity.
        config = replace(
            store_config(args), io_api="plain", verify=False, global_segment_size=0
        )
        store = MooncakeStoreStorage(64, "cleanup", config, client_factory)
        for key in keys:
            namespace, page = key.rsplit(":", 1)
            store.namespace = namespace
            try:
                removed = store.delete(page, force=True, missing_ok=True)
                field = "deleted_keys" if removed else "already_missing_keys"
                result[field] += 1
            except Exception as exc:
                result["errors"].append(f"{key}: {exc}")
    except Exception as exc:
        result["errors"].append(str(exc))
    finally:
        if store is not None:
            try:
                store.close()
            except Exception as exc:
                result["cleanup_errors"].append(str(exc))
    if result["errors"] or result["cleanup_errors"]:
        result["status"] = "failed"
    return result
