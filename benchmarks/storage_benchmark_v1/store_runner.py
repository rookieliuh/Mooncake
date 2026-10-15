"""Store replay orchestration, isolated from the legacy disk benchmark."""

import hashlib
import math
import os
import platform
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from storage.mooncake_store import MooncakeStoreStorage, StoreConfig

SOURCE_BASE = {
    "repository": "https://github.com/rookieliuh/Mooncake",
    "branch": "supercache-snapshot-930",
    "commit": "d869f481e11aed2a6ee4860135646188a50bce5a",
}
# Only benchmark-relevant variables: do not export the process environment.
ENV_FIELDS = (
    "MC_FORCE_TCP",
    "MC_UB_NUMA_AFFINITY_ENABLE",
    "MC_URMA_TRANS_MODE",
    "MC_URMA_ACTIVE_PORT",
    "MC_URMA_BONDING_BALANCE",
    "MC_URMA_BONDING_MULTIPATH_ENABLE",
    "MC_UB_TRANSPORT_CPU_STAGING",
    "MC_UB_STAGING_POOL_SIZE",
    "MC_STORE_USE_HUGEPAGE",
    "MC_LOG_LEVEL",
    "MC_HIFREQ_LOG_SAMPLE_RATE",
    "MC_SLICE_SIZE",
    "MC_MAX_MR_SIZE",
)


def add_store_arguments(parser):
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Workload sizing only: no native binding, Store connection or Store I/O",
    )
    parser.add_argument("--backend", choices=("disk", "mooncake"), default="disk")
    parser.add_argument("--master-server", help="Mooncake Master host:port")
    parser.add_argument("--metadata-server", help="Metadata URL or P2PHANDSHAKE")
    parser.add_argument(
        "--local-hostname",
        help="Routable client address; optional {worker} placeholder",
    )
    parser.add_argument(
        "--protocol",
        default="tcp",
        help="Transport supported by installed binding; 930 supports ub with USE_UB",
    )
    parser.add_argument(
        "--device-name",
        default="",
        help="Transport device(s), passed unchanged to setup",
    )
    parser.add_argument(
        "--global-segment-size",
        type=int,
        default=0,
        help="Store capacity contributed per client, bytes; 0 for remote-only clients",
    )
    parser.add_argument(
        "--local-buffer-size",
        type=int,
        default=128 * 1024**2,
        help="SDK local buffer pool per client, bytes",
    )
    parser.add_argument("--io-api", choices=("plain", "buffer"), default="buffer")
    parser.add_argument(
        "--key-scope", choices=("independent", "shared"), default="independent"
    )
    parser.add_argument(
        "--access-mode", choices=("cache", "read", "write"), default="cache"
    )
    parser.add_argument(
        "--warmup", action="store_true", help="Prefill unique keys outside measurement"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify full read payloads; adds request CPU time",
    )
    parser.add_argument(
        "--keep-keys", action="store_true", help="Skip run-scoped object cleanup"
    )
    parser.add_argument(
        "--output-json", help="Save configuration, outcomes and metrics as JSON"
    )


def validate_store_arguments(parser, args):
    if args.backend != "mooncake":
        if args.dry_run:
            parser.error("--dry-run requires --backend mooncake")
        return
    for name in ("master_server", "metadata_server", "local_hostname"):
        if not args.dry_run and not getattr(args, name):
            parser.error(f"--{name.replace('_', '-')} is required for Mooncake")
    if args.page_size_tokens != 512:
        parser.error(
            "FAST25 hash IDs represent 512 tokens; Store replay requires --page-size-tokens 512"
        )
    if args.fsync_mode != "none":
        parser.error("fsync options apply only to the disk backend")
    if args.global_segment_size < 0 or args.local_buffer_size <= 0:
        parser.error("Store memory sizes must be nonnegative/positive respectively")
    if args.max_requests is not None and args.max_requests <= 0:
        parser.error("--max-requests must be positive")
    if args.access_mode == "read" and not args.warmup:
        parser.error("--access-mode read requires --warmup for the fresh run namespace")
    if args.access_mode == "write" and args.warmup:
        parser.error(
            "write mode inserts fresh keys per operation; --warmup does not apply"
        )
    if (
        args.threads > 1
        and args.local_hostname
        and ":" in args.local_hostname
        and "{worker}" not in args.local_hostname
    ):
        parser.error(
            "multiple clients need distinct explicit ports; use {worker} or a hostname without a port"
        )


def run_store_benchmark(
    trace_path, model_config, args, replay_scale, client_factory=None
):
    from benchmark import KVCacheRequest, StorageBenchmark, latency_stats
    from layout import create_layout
    import json

    trace_bytes = Path(trace_path).read_bytes()
    requests = []
    for number, line in enumerate(trace_bytes.decode("utf-8").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        required = ("timestamp", "input_length", "output_length", "hash_ids")
        if any(field not in row for field in required):
            raise ValueError(f"trace line {number}: missing required fields")
        if (
            not isinstance(row["timestamp"], (int, float))
            or not math.isfinite(row["timestamp"])
            or row["timestamp"] < 0
        ):
            raise ValueError(f"trace line {number}: invalid timestamp")
        if not isinstance(row["hash_ids"], list) or any(
            type(x) is not int or x < 0 for x in row["hash_ids"]
        ):
            raise ValueError(f"trace line {number}: invalid hash_ids")
        if any(
            type(row[k]) is not int or row[k] < 0
            for k in ("input_length", "output_length")
        ):
            raise ValueError(f"trace line {number}: invalid token lengths")
        if requests and row["timestamp"] < requests[-1].timestamp:
            raise ValueError(f"trace line {number}: timestamps must be nondecreasing")
        requests.append(KVCacheRequest(**{k: row[k] for k in required}))
        if args.max_requests is not None and len(requests) >= args.max_requests:
            break
    if not requests:
        raise ValueError("trace has no requests")

    config = StoreConfig(
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
    page_size = create_layout(model_config, 512).value_size_bytes
    unique_ids = sorted({page for req in requests for page in req.hash_ids})
    key_copies = args.threads if args.key_scope == "independent" else 1
    accesses = sum(len(req.hash_ids) for req in requests)
    retained_objects = (
        accesses * args.threads
        if args.access_mode == "write"
        else len(unique_ids) * key_copies
    )
    sizing = {
        "unique_trace_pages": len(unique_ids),
        "page_size_bytes": page_size,
        "unique_working_set_bytes": retained_objects * page_size,
        "unique_working_set_gib": retained_objects * page_size / 1024**3,
        "expected_object_count_without_eviction": retained_objects,
        "write_key_mode": "unique_per_operation"
        if args.access_mode == "write"
        else "trace_hash",
        "page_accesses_per_worker": accesses,
        "request_count_per_worker": len(requests),
        "replication_and_allocator_overhead_included": False,
    }
    if getattr(args, "dry_run", False):
        return {
            "schema_version": 1,
            "source_base": SOURCE_BASE,
            "backend": "mooncake",
            "status": "dry_run",
            "trace_file": Path(trace_path).name,
            "trace_sha256": hashlib.sha256(trace_bytes).hexdigest(),
            "model": model_config["name"],
            "model_config": model_config,
            "threads": args.threads,
            "key_scope": args.key_scope,
            "replay_scale": replay_scale,
            "access_mode": args.access_mode,
            **sizing,
        }
    namespace = f"mcbench-{uuid.uuid4().hex}"
    print(f"Store namespace: {namespace}")
    print(
        f"Unique pages: {len(unique_ids)}; page size: {page_size} bytes; key scope: {args.key_scope}"
    )
    print(
        "Store capacity is controlled by the cluster; disk --max-pages does not apply."
    )
    workers = []
    errors = []
    cleanup_errors = []
    stop = threading.Event()
    elapsed = 0.0
    phase = "setup"
    status = "ok"

    def process(worker, start):
        bench = worker["benchmark"]
        store = bench.storage
        for req in requests:
            if stop.is_set():
                break
            if replay_scale > 0:
                target = start + (req.timestamp - requests[0].timestamp) / (
                    1000 * replay_scale
                )
                if stop.wait(max(0, target - time.perf_counter())):
                    break
            if stop.is_set():
                break
            scheduled = (
                start + (req.timestamp - requests[0].timestamp) / (1000 * replay_scale)
                if replay_scale > 0
                else None
            )
            request_start = time.perf_counter()
            worker["attempted"] += 1
            if scheduled is not None:
                worker["lag"].append(max(0, request_start - scheduled) * 1000)
            try:
                if args.access_mode == "cache":
                    bench.process_request(req)
                else:
                    io_latency = 0.0
                    op = store.read if args.access_mode == "read" else store.write
                    for page_id in req.hash_ids:
                        # Client::Put returns success without transfer for an
                        # existing key. Fresh worker/sequence keys prevent an
                        # idempotent no-op from inflating write throughput.
                        if args.access_mode == "write":
                            worker["write_sequence"] += 1
                            page_id = f"write-{worker['id']}-{worker['write_sequence']}-{page_id}"
                        io_latency += op(page_id)
                    bench.stats["request_io_latencies_ms"].append(io_latency)
                    bench.stats["request_wall_latencies_ms"].append(
                        (time.perf_counter() - request_start) * 1000
                    )
                completed_at = time.perf_counter()
                worker["completed"] += 1
                worker["tokens"] += req.input_length + req.output_length
                interval = getattr(args, "progress_interval", 0)
                if interval > 0 and worker["completed"] % interval == 0:
                    print(
                        f"worker {worker['id']}: {worker['completed']}/{len(requests)} requests",
                        flush=True,
                    )
                if scheduled is not None:
                    worker["arrival"].append((completed_at - scheduled) * 1000)
            except Exception as exc:
                worker["failed"] += 1
                worker["failed_wall"].append(
                    (time.perf_counter() - request_start) * 1000
                )
                errors.append(f"worker {worker['id']}: {exc}")
                stop.set()
                break

    try:
        for worker_id in range(args.threads):
            worker_config = replace(
                config,
                local_hostname=config.local_hostname.replace(
                    "{worker}", str(worker_id)
                ),
            )
            prefix = (
                namespace
                if args.key_scope == "shared"
                else f"{namespace}:worker-{worker_id}"
            )
            store = MooncakeStoreStorage(
                page_size, prefix, worker_config, client_factory
            )
            bench = StorageBenchmark("", model_config, storage=store)
            workers.append(
                {
                    "id": worker_id,
                    "benchmark": bench,
                    "attempted": 0,
                    "completed": 0,
                    "failed": 0,
                    "tokens": 0,
                    "write_sequence": 0,
                    "lag": [],
                    "arrival": [],
                    "failed_wall": [],
                }
            )
        phase = "warmup"
        if args.warmup:
            for worker in workers:
                if args.key_scope == "shared" and worker["id"] != 0:
                    continue
                for page_id in unique_ids:
                    worker["benchmark"].storage.write(page_id)
        for worker in workers:
            worker["benchmark"].storage.reset_stats()
        phase = "measurement"
        start = time.perf_counter()
        try:
            with ThreadPoolExecutor(max_workers=args.threads) as pool:
                futures = [pool.submit(process, worker, start) for worker in workers]
                try:
                    for future in futures:
                        future.result()
                except BaseException:
                    stop.set()
                    raise
        finally:
            elapsed = time.perf_counter() - start
        if errors:
            status = "failed"
    except KeyboardInterrupt:
        stop.set()
        status = "interrupted"
        errors.append(f"interrupted during {phase}")
    except Exception as exc:
        status = "failed"
        errors.append(f"{phase}: {exc}")

    # Snapshot measured counters before cleanup adds metadata/delete operations.
    store_stats = [w["benchmark"].storage.get_stats() for w in workers]
    raw = [w["benchmark"].storage.stats for w in workers]
    measurement = phase == "measurement"
    if not measurement:
        # Setup/warmup data are not performance samples.
        for worker in workers:
            worker["benchmark"].storage.reset_stats()
        raw = [w["benchmark"].storage.stats for w in workers]
    storage_result = {}
    for op in ("read", "write"):
        latencies = [x for s in raw for x in s[f"{op}_latencies_ms"]]
        byte_count = sum(s[f"{op}_bytes"] for s in raw)
        storage_result[op] = {
            "count": sum(s[f"{op}_count"] for s in raw),
            "mb": byte_count / 1024**2,
            "bytes": byte_count,
            "time_s": sum(s[f"{op}_time_s"] for s in raw),
            "wall_mib_per_second": byte_count / 1024**2 / elapsed if elapsed else 0,
            **latency_stats(latencies),
        }
    exists_hits = sum(s["exists_hits"] for s in raw)
    exists_misses = sum(s["exists_misses"] for s in raw)
    storage_result.update(
        sync_count=0,
        written_pages=len(
            {
                w["benchmark"].storage.key(page)
                for w in workers
                for page in w["benchmark"].storage._written_pages
            }
        ),
        exists={
            "count": exists_hits + exists_misses,
            "hits": exists_hits,
            "misses": exists_misses,
            **latency_stats([x for s in raw for x in s["exists_latencies_ms"]]),
        },
        attempts={
            op: sum(s["attempts"][op] for s in raw)
            for op in ("exists", "read", "write", "delete")
        },
        errors={
            op: sum(s["errors"][op] for s in raw)
            for op in ("exists", "read", "write", "delete")
        },
        verification_failures=sum(s["verification_failures"] for s in raw),
    )
    for worker in workers:
        store = worker["benchmark"].storage
        try:
            if not args.keep_keys:
                for page_id in list(store._touched_pages):
                    try:
                        # All workers have stopped; exact run-owned keys only.
                        # An exists query would renew the lease before remove.
                        store.delete(page_id, force=True, missing_ok=True)
                    except Exception as exc:
                        cleanup_errors.append(
                            f"worker {worker['id']} cleanup {page_id}: {exc}"
                        )
        except Exception as exc:
            cleanup_errors.append(f"worker {worker['id']} cleanup: {exc}")
        finally:
            try:
                store.close()
            except Exception as exc:
                cleanup_errors.append(f"worker {worker['id']} close: {exc}")
    if cleanup_errors and status == "ok":
        status = "failed"
    completed = sum(w["completed"] for w in workers)
    reads = storage_result["read"]["count"]
    writes = storage_result["write"]["count"]
    try:
        binding_version = version("mooncake-transfer-engine")
    except PackageNotFoundError:
        binding_version = "unknown (record deployed build commit separately)"
    return {
        "schema_version": 1,
        "source_base": SOURCE_BASE,
        "client_environment": {name: os.environ.get(name) for name in ENV_FIELDS},
        "byte_accounting": "successful logical API payload bytes; shared-cache duplicate puts can be no-ops",
        "backend": "mooncake",
        "status": status,
        "phase": phase,
        "errors": errors,
        "cleanup_errors": cleanup_errors,
        "preparation_failure_stats": store_stats if not measurement else [],
        "trace_file": Path(trace_path).name,
        "trace_sha256": hashlib.sha256(trace_bytes).hexdigest(),
        "namespace": namespace,
        "config": asdict(config),
        "model_config": model_config,
        "binding_version": binding_version,
        "python": platform.python_version(),
        "model": model_config["name"],
        "page_size_bytes": page_size,
        "sizing": sizing,
        "unique_trace_pages": len(unique_ids),
        "threads": args.threads,
        "key_scope": args.key_scope,
        "access_mode": args.access_mode,
        "warmup": args.warmup,
        "keep_keys": args.keep_keys,
        "replay_scale": replay_scale,
        "fsync_mode": "not_applicable",
        "io_time_s": elapsed,
        "requests_per_second": completed / elapsed if elapsed else 0,
        "total_requests": completed,
        "attempted_requests": sum(w["attempted"] for w in workers),
        "failed_requests": sum(w["failed"] for w in workers),
        "offered_requests": len(requests) * args.threads,
        "total_tokens": sum(w["tokens"] for w in workers),
        "total_pages": reads + writes,
        "read_pages": reads,
        "write_pages": writes,
        "page_hit_rate": reads / (reads + writes) if reads + writes else 0,
        "metadata_hit_rate": exists_hits / (exists_hits + exists_misses)
        if exists_hits + exists_misses
        else None,
        "hit_rate_meaning": "successful cache reads / successful page operations"
        if args.access_mode == "cache"
        else "not_applicable_for_forced_io",
        "request_io_latency": latency_stats(
            [
                x
                for w in workers
                for x in w["benchmark"].stats["request_io_latencies_ms"]
            ]
        ),
        "request_wall_latency": latency_stats(
            [
                x
                for w in workers
                for x in w["benchmark"].stats["request_wall_latencies_ms"]
            ]
        ),
        "schedule_lag_latency": latency_stats([x for w in workers for x in w["lag"]]),
        "arrival_to_completion_latency": latency_stats(
            [x for w in workers for x in w["arrival"]]
        ),
        "failed_request_wall_latency": latency_stats(
            [x for w in workers for x in w["failed_wall"]]
        ),
        "storage": storage_result,
    }
