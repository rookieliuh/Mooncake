"""Offline API contract and replay tests; no native wheel or service needed."""

import argparse
import contextlib
import ctypes
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark import StorageBenchmark, format_storage_stats
from storage.mooncake_store import (
    MooncakeStoreStorage,
    StorageOperationError,
    StoreConfig,
)
from store_runner import (
    add_store_arguments,
    run_store_benchmark,
    validate_store_arguments,
)
from store_lifecycle import run_store_cleanup


class FakeStore:
    def __init__(self, objects=None):
        self.objects = objects if objects is not None else {}
        self.registered = set()
        self.closed = False
        self.transferred_write_bytes = 0
        self.remove_calls = []

    def setup(self, *args):
        self.setup_args = args
        return 0

    def register_buffer(self, ptr, size):
        self.registered.add(ptr)
        return 0

    def unregister_buffer(self, ptr):
        self.registered.remove(ptr)
        return 0

    def is_exist(self, key):
        return int(key in self.objects)

    def put(self, key, data):
        # 930 Client::Put treats an existing object as a successful no-op.
        if key not in self.objects:
            self.objects[key] = data
            self.transferred_write_bytes += len(data)
        return 0

    def get(self, key):
        return self.objects.get(key, b"")

    def put_from(self, key, ptr, size):
        assert ptr in self.registered
        return self.put(key, ctypes.string_at(ptr, size))

    def get_into(self, key, ptr, size):
        assert ptr in self.registered
        data = self.get(key)
        ctypes.memmove(ptr, data, min(len(data), size))
        return len(data)

    def remove(self, key, force=False):
        self.remove_calls.append((key, force))
        if key not in self.objects:
            return -704
        del self.objects[key]
        return 0

    def close(self):
        self.closed = True
        return 0


class BackendTests(unittest.TestCase):
    def make_store(self, api="buffer", verify=True):
        client = FakeStore()
        store = MooncakeStoreStorage(
            64,
            "test-run",
            StoreConfig("host", "metadata", "master", io_api=api, verify=verify),
            lambda: client,
        )
        self.addCleanup(store.close, suppress_errors=True)
        return store, client

    def test_roundtrip_and_exact_keys(self):
        for api in ("plain", "buffer"):
            store, client = self.make_store(api)
            self.assertFalse(store.exists(7))
            store.write(7)
            store.write(2007)
            self.assertEqual(len(client.objects), 2)
            self.assertNotEqual(
                client.objects[store.key(7)], client.objects[store.key(2007)]
            )
            self.assertTrue(store.exists(7))
            store.read(7)
            store.read(2007)
            store.delete(7)
            self.assertFalse(store.exists(7))
            self.assertEqual(store.get_stats()["read"]["count"], 2)

    def test_exists_error_is_not_truthy_hit(self):
        store, client = self.make_store()
        client.is_exist = lambda key: -1
        with self.assertRaises(StorageOperationError):
            store.exists(1)
        self.assertEqual(store.stats["errors"]["exists"], 1)
        self.assertEqual(store.stats["exists_count"], 0)

    def test_read_lengths_and_failed_put(self):
        for code in (-1, 0, 63, 65):
            store, client = self.make_store()
            client.get_into = lambda *args: code
            with self.assertRaises(StorageOperationError):
                store.read(1)
            self.assertEqual(store.stats["read_bytes"], 0)
        store, client = self.make_store()
        client.put_from = lambda *args: -5
        with self.assertRaises(StorageOperationError):
            store.write(1)
        self.assertEqual(store.stats["write_count"], 0)
        self.assertIn(1, store._touched_pages)
        self.assertNotIn(1, store._written_pages)

    def test_plain_invalid_results(self):
        for data in (None, b"", b"short", -1):
            store, client = self.make_store("plain")
            client.get = lambda key: data
            with self.assertRaises(StorageOperationError):
                store.read(0)

    def test_verification_detects_wrong_key_and_tail_corruption(self):
        for api in ("plain", "buffer"):
            store, client = self.make_store(api)
            store.write(1)
            store.write(2)
            client.objects[store.key(1)] = client.objects[store.key(2)]
            with self.assertRaises(StorageOperationError):
                store.read(1)
            store.delete(1)
            store.write(1)
            data = client.objects[store.key(1)]
            client.objects[store.key(1)] = data[:-1] + bytes([data[-1] ^ 1])
            with self.assertRaises(StorageOperationError):
                store.read(1)
            self.assertEqual(store.stats["verification_failures"], 2)

    def test_partial_access_rejected(self):
        store, client = self.make_store()
        for method in (store.read, store.write):
            with self.assertRaises(ValueError):
                method(0, offset_in_page=1, length=63)
        self.assertEqual(client.objects, {})

    def test_partial_registration_cleanup(self):
        client = FakeStore()
        original = client.register_buffer
        client.register_buffer = (
            lambda ptr, size: -2 if client.registered else original(ptr, size)
        )
        with self.assertRaises(StorageOperationError):
            MooncakeStoreStorage(64, "run", StoreConfig("h", "m", "s"), lambda: client)
        self.assertFalse(client.registered)
        self.assertTrue(client.closed)

    def test_setup_failure_cleanup(self):
        client = FakeStore()
        client.setup = lambda *args: -1
        with self.assertRaises(StorageOperationError):
            MooncakeStoreStorage(64, "run", StoreConfig("h", "m", "s"), lambda: client)
        self.assertTrue(client.closed)

    def test_close_idempotent(self):
        store, client = self.make_store()
        store.close()
        store.close()
        self.assertFalse(client.registered)
        self.assertTrue(client.closed)

    def test_exception_and_remove_failure(self):
        store, client = self.make_store()

        def bad_put(*args):
            raise OSError("network failed")

        client.put_from = bad_put
        with self.assertRaises(StorageOperationError):
            store.write(1)
        client.remove = lambda key, force=False: -1
        with self.assertRaises(StorageOperationError):
            store.delete(1)
        self.assertEqual(store.stats["errors"]["write"], 1)
        self.assertEqual(store.stats["errors"]["delete"], 1)

    def test_930_ub_setup_argument_contract(self):
        client = FakeStore()
        config = StoreConfig(
            "client:12345",
            "P2PHANDSHAKE",
            "master:50051",
            protocol="ub",
            device_name="urma0",
            local_buffer_size=4096,
        )
        with patch.dict(os.environ, {}, clear=True):
            store = MooncakeStoreStorage(64, "ub-run", config, lambda: client)
        self.addCleanup(store.close)
        self.assertEqual(
            client.setup_args,
            ("client:12345", "P2PHANDSHAKE", 0, 4096, "ub", "urma0", "master:50051"),
        )
        store.write(1)
        store.read(1)

    def test_force_tcp_environment_rejected_before_connect(self):
        with patch.dict(os.environ, {"MC_FORCE_TCP": "0"}):
            with self.assertRaisesRegex(ValueError, "MC_FORCE_TCP"):
                MooncakeStoreStorage(
                    64,
                    "run",
                    StoreConfig("h", "m", "s", protocol="ub"),
                    lambda: self.fail("must not construct client"),
                )

    def test_930_close_error_reported(self):
        store, client = self.make_store()
        client.close = lambda: -1
        with self.assertRaisesRegex(StorageOperationError, "close: -1"):
            store.close()
        self.assertFalse(client.registered)

    def test_missing_cleanup_is_idempotent_but_normal_delete_is_strict(self):
        store, client = self.make_store()
        self.assertFalse(store.delete(77, force=True, missing_ok=True))
        self.assertEqual(store.stats["errors"]["delete"], 0)
        with self.assertRaises(StorageOperationError):
            store.delete(77)


MODEL = {
    "name": "tiny-test",
    "num_layers": 1,
    "kv_lora_rank": 1,
    "qk_rope_head_dim": 0,
    "index_head_dim": 0,
    "kv_precision_bytes": 1,
}


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.trace = Path(self.temp.name) / "trace.jsonl"
        self.rows = [
            {
                "timestamp": 0,
                "input_length": 1024,
                "output_length": 10,
                "hash_ids": [1, 2],
            },
            {
                "timestamp": 1,
                "input_length": 1024,
                "output_length": 10,
                "hash_ids": [1, 3],
            },
        ]
        self.trace.write_text(
            "\n".join(json.dumps(row) for row in self.rows), encoding="utf-8"
        )
        self.args = argparse.Namespace(
            local_hostname="host",
            metadata_server="metadata",
            master_server="master",
            protocol="tcp",
            device_name="",
            global_segment_size=0,
            local_buffer_size=4096,
            io_api="buffer",
            verify=True,
            max_requests=None,
            key_scope="independent",
            access_mode="cache",
            warmup=False,
            threads=1,
            keep_keys=False,
        )
        self.objects = {"unrelated-production-key": b"keep"}
        self.clients = []

    def factory(self):
        client = FakeStore(self.objects)
        self.clients.append(client)
        return client

    def run_case(self, factory=None, scale=0):
        with contextlib.redirect_stdout(io.StringIO()):
            return run_store_benchmark(
                str(self.trace), MODEL, self.args, scale, factory or self.factory
            )

    def test_cold_trace_counts_cleanup_json(self):
        result = self.run_case()
        self.assertEqual(result["status"], "ok")
        self.assertEqual((result["read_pages"], result["write_pages"]), (1, 3))
        self.assertEqual(result["page_hit_rate"], 0.25)
        self.assertEqual(result["total_requests"], 2)
        self.assertAlmostEqual(result["requests_per_second"], 2 / result["io_time_s"])
        self.assertAlmostEqual(
            result["storage"]["write"]["wall_mib_per_second"],
            3 * 512 / 1024**2 / result["io_time_s"],
        )
        self.assertEqual(result["storage"]["exists"]["count"], 4)
        self.assertEqual(result["storage"]["attempts"]["delete"], 0)
        self.assertEqual(self.objects, {"unrelated-production-key": b"keep"})
        self.assertTrue(all(c.closed and not c.registered for c in self.clients))
        json.dumps(result, allow_nan=False)
        self.assertIn("Status:           ok", format_storage_stats(result))

    def test_warm_shared_and_independent_replay(self):
        self.args.threads = 2
        result = self.run_case()
        self.assertEqual((result["read_pages"], result["write_pages"]), (2, 6))
        self.args.key_scope = "shared"
        self.args.warmup = True
        self.args.access_mode = "read"
        result = self.run_case(scale=1)
        self.assertEqual(result["status"], "ok")
        self.assertEqual((result["read_pages"], result["write_pages"]), (8, 0))
        self.assertEqual(result["storage"]["exists"]["count"], 0)
        self.assertEqual(result["total_requests"], 4)
        self.assertIn("N/A (forced read/write)", format_storage_stats(result))

    def test_keep_keys_unique_namespace(self):
        self.args.keep_keys = True
        one = self.run_case()
        two = self.run_case()
        self.assertNotEqual(one["namespace"], two["namespace"])
        self.assertEqual(two["write_pages"], 3)
        self.assertEqual(len(self.objects), 7)

    def test_dry_run_sizes_without_connecting(self):
        self.args.dry_run = True
        self.args.threads = 2
        result = self.run_case()
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["unique_working_set_bytes"], 3 * 512 * 2)
        self.assertFalse(self.clients)
        self.args.key_scope = "shared"
        self.assertEqual(self.run_case()["unique_working_set_bytes"], 3 * 512)

    def test_forced_write_skips_exists(self):
        self.args.access_mode = "write"
        self.args.key_scope = "shared"
        self.args.threads = 2
        result = self.run_case()
        self.assertEqual(result["write_pages"], 8)
        self.assertEqual(result["storage"]["exists"]["count"], 0)
        self.assertEqual(sum(c.transferred_write_bytes for c in self.clients), 8 * 512)
        self.assertEqual(result["sizing"]["unique_working_set_bytes"], 8 * 512)

    def test_cleanup_uses_force_only_on_owned_keys(self):
        def factory():
            client = self.factory()
            remove = client.remove
            client.remove = (
                lambda key, force=False: remove(key, force) if force else -706
            )
            return client

        result = self.run_case(factory)
        self.assertEqual(result["status"], "ok")
        calls = self.clients[0].remove_calls
        self.assertEqual(len(calls), 3)
        self.assertTrue(
            all(
                force and key.startswith(result["namespace"] + ":")
                for key, force in calls
            )
        )
        self.assertEqual(self.objects, {"unrelated-production-key": b"keep"})

    def test_mid_request_failure(self):
        def factory():
            client = self.factory()
            original = client.put_from
            client.put_from = (
                lambda key, ptr, size: -1
                if key.endswith(":2")
                else original(key, ptr, size)
            )
            return client

        result = self.run_case(factory)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["total_requests"], 0)
        self.assertEqual(result["failed_requests"], 1)
        self.assertEqual(result["write_pages"], 1)
        self.assertEqual(result["storage"]["errors"]["write"], 1)
        self.assertEqual(result["attempted_requests"], 1)
        self.assertTrue(self.clients[0].closed)

    def test_warmup_failure_excluded(self):
        self.args.warmup = True

        def factory():
            client = self.factory()
            client.put_from = lambda *args: -1
            return client

        result = self.run_case(factory)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["phase"], "warmup")
        self.assertEqual(result["total_pages"], 0)
        self.assertEqual(result["io_time_s"], 0)
        self.assertTrue(result["errors"])

    def test_cleanup_failure_releases_resources(self):
        def factory():
            client = self.factory()
            client.remove = lambda key, force=False: -1
            return client

        result = self.run_case(factory)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["cleanup_errors"])
        self.assertTrue(self.clients[0].closed)
        self.assertFalse(self.clients[0].registered)

    def test_invalid_trace_before_connection(self):
        self.rows[1]["timestamp"] = -1
        self.trace.write_text(
            "\n".join(json.dumps(row) for row in self.rows), encoding="utf-8"
        )
        with self.assertRaises(ValueError):
            self.run_case()
        self.assertFalse(self.clients)

    def test_legacy_disk_constructor(self):
        with patch("benchmark.DiskHashTable") as disk:
            bench = StorageBenchmark("test-path", MODEL, max_pages=2)
            self.assertIs(bench.storage, disk.return_value)
            self.assertEqual(disk.call_args.kwargs["max_pages"], 2)

    def test_cli_required_connections(self):
        parser = argparse.ArgumentParser()
        add_store_arguments(parser)
        args = parser.parse_args(["--backend", "mooncake"])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            validate_store_arguments(parser, args)

    def shared_writer(self, name="A", action="prefill"):
        self.args.namespace = "experiment-1"
        self.args.client_id = name
        self.args.key_scope = "shared"
        self.args.action = action
        self.args.key_manifest = str(Path(self.temp.name) / f"{name}.keys.json")
        return self.run_case()

    def cleanup(self, paths):
        self.args.cleanup_manifest = paths
        self.args.dry_run = False
        return run_store_cleanup(self.args, self.factory)

    def test_shared_prefill_reader_and_central_cleanup(self):
        prefill = self.shared_writer()
        manifest = self.args.key_manifest
        self.assertEqual(prefill["status"], "ok")
        self.assertEqual(prefill["phase"], "prefill_complete")
        self.assertEqual(prefill["total_requests"], 0)
        self.assertEqual(prefill["offered_requests"], 0)
        self.assertEqual(prefill["preparation_stats"][0]["write"]["count"], 3)
        self.assertEqual(len(self.objects), 4)
        self.assertFalse(self.clients[0].remove_calls)
        self.args.action = "replay"
        self.args.client_id = "C"
        self.args.local_hostname = "other-host"
        self.args.key_manifest = None
        self.args.access_mode = "read"
        self.args.threads = 2
        read = self.run_case()
        self.assertEqual(read["namespace"], prefill["namespace"])
        self.assertEqual((read["read_pages"], read["write_pages"]), (8, 0))
        self.assertEqual(read["status"], "ok")
        self.assertEqual(len(self.objects), 4)
        self.assertTrue(all(not c.remove_calls for c in self.clients))
        cleaned = self.cleanup([manifest, manifest])
        self.assertEqual(cleaned["deleted_keys"], 3)
        self.assertEqual(cleaned["planned_keys"], 3)
        self.assertEqual(self.objects, {"unrelated-production-key": b"keep"})
        self.assertEqual(self.cleanup([manifest])["already_missing_keys"], 3)

    def test_shared_identity_separates_model_trace_and_experiment(self):
        first = self.shared_writer()
        self.args.action = "replay"
        self.args.access_mode = "read"
        self.args.key_manifest = None
        self.args.max_requests = 1
        self.assertEqual(self.run_case()["read_pages"], 2)
        self.args.namespace = "other-experiment"
        self.assertEqual(self.run_case()["status"], "failed")
        self.args.namespace = "experiment-1"
        self.trace.write_text(self.trace.read_text() + "\n", encoding="utf-8")
        self.assertNotEqual(self.run_case()["namespace"], first["namespace"])
        with contextlib.redirect_stdout(io.StringIO()):
            result = run_store_benchmark(
                str(self.trace), dict(MODEL, num_layers=2), self.args, 0, self.factory
            )
        self.assertEqual(result["status"], "failed")
        self.assertNotEqual(result["namespace"], first["namespace"])

    def test_explicit_independent_clients_do_not_collide(self):
        self.args.namespace = "experiment-1"
        self.args.action = "prefill"
        manifests = []
        for name in ("A", "C"):
            self.args.client_id = name
            self.args.key_manifest = str(Path(self.temp.name) / f"{name}.json")
            manifests.append(self.args.key_manifest)
            self.assertEqual(self.run_case()["status"], "ok")
        self.assertEqual(len(self.objects), 7)
        self.assertEqual(self.cleanup(manifests)["deleted_keys"], 6)

    def test_explicit_write_keys_unique_across_clients_and_restarts(self):
        manifests = []
        self.args.access_mode = "write"
        for index, name in enumerate(("A", "C", "A")):
            # Even a restarted client with the same ID cannot repeat put keys.
            self.args.namespace = "experiment-1"
            self.args.key_scope = "shared"
            self.args.client_id = name
            self.args.key_manifest = str(Path(self.temp.name) / f"{index}.json")
            result = self.run_case()
            manifests.append(self.args.key_manifest)
            self.assertEqual(result["write_pages"], 4)
        self.assertEqual(sum(c.transferred_write_bytes for c in self.clients), 12 * 512)
        self.assertEqual(self.cleanup(manifests)["deleted_keys"], 12)

    def test_failed_prefill_retains_inventory_and_objects_for_cleanup(self):
        self.args.namespace = "experiment-1"
        self.args.key_scope = "shared"
        self.args.action = "prefill"
        manifest = str(Path(self.temp.name) / "partial.json")
        self.args.key_manifest = manifest

        def factory():
            # Inventory must exist before the first connection/write.
            self.assertEqual(len(json.loads(Path(manifest).read_text())["keys"]), 3)
            client = self.factory()
            put = client.put_from

            def fail_after_commit(key, ptr, size):
                result = put(key, ptr, size)
                return -1 if key.endswith(":2") else result

            client.put_from = fail_after_commit
            return client

        result = self.run_case(factory)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(len(self.objects), 3)
        self.assertFalse(self.clients[0].remove_calls)
        cleanup = self.cleanup([manifest])
        self.assertEqual(cleanup["deleted_keys"], 2)
        self.assertEqual(cleanup["already_missing_keys"], 1)

    def test_manifest_overwrite_and_invalid_cleanup_rejected_before_connect(self):
        self.shared_writer()
        manifest = self.args.key_manifest
        clients = len(self.clients)
        with self.assertRaises(FileExistsError):
            self.run_case()
        self.assertEqual(len(self.clients), clients)
        record = json.loads(Path(manifest).read_text())
        record["keys"].append("unrelated-production-key")
        Path(manifest).write_text(json.dumps(record))
        with self.assertRaisesRegex(ValueError, "outside"):
            self.cleanup([manifest])
        self.assertEqual(len(self.clients), clients)
        record["keys"].pop()
        record["master_server"] = "other-cluster"
        Path(manifest).write_text(json.dumps(record))
        with self.assertRaisesRegex(ValueError, "Master differs"):
            self.cleanup([manifest])

    def test_central_cleanup_continues_after_error_and_is_retryable(self):
        self.shared_writer()
        manifest = self.args.key_manifest
        self.args.cleanup_manifest = [manifest]
        self.args.dry_run = True
        clients = len(self.clients)
        self.assertEqual(run_store_cleanup(self.args, self.factory)["planned_keys"], 3)
        self.assertEqual(len(self.clients), clients)
        self.args.dry_run = False

        def factory():
            client = self.factory()
            remove = client.remove
            client.remove = (
                lambda key, force=False: -1
                if key.endswith(":2")
                else remove(key, force)
            )
            return client

        result = run_store_cleanup(self.args, factory)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["deleted_keys"], 2)
        self.assertTrue(self.clients[-1].closed)
        self.assertEqual(self.cleanup([manifest])["deleted_keys"], 1)

    def test_shared_cli_validation(self):
        parser = argparse.ArgumentParser()
        add_store_arguments(parser)
        defaults = dict(
            page_size_tokens=512,
            fsync_mode="none",
            max_requests=2,
            threads=1,
            scenario="conversation",
            replay_scales="0",
        )
        base = [
            "--backend",
            "mooncake",
            "--master-server",
            "master",
            "--metadata-server",
            "metadata",
            "--local-hostname",
            "host",
        ]
        valid = [
            ["--namespace", "exp", "--key-scope", "shared", "--access-mode", "read"],
            [
                "--namespace",
                "exp",
                "--key-scope",
                "shared",
                "--action",
                "prefill",
                "--key-manifest",
                "keys.json",
            ],
            ["--action", "cleanup", "--cleanup-manifest", "keys.json"],
        ]
        for options in valid:
            args = parser.parse_args(base + options)
            vars(args).update(defaults)
            validate_store_arguments(parser, args)
        invalid = [
            ["--namespace", "exp"],
            ["--namespace", "exp", "--key-scope", "shared"],
            ["--action", "prefill"],
            ["--action", "cleanup"],
            [
                "--namespace",
                "bad:name",
                "--key-scope",
                "shared",
                "--access-mode",
                "read",
            ],
            ["--access-mode", "read"],
            [
                "--action",
                "cleanup",
                "--cleanup-manifest",
                "keys.json",
                "--output-json",
                "keys.json",
            ],
        ]
        for options in invalid:
            args = parser.parse_args(base + options)
            vars(args).update(defaults)
            with self.subTest(options=options), contextlib.redirect_stderr(
                io.StringIO()
            ), self.assertRaises(SystemExit):
                validate_store_arguments(parser, args)

    def test_cli_lifecycle_across_three_separate_processes(self):
        # Three real Python processes share a persisted FakeStore, exercising
        # CLI parsing, imports, key identity, verification and JSON exit status.
        # This is not a native/network test.
        script = """
import sys, types, json, base64
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / "tests"))
from test_mooncake_store import FakeStore, MODEL
db = Path(sys.argv.pop(1))
objects = {k: base64.b64decode(v) for k,v in json.loads(db.read_text()).items()} if db.exists() else {}
class PersistentFake(FakeStore):
    def __init__(self): super().__init__(objects)
    def close(self):
        db.write_text(json.dumps({k: base64.b64encode(v).decode() for k,v in objects.items()}))
        return super().close()
module = types.ModuleType("mooncake.store")
module.MooncakeDistributedStore = PersistentFake
sys.modules["mooncake"] = types.ModuleType("mooncake")
sys.modules["mooncake.store"] = module
import benchmark
benchmark.get_model_config = lambda name: MODEL
benchmark.main()
"""
        root = Path(self.temp.name)
        (root / "conversation_trace.jsonl").write_bytes(self.trace.read_bytes())
        common = [
            "--backend",
            "mooncake",
            "--master-server",
            "master",
            "--metadata-server",
            "metadata",
            "--local-hostname",
            "host",
        ]
        replay = [
            "--namespace",
            "three-process",
            "--key-scope",
            "shared",
            "--trace-dir",
            str(root),
            "--scenario",
            "conversation",
            "--verify",
        ]
        manifest = str(root / "keys.json")
        steps = [
            replay
            + ["--action", "prefill", "--key-manifest", manifest, "--client-id", "A"],
            replay + ["--access-mode", "read", "--client-id", "C"],
            [
                "--action",
                "cleanup",
                "--cleanup-manifest",
                manifest,
                "--trace-dir",
                str(root / "absent"),
            ],
        ]
        results = []
        for index, options in enumerate(steps):
            output = root / f"result-{index}.json"
            done = subprocess.run(
                [sys.executable, "-B", "-c", script, str(root / "db.json")]
                + common
                + options
                + ["--output-json", str(output)],
                cwd=Path(__file__).resolve().parents[1],
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
            results.append(json.loads(output.read_text())[0])
        self.assertEqual(results[0]["namespace"], results[1]["namespace"])
        self.assertEqual((results[1]["read_pages"], results[1]["write_pages"]), (4, 0))
        self.assertEqual(results[2]["deleted_keys"], 3)
        self.assertEqual(json.loads((root / "db.json").read_text()), {})


if __name__ == "__main__":
    unittest.main()
