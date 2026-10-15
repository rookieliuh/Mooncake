"""Offline API contract and replay tests; no native wheel or service needed."""

import argparse
import contextlib
import ctypes
import io
import json
import os
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


if __name__ == "__main__":
    unittest.main()
