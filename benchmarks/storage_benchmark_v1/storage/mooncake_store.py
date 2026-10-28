"""Synchronous Mooncake Store adapter; no Mooncake import on the disk path."""

import ctypes
import hashlib
import mmap
import os
import random
import time
from dataclasses import dataclass

from .disk import calc_percentiles
from .interface import Storage


class StorageOperationError(RuntimeError):
    """A Store error is never interpreted as a cache miss or successful I/O."""


@dataclass(frozen=True)
class StoreConfig:
    local_hostname: str
    metadata_server: str
    master_server_addr: str
    protocol: str = "tcp"
    device_name: str = ""
    global_segment_size: int = 0
    local_buffer_size: int = 128 * 1024 * 1024
    io_api: str = "buffer"
    verify: bool = False
    batch_size: int = 8


class MooncakeStoreStorage(Storage):
    """One client and two reusable buffers per worker, full-page objects only.

    The namespace must be unique to a run. Keys are never reduced modulo a
    capacity. Store placement/eviction is controlled by the deployed cluster.
    Buffer preparation and validation are outside the API latency interval,
    but remain part of request wall time. This class is not thread-safe.
    """

    def __init__(self, page_size, namespace, config, client_factory=None):
        if page_size <= 0 or not namespace:
            raise ValueError("positive page_size and nonempty namespace required")
        if config.io_api not in ("plain", "buffer", "batch"):
            raise ValueError("io_api must be plain, buffer or batch")
        if type(config.batch_size) is not int or config.batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if config.global_segment_size < 0 or config.local_buffer_size <= 0:
            raise ValueError("invalid Store memory sizes")
        if config.protocol != "tcp" and "MC_FORCE_TCP" in os.environ:
            raise ValueError(
                "MC_FORCE_TCP is set; unset it before a non-TCP experiment"
            )
        self.page_size = page_size
        self.namespace = namespace
        self.config = config
        self._written_pages = set()
        self._touched_pages = set()
        self._registered = []
        self._closed = False
        self.client = None
        self._maps = []
        self._send_slots = []
        self._recv_slots = []
        self.reset_stats()
        if client_factory is None:
            try:
                from mooncake.store import MooncakeDistributedStore
            except ImportError as exc:
                raise RuntimeError(
                    "Install the Mooncake Python binding built for your server "
                    "and transport before using --backend mooncake."
                ) from exc
            client_factory = MooncakeDistributedStore
        try:
            self.client = client_factory()
            # Verified against 930 store_py.cpp's positional setup overload.
            code = self.client.setup(
                config.local_hostname,
                config.metadata_server,
                config.global_segment_size,
                config.local_buffer_size,
                config.protocol,
                config.device_name,
                config.master_server_addr,
            )
            if code != 0:
                raise StorageOperationError(f"setup failed: {code}")
            self._payload = random.Random(0).randbytes(page_size)
            if config.io_api in ("buffer", "batch"):
                total = page_size * self.batch_window
                aligned = ((total + mmap.PAGESIZE - 1) // mmap.PAGESIZE) * mmap.PAGESIZE
                for slots in (self._send_slots, self._recv_slots):
                    # An unregistered guard page prevents adjacent registered
                    # regions even if the OS places these mappings together.
                    region = mmap.mmap(-1, aligned + mmap.PAGESIZE)
                    self._maps.append(region)
                    slots.extend(
                        (ctypes.c_char * page_size).from_buffer(region, i * page_size)
                        for i in range(self.batch_window)
                    )
                    ptr = ctypes.addressof(slots[0])
                    code = self.client.register_buffer(ptr, aligned)
                    if code != 0:
                        raise StorageOperationError(f"register_buffer failed: {code}")
                    self._registered.append(ptr)
                self._send, self._recv = self._send_slots[0], self._recv_slots[0]
                for slot in self._send_slots:
                    ctypes.memmove(ctypes.addressof(slot), self._payload, page_size)
        except BaseException:
            self.close(suppress_errors=True)
            raise

    def reset_stats(self):
        self.stats = {
            "read_count": 0,
            "write_count": 0,
            "read_bytes": 0,
            "write_bytes": 0,
            "read_time_s": 0.0,
            "write_time_s": 0.0,
            "read_latencies_ms": [],
            "write_latencies_ms": [],
            "exists_latencies_ms": [],
            "exists_count": 0,
            "exists_hits": 0,
            "exists_misses": 0,
            "attempts": {op: 0 for op in ("exists", "read", "write", "delete")},
            "errors": {op: 0 for op in ("exists", "read", "write", "delete")},
            "verification_failures": 0,
            "sync_count": 0,
            "batch_latencies_ms": {op: [] for op in ("exists", "read", "write")},
        }

    @property
    def batch_window(self):
        return self.config.batch_size if self.config.io_api == "batch" else 1

    def _batch_call(self, op, page_ids, fn):
        if not page_ids or len(page_ids) > self.batch_window:
            raise ValueError("batch must contain 1..batch_window pages")
        values, latency = self._call(op, fn)
        self.stats["batch_latencies_ms"][op].append(latency)
        if not isinstance(values, (list, tuple)) or len(values) != len(page_ids):
            self._fail(op, f"batch {op}: invalid result vector {values!r}")
        return values, latency

    def batch_exists(self, page_ids):
        values, latency = self._batch_call(
            "exists",
            page_ids,
            lambda: self.client.batch_is_exist([self.key(p) for p in page_ids]),
        )
        hits = []
        invalid = []
        for page, code in zip(page_ids, values):
            if code not in (0, 1):
                invalid.append(f"{page}: {code}")
                continue
            hits.append(code == 1)
            self.stats["exists_count"] += 1
            self.stats["exists_hits" if code == 1 else "exists_misses"] += 1
            self.stats["exists_latencies_ms"].append(latency / len(page_ids))
        if invalid:
            self._fail("exists", "batch_is_exist failed: " + ", ".join(invalid))
        return hits

    def batch_write(self, page_ids):
        return self._batch_io("write", page_ids)

    def batch_read(self, page_ids):
        return self._batch_io("read", page_ids)

    def _batch_io(self, op, page_ids):
        if self.config.io_api != "batch" or not 0 < len(page_ids) <= self.batch_window:
            raise ValueError("batch I/O requires batch mode and 1..batch_size pages")
        slots = self._send_slots if op == "write" else self._recv_slots
        ptrs = [ctypes.addressof(slot) for slot in slots[: len(page_ids)]]
        keys = [self.key(page) for page in page_ids]
        if op == "write":
            for page, ptr in zip(page_ids, ptrs):
                marker = self._marker(page)
                ctypes.memmove(ptr, marker, len(marker))
            self._touched_pages.update(page_ids)
        fn = self.client.batch_put_from if op == "write" else self.client.batch_get_into
        values, latency = self._batch_call(
            op, page_ids, lambda: fn(keys, ptrs, [self.page_size] * len(keys))
        )
        failures = []
        for page, ptr, value in zip(page_ids, ptrs, values):
            expected = 0 if op == "write" else self.page_size
            if value != expected:
                failures.append(f"{page}: expected {expected}, got {value}")
                continue
            if op == "read" and self.config.verify:
                marker = self._marker(page)
                if (
                    ctypes.string_at(ptr, self.page_size)
                    != marker + self._payload[len(marker) :]
                ):
                    self.stats["verification_failures"] += 1
                    failures.append(f"{page}: data verification failed")
                    continue
            if op == "write":
                self._written_pages.add(page)
            self._record(op, latency / len(page_ids))
        if failures:
            self._fail(op, f"batch {op} failed: " + ", ".join(failures))
        return latency

    def key(self, page_id):
        return f"{self.namespace}:{page_id}"

    def _full_page(self, offset, length):
        if offset != 0 or length not in (None, self.page_size):
            raise ValueError("Mooncake backend requires whole-page accesses")

    def _marker(self, page_id):
        return hashlib.sha256(self.key(page_id).encode()).digest()[: self.page_size]

    def _call(self, op, function):
        self.stats["attempts"][op] += 1
        start = time.perf_counter()
        try:
            value = function()
        except Exception as exc:
            self.stats["errors"][op] += 1
            raise StorageOperationError(f"{op} raised: {exc}") from exc
        return value, (time.perf_counter() - start) * 1000.0

    def _fail(self, op, message):
        self.stats["errors"][op] += 1
        raise StorageOperationError(message)

    def exists(self, page_id):
        code, latency = self._call(
            "exists", lambda: self.client.is_exist(self.key(page_id))
        )
        # bool(-1) is True: explicitly distinguish errors from hits.
        if code not in (0, 1):
            self._fail("exists", f"is_exist failed for {page_id}: {code}")
        self.stats["exists_count"] += 1
        self.stats["exists_latencies_ms"].append(latency)
        self.stats["exists_hits" if code == 1 else "exists_misses"] += 1
        return code == 1

    def _record(self, op, latency):
        self.stats[f"{op}_count"] += 1
        self.stats[f"{op}_bytes"] += self.page_size
        self.stats[f"{op}_time_s"] += latency / 1000
        self.stats[f"{op}_latencies_ms"].append(latency)
        return latency

    def write(self, page_id, offset_in_page=0, length=None):
        self._full_page(offset_in_page, length)
        marker = self._marker(page_id)
        if self.config.io_api in ("buffer", "batch"):
            ptr = ctypes.addressof(self._send)
            ctypes.memmove(ptr, marker, len(marker))

            def call():
                return self.client.put_from(self.key(page_id), ptr, self.page_size)
        else:
            payload = marker + self._payload[len(marker) :]

            def call():
                return self.client.put(self.key(page_id), payload)

        # Track attempted keys too: a failed RPC can have committed remotely.
        self._touched_pages.add(page_id)
        code, latency = self._call("write", call)
        if code != 0:
            self._fail("write", f"put failed for {page_id}: {code}")
        self._written_pages.add(page_id)
        return self._record("write", latency)

    def read(self, page_id, offset_in_page=0, length=None):
        self._full_page(offset_in_page, length)
        if self.config.io_api in ("buffer", "batch"):
            ptr = ctypes.addressof(self._recv)
            size, latency = self._call(
                "read",
                lambda: self.client.get_into(self.key(page_id), ptr, self.page_size),
            )
            if size != self.page_size:
                self._fail(
                    "read",
                    f"get_into for {page_id}: expected {self.page_size}, got {size}",
                )
            data = ctypes.string_at(ptr, size) if self.config.verify else None
        else:
            data, latency = self._call(
                "read", lambda: self.client.get(self.key(page_id))
            )
            if not isinstance(data, (bytes, bytearray)) or len(data) != self.page_size:
                self._fail(
                    "read", f"get returned missing/short/invalid data for {page_id}"
                )
        if self.config.verify:
            marker = self._marker(page_id)
            if data != marker + self._payload[len(marker) :]:
                self.stats["verification_failures"] += 1
                self._fail("read", f"data verification failed for {page_id}")
        return self._record("read", latency)

    def delete(self, page_id, force=False, missing_ok=False):
        code, _ = self._call(
            "delete", lambda: self.client.remove(self.key(page_id), force)
        )
        # 930 types.h: OBJECT_NOT_FOUND=-704. Cleanup can race native eviction.
        if code != 0 and not (missing_ok and code == -704):
            self._fail("delete", f"remove failed for {page_id}: {code}")
        self._written_pages.discard(page_id)
        self._touched_pages.discard(page_id)
        return code == 0

    def get_stats(self):
        result = {
            op: {
                "count": self.stats[f"{op}_count"],
                "mb": self.stats[f"{op}_bytes"] / 1024**2,
                "time_s": self.stats[f"{op}_time_s"],
                **calc_percentiles(self.stats[f"{op}_latencies_ms"]),
            }
            for op in ("read", "write")
        }
        result.update(
            latency_semantics="amortized_batch_time_per_page"
            if self.config.io_api == "batch"
            else "single_page_api_time",
            batch_calls={
                op: {"count": len(values), **calc_percentiles(values)}
                for op, values in self.stats["batch_latencies_ms"].items()
            },
            sync_count=0,
            written_pages=len(self._written_pages),
            exists={
                "count": self.stats["exists_count"],
                "hits": self.stats["exists_hits"],
                "misses": self.stats["exists_misses"],
                **calc_percentiles(self.stats["exists_latencies_ms"]),
            },
            attempts=dict(self.stats["attempts"]),
            errors=dict(self.stats["errors"]),
            verification_failures=self.stats["verification_failures"],
        )
        return result

    def close(self, force_sync=True, suppress_errors=False):
        if self._closed:
            return
        self._closed = True
        errors = []
        if self.client is not None:
            for ptr in reversed(self._registered):
                try:
                    code = self.client.unregister_buffer(ptr)
                    if code != 0:
                        errors.append(f"unregister_buffer: {code}")
                except Exception as exc:
                    errors.append(str(exc))
            # Buffers stay alive through unregister and client shutdown.
            try:
                code = self.client.close()
                if code != 0:
                    errors.append(f"close: {code}")
            except Exception as exc:
                errors.append(str(exc))
        if errors and not suppress_errors:
            raise StorageOperationError("; ".join(errors))
        if not errors:
            self._send = self._recv = None
            self._send_slots.clear()
            self._recv_slots.clear()
            for region in self._maps:
                region.close()
            self._maps.clear()
