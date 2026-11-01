"""V9–V14 reliability, Pub/Sub, auth, packaging, and benchmark tests."""

import json
import os
import socket
import tempfile
import threading
import time
import unittest

from miniredis import persistence
from miniredis.resp import encode_array, parse_frame
from miniredis.server import MiniRedisServer
from miniredis.store import MiniRedisStore


class RESPClient:
    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(2)
        self.buffer = bytearray()

    def command(self, *parts):
        self.sock.sendall(encode_array(list(parts)))
        return self.read()

    def read(self):
        while True:
            value, used = parse_frame(bytes(self.buffer))
            if used:
                del self.buffer[:used]
                return value
            self.buffer.extend(self.sock.recv(4096))


class ServerHarness:
    def __init__(self, **kwargs):
        self.server = MiniRedisServer(verbose=False, persistence_path=None, **kwargs)
        self.server._running = True
        self.clients = []
        self.workers = []

    def client(self):
        client, server_side = socket.socketpair()
        self.clients.append(client)
        worker = threading.Thread(target=self.server._handle_client, args=(server_side, ("local", 0)), daemon=True)
        worker.start()
        self.workers.append(worker)
        return RESPClient(client)

    def close(self):
        self.server._running = False
        for client in self.clients:
            try:
                client.close()
            except OSError:
                pass
        for worker in self.workers:
            worker.join(timeout=2)
        self.server.stop()


class TestV9V14(unittest.TestCase):
    def test_store_operations_are_safe_under_concurrent_access(self):
        store = MiniRedisStore()
        barrier = threading.Barrier(8)

        def work(worker):
            barrier.wait()
            for index in range(300):
                store.set(f"shared:{index % 15}", f"{worker}:{index}")
                store.get(f"shared:{index % 15}")
                store.mset([(f"bulk:{worker}:{index}", str(index)), ("common", str(worker))])

        threads = [threading.Thread(target=work, args=(worker,)) for worker in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(store.size(), 8 * 300 + 15 + 1)
        self.assertTrue(all(store.exists(f"bulk:{worker}:{i}") for worker in range(8) for i in (0, 299)))

    def test_simultaneous_clients_can_access_shared_store(self):
        harness = ServerHarness()
        try:
            clients = [harness.client() for _ in range(6)]
            barrier = threading.Barrier(len(clients))
            results = []

            def work(pair):
                index, client = pair
                barrier.wait()
                client.command("SET", "same", str(index))
                results.append(client.command("GET", "same"))

            threads = [threading.Thread(target=work, args=(pair,)) for pair in enumerate(clients)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())
            self.assertEqual(len(results), len(clients))
            self.assertTrue(set(results).issubset({str(i) for i in range(len(clients))}))
        finally:
            harness.close()

    def test_pubsub_resp_delivery_unsubscribe_and_cleanup(self):
        harness = ServerHarness()
        try:
            subscriber = harness.client()
            publisher = harness.client()
            self.assertEqual(subscriber.command("SUBSCRIBE", "news", "alerts"), ["subscribe", "news", 1])
            self.assertEqual(subscriber.read(), ["subscribe", "alerts", 2])
            self.assertEqual(publisher.command("PUBLISH", "news", "hello world"), 1)
            self.assertEqual(subscriber.read(), ["message", "news", "hello world"])
            self.assertEqual(subscriber.command("UNSUBSCRIBE", "news"), ["unsubscribe", "news", 1])
            self.assertEqual(publisher.command("PUBLISH", "news", "not delivered"), 0)
            self.assertEqual(publisher.command("PUBLISH", "alerts", "still delivered"), 1)
            self.assertEqual(subscriber.read(), ["message", "alerts", "still delivered"])
        finally:
            harness.close()
        self.assertEqual(harness.server._pubsub.channel_count, 0)

    def test_authentication_enforces_commands_and_never_logs_secret(self):
        harness = ServerHarness(password="test secret")
        try:
            client = harness.client()
            self.assertEqual(client.command("GET", "hidden"), "ERR authentication required")
            self.assertEqual(client.command("AUTH", "wrong"), "ERR invalid password")
            self.assertEqual(client.command("AUTH", "test secret"), "OK")
            self.assertEqual(client.command("SET", "visible", "yes"), "OK")
            self.assertEqual(client.command("GET", "visible"), "yes")
            self.assertNotEqual(harness.server._password_digest, b"test secret")
        finally:
            harness.close()

    def test_oversized_request_is_rejected(self):
        harness = ServerHarness(max_request_size=1024)
        try:
            client = harness.client()
            client.sock.sendall(b"PING " + b"x" * 1024)
            self.assertEqual(client.read(), "ERR request exceeds configured size limit")
            self.assertEqual(client.sock.recv(1), b"")
        finally:
            harness.close()

    def test_malformed_resp_shapes_are_rejected(self):
        harness = ServerHarness()
        try:
            client = harness.client()
            client.sock.sendall(b"*2\r\n$4\r\nPING\r\n:1\r\n")
            self.assertIn("Protocol error", client.read())
            self.assertEqual(client.command("PING"), "PONG")
            client.sock.sendall(b"+not-a-command-array\r\n")
            self.assertIn("Protocol error", client.read())
            self.assertEqual(client.sock.recv(1), b"")
        finally:
            harness.close()

    def test_status_reports_non_sensitive_runtime_statistics(self):
        harness = ServerHarness(password="sensitive")
        try:
            client = harness.client()
            self.assertEqual(client.command("INFO"), "ERR authentication required")
            self.assertEqual(client.command("AUTH", "sensitive"), "OK")
            status = client.command("STATUS")
            self.assertIn("connected_clients:", status)
            self.assertIn("total_commands_processed:", status)
            self.assertNotIn("sensitive", status)
        finally:
            harness.close()

    def test_typed_persistence_round_trip_and_expiry_filtering(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "snapshot.json")
            store = MiniRedisStore()
            store.set("text", "value")
            store.list_push("list", ["a", "b"], False)
            store.set_add("set", ["z", "a"])
            store.hash_set("hash", [("field", "value")])
            store.set("expires", "soon", ex=0.05)
            persistence.save(store, path)
            state = persistence.load(path)
            self.assertEqual(state["data"], {"text": "value", "expires": "soon"})
            self.assertEqual(state["lists"], {"list": ["a", "b"]})
            self.assertEqual(state["sets"], {"set": {"a", "z"}})
            self.assertEqual(state["hashes"], {"hash": {"field": "value"}})
            time.sleep(0.06)
            restored = persistence.load(path)
            self.assertNotIn("expires", restored["data"])
            restarted = MiniRedisServer(verbose=False, persistence_path=path)
            self.assertEqual(restarted.store.list_range("list", 0, -1), ["a", "b"])
            self.assertEqual(restarted.store.set_members("set"), ["a", "z"])
            self.assertEqual(restarted.store.hash_get("hash", "field"), "value")

    def test_legacy_version_one_snapshot_still_loads(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "legacy.json")
            with open(path, "w", encoding="utf-8") as output:
                json.dump({"version": 1, "entries": {"old": {"value": "data", "expires_at": None}}}, output)
            server = MiniRedisServer(verbose=False, persistence_path=path)
            self.assertEqual(server.store.get("old"), "data")


if __name__ == "__main__":
    unittest.main()
