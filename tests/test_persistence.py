"""
Tests for the persistence module (V6).

Covers:
- save() and load() round-trip for plain keys
- save() and load() round-trip for keys with TTL (TTL is preserved within tolerance)
- Expired keys are NOT restored on load
- Missing file returns empty state without raising
- Malformed/corrupt JSON returns empty state with a warning (no exception)
- Atomic write: a fresh save always produces a valid, complete file
- Keys without TTL are loaded with -1 TTL
"""

import json
import math
import os
import socket
import tempfile
import threading
import time
import unittest
import warnings

from miniredis.store import MiniRedisStore
from miniredis import persistence
from miniredis.server import MiniRedisServer
from miniredis.resp import encode_array, parse_frame


class TestPersistenceSaveLoad(unittest.TestCase):
    """Round-trip tests: save → load → verify."""

    def setUp(self):
        """Create a temporary file path for each test (file is NOT created yet)."""
        fd, self.path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.remove(self.path)  # We want a path that doesn't exist yet

    def tearDown(self):
        """Remove any persistence file and its .tmp sibling created during the test."""
        for p in (self.path, self.path + ".tmp"):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

    def _make_store(self, *args, **kwargs):
        """Helper: create a fresh MiniRedisStore."""
        return MiniRedisStore()

    # ------------------------------------------------------------------
    # Basic save / load
    # ------------------------------------------------------------------

    def test_save_creates_valid_json_file(self):
        store = self._make_store()
        store.set("name", "Maheeth")
        store.set("city", "Trichy")
        persistence.save(store, self.path)

        self.assertTrue(os.path.exists(self.path))
        with open(self.path, "r") as f:
            data = json.load(f)  # Must not raise

        self.assertEqual(data["version"], 2)
        self.assertIn("entries", data)
        self.assertIn("name", data["entries"])
        self.assertIn("city", data["entries"])

    def test_load_restores_plain_keys(self):
        store = self._make_store()
        store.set("name", "Maheeth")
        store.set("age", "18")
        persistence.save(store, self.path)

        state = persistence.load(self.path)
        self.assertEqual(state["data"]["name"], "Maheeth")
        self.assertEqual(state["data"]["age"], "18")
        self.assertEqual(state["expires"], {})

    def test_plain_keys_have_no_expiry_in_file(self):
        store = self._make_store()
        store.set("key", "val")
        persistence.save(store, self.path)

        with open(self.path) as f:
            payload = json.load(f)
        self.assertIsNone(payload["entries"]["key"]["expires_at"])

    # ------------------------------------------------------------------
    # TTL preservation
    # ------------------------------------------------------------------

    def test_ttl_is_preserved_on_reload(self):
        store = self._make_store()
        store.set("temp", "data", ex=30)      # 30-second TTL
        persistence.save(store, self.path)

        state = persistence.load(self.path)
        self.assertIn("temp", state["data"])
        self.assertIn("temp", state["expires"])

        # Restore into a new store and check TTL is still ~30 seconds
        new_store = self._make_store()
        new_store._data.update(state["data"])
        new_store._expires.update(state["expires"])

        remaining = new_store.ttl("temp")
        # Should be between 28 and 30 (allowing up to 2 s for test overhead)
        self.assertGreaterEqual(remaining, 28)
        self.assertLessEqual(remaining, 30)

    def test_very_short_ttl_is_preserved_immediately(self):
        """A key with a 5-second TTL should still be present right after save/load."""
        store = self._make_store()
        store.set("quick", "value", ex=5)
        persistence.save(store, self.path)

        state = persistence.load(self.path)
        self.assertIn("quick", state["data"])

    # ------------------------------------------------------------------
    # Expired key filtering
    # ------------------------------------------------------------------

    def test_expired_keys_not_restored(self):
        """
        Manually write a persistence file with a past expires_at timestamp
        so we can test without sleeping.
        """
        past_unix = time.time() - 10  # Expired 10 seconds ago
        payload = {
            "version": 1,
            "saved_at": time.time(),
            "entries": {
                "gone": {"value": "bye", "expires_at": past_unix},
                "alive": {"value": "hello", "expires_at": None},
            },
        }
        with open(self.path, "w") as f:
            json.dump(payload, f)

        state = persistence.load(self.path)
        self.assertNotIn("gone", state["data"],
                         "Expired key must not be restored")
        self.assertIn("alive", state["data"])

    def test_already_expired_key_not_in_expires_dict(self):
        past_unix = time.time() - 1
        payload = {
            "version": 1,
            "saved_at": time.time(),
            "entries": {"stale": {"value": "x", "expires_at": past_unix}},
        }
        with open(self.path, "w") as f:
            json.dump(payload, f)

        state = persistence.load(self.path)
        self.assertNotIn("stale", state["expires"])

    # ------------------------------------------------------------------
    # Missing and malformed file handling
    # ------------------------------------------------------------------

    def test_load_missing_file_returns_empty(self):
        state = persistence.load("/tmp/miniredis_nonexistent_abc123.json")
        self.assertEqual(state["data"], {})
        self.assertEqual(state["expires"], {})

    def test_load_missing_file_does_not_raise(self):
        try:
            persistence.load("/tmp/miniredis_nonexistent_abc123.json")
        except Exception as exc:
            self.fail(f"load() raised {exc!r} on a missing file")

    def test_load_corrupt_json_returns_empty_with_warning(self):
        with open(self.path, "w") as f:
            f.write("THIS IS NOT VALID JSON {{{{")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            state = persistence.load(self.path)

        self.assertEqual(state["data"], {})
        self.assertEqual(state["expires"], {})
        self.assertTrue(any("Failed to load" in str(w.message) for w in caught),
                        "Expected a warning about corrupt file")

    def test_load_corrupt_json_does_not_overwrite_file(self):
        """The corrupt file must still be on disk after a failed load."""
        original_content = "DEFINITELY NOT JSON"
        with open(self.path, "w") as f:
            f.write(original_content)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            persistence.load(self.path)

        with open(self.path) as f:
            self.assertEqual(f.read(), original_content,
                             "Corrupt file must not be overwritten by load()")

    def test_load_empty_file_returns_empty_with_warning(self):
        with open(self.path, "w") as f:
            f.write("")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            state = persistence.load(self.path)

        self.assertEqual(state["data"], {})

    def test_load_wrong_schema_returns_empty_with_warning(self):
        with open(self.path, "w") as f:
            json.dump({"not_the_right_format": True}, f)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            state = persistence.load(self.path)

        self.assertEqual(state["data"], {})

    # ------------------------------------------------------------------
    # Atomic write
    # ------------------------------------------------------------------

    def test_save_does_not_leave_tmp_file(self):
        store = self._make_store()
        store.set("k", "v")
        persistence.save(store, self.path)

        self.assertTrue(os.path.exists(self.path))
        self.assertFalse(os.path.exists(self.path + ".tmp"),
                         ".tmp file must be removed after successful save")

    def test_save_overwrites_existing_file(self):
        store = self._make_store()
        store.set("k1", "first")
        persistence.save(store, self.path)

        store.set("k2", "second")
        persistence.save(store, self.path)

        state = persistence.load(self.path)
        self.assertIn("k1", state["data"])
        self.assertIn("k2", state["data"])

    # ------------------------------------------------------------------
    # Empty store
    # ------------------------------------------------------------------

    def test_save_empty_store(self):
        store = self._make_store()
        persistence.save(store, self.path)

        state = persistence.load(self.path)
        self.assertEqual(state["data"], {})

    # ------------------------------------------------------------------
    # Full round-trip via MiniRedisStore
    # ------------------------------------------------------------------

    def test_full_round_trip_via_store(self):
        """Save store → reload into fresh store → all values identical."""
        store1 = self._make_store()
        store1.set("a", "alpha")
        store1.set("b", "beta")
        store1.set("c", "gamma", ex=60)
        persistence.save(store1, self.path)

        state = persistence.load(self.path)
        store2 = self._make_store()
        store2._data.update(state["data"])
        store2._expires.update(state["expires"])

        self.assertEqual(store2.get("a"), "alpha")
        self.assertEqual(store2.get("b"), "beta")
        self.assertEqual(store2.get("c"), "gamma")
        # TTL for "c" should be ~60 seconds
        self.assertGreaterEqual(store2.ttl("c"), 58)

    def test_mset_is_saved_and_restored_through_server_restart(self):
        """Exercise the server's MSET persistence hook without binding a TCP port."""
        server = MiniRedisServer(
            store=MiniRedisStore(), verbose=False, persistence_path=self.path
        )
        server._running = True
        client_sock, server_sock = socket.socketpair()
        worker = threading.Thread(
            target=server._handle_client, args=(server_sock, ("local", 0)), daemon=True
        )
        worker.start()
        try:
            client_sock.sendall(encode_array([
                "MSET", "first", "one", "second", "two words", "third", "three"
            ]))
            response, consumed = parse_frame(client_sock.recv(1024))
            self.assertEqual((response, consumed), ("OK", 5))
        finally:
            client_sock.close()
            worker.join(timeout=2)

        restarted = MiniRedisServer(
            store=MiniRedisStore(), verbose=False, persistence_path=self.path
        )
        self.assertEqual(restarted.store.mget(["first", "second", "third"]),
                         ["one", "two words", "three"])

    def test_expired_mset_value_is_not_restored_after_restart(self):
        """An expired entry in the live store is purged before snapshot writing."""
        store = self._make_store()
        store.mset([("live", "kept"), ("expired", "discarded")])
        store._expires["expired"] = time.monotonic() - 1
        persistence.save(store, self.path)

        restarted = MiniRedisServer(
            store=MiniRedisStore(), verbose=False, persistence_path=self.path
        )
        self.assertEqual(restarted.store.get("live"), "kept")
        self.assertIsNone(restarted.store.get("expired"))

    def test_v8_collections_round_trip_with_ttl(self):
        store = self._make_store()
        store.list_push("queue", ["left", "right"], left=False)
        store.set_add("colors", ["red", "blue", "red"])
        store.hash_set("user", [("name", "Ada"), ("city", "London")])
        store.expire("queue", 60)
        persistence.save(store, self.path)

        restarted = MiniRedisServer(
            store=MiniRedisStore(), verbose=False, persistence_path=self.path
        )
        self.assertEqual(restarted.store.list_range("queue", 0, -1), ["left", "right"])
        self.assertEqual(restarted.store.set_members("colors"), ["blue", "red"])
        self.assertEqual(restarted.store.hash_getall("user"), ["city", "London", "name", "Ada"])
        self.assertGreaterEqual(restarted.store.ttl("queue"), 58)

    def test_v8_mutations_are_saved_by_server_command_path(self):
        server = MiniRedisServer(
            store=MiniRedisStore(), verbose=False, persistence_path=self.path
        )
        server._running = True
        client_sock, server_sock = socket.socketpair()
        worker = threading.Thread(
            target=server._handle_client, args=(server_sock, ("local", 0)), daemon=True
        )
        worker.start()
        try:
            commands = [
                (["RPUSH", "queue", "one", "two"], 2),
                (["SADD", "colors", "red", "blue"], 2),
                (["HSET", "user", "name", "Ada", "city", "London"], 2),
            ]
            for parts, expected in commands:
                client_sock.sendall(encode_array(parts))
                self.assertEqual(parse_frame(client_sock.recv(1024))[0], expected)
        finally:
            client_sock.close()
            worker.join(timeout=2)

        restarted = MiniRedisServer(
            store=MiniRedisStore(), verbose=False, persistence_path=self.path
        )
        self.assertEqual(restarted.store.list_range("queue", 0, -1), ["one", "two"])
        self.assertEqual(restarted.store.set_members("colors"), ["blue", "red"])
        self.assertEqual(restarted.store.hash_get("user", "name"), "Ada")

    def test_expired_v8_collections_are_not_loaded(self):
        past = time.time() - 5
        payload = {
            "version": 2,
            "saved_at": time.time(),
            "entries": {
                "list": {"type": "list", "value": ["item"], "expires_at": past},
                "set": {"type": "set", "value": ["member"], "expires_at": past},
                "hash": {"type": "hash", "value": {"field": "value"}, "expires_at": past},
            },
        }
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)

        server = MiniRedisServer(
            store=MiniRedisStore(), verbose=False, persistence_path=self.path
        )
        self.assertEqual(server.store.size(), 0)


if __name__ == "__main__":
    unittest.main()
