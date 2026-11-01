"""
Unit tests for MiniRedisStore.
"""

import unittest
from miniredis.store import MiniRedisStore


class TestMiniRedisStore(unittest.TestCase):
    """Test suite for core in-memory key-value operations."""

    def setUp(self) -> None:
        """Create a fresh store instance before each test."""
        self.store = MiniRedisStore()

    def test_set_new_key(self) -> None:
        """Test setting a brand new key returns True and increases size."""
        result = self.store.set("name", "Maheeth")
        self.assertTrue(result)
        self.assertEqual(self.store.size(), 1)

    def test_get_existing_key(self) -> None:
        """Test retrieving the value of an existing key."""
        self.store.set("city", "Trichy")
        value = self.store.get("city")
        self.assertEqual(value, "Trichy")

    def test_get_missing_key(self) -> None:
        """Test retrieving a key that does not exist returns None."""
        value = self.store.get("non_existent_key")
        self.assertIsNone(value)

    def test_update_existing_key(self) -> None:
        """Test updating an existing key replaces its value and does not increase size."""
        self.store.set("age", "17")
        self.assertEqual(self.store.get("age"), "17")
        self.assertEqual(self.store.size(), 1)

        # Update the key
        update_result = self.store.set("age", "18")
        self.assertTrue(update_result)
        self.assertEqual(self.store.get("age"), "18")
        self.assertEqual(self.store.size(), 1)

    def test_delete_existing_key(self) -> None:
        """Test deleting an existing key returns True and removes it from store."""
        self.store.set("temp", "data")
        self.assertTrue(self.store.exists("temp"))

        deleted = self.store.delete("temp")
        self.assertTrue(deleted)
        self.assertFalse(self.store.exists("temp"))
        self.assertIsNone(self.store.get("temp"))
        self.assertEqual(self.store.size(), 0)

    def test_delete_missing_key(self) -> None:
        """Test deleting a non-existent key returns False without error."""
        deleted = self.store.delete("ghost_key")
        self.assertFalse(deleted)
        self.assertEqual(self.store.size(), 0)

    def test_exists_for_existing_and_missing_keys(self) -> None:
        """Test checking existence for present and absent keys."""
        self.assertFalse(self.store.exists("user:1"))

        self.store.set("user:1", "active")
        self.assertTrue(self.store.exists("user:1"))

        self.store.delete("user:1")
        self.assertFalse(self.store.exists("user:1"))

    def test_empty_string_values(self) -> None:
        """Test that empty string values are stored properly and not confused with None."""
        self.store.set("empty", "")
        self.assertTrue(self.store.exists("empty"))
        self.assertEqual(self.store.get("empty"), "")

    def test_multiple_keys(self) -> None:
        """Test storing and managing multiple independent keys."""
        items = {
            "name": "Maheeth",
            "age": "18",
            "city": "Trichy"
        }
        for k, v in items.items():
            self.store.set(k, v)

        self.assertEqual(self.store.size(), 3)
        for k, v in items.items():
            self.assertEqual(self.store.get(k), v)

    # TTL & Expiration Tests
    def test_set_with_expiration_and_passive_expiry(self) -> None:
        """Test SET with EX expires passively after the timeout."""
        import time
        self.store.set("temp", "val", ex=0.05)
        self.assertEqual(self.store.get("temp"), "val")
        self.assertTrue(self.store.exists("temp"))
        self.assertGreater(self.store.ttl("temp"), 0)

        # Wait for expiration
        time.sleep(0.06)
        self.assertIsNone(self.store.get("temp"))
        self.assertFalse(self.store.exists("temp"))
        self.assertFalse(self.store.delete("temp"))
        self.assertEqual(self.store.ttl("temp"), -2)

    def test_overwrite_without_ex_removes_expiration(self) -> None:
        """Test overwriting a key without EX discards the expiration."""
        import time
        self.store.set("key", "val1", ex=0.05)
        self.store.set("key", "val2")  # No EX, clears timeout
        time.sleep(0.06)
        self.assertEqual(self.store.get("key"), "val2")
        self.assertEqual(self.store.ttl("key"), -1)

    def test_expire_command_on_existing_and_missing(self) -> None:
        """Test EXPIRE sets timeout on existing key and returns False on missing."""
        import time
        self.assertFalse(self.store.expire("missing", 10))

        self.store.set("session", "xyz")
        self.assertTrue(self.store.expire("session", 0.05))
        self.assertGreater(self.store.ttl("session"), 0)

        time.sleep(0.06)
        self.assertIsNone(self.store.get("session"))
        self.assertEqual(self.store.ttl("session"), -2)

    def test_expire_with_zero_or_negative_immediately_deletes(self) -> None:
        """Test setting EXPIRE <= 0 immediately deletes the key."""
        self.store.set("k1", "v1")
        self.assertTrue(self.store.expire("k1", 0))
        self.assertFalse(self.store.exists("k1"))
        self.assertEqual(self.store.ttl("k1"), -2)

    def test_ttl_values(self) -> None:
        """Test TTL returns -2 for missing/expired, -1 for no timeout, and >= 0 for active."""
        self.assertEqual(self.store.ttl("ghost"), -2)

        self.store.set("k_persistent", "forever")
        self.assertEqual(self.store.ttl("k_persistent"), -1)

        self.store.set("k_expiring", "value", ex=10)
        self.assertEqual(self.store.ttl("k_expiring"), 10)


class TestStoreV7MultiKey(unittest.TestCase):
    """Test suite for V7 multi-key and statistics store methods."""

    def setUp(self) -> None:
        self.store = MiniRedisStore()

    # MSET tests
    def test_mset_basic(self) -> None:
        """Test MSET sets multiple keys at once and all values are retrievable."""
        self.store.mset([("a", "1"), ("b", "2"), ("c", "3")])
        self.assertEqual(self.store.get("a"), "1")
        self.assertEqual(self.store.get("b"), "2")
        self.assertEqual(self.store.get("c"), "3")
        self.assertEqual(self.store.size(), 3)

    def test_mset_overwrites_existing_value(self) -> None:
        """Test MSET overwrites an existing key's value."""
        self.store.set("x", "old")
        self.store.mset([("x", "new"), ("y", "fresh")])
        self.assertEqual(self.store.get("x"), "new")
        self.assertEqual(self.store.get("y"), "fresh")

    def test_mset_clears_existing_expiry(self) -> None:
        """Test MSET on a key that has a TTL removes the expiration."""
        self.store.set("k", "v", ex=100)
        self.assertGreater(self.store.ttl("k"), 0)
        self.store.mset([("k", "v2")])
        # No expiry after MSET
        self.assertEqual(self.store.ttl("k"), -1)

    def test_mset_single_pair(self) -> None:
        """Test MSET with just one key-value pair works correctly."""
        result = self.store.mset([("solo", "val")])
        self.assertTrue(result)
        self.assertEqual(self.store.get("solo"), "val")

    # MGET tests
    def test_mget_all_present(self) -> None:
        """Test MGET returns values in request order when all keys exist."""
        self.store.set("k1", "v1")
        self.store.set("k2", "v2")
        self.store.set("k3", "v3")
        result = self.store.mget(["k1", "k2", "k3"])
        self.assertEqual(result, ["v1", "v2", "v3"])

    def test_mget_with_missing_keys(self) -> None:
        """Test MGET returns None for missing keys in their position."""
        self.store.set("present", "yes")
        result = self.store.mget(["missing", "present", "also_missing"])
        self.assertIsNone(result[0])
        self.assertEqual(result[1], "yes")
        self.assertIsNone(result[2])

    def test_mget_with_expired_key(self) -> None:
        """Test MGET returns None for expired keys."""
        import time
        self.store.set("expiring", "bye", ex=0.05)
        time.sleep(0.06)
        result = self.store.mget(["expiring"])
        self.assertIsNone(result[0])

    def test_mget_empty_store(self) -> None:
        """Test MGET on an empty store returns all None."""
        result = self.store.mget(["a", "b"])
        self.assertEqual(result, [None, None])

    # keys() tests
    def test_keys_basic(self) -> None:
        """Test keys() returns all key names."""
        self.store.set("name", "Maheeth")
        self.store.set("city", "Trichy")
        ks = self.store.keys()
        self.assertIn("name", ks)
        self.assertIn("city", ks)
        self.assertEqual(len(ks), 2)

    def test_keys_are_sorted_deterministically(self) -> None:
        self.store.mset([("z", "last"), ("a", "first"), ("m", "middle")])
        self.assertEqual(self.store.keys(), ["a", "m", "z"])

    def test_keys_excludes_expired(self) -> None:
        """Test keys() does not include expired keys."""
        import time
        self.store.set("live", "yes")
        self.store.set("dying", "no", ex=0.05)
        time.sleep(0.06)
        ks = self.store.keys()
        self.assertIn("live", ks)
        self.assertNotIn("dying", ks)

    def test_keys_empty_store(self) -> None:
        """Test keys() returns an empty list when store is empty."""
        self.assertEqual(self.store.keys(), [])

    # size() via DBSIZE
    def test_size_excludes_expired(self) -> None:
        """Test size() does not count expired keys."""
        import time
        self.store.set("live", "yes")
        self.store.set("dying", "no", ex=0.05)
        self.assertEqual(self.store.size(), 2)
        time.sleep(0.06)
        self.assertEqual(self.store.size(), 1)


if __name__ == "__main__":
    unittest.main()
