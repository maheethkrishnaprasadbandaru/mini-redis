"""V8 command tests covering lists, sets, hashes, and type safety."""

import time
import unittest

from miniredis.commands import CommandHandler
from miniredis.store import MiniRedisStore


class TestV8Commands(unittest.TestCase):
    def setUp(self):
        self.store = MiniRedisStore()
        self.commands = CommandHandler(self.store)

    def run_cmd(self, *parts):
        return self.commands.execute(list(parts))

    def test_lists_push_order_length_and_range(self):
        self.assertEqual(self.run_cmd("LPUSH", "queue", "one", "two"), "2")
        self.assertEqual(self.run_cmd("RPUSH", "queue", "three", "four"), "4")
        self.assertEqual(self.run_cmd("LLEN", "queue"), "4")
        self.assertEqual(self.run_cmd("LRANGE", "queue", "0", "-1"),
                         ["two", "one", "three", "four"])

    def test_lrange_negative_and_out_of_range_indexes(self):
        self.run_cmd("RPUSH", "q", "a", "b", "c")
        self.assertEqual(self.run_cmd("LRANGE", "q", "-2", "-1"), ["b", "c"])
        self.assertEqual(self.run_cmd("LRANGE", "q", "9", "12"), [])
        self.assertEqual(self.run_cmd("LRANGE", "q", "2", "1"), [])

    def test_list_pop_missing_and_removes_empty_key(self):
        self.assertIsNone(self.run_cmd("LPOP", "missing"))
        self.run_cmd("RPUSH", "q", "only")
        self.assertEqual(self.run_cmd("RPOP", "q"), "only")
        self.assertEqual(self.run_cmd("LLEN", "q"), "0")
        self.assertFalse(self.store.exists("q"))

    def test_inline_list_values_can_be_quoted(self):
        self.assertEqual(self.commands.execute('RPUSH queue "hello world" "second item"'), "2")
        self.assertEqual(self.commands.execute("LRANGE queue 0 -1"), ["hello world", "second item"])

    def test_list_argument_validation(self):
        for command in (
            ["LPUSH", "q"], ["RPUSH", "q"], ["LPOP"], ["LPOP", "q", "extra"],
            ["LRANGE", "q", "0"], ["LLEN"],
        ):
            with self.subTest(command=command):
                self.assertTrue(self.commands.execute(command).startswith("ERR"))
        self.assertIn("integer", self.commands.execute(["LRANGE", "q", "x", "2"]))

    def test_sets_deduplicate_and_sort_members(self):
        self.assertEqual(self.run_cmd("SADD", "tags", "z", "a", "z", "m"), "3")
        self.assertEqual(self.run_cmd("SMEMBERS", "tags"), ["a", "m", "z"])
        self.assertEqual(self.run_cmd("SCARD", "tags"), "3")

    def test_set_remove_membership_and_empty_key(self):
        self.run_cmd("SADD", "tags", "a", "b")
        self.assertEqual(self.run_cmd("SREM", "tags", "a", "missing"), "1")
        self.assertEqual(self.run_cmd("SISMEMBER", "tags", "b"), "1")
        self.assertEqual(self.run_cmd("SISMEMBER", "tags", "a"), "0")
        self.assertEqual(self.run_cmd("SREM", "tags", "b"), "1")
        self.assertFalse(self.store.exists("tags"))
        self.assertEqual(self.run_cmd("SMEMBERS", "missing"), [])
        self.assertEqual(self.run_cmd("SCARD", "missing"), "0")

    def test_set_argument_validation(self):
        for command in (
            ["SADD", "s"], ["SREM", "s"], ["SMEMBERS"],
            ["SISMEMBER", "s"], ["SCARD", "s", "extra"],
        ):
            with self.subTest(command=command):
                self.assertTrue(self.commands.execute(command).startswith("ERR"))

    def test_hash_set_get_getall_and_length(self):
        self.assertEqual(self.run_cmd("HSET", "user", "name", "Ada", "city", "London"), "2")
        self.assertEqual(self.run_cmd("HSET", "user", "name", "Grace"), "0")
        self.assertEqual(self.run_cmd("HGET", "user", "name"), "Grace")
        self.assertEqual(self.run_cmd("HLEN", "user"), "2")
        self.assertEqual(self.run_cmd("HGETALL", "user"), ["city", "London", "name", "Grace"])

    def test_hash_delete_and_missing_fields(self):
        self.run_cmd("HSET", "user", "name", "Ada", "city", "London")
        self.assertEqual(self.run_cmd("HGET", "user", "missing"), None)
        self.assertEqual(self.run_cmd("HDEL", "user", "missing", "name"), "1")
        self.assertEqual(self.run_cmd("HLEN", "user"), "1")
        self.assertEqual(self.run_cmd("HDEL", "user", "city"), "1")
        self.assertFalse(self.store.exists("user"))
        self.assertEqual(self.run_cmd("HGETALL", "missing"), [])

    def test_inline_hash_values_can_be_quoted(self):
        self.assertEqual(self.commands.execute('HSET user name "Ada Lovelace" city "New York"'), "2")
        self.assertEqual(self.commands.execute("HGET user name"), "Ada Lovelace")

    def test_hash_argument_validation(self):
        for command in (
            ["HSET", "h"], ["HSET", "h", "field"], ["HGET", "h"],
            ["HGETALL"], ["HDEL", "h"], ["HLEN", "h", "extra"],
        ):
            with self.subTest(command=command):
                self.assertTrue(self.commands.execute(command).startswith("ERR"))

    def test_all_data_types_reject_wrong_type_operations(self):
        self.run_cmd("SET", "string", "value")
        self.run_cmd("RPUSH", "list", "value")
        self.run_cmd("SADD", "set", "value")
        self.run_cmd("HSET", "hash", "field", "value")
        wrong_type = "WRONGTYPE"
        cases = [
            ["GET", "list"], ["LPUSH", "string", "value"], ["SADD", "hash", "x"],
            ["HSET", "set", "field", "value"], ["LRANGE", "set", "0", "-1"],
            ["SMEMBERS", "list"], ["HGET", "string", "field"],
        ]
        for command in cases:
            with self.subTest(command=command):
                self.assertIn(wrong_type, self.commands.execute(command))

    def test_set_and_mset_replace_collections(self):
        self.run_cmd("RPUSH", "key", "list-value")
        self.assertEqual(self.run_cmd("SET", "key", "string-value"), "OK")
        self.assertEqual(self.run_cmd("GET", "key"), "string-value")
        self.run_cmd("SADD", "other", "member")
        self.assertEqual(self.run_cmd("MSET", "other", "plain"), "OK")
        self.assertEqual(self.run_cmd("GET", "other"), "plain")

    def test_expiration_and_database_stats_apply_to_collections(self):
        self.run_cmd("RPUSH", "queue", "item")
        self.assertEqual(self.run_cmd("EXPIRE", "queue", "0"), "1")
        self.assertEqual(self.run_cmd("LLEN", "queue"), "0")
        self.run_cmd("SADD", "tags", "a")
        self.run_cmd("HSET", "user", "name", "Ada")
        self.assertEqual(self.run_cmd("DBSIZE"), "2")
        self.assertEqual(self.run_cmd("KEYS", "*"), ["tags", "user"])
        self.assertEqual(self.run_cmd("EXPIRE", "tags", "0"), "1")
        self.assertEqual(self.run_cmd("EXPIRE", "user", "0"), "1")
        self.assertEqual(self.run_cmd("DBSIZE"), "0")
        self.assertEqual(self.run_cmd("KEYS", "*"), [])


if __name__ == "__main__":
    unittest.main()
