"""
Unit tests for CommandHandler.
"""

import unittest
from miniredis.store import MiniRedisStore
from miniredis.commands import CommandHandler


class TestCommandHandler(unittest.TestCase):
    """Test suite for Mini Redis command parsing and execution."""

    def setUp(self) -> None:
        """Create a fresh store and command handler for each test."""
        self.store = MiniRedisStore()
        self.handler = CommandHandler(self.store)

    # 1. SET command tests
    def test_set_basic(self) -> None:
        """Test SET with key and value returns OK."""
        res = self.handler.execute("SET name Maheeth")
        self.assertEqual(res, "OK")
        self.assertEqual(self.store.get("name"), "Maheeth")

    def test_set_value_with_spaces(self) -> None:
        """Test SET message hello world stores 'hello world'."""
        res = self.handler.execute("SET message hello world")
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute("GET message"), "hello world")

    def test_set_value_with_multiple_words_and_spaces(self) -> None:
        """Test SET with complex sentences preserves internal spaces."""
        res = self.handler.execute("SET bio Software engineer from Trichy")
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute("GET bio"), "Software engineer from Trichy")

    def test_set_update_existing_key(self) -> None:
        """Test updating an existing key replaces its value."""
        self.handler.execute("SET age 17")
        self.assertEqual(self.handler.execute("GET age"), "17")

        res = self.handler.execute("SET age 18")
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute("GET age"), "18")

    # 2. GET command tests
    def test_get_existing_key(self) -> None:
        """Test GET retrieves stored value."""
        self.handler.execute("SET city Trichy")
        self.assertEqual(self.handler.execute("GET city"), "Trichy")

    def test_get_missing_key(self) -> None:
        """Test GET on a non-existent key returns (nil)."""
        self.assertEqual(self.handler.execute("GET missing_key"), "(nil)")

    # 3. DEL command tests
    def test_del_existing_key(self) -> None:
        """Test DEL on existing key returns 1 and deletes it."""
        self.handler.execute("SET temp 123")
        self.assertEqual(self.handler.execute("EXISTS temp"), "1")

        res = self.handler.execute("DEL temp")
        self.assertEqual(res, "1")
        self.assertEqual(self.handler.execute("EXISTS temp"), "0")
        self.assertEqual(self.handler.execute("GET temp"), "(nil)")

    def test_del_missing_key(self) -> None:
        """Test DEL on missing key returns 0."""
        res = self.handler.execute("DEL non_existent")
        self.assertEqual(res, "0")

    # 4. EXISTS command tests
    def test_exists_present_key(self) -> None:
        """Test EXISTS returns 1 when key is present."""
        self.handler.execute("SET fruit apple")
        self.assertEqual(self.handler.execute("EXISTS fruit"), "1")

    def test_exists_missing_key(self) -> None:
        """Test EXISTS returns 0 when key is missing."""
        self.assertEqual(self.handler.execute("EXISTS fruit"), "0")

    # 5. Case-insensitivity tests
    def test_case_insensitive_commands(self) -> None:
        """Test that commands are case-insensitive (set, Set, sEt, etc.)."""
        self.assertEqual(self.handler.execute("set greeting hi"), "OK")
        self.assertEqual(self.handler.execute("Get greeting"), "hi")
        self.assertEqual(self.handler.execute("exists greeting"), "1")
        self.assertEqual(self.handler.execute("dEl greeting"), "1")
        self.assertEqual(self.handler.execute("GET greeting"), "(nil)")

    def test_case_sensitive_keys(self) -> None:
        """Test that keys remain case-sensitive."""
        self.handler.execute("SET myKey value1")
        self.handler.execute("SET mykey value2")

        self.assertEqual(self.handler.execute("GET myKey"), "value1")
        self.assertEqual(self.handler.execute("GET mykey"), "value2")

    # 6. Unknown command tests
    def test_unknown_command(self) -> None:
        """Test that an unknown command returns a helpful error."""
        res = self.handler.execute("FOOBAR key value")
        self.assertEqual(res, "ERR unknown command 'FOOBAR'")

        res2 = self.handler.execute("invalid")
        self.assertEqual(res2, "ERR unknown command 'invalid'")

    # 7. Invalid argument count tests
    def test_set_invalid_argument_counts(self) -> None:
        """Test SET with insufficient arguments."""
        self.assertEqual(
            self.handler.execute("SET"),
            "ERR wrong number of arguments for 'set' command",
        )
        self.assertEqual(
            self.handler.execute("SET only_key"),
            "ERR wrong number of arguments for 'set' command",
        )

    def test_get_invalid_argument_counts(self) -> None:
        """Test GET with wrong number of arguments."""
        self.assertEqual(
            self.handler.execute("GET"),
            "ERR wrong number of arguments for 'get' command",
        )
        self.assertEqual(
            self.handler.execute("GET key extra"),
            "ERR wrong number of arguments for 'get' command",
        )

    def test_del_invalid_argument_counts(self) -> None:
        """Test DEL with wrong number of arguments."""
        self.assertEqual(
            self.handler.execute("DEL"),
            "ERR wrong number of arguments for 'del' command",
        )
        self.assertEqual(
            self.handler.execute("DEL key extra"),
            "ERR wrong number of arguments for 'del' command",
        )

    def test_exists_invalid_argument_counts(self) -> None:
        """Test EXISTS with wrong number of arguments."""
        self.assertEqual(
            self.handler.execute("EXISTS"),
            "ERR wrong number of arguments for 'exists' command",
        )
        self.assertEqual(
            self.handler.execute("EXISTS key extra"),
            "ERR wrong number of arguments for 'exists' command",
        )

    # 8. Empty and whitespace input tests
    def test_empty_commands(self) -> None:
        """Test executing empty strings or whitespace."""
        self.assertEqual(self.handler.execute(""), "ERR empty command")
        self.assertEqual(self.handler.execute("   "), "ERR empty command")

    # 9. List token input tests
    def test_execute_list_tokens(self) -> None:
        """Test executing commands passed as lists of tokens."""
        res = self.handler.execute(["SET", "message", "hello world"])
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute(["GET", "message"]), "hello world")
        self.assertEqual(self.handler.execute(["EXISTS", "message"]), "1")
        self.assertEqual(self.handler.execute(["DEL", "message"]), "1")
        self.assertEqual(self.handler.execute(["DEL", "message"]), "0")

    # 10. PING command tests
    def test_ping_without_argument(self) -> None:
        """Test PING with no arguments returns PONG."""
        self.assertEqual(self.handler.execute("PING"), "PONG")
        self.assertEqual(self.handler.execute("ping"), "PONG")
        self.assertEqual(self.handler.execute("Ping"), "PONG")
        self.assertEqual(self.handler.execute(["PING"]), "PONG")
        self.assertEqual(self.handler.execute(["ping"]), "PONG")

    def test_ping_with_message_argument(self) -> None:
        """Test PING with a single message argument echoes the message."""
        self.assertEqual(self.handler.execute("PING hello"), "hello")
        self.assertEqual(self.handler.execute(["PING", "hello world"]), "hello world")

    def test_ping_with_too_many_arguments(self) -> None:
        """Test PING with multiple arguments returns wrong number of arguments error."""
        self.assertEqual(
            self.handler.execute("PING hello world"),
            "ERR wrong number of arguments for 'ping' command",
        )
        self.assertEqual(
            self.handler.execute(["PING", "hello", "world"]),
            "ERR wrong number of arguments for 'ping' command",
        )

    # 11. SET with EX option tests
    def test_set_with_ex(self) -> None:
        """Test SET key value EX seconds sets expiration."""
        res = self.handler.execute("SET mykey myval EX 10")
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute("GET mykey"), "myval")
        self.assertEqual(self.handler.execute("TTL mykey"), "10")

    def test_set_with_spaces_and_ex(self) -> None:
        """Test SET message hello world EX 20 preserves value spaces."""
        res = self.handler.execute("SET message hello world EX 20")
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute("GET message"), "hello world")
        self.assertEqual(self.handler.execute("TTL message"), "20")

    def test_set_with_invalid_ex(self) -> None:
        """Test SET with invalid EX arguments returns error."""
        self.assertEqual(
            self.handler.execute("SET k v EX"),
            "ERR syntax error",
        )
        self.assertEqual(
            self.handler.execute("SET k v EX not_int"),
            "ERR value is not an integer or out of range",
        )
        self.assertEqual(
            self.handler.execute("SET k v EX 0"),
            "ERR invalid expire time in 'set' command",
        )
        self.assertEqual(
            self.handler.execute("SET k v EX -10"),
            "ERR invalid expire time in 'set' command",
        )

    # 12. EXPIRE and TTL command tests
    def test_expire_and_ttl_basic(self) -> None:
        """Test EXPIRE sets timeout and TTL returns remaining seconds."""
        self.handler.execute("SET session token123")
        self.assertEqual(self.handler.execute("TTL session"), "-1")

        # Set expire
        res_exp = self.handler.execute("EXPIRE session 15")
        self.assertEqual(res_exp, "1")
        self.assertEqual(self.handler.execute("TTL session"), "15")

        # EXPIRE missing key
        self.assertEqual(self.handler.execute("EXPIRE non_existent 10"), "0")

    def test_expire_invalid_arguments(self) -> None:
        """Test EXPIRE syntax validation."""
        self.assertEqual(
            self.handler.execute("EXPIRE"),
            "ERR wrong number of arguments for 'expire' command",
        )
        self.assertEqual(
            self.handler.execute("EXPIRE k"),
            "ERR wrong number of arguments for 'expire' command",
        )
        self.assertEqual(
            self.handler.execute("EXPIRE k abc"),
            "ERR value is not an integer or out of range",
        )

    def test_ttl_invalid_arguments(self) -> None:
        """Test TTL syntax validation."""
        self.assertEqual(
            self.handler.execute("TTL"),
            "ERR wrong number of arguments for 'ttl' command",
        )
        self.assertEqual(
            self.handler.execute("TTL k extra"),
            "ERR wrong number of arguments for 'ttl' command",
        )


class TestCommandHandlerV7(unittest.TestCase):
    """Test suite for V7 multi-key and database statistics commands."""

    def setUp(self) -> None:
        self.store = MiniRedisStore()
        self.handler = CommandHandler(self.store)

    # MSET
    def test_mset_basic_string(self) -> None:
        """Test MSET via inline string sets all key-value pairs."""
        res = self.handler.execute("MSET k1 v1 k2 v2 k3 v3")
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute("GET k1"), "v1")
        self.assertEqual(self.handler.execute("GET k2"), "v2")
        self.assertEqual(self.handler.execute("GET k3"), "v3")

    def test_mset_inline_quoted_values_with_spaces(self) -> None:
        """Inline MSET quotes keep multi-word values paired with their keys."""
        self.assertEqual(
            self.handler.execute('MSET greeting "hello world" city "New York"'),
            "OK",
        )
        self.assertEqual(self.handler.execute("GET greeting"), "hello world")
        self.assertEqual(self.handler.execute("GET city"), "New York")

    def test_mset_inline_unmatched_quote_is_error_without_partial_write(self) -> None:
        result = self.handler.execute('MSET first one second "unfinished value')
        self.assertTrue(result.startswith("ERR"))
        self.assertIsNone(self.store.get("first"))

    def test_mset_clears_previous_ttl(self) -> None:
        """MSET overwrite removes a previous TTL, matching SET semantics."""
        self.store.set("temporary", "old", ex=30)
        self.assertGreaterEqual(self.store.ttl("temporary"), 1)
        self.assertEqual(self.handler.execute("MSET temporary new other value"), "OK")
        self.assertEqual(self.store.ttl("temporary"), -1)
        self.assertEqual(self.store.get("temporary"), "new")

    def test_mset_basic_list(self) -> None:
        """Test MSET via token list (RESP path)."""
        res = self.handler.execute(["MSET", "a", "alpha", "b", "beta"])
        self.assertEqual(res, "OK")
        self.assertEqual(self.handler.execute(["GET", "a"]), "alpha")
        self.assertEqual(self.handler.execute(["GET", "b"]), "beta")

    def test_mset_no_args(self) -> None:
        """Test MSET with no arguments returns error."""
        self.assertTrue(self.handler.execute("MSET").startswith("ERR"))
        self.assertTrue(self.handler.execute(["MSET"]).startswith("ERR"))

    def test_mset_odd_args(self) -> None:
        """Test MSET with odd number of arguments (unpaired key) returns error."""
        res = self.handler.execute("MSET k1 v1 k2")
        self.assertTrue(res.startswith("ERR"))
        self.assertIsNone(self.store.get("k1"))
        res2 = self.handler.execute(["MSET", "a", "1", "b"])
        self.assertTrue(res2.startswith("ERR"))

    def test_mset_case_insensitive(self) -> None:
        """Test mset and MSet are treated the same as MSET."""
        self.assertEqual(self.handler.execute("mset x 10"), "OK")
        self.assertEqual(self.handler.execute("GET x"), "10")

    def test_mset_single_pair(self) -> None:
        """Test MSET with a single key-value pair."""
        self.assertEqual(self.handler.execute(["MSET", "only", "one"]), "OK")
        self.assertEqual(self.handler.execute("GET only"), "one")

    # MGET
    def test_mget_all_present_list(self) -> None:
        """Test MGET returns list of values for all existing keys."""
        self.handler.execute(["SET", "n1", "val1"])
        self.handler.execute(["SET", "n2", "val2"])
        result = self.handler.execute(["MGET", "n1", "n2"])
        self.assertIsInstance(result, list)
        self.assertEqual(result, ["val1", "val2"])

    def test_mget_with_missing_keys(self) -> None:
        """Test MGET returns None for missing keys in their position."""
        self.handler.execute("SET exists yes")
        result = self.handler.execute(["MGET", "missing", "exists", "also_missing"])
        self.assertIsInstance(result, list)
        self.assertIsNone(result[0])
        self.assertEqual(result[1], "yes")
        self.assertIsNone(result[2])

    def test_mget_preserves_requested_order_and_duplicates(self) -> None:
        self.handler.execute(["MSET", "a", "first", "b", "second"])
        self.assertEqual(
            self.handler.execute(["MGET", "b", "a", "b"]),
            ["second", "first", "second"],
        )

    def test_mget_with_expired_key(self) -> None:
        """Test MGET returns None for expired keys."""
        import time
        self.handler.execute(["SET", "exp_key", "temp", "EX", "1"])
        # Simulate expiry in store directly for speed
        self.store.set("exp_key", "temp", ex=0.05)
        time.sleep(0.06)
        result = self.handler.execute(["MGET", "exp_key"])
        self.assertIsNone(result[0])

    def test_mget_no_args(self) -> None:
        """Test MGET with no key arguments returns error string."""
        res = self.handler.execute("MGET")
        self.assertTrue(isinstance(res, str) and res.startswith("ERR"))
        res2 = self.handler.execute(["MGET"])
        self.assertTrue(isinstance(res2, str) and res2.startswith("ERR"))

    def test_mget_string_path(self) -> None:
        """Test MGET via inline string path."""
        self.handler.execute("SET city Trichy")
        result = self.handler.execute("MGET city missing")
        self.assertIsInstance(result, list)
        self.assertEqual(result[0], "Trichy")
        self.assertIsNone(result[1])

    # DBSIZE
    def test_dbsize_empty_store(self) -> None:
        """Test DBSIZE returns 0 on empty store."""
        self.assertEqual(self.handler.execute("DBSIZE"), "0")
        self.assertEqual(self.handler.execute(["DBSIZE"]), "0")

    def test_dbsize_with_keys(self) -> None:
        """Test DBSIZE increments correctly as keys are added."""
        self.handler.execute("MSET a 1 b 2 c 3")
        self.assertEqual(self.handler.execute("DBSIZE"), "3")

    def test_dbsize_excludes_expired(self) -> None:
        """Test DBSIZE does not count expired keys."""
        import time
        self.store.set("live", "yes")
        self.store.set("dying", "no", ex=0.05)
        self.assertEqual(self.handler.execute("DBSIZE"), "2")
        time.sleep(0.06)
        self.assertEqual(self.handler.execute("DBSIZE"), "1")

    def test_dbsize_wrong_args(self) -> None:
        """Test DBSIZE with arguments returns error."""
        res = self.handler.execute("DBSIZE extra")
        self.assertTrue(res.startswith("ERR"))
        res2 = self.handler.execute(["DBSIZE", "extra"])
        self.assertTrue(res2.startswith("ERR"))

    # KEYS
    def test_keys_star_returns_all(self) -> None:
        """Test KEYS * returns all non-expired key names."""
        self.handler.execute("MSET x 1 y 2 z 3")
        result = self.handler.execute("KEYS *")
        self.assertIsInstance(result, list)
        self.assertEqual(result, ["x", "y", "z"])

    def test_keys_star_list_path(self) -> None:
        """Test KEYS * via token list (RESP path)."""
        self.handler.execute(["SET", "hello", "world"])
        result = self.handler.execute(["KEYS", "*"])
        self.assertIsInstance(result, list)
        self.assertIn("hello", result)

    def test_keys_empty_store(self) -> None:
        """Test KEYS * on empty store returns empty list."""
        result = self.handler.execute("KEYS *")
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])

    def test_keys_excludes_expired(self) -> None:
        """Test KEYS * does not include expired keys."""
        import time
        self.store.set("live", "yes")
        self.store.set("dying", "no", ex=0.05)
        time.sleep(0.06)
        result = self.handler.execute("KEYS *")
        self.assertIn("live", result)
        self.assertNotIn("dying", result)

    def test_keys_unsupported_pattern(self) -> None:
        """Test KEYS with unsupported pattern returns clear error."""
        res = self.handler.execute("KEYS h?llo")
        self.assertTrue(isinstance(res, str) and res.startswith("ERR"))
        self.assertIn("only '*'", res)
        res2 = self.handler.execute(["KEYS", "user:*"])
        self.assertTrue(isinstance(res2, str) and res2.startswith("ERR"))

    def test_keys_wrong_arg_count(self) -> None:
        """Test KEYS with no args or too many args returns error."""
        res = self.handler.execute("KEYS")
        self.assertTrue(res.startswith("ERR"))
        res2 = self.handler.execute(["KEYS", "*", "extra"])
        self.assertTrue(res2.startswith("ERR"))


if __name__ == "__main__":
    unittest.main()
