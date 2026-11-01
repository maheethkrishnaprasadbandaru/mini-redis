"""
Unit tests for RESP2 encoding, decoding, and command response mapping.
"""

import unittest
from miniredis.resp import (
    RESPError,
    RESPParser,
    parse_frame,
    encode_simple_string,
    encode_error,
    encode_integer,
    encode_bulk_string,
    encode_null_bulk_string,
    encode_array,
    encode_command_response,
)


class TestRESPCodec(unittest.TestCase):
    """Test suite for RESP2 primitives and frame parsing."""

    # 1. Simple Strings
    def test_simple_string(self) -> None:
        raw = encode_simple_string("OK")
        self.assertEqual(raw, b"+OK\r\n")
        val, consumed = parse_frame(b"+OK\r\n")
        self.assertEqual(val, "OK")
        self.assertEqual(consumed, 5)

    def test_simple_string_with_newline_raises_error(self) -> None:
        with self.assertRaises(RESPError):
            encode_simple_string("line1\nline2")

    # 2. Errors
    def test_error(self) -> None:
        raw = encode_error("ERR unknown command 'FOO'")
        self.assertEqual(raw, b"-ERR unknown command 'FOO'\r\n")
        val, consumed = parse_frame(b"-ERR unknown command 'FOO'\r\n")
        self.assertEqual(val, "ERR unknown command 'FOO'")
        self.assertEqual(consumed, len(raw))

    # 3. Integers
    def test_integers(self) -> None:
        self.assertEqual(encode_integer(0), b":0\r\n")
        self.assertEqual(encode_integer(1), b":1\r\n")
        self.assertEqual(encode_integer(1000), b":1000\r\n")

        val, consumed = parse_frame(b":1\r\n")
        self.assertEqual(val, 1)
        self.assertEqual(consumed, 4)

        val_neg, consumed_neg = parse_frame(b":-42\r\n")
        self.assertEqual(val_neg, -42)
        self.assertEqual(consumed_neg, 6)

    # 4. Bulk Strings
    def test_bulk_string(self) -> None:
        raw = encode_bulk_string("Maheeth")
        self.assertEqual(raw, b"$7\r\nMaheeth\r\n")
        val, consumed = parse_frame(b"$7\r\nMaheeth\r\n")
        self.assertEqual(val, "Maheeth")
        self.assertEqual(consumed, 13)

    def test_bulk_string_byte_length_vs_char_length(self) -> None:
        # 'café' has 4 characters but 5 UTF-8 bytes (c, a, f, \xc3\xa9)
        val_str = "café"
        raw = encode_bulk_string(val_str)
        self.assertEqual(raw, b"$5\r\ncaf\xc3\xa9\r\n")
        parsed, consumed = parse_frame(raw)
        self.assertEqual(parsed, "café")
        self.assertEqual(consumed, len(raw))

        # Emoji 🚀 has 1 character but 4 UTF-8 bytes
        emoji_str = "🚀"
        raw_emoji = encode_bulk_string(emoji_str)
        self.assertEqual(raw_emoji, b"$4\r\n\xf0\x9f\x9a\x80\r\n")
        parsed_emoji, consumed_emoji = parse_frame(raw_emoji)
        self.assertEqual(parsed_emoji, "🚀")
        self.assertEqual(consumed_emoji, len(raw_emoji))

    def test_null_bulk_string(self) -> None:
        raw = encode_null_bulk_string()
        self.assertEqual(raw, b"$-1\r\n")
        val, consumed = parse_frame(b"$-1\r\n")
        self.assertIsNone(val)
        self.assertEqual(consumed, 5)

    # 5. Arrays
    def test_array(self) -> None:
        cmd = ["SET", "name", "Maheeth"]
        raw = encode_array(cmd)
        expected = b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nMaheeth\r\n"
        self.assertEqual(raw, expected)

        parsed, consumed = parse_frame(expected)
        self.assertEqual(parsed, ["SET", "name", "Maheeth"])
        self.assertEqual(consumed, len(expected))

    # 6. Incomplete / Fragmented Frames
    def test_incomplete_frames_return_none(self) -> None:
        # Incomplete simple string
        self.assertEqual(parse_frame(b"+OK"), (None, 0))

        # Incomplete bulk string header
        self.assertEqual(parse_frame(b"$7\r"), (None, 0))

        # Incomplete bulk string body
        self.assertEqual(parse_frame(b"$7\r\nMah"), (None, 0))

        # Incomplete array
        self.assertEqual(parse_frame(b"*3\r\n$3\r\nSET\r\n$4\r\nna"), (None, 0))

    # 7. Incremental RESPParser
    def test_incremental_parser_handles_fragmentation(self) -> None:
        parser = RESPParser()
        full_msg = b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nMaheeth\r\n"

        # Feed byte by byte
        for b in full_msg[:-1]:
            parser.feed(bytes([b]))
            self.assertIsNone(parser.next_frame())

        # Feed the final byte
        parser.feed(bytes([full_msg[-1]]))
        frame = parser.next_frame()
        self.assertEqual(frame, ["SET", "name", "Maheeth"])
        self.assertIsNone(parser.next_frame())

    # 8. Malformed Frames
    def test_malformed_integer_raises(self) -> None:
        with self.assertRaises(RESPError):
            parse_frame(b":invalid\r\n")

    def test_malformed_bulk_string_missing_crlf_raises(self) -> None:
        # Declares 4 bytes, but trailing is XX instead of \r\n
        with self.assertRaises(RESPError):
            parse_frame(b"$4\r\ntestXX")

    def test_unknown_prefix_raises(self) -> None:
        with self.assertRaises(RESPError):
            parse_frame(b"?unknown\r\n")

    # 9. Response Mapping
    def test_response_mapping_set(self) -> None:
        res = encode_command_response("SET", "OK")
        self.assertEqual(res, b"+OK\r\n")

    def test_response_mapping_get(self) -> None:
        res_found = encode_command_response("GET", "Maheeth")
        self.assertEqual(res_found, b"$7\r\nMaheeth\r\n")

        res_missing = encode_command_response("GET", "(nil)")
        self.assertEqual(res_missing, b"$-1\r\n")

    def test_response_mapping_del(self) -> None:
        res_deleted = encode_command_response("DEL", "1")
        self.assertEqual(res_deleted, b":1\r\n")

        res_not_deleted = encode_command_response("DEL", "0")
        self.assertEqual(res_not_deleted, b":0\r\n")

    def test_response_mapping_exists(self) -> None:
        res_exists = encode_command_response("EXISTS", "1")
        self.assertEqual(res_exists, b":1\r\n")

        res_not_exists = encode_command_response("EXISTS", "0")
        self.assertEqual(res_not_exists, b":0\r\n")

    def test_response_mapping_errors(self) -> None:
        res_err = encode_command_response("FOO", "ERR unknown command 'FOO'")
        self.assertEqual(res_err, b"-ERR unknown command 'FOO'\r\n")

    def test_response_mapping_ping(self) -> None:
        # PING without args returns Simple String +PONG\r\n
        res_pong = encode_command_response("PING", "PONG")
        self.assertEqual(res_pong, b"+PONG\r\n")

        # PING with custom message returns Bulk String
        res_echo = encode_command_response("PING", "hello")
        self.assertEqual(res_echo, b"$5\r\nhello\r\n")

    def test_response_mapping_expire_and_ttl(self) -> None:
        self.assertEqual(encode_command_response("EXPIRE", "1"), b":1\r\n")
        self.assertEqual(encode_command_response("EXPIRE", "0"), b":0\r\n")
        self.assertEqual(encode_command_response("TTL", "-2"), b":-2\r\n")
        self.assertEqual(encode_command_response("TTL", "-1"), b":-1\r\n")
        self.assertEqual(encode_command_response("TTL", "10"), b":10\r\n")

    # 10. V7 response mappings
    def test_response_mapping_mset(self) -> None:
        """Test MSET OK maps to a RESP Simple String +OK\\r\\n."""
        res = encode_command_response("MSET", "OK")
        self.assertEqual(res, b"+OK\r\n")

    def test_response_mapping_dbsize(self) -> None:
        """Test DBSIZE integer string maps to a RESP Integer."""
        self.assertEqual(encode_command_response("DBSIZE", "0"), b":0\r\n")
        self.assertEqual(encode_command_response("DBSIZE", "5"), b":5\r\n")
        self.assertEqual(encode_command_response("DBSIZE", "42"), b":42\r\n")

    def test_array_with_nulls_for_mget(self) -> None:
        """Test encode_array correctly encodes None entries as $-1\\r\\n (MGET nil slots)."""
        raw = encode_array(["val1", None, "val3"])
        expected = b"*3\r\n$4\r\nval1\r\n$-1\r\n$4\r\nval3\r\n"
        self.assertEqual(raw, expected)

    def test_array_empty_for_keys_no_results(self) -> None:
        """Test encode_array with empty list produces *0\\r\\n (KEYS on empty store)."""
        raw = encode_array([])
        self.assertEqual(raw, b"*0\r\n")

    def test_v8_null_replies_and_integer_command_mapping(self) -> None:
        self.assertEqual(encode_command_response("LPOP", None), b"$-1\r\n")
        for command in ("LLEN", "SADD", "SREM", "SISMEMBER", "SCARD", "HSET", "HDEL", "HLEN"):
            with self.subTest(command=command):
                self.assertEqual(encode_command_response(command, "3"), b":3\r\n")


if __name__ == "__main__":
    unittest.main()
