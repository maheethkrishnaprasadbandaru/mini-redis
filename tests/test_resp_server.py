"""
Integration tests for Mini Redis TCP Server using the RESP2 protocol.
"""

import socket
import threading
import time
import unittest
from miniredis.server import MiniRedisServer
from miniredis.store import MiniRedisStore
from miniredis.resp import encode_array, parse_frame


class TestRESPTCPServer(unittest.TestCase):
    """Integration test suite for RESP protocol over TCP."""

    def setUp(self) -> None:
        """Start a fresh MiniRedisServer on an ephemeral port."""
        self.store = MiniRedisStore()
        # persistence_path=None disables file I/O so RESP TCP tests stay self-contained
        self.server = MiniRedisServer(host="127.0.0.1", port=0, store=self.store, verbose=False, persistence_path=None)
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()

        start_time = time.time()
        while not self.server.is_running and time.time() - start_time < 2.0:
            time.sleep(0.01)

        self.assertTrue(self.server.is_running)
        self.port = self.server.actual_port
        self.assertGreater(self.port, 0)

    def tearDown(self) -> None:
        """Stop the server."""
        self.server.stop()
        self.server_thread.join(timeout=2.0)

    def _create_client(self) -> socket.socket:
        """Create a client socket with safety timeout."""
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(2.0)
        client.connect(("127.0.0.1", self.port))
        return client

    def _send_resp_cmd(self, client: socket.socket, cmd_parts: list) -> bytes:
        """Send a RESP array and read raw RESP bytes back."""
        encoded = encode_array(cmd_parts)
        client.sendall(encoded)
        # Read response
        buf = bytearray()
        while True:
            chunk = client.recv(1024)
            if not chunk:
                break
            buf.extend(chunk)
            val, consumed = parse_frame(bytes(buf))
            if consumed > 0:
                return bytes(buf[:consumed])
        return bytes(buf)

    def test_set_and_get_resp(self) -> None:
        """Test SET and GET return Simple String and Bulk String."""
        with self._create_client() as client:
            raw_set = self._send_resp_cmd(client, ["SET", "name", "Maheeth"])
            self.assertEqual(raw_set, b"+OK\r\n")

            raw_get = self._send_resp_cmd(client, ["GET", "name"])
            self.assertEqual(raw_get, b"$7\r\nMaheeth\r\n")

    def test_get_missing_key_resp(self) -> None:
        """Test GET missing key returns Null Bulk String ($-1\r\n)."""
        with self._create_client() as client:
            raw_get = self._send_resp_cmd(client, ["GET", "non_existent"])
            self.assertEqual(raw_get, b"$-1\r\n")

    def test_del_and_exists_resp(self) -> None:
        """Test DEL and EXISTS return RESP Integers."""
        with self._create_client() as client:
            self.assertEqual(self._send_resp_cmd(client, ["SET", "k", "v"]), b"+OK\r\n")

            raw_exists = self._send_resp_cmd(client, ["EXISTS", "k"])
            self.assertEqual(raw_exists, b":1\r\n")

            raw_del = self._send_resp_cmd(client, ["DEL", "k"])
            self.assertEqual(raw_del, b":1\r\n")

            raw_exists2 = self._send_resp_cmd(client, ["EXISTS", "k"])
            self.assertEqual(raw_exists2, b":0\r\n")

            raw_del2 = self._send_resp_cmd(client, ["DEL", "k"])
            self.assertEqual(raw_del2, b":0\r\n")

    def test_values_with_spaces_and_unicode_resp(self) -> None:
        """Test multi-word and multi-byte UTF-8 bulk strings."""
        with self._create_client() as client:
            raw_set = self._send_resp_cmd(client, ["SET", "msg", "hello world 🚀"])
            self.assertEqual(raw_set, b"+OK\r\n")

            raw_get = self._send_resp_cmd(client, ["GET", "msg"])
            # 'hello world 🚀' is 16 UTF-8 bytes (11 ascii + 1 space + 4 emoji)
            expected_payload = "hello world 🚀".encode("utf-8")
            self.assertEqual(raw_get, f"${len(expected_payload)}\r\n".encode("utf-8") + expected_payload + b"\r\n")

    def test_multiple_sequential_commands_on_single_connection(self) -> None:
        """Test sending multiple commands sequentially over one TCP connection."""
        with self._create_client() as client:
            for i in range(5):
                self.assertEqual(
                    self._send_resp_cmd(client, ["SET", f"key{i}", f"val{i}"]),
                    b"+OK\r\n"
                )
            for i in range(5):
                raw = self._send_resp_cmd(client, ["GET", f"key{i}"])
                self.assertEqual(raw, f"$4\r\nval{i}\r\n".encode("utf-8"))

    def test_malformed_input_does_not_crash_server(self) -> None:
        """Test server handles malformed RESP input gracefully with an error reply."""
        with self._create_client() as client:
            # Send garbage / malformed frame
            client.sendall(b"*invalid\r\n")
            err_resp = client.recv(1024)
            self.assertTrue(err_resp.startswith(b"-ERR Protocol error"))

            # Server remains healthy and accepts next valid client
        with self._create_client() as new_client:
            self.assertEqual(self._send_resp_cmd(new_client, ["SET", "alive", "yes"]), b"+OK\r\n")
            self.assertEqual(self._send_resp_cmd(new_client, ["GET", "alive"]), b"$3\r\nyes\r\n")

    def test_ping_resp(self) -> None:
        """Test sending *1\r\n$4\r\nPING\r\n returns exactly +PONG\r\n."""
        with self._create_client() as client:
            client.sendall(b"*1\r\n$4\r\nPING\r\n")
            response = client.recv(1024)
            self.assertEqual(response, b"+PONG\r\n")

    def test_ping_with_message_resp(self) -> None:
        """Test sending PING with custom message returns a RESP2 bulk string."""
        with self._create_client() as client:
            raw = self._send_resp_cmd(client, ["PING", "hello"])
            self.assertEqual(raw, b"$5\r\nhello\r\n")

    def test_set_ex_and_ttl_over_resp(self) -> None:
        """Test SET key value EX 1 and expiration verification over TCP."""
        with self._create_client() as client:
            raw_set = self._send_resp_cmd(client, ["SET", "temp", "val", "EX", "1"])
            self.assertEqual(raw_set, b"+OK\r\n")

            raw_ttl = self._send_resp_cmd(client, ["TTL", "temp"])
            self.assertEqual(raw_ttl, b":1\r\n")

            self.assertEqual(self._send_resp_cmd(client, ["GET", "temp"]), b"$3\r\nval\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["EXISTS", "temp"]), b":1\r\n")

            # Wait for key to expire
            time.sleep(1.05)

            # Expired key must behave as missing for GET, EXISTS, DEL, and TTL
            self.assertEqual(self._send_resp_cmd(client, ["GET", "temp"]), b"$-1\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["EXISTS", "temp"]), b":0\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["DEL", "temp"]), b":0\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["TTL", "temp"]), b":-2\r\n")

    def test_expire_command_over_resp(self) -> None:
        """Test EXPIRE sets timeout and returns 1 on existing, 0 on missing key."""
        with self._create_client() as client:
            self.assertEqual(self._send_resp_cmd(client, ["SET", "k", "v"]), b"+OK\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["TTL", "k"]), b":-1\r\n")

            raw_exp = self._send_resp_cmd(client, ["EXPIRE", "k", "5"])
            self.assertEqual(raw_exp, b":1\r\n")

            raw_ttl = self._send_resp_cmd(client, ["TTL", "k"])
            self.assertIn(raw_ttl, (b":5\r\n", b":4\r\n"))

            raw_exp_miss = self._send_resp_cmd(client, ["EXPIRE", "missing_k", "5"])
            self.assertEqual(raw_exp_miss, b":0\r\n")

    # ------------------------------------------------------------------
    # V7 RESP integration tests
    # ------------------------------------------------------------------

    def _recv_array(self, client: socket.socket) -> bytes:
        """
        Read a full RESP Array frame from the socket.
        Keeps reading until parse_frame returns a consumed > 0.
        """
        buf = bytearray()
        while True:
            chunk = client.recv(4096)
            if not chunk:
                break
            buf.extend(chunk)
            val, consumed = parse_frame(bytes(buf))
            if consumed > 0:
                return bytes(buf[:consumed])
        return bytes(buf)

    def test_mset_resp(self) -> None:
        """Test MSET sets multiple keys and returns +OK\\r\\n."""
        with self._create_client() as client:
            raw = self._send_resp_cmd(client, ["MSET", "a", "alpha", "b", "beta"])
            self.assertEqual(raw, b"+OK\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["GET", "a"]), b"$5\r\nalpha\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["GET", "b"]), b"$4\r\nbeta\r\n")

    def test_mset_odd_args_returns_error(self) -> None:
        """Test MSET with odd number of arguments returns RESP error."""
        with self._create_client() as client:
            raw = self._send_resp_cmd(client, ["MSET", "k1", "v1", "k2"])
            self.assertTrue(raw.startswith(b"-ERR"))

    def test_mget_all_present_resp(self) -> None:
        """Test MGET returns a RESP Array of bulk strings for present keys."""
        # Set up keys, then read array on the same connection
        with self._create_client() as client:
            self._send_resp_cmd(client, ["SET", "x", "10"])
            self._send_resp_cmd(client, ["SET", "y", "20"])
            client.sendall(b"*3\r\n$4\r\nMGET\r\n$1\r\nx\r\n$1\r\ny\r\n")
            raw = self._recv_array(client)
            val, _ = parse_frame(raw)
            self.assertIsInstance(val, list)
            self.assertEqual(val, ["10", "20"])

    def test_mget_with_missing_key_resp(self) -> None:
        """Test MGET returns $-1\\r\\n for missing/expired keys."""
        with self._create_client() as client:
            self._send_resp_cmd(client, ["SET", "present", "here"])
            # MGET present missing_key
            client.sendall(b"*3\r\n$4\r\nMGET\r\n$7\r\npresent\r\n$7\r\nmissing\r\n")
            raw = self._recv_array(client)
            val, _ = parse_frame(raw)
            self.assertIsInstance(val, list)
            self.assertEqual(val[0], "here")
            self.assertIsNone(val[1])  # null bulk string → None after parse

    def test_mget_expired_key_resp(self) -> None:
        """Test MGET returns null bulk string for an expired key over TCP."""
        with self._create_client() as client:
            self._send_resp_cmd(client, ["SET", "temp", "gone", "EX", "1"])
            time.sleep(1.05)
            client.sendall(b"*2\r\n$4\r\nMGET\r\n$4\r\ntemp\r\n")
            raw = self._recv_array(client)
            val, _ = parse_frame(raw)
            self.assertIsInstance(val, list)
            self.assertIsNone(val[0])

    def test_dbsize_resp(self) -> None:
        """Test DBSIZE returns a RESP Integer with the count of live keys."""
        with self._create_client() as client:
            # Empty store
            raw_zero = self._send_resp_cmd(client, ["DBSIZE"])
            self.assertEqual(raw_zero, b":0\r\n")
            # Add keys
            self._send_resp_cmd(client, ["MSET", "a", "1", "b", "2"])
            raw_two = self._send_resp_cmd(client, ["DBSIZE"])
            self.assertEqual(raw_two, b":2\r\n")

    def test_keys_star_resp(self) -> None:
        """Test KEYS * returns a RESP Array of all non-expired key names."""
        with self._create_client() as client:
            self._send_resp_cmd(client, ["MSET", "p", "1", "q", "2"])
            client.sendall(b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")
            raw = self._recv_array(client)
            val, _ = parse_frame(raw)
            self.assertIsInstance(val, list)
            self.assertCountEqual(val, ["p", "q"])

    def test_keys_star_empty_store_resp(self) -> None:
        """Test KEYS * on empty store returns *0\\r\\n."""
        with self._create_client() as client:
            client.sendall(b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")
            raw = self._recv_array(client)
            val, _ = parse_frame(raw)
            self.assertIsInstance(val, list)
            self.assertEqual(val, [])

    def test_keys_unsupported_pattern_resp(self) -> None:
        """Test KEYS with unsupported pattern returns a RESP error."""
        with self._create_client() as client:
            raw = self._send_resp_cmd(client, ["KEYS", "h?llo"])
            self.assertTrue(raw.startswith(b"-ERR"))

    def test_v8_lists_sets_hashes_and_wrong_type_resp(self) -> None:
        with self._create_client() as client:
            self.assertEqual(self._send_resp_cmd(client, ["LPUSH", "queue", "one", "two"]), b":2\r\n")
            raw_range = self._send_resp_cmd(client, ["LRANGE", "queue", "0", "-1"])
            self.assertEqual(parse_frame(raw_range)[0], ["two", "one"])
            self.assertEqual(self._send_resp_cmd(client, ["RPUSH", "queue", "three"]), b":3\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["LPOP", "queue"]), b"$3\r\ntwo\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["RPOP", "missing"]), b"$-1\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["LLEN", "queue"]), b":2\r\n")

            self.assertEqual(self._send_resp_cmd(client, ["SADD", "tags", "z", "a", "a"]), b":2\r\n")
            raw_members = self._send_resp_cmd(client, ["SMEMBERS", "tags"])
            self.assertEqual(parse_frame(raw_members)[0], ["a", "z"])
            self.assertEqual(self._send_resp_cmd(client, ["SISMEMBER", "tags", "a"]), b":1\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["SREM", "tags", "z"]), b":1\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["SCARD", "tags"]), b":1\r\n")

            self.assertEqual(
                self._send_resp_cmd(client, ["HSET", "user", "name", "Ada", "city", "New York"]),
                b":2\r\n",
            )
            raw_hash = self._send_resp_cmd(client, ["HGETALL", "user"])
            self.assertEqual(parse_frame(raw_hash)[0], ["city", "New York", "name", "Ada"])
            self.assertEqual(self._send_resp_cmd(client, ["HGET", "user", "missing"]), b"$-1\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["HDEL", "user", "name"]), b":1\r\n")
            self.assertEqual(self._send_resp_cmd(client, ["HLEN", "user"]), b":1\r\n")

            wrong_type = self._send_resp_cmd(client, ["SADD", "user", "member"])
            self.assertTrue(wrong_type.startswith(b"-ERR WRONGTYPE"))

    def test_v8_inline_collection_commands(self) -> None:
        """The legacy newline protocol accepts the V8 command argument forms."""
        with self._create_client() as client:
            self.assertEqual(self._send_inline_cmd(client, 'RPUSH queue "hello world" tail'), "2")
            self.assertEqual(self._send_inline_cmd(client, "LRANGE queue 0 -1"), "1) hello world\n2) tail")
            self.assertEqual(self._send_inline_cmd(client, "SADD tags red blue red"), "2")
            self.assertEqual(self._send_inline_cmd(client, "SMEMBERS tags"), "1) blue\n2) red")
            self.assertEqual(self._send_inline_cmd(client, 'HSET user city "New York"'), "1")
            self.assertEqual(self._send_inline_cmd(client, "HGET user city"), "New York")

    def _send_inline_cmd(self, client: socket.socket, command: str) -> str:
        client.sendall((command + "\n").encode("utf-8"))
        data = bytearray()
        expected_lines = 2 if command.startswith(("LRANGE", "SMEMBERS")) else 1
        while data.count(b"\n") < expected_lines:
            chunk = client.recv(1024)
            if not chunk:
                break
            data.extend(chunk)
        return data.decode("utf-8").rstrip("\r\n")


if __name__ == "__main__":
    unittest.main()
