"""
Unit and integration tests for Mini Redis TCP Server (V3).
"""

import socket
import threading
import time
import unittest
from miniredis.server import MiniRedisServer
from miniredis.store import MiniRedisStore


class TestMiniRedisServer(unittest.TestCase):
    """Integration test suite for the Mini Redis TCP server."""

    def setUp(self) -> None:
        """Start a fresh MiniRedisServer on an ephemeral port in a daemon thread."""
        self.store = MiniRedisStore()
        # Port 0 instructs the OS to assign an available ephemeral port
        # persistence_path=None disables file I/O so TCP tests stay self-contained
        self.server = MiniRedisServer(host="127.0.0.1", port=0, store=self.store, verbose=False, persistence_path=None)
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()

        # Wait until server is up and listening
        start_time = time.time()
        while not self.server.is_running and time.time() - start_time < 2.0:
            time.sleep(0.01)

        self.assertTrue(self.server.is_running, "Server failed to start within timeout.")
        self.port = self.server.actual_port
        self.assertGreater(self.port, 0)

    def tearDown(self) -> None:
        """Stop the server and join the background thread."""
        self.server.stop()
        self.server_thread.join(timeout=2.0)

    def _create_client(self) -> socket.socket:
        """Helper to create a connected client socket with a safety timeout."""
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(2.0)
        client.connect(("127.0.0.1", self.port))
        return client

    def _send_cmd(self, client: socket.socket, cmd: str) -> str:
        """Send a newline-delimited command and read the newline-terminated response."""
        client.sendall((cmd + "\n").encode("utf-8"))
        data = ""
        while "\n" not in data:
            chunk = client.recv(1024).decode("utf-8")
            if not chunk:
                break
            data += chunk
        return data.rstrip("\r\n")

    def test_single_command_set_and_get(self) -> None:
        """Test sending SET and GET commands over TCP."""
        with self._create_client() as client:
            res_set = self._send_cmd(client, "SET name Maheeth")
            self.assertEqual(res_set, "OK")

            res_get = self._send_cmd(client, "GET name")
            self.assertEqual(res_get, "Maheeth")

    def test_multiple_sequential_commands_on_same_connection(self) -> None:
        """Test sending several commands consecutively across a single TCP connection."""
        with self._create_client() as client:
            self.assertEqual(self._send_cmd(client, "SET count 1"), "OK")
            self.assertEqual(self._send_cmd(client, "EXISTS count"), "1")
            self.assertEqual(self._send_cmd(client, "DEL count"), "1")
            self.assertEqual(self._send_cmd(client, "EXISTS count"), "0")
            self.assertEqual(self._send_cmd(client, "GET count"), "(nil)")

    def test_values_with_spaces_over_tcp(self) -> None:
        """Test storing and retrieving values containing spaces over TCP."""
        with self._create_client() as client:
            self.assertEqual(self._send_cmd(client, "SET message hello world"), "OK")
            self.assertEqual(self._send_cmd(client, "GET message"), "hello world")

    def test_case_insensitivity_and_errors_over_tcp(self) -> None:
        """Test command case-insensitivity and syntax errors over TCP."""
        with self._create_client() as client:
            self.assertEqual(self._send_cmd(client, "set item book"), "OK")
            self.assertEqual(self._send_cmd(client, "Get item"), "book")

            # Unknown command
            unknown_res = self._send_cmd(client, "FOOBAR item")
            self.assertEqual(unknown_res, "ERR unknown command 'FOOBAR'")

            # Wrong arguments
            wrong_args_res = self._send_cmd(client, "SET")
            self.assertEqual(wrong_args_res, "ERR wrong number of arguments for 'set' command")

    def test_multiple_sequential_clients_share_state(self) -> None:
        """Test that different clients connecting consecutively share the store state."""
        # Client 1 writes data
        with self._create_client() as client1:
            self.assertEqual(self._send_cmd(client1, "SET shared key_value"), "OK")

        # Client 2 reads the data written by Client 1
        with self._create_client() as client2:
            self.assertEqual(self._send_cmd(client2, "GET shared"), "key_value")

    def test_clean_client_disconnection(self) -> None:
        """Test that a client closing its connection does not crash the server."""
        client = self._create_client()
        self.assertEqual(self._send_cmd(client, "SET test_disconnect ok"), "OK")
        client.close()  # Disconnect cleanly

        # Reconnect with a new client to verify server remains healthy
        with self._create_client() as new_client:
            self.assertEqual(self._send_cmd(new_client, "GET test_disconnect"), "ok")

    def test_ping_inline_over_tcp(self) -> None:
        """Test sending PING over inline newline text TCP connection."""
        with self._create_client() as client:
            self.assertEqual(self._send_cmd(client, "PING"), "PONG")
            self.assertEqual(self._send_cmd(client, "ping"), "PONG")
            self.assertEqual(self._send_cmd(client, "PING hello"), "hello")

    def test_expire_and_ttl_inline_over_tcp(self) -> None:
        """Test inline commands for SET EX, EXPIRE, and TTL."""
        with self._create_client() as client:
            self.assertEqual(self._send_cmd(client, "SET test_ttl 123 EX 10"), "OK")
            self.assertEqual(self._send_cmd(client, "TTL test_ttl"), "10")
            self.assertEqual(self._send_cmd(client, "EXPIRE test_ttl 30"), "1")
            self.assertEqual(self._send_cmd(client, "TTL test_ttl"), "30")


if __name__ == "__main__":
    unittest.main()
