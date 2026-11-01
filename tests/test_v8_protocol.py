"""Exercise the server's RESP2 and inline paths without binding TCP ports."""

import socket
import threading
import unittest

from miniredis.resp import encode_array, parse_frame
from miniredis.server import MiniRedisServer


class TestV8ProtocolOverSocketPair(unittest.TestCase):
    def test_v8_collection_replies_over_resp_and_inline_protocols(self):
        server = MiniRedisServer(verbose=False, persistence_path=None)
        server._running = True
        client, server_side = socket.socketpair()
        client.settimeout(2)
        worker = threading.Thread(
            target=server._handle_client, args=(server_side, ("local", 0)), daemon=True
        )
        worker.start()

        def send_resp(*parts):
            client.sendall(encode_array(list(parts)))
            buffer = bytearray()
            while True:
                value, consumed = parse_frame(bytes(buffer))
                if consumed:
                    return value
                buffer.extend(client.recv(4096))

        try:
            self.assertEqual(send_resp("LPUSH", "queue", "first", "second"), 2)
            self.assertEqual(send_resp("LRANGE", "queue", "0", "-1"), ["second", "first"])
            self.assertEqual(send_resp("RPOP", "queue"), "first")
            self.assertEqual(send_resp("LPOP", "absent"), None)
            self.assertEqual(send_resp("SADD", "tags", "z", "a", "a"), 2)
            self.assertEqual(send_resp("SMEMBERS", "tags"), ["a", "z"])
            self.assertEqual(send_resp("SISMEMBER", "tags", "a"), 1)
            self.assertEqual(send_resp("HSET", "user", "name", "Ada", "city", "New York"), 2)
            self.assertEqual(send_resp("HGETALL", "user"), ["city", "New York", "name", "Ada"])
            self.assertEqual(send_resp("HGET", "user", "missing"), None)
            self.assertTrue(send_resp("GET", "queue").startswith("ERR WRONGTYPE"))

            client.sendall(b'HGET user city\n')
            self.assertEqual(client.recv(1024), b"New York\n")
            client.sendall(b'RPUSH notes "hello world"\n')
            self.assertEqual(client.recv(1024), b"1\n")
            client.sendall(b'LRANGE notes 0 -1\n')
            self.assertEqual(client.recv(1024), b"1) hello world\n")
        finally:
            client.close()
            worker.join(timeout=2)
            server.stop()


if __name__ == "__main__":
    unittest.main()
