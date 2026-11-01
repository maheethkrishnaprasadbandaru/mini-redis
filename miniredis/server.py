"""
TCP Server module for Mini Redis (V4 / V6).

Provides a lightweight TCP server that listens for incoming client connections,
decodes RESP2-encoded commands (as well as supporting inline commands),
executes them using CommandHandler, and returns RESP2-encoded responses.

V6 adds optional persistence: the store is loaded from a JSON snapshot on
startup and saved (atomically) after every mutating command.
"""

import socket
import argparse
import hashlib
import hmac
import json
import logging
import queue
import select
import shlex
import threading
import time
from typing import Optional
from miniredis.store import MiniRedisStore
from miniredis.commands import CommandHandler
from miniredis.resp import parse_frame, encode_command_response, encode_array, encode_error, RESPError
from miniredis import persistence
from miniredis.pubsub import PubSubClient, PubSubManager


class MiniRedisServer:
    """
    A TCP server for Mini Redis supporting RESP2 (Redis Serialization Protocol).

    Accepts client connections and processes commands formatted as RESP Arrays:
        Client sends:   *<count>\r\n$<len>\r\n<arg>...\r\n
        Server returns: <RESP response>\r\n

    Also maintains backward compatibility with inline newline-delimited text commands.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        store: Optional[MiniRedisStore] = None,
        verbose: bool = True,
        persistence_path: Optional[str] = "./miniredis_data.json",
        password: Optional[str] = None,
        max_request_size: int = 1024 * 1024,
        client_timeout: float = 30.0,
        log_level: str = "INFO",
    ) -> None:
        """
        Initialize the server with a host, port, storage engine, and persistence path.

        Args:
            host:             IP address or hostname to bind (default: '127.0.0.1').
            port:             Port to listen on (default: 6379; use 0 for OS-assigned ephemeral port).
            store:            Optional existing MiniRedisStore instance. If None, a new store is created.
            verbose:          Whether to log connection and startup status to stdout.
            persistence_path: Path to the JSON snapshot file used for persistence.
                              Set to None to disable persistence entirely.
        """
        self.host = host
        self.port = port
        self.store = store if store is not None else MiniRedisStore()
        self.handler = CommandHandler(self.store)
        self.verbose = verbose
        self.persistence_path = persistence_path
        if max_request_size < 1024:
            raise ValueError("max_request_size must be at least 1024 bytes")
        self.max_request_size = max_request_size
        self.client_timeout = client_timeout
        self._password_digest = hashlib.sha256(password.encode("utf-8")).digest() if password else None
        self._pubsub = PubSubManager()
        self._state_lock = threading.RLock()
        self._stop_lock = threading.Lock()
        self._client_sockets = set()
        self._client_threads = set()
        self._command_counts = {}
        self._started_at = time.time()
        self._logger = logging.getLogger("miniredis")
        self._logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        if not self._logger.handlers:
            self._logger.addHandler(logging.NullHandler())
        self._server_socket: Optional[socket.socket] = None
        self._running: bool = False

        # Load persisted data from disk (if a snapshot file exists)
        if self.persistence_path:
            self._load_snapshot()

    @property
    def actual_port(self) -> int:
        """
        Return the actual port the server is listening on.
        Useful when bound to port 0 (ephemeral port assigned by OS).
        """
        if self._server_socket:
            return self._server_socket.getsockname()[1]
        return self.port

    @property
    def is_running(self) -> bool:
        """Check if the server is currently running."""
        return self._running

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _load_snapshot(self) -> None:
        """
        Load data from the persistence file into the store.

        Called once at server startup. Missing files are silently ignored.
        Corrupt files log a warning and leave the store empty.
        Keys that already expired while the server was offline are discarded.
        """
        state = persistence.load(self.persistence_path)
        for kind, target in (
            ("data", self.store._data),
            ("lists", self.store._lists),
            ("sets", self.store._sets),
            ("hashes", self.store._hashes),
        ):
            for key, value in state[kind].items():
                self.store._drop_key(key)
                target[key] = value
        self.store._expires.update(state["expires"])
        loaded_count = sum(len(state[kind]) for kind in ("data", "lists", "sets", "hashes"))
        if self.verbose and loaded_count:
            print(f"[*] Loaded {loaded_count} key(s) from '{self.persistence_path}'.")

    def _save_snapshot(self) -> None:
        """
        Atomically save the current store state to the persistence file.

        Called after every mutating command (SET, DEL, EXPIRE) and at shutdown.
        Silently ignores failures (a warning is printed).
        """
        if not self.persistence_path:
            return
        try:
            persistence.save(self.store, self.persistence_path)
        except Exception as exc:
            print(f"[!] Warning: failed to save snapshot to '{self.persistence_path}': {exc}")

    def start(self, max_connections: Optional[int] = None) -> None:
        """
        Bind, listen, and accept incoming connections.

        Args:
            max_connections: Optional limit on the number of client connections
                             to accept before stopping (mainly used for testing).
                             If None, runs indefinitely until stop() is called.
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((self.host, self.port))
            listener.listen(128)
        except Exception:
            listener.close()
            raise
        listener.settimeout(0.5)
        self._server_socket = listener
        self._running = True

        if self.verbose:
            print(f"[*] Mini Redis RESP TCP Server running on {self.host}:{self.actual_port}")
            print("[*] Ready to accept client connections (RESP2 protocol)...")
        self._logger.info("server started host=%s port=%s", self.host, self.actual_port)

        accepted_count = 0
        try:
            while self._running:
                try:
                    client_socket, client_address = self._server_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    # Socket closed during shutdown
                    break

                client_socket.settimeout(0.25)
                with self._state_lock:
                    self._client_sockets.add(client_socket)
                worker = threading.Thread(
                    target=self._client_worker,
                    args=(client_socket, client_address),
                    name=f"miniredis-client-{client_address[0]}:{client_address[1]}",
                    daemon=True,
                )
                with self._state_lock:
                    self._client_threads.add(worker)
                worker.start()
                accepted_count += 1
                if max_connections is not None and accepted_count >= max_connections:
                    break
        finally:
            self.stop()

    def _client_worker(self, client_socket: socket.socket, client_address: tuple) -> None:
        self._logger.info("client connected address=%s", client_address[0])
        try:
            self._handle_client(client_socket, client_address)
        finally:
            with self._state_lock:
                self._client_sockets.discard(client_socket)
                self._client_threads.discard(threading.current_thread())
            self._logger.info("client disconnected address=%s", client_address[0])

    def _encode_pubsub(self, kind: str, channel, count: int, message=None) -> bytes:
        parts = [kind, channel, count] if message is None else [kind, channel, message]
        return encode_array(parts)

    def _send_result(self, client_socket: socket.socket, cmd_name: str, result, protocol: str) -> None:
        if protocol == "resp":
            response = encode_array(result) if isinstance(result, list) else encode_command_response(cmd_name, result)
        elif isinstance(result, list):
            lines = [f"{i + 1}) {value if value is not None else '(nil)'}" for i, value in enumerate(result)]
            response = (("\n".join(lines) + "\n") if lines else "(empty array)\n").encode("utf-8")
        else:
            response = ("(nil)" if result is None else str(result))
            response = (response + "\n").encode("utf-8")
        client_socket.sendall(response)

    def _execute_client_command(self, client_socket, tokens, protocol, auth_state, pubsub_client):
        if isinstance(tokens, str):
            parts = tokens.strip().split(maxsplit=1)
            if not parts:
                return auth_state
            command = parts[0].upper()
            try:
                args = shlex.split(parts[1]) if len(parts) > 1 else []
            except ValueError:
                self._send_result(client_socket, command, "ERR syntax error", protocol)
                return auth_state
        else:
            if not tokens:
                return auth_state
            command = str(tokens[0]).upper()
            args = [str(value) for value in tokens[1:]]
        if not command:
            return auth_state
        with self._state_lock:
            self._command_counts[command] = self._command_counts.get(command, 0) + 1
        # AUTH is connection-scoped. Only a SHA-256 digest is retained by the server.
        if command == "AUTH":
            if self._password_digest is None:
                self._send_result(client_socket, command, "ERR AUTH is not enabled", protocol)
            elif len(args) != 1:
                self._send_result(client_socket, command, "ERR wrong number of arguments for 'auth' command", protocol)
            elif hmac.compare_digest(hashlib.sha256(args[0].encode("utf-8")).digest(), self._password_digest):
                auth_state = True
                self._send_result(client_socket, command, "OK", protocol)
            else:
                self._logger.warning("authentication failed")
                self._send_result(client_socket, command, "ERR invalid password", protocol)
            return auth_state
        if self._password_digest is not None and not auth_state and command not in ("PING",):
            self._send_result(client_socket, command, "ERR authentication required", protocol)
            return auth_state
        if command == "SUBSCRIBE":
            if not args:
                self._send_result(client_socket, command, "ERR wrong number of arguments for 'subscribe' command", protocol)
                return auth_state
            for channel in args:
                count = self._pubsub.subscribe(pubsub_client, [channel])
                if protocol == "resp":
                    client_socket.sendall(self._encode_pubsub("subscribe", channel, count))
                else:
                    client_socket.sendall(f"subscribe {channel} {count}\n".encode())
                count = len(pubsub_client.channels)
            return auth_state
        if command == "UNSUBSCRIBE":
            channels = list(args) if args else sorted(pubsub_client.channels)
            if not channels:
                channels = [None]
            for channel in channels:
                if channel is None:
                    count, _ = self._pubsub.unsubscribe(pubsub_client)
                else:
                    count, _ = self._pubsub.unsubscribe(pubsub_client, [channel])
                if protocol == "resp":
                    client_socket.sendall(self._encode_pubsub("unsubscribe", channel, count))
                else:
                    client_socket.sendall(f"unsubscribe {channel or '(nil)'} {count}\n".encode())
            return auth_state
        if command == "PUBLISH":
            if len(args) != 2:
                result = "ERR wrong number of arguments for 'publish' command"
            else:
                result = str(self._pubsub.publish(args[0], args[1]))
            self._send_result(client_socket, command, result, protocol)
            return auth_state
        if command in ("INFO", "STATUS"):
            if args:
                result = f"ERR wrong number of arguments for '{command.lower()}' command"
            else:
                with self._state_lock:
                    clients = len(self._client_sockets)
                    commands = sum(self._command_counts.values())
                    command_counts = dict(sorted(self._command_counts.items()))
                result = "\n".join((
                    "# MiniRedis",
                    f"uptime_seconds:{int(time.time() - self._started_at)}",
                    f"connected_clients:{clients}",
                    f"total_commands_processed:{commands}",
                    f"pubsub_channels:{self._pubsub.channel_count}",
                    "command_counts:" + json.dumps(command_counts, sort_keys=True),
                ))
            self._send_result(client_socket, command, result, protocol)
            return auth_state
        result = self.handler.execute(tokens)
        self._send_result(client_socket, command, result, protocol)
        if command in (
            "SET", "DEL", "EXPIRE", "MSET", "LPUSH", "RPUSH", "LPOP", "RPOP",
            "SADD", "SREM", "HSET", "HDEL",
        ) and not (isinstance(result, str) and result.startswith("ERR")):
            self._save_snapshot()
        if isinstance(result, str) and result.startswith("ERR"):
            self._logger.warning("command rejected command=%s", command)
        return auth_state

    def _handle_client(self, client_socket: socket.socket, client_address: tuple) -> None:
        """Process RESP2 arrays and legacy inline lines for one concurrent client."""
        buffer = bytearray()
        client_socket.settimeout(0.25)
        auth_state = self._password_digest is None
        pubsub_client = PubSubClient()
        protocol_mode = "resp"
        last_activity = time.monotonic()
        try:
            while self._running:
                try:
                    ready, _, _ = select.select([client_socket], [], [], 0.1)
                except (OSError, ValueError):
                    break
                if pubsub_client.channels:
                    while True:
                        try:
                            channel, message = pubsub_client.messages.get_nowait()
                        except queue.Empty:
                            break
                        try:
                            client_socket.sendall(
                                encode_array(["message", channel, message])
                                if protocol_mode == "resp" else f"message {channel} {message}\n".encode("utf-8")
                            )
                        except OSError:
                            return
                if not ready:
                    if self.client_timeout > 0 and time.monotonic() - last_activity > self.client_timeout:
                        break
                    continue
                try:
                    data = client_socket.recv(4096)
                except socket.timeout:
                    continue
                except (ConnectionResetError, BrokenPipeError, OSError):
                    break
                if not data:
                    break
                last_activity = time.monotonic()
                buffer.extend(data)
                if len(buffer) > self.max_request_size:
                    try:
                        client_socket.sendall(encode_error("ERR request exceeds configured size limit"))
                    except OSError:
                        pass
                    return
                while buffer and self._running:
                    while buffer and buffer[:1] in (b"\r", b"\n", b" "):
                        del buffer[:1]
                    if not buffer:
                        break
                    if buffer.startswith(b"*"):
                        try:
                            frame, consumed = parse_frame(bytes(buffer))
                        except RESPError as err:
                            self._logger.warning("malformed RESP request")
                            client_socket.sendall(encode_error(f"ERR Protocol error: {err}"))
                            return
                        if consumed == 0:
                            break
                        del buffer[:consumed]
                        if not isinstance(frame, list) or not frame or any(not isinstance(item, str) for item in frame):
                            client_socket.sendall(encode_error("ERR Protocol error: expected command array of strings"))
                            continue
                        tokens = [str(item) for item in frame]
                        protocol_mode = "resp"
                        auth_state = self._execute_client_command(client_socket, tokens, "resp", auth_state, pubsub_client)
                    else:
                        if buffer[:1] in (b"+", b"-", b":", b"$"):
                            client_socket.sendall(encode_error("ERR Protocol error: expected RESP command array"))
                            return
                        nl_pos = buffer.find(b"\n")
                        if nl_pos < 0:
                            break
                        line = bytes(buffer[:nl_pos]).decode("utf-8", errors="replace").rstrip("\r").strip()
                        del buffer[:nl_pos + 1]
                        if line:
                            protocol_mode = "inline"
                            auth_state = self._execute_client_command(
                                client_socket, line, "inline", auth_state, pubsub_client
                            )
        except (OSError, ValueError) as exc:
            self._logger.debug("client I/O ended: %s", type(exc).__name__)
        except Exception as exc:
            self._logger.error("unexpected client handler failure type=%s", type(exc).__name__)
            try:
                client_socket.sendall(encode_error("ERR internal server error"))
            except OSError:
                pass
        finally:
            self._pubsub.cleanup(pubsub_client)
            try:
                client_socket.close()
            except OSError:
                pass

    def stop(self) -> None:
        """Gracefully shut down the server, save a final snapshot, and close the listening socket."""
        with self._stop_lock:
            was_running = self._running
            self._running = False
            listener, self._server_socket = self._server_socket, None
            if listener:
                try:
                    listener.close()
                except OSError:
                    pass
            with self._state_lock:
                sockets = tuple(self._client_sockets)
                workers = tuple(self._client_threads)
            for client in sockets:
                try:
                    client.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    client.close()
                except OSError:
                    pass
            for worker in workers:
                if worker is not threading.current_thread():
                    worker.join(timeout=1.0)
            self._save_snapshot()
            if was_running:
                self._logger.info("server stopped")


def main(argv=None) -> None:
    """
    Entry point for running the Mini Redis TCP server from the terminal.

    Usage:
        python3 -m miniredis.server [PORT] [--persistence-file PATH]

    Examples:
        python3 -m miniredis.server                        # port 6379, default data file
        python3 -m miniredis.server 6380                   # custom port
        python3 -m miniredis.server 6379 --no-persistence  # disable persistence
        python3 -m miniredis.server 6379 --persistence-file /tmp/mydata.json
    """
    parser = argparse.ArgumentParser(description="Run the Mini Redis server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("port_positional", nargs="?", type=int, help="legacy positional port")
    parser.add_argument("--port", dest="port_option", type=int)
    parser.add_argument("--persistence-file", default="./miniredis_data.json")
    parser.add_argument("--no-persistence", action="store_true")
    parser.add_argument("--password-env", default="MINIREDIS_PASSWORD", help="Environment variable containing the AUTH password.")
    parser.add_argument("--max-request-size", type=int, default=1024 * 1024)
    parser.add_argument("--client-timeout", type=float, default=30.0)
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    options = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, options.log_level), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    import os
    password = os.environ.get(options.password_env)
    server = MiniRedisServer(
        host=options.host, port=options.port_option if options.port_option is not None else (options.port_positional or 6379),
        persistence_path=None if options.no_persistence else options.persistence_file,
        password=password, max_request_size=options.max_request_size,
        client_timeout=options.client_timeout, log_level=options.log_level,
    )
    try:
        server.start()
    except KeyboardInterrupt:
        logging.getLogger("miniredis").info("shutdown requested by keyboard interrupt")
    finally:
        server.stop()
        logging.getLogger("miniredis").info("shutdown complete")


if __name__ == "__main__":
    main()
