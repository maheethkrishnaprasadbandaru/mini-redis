"""
RESP (Redis Serialization Protocol) codec module for Mini Redis.

Implements RESP2 encoding and decoding for:
- Simple Strings: +<string>\r\n
- Errors: -<error>\r\n
- Integers: :<integer>\r\n
- Bulk Strings: $<length>\r\n<data>\r\n
- Null Bulk Strings: $-1\r\n
- Arrays: *<count>\r\n<element_1>...<element_n>

All lengths for Bulk Strings are measured in bytes, not characters.
"""

from typing import Any, List, Optional, Tuple, Union


class RESPError(Exception):
    """Raised when encountering invalid, unsupported, or malformed RESP input."""
    pass


# ---------------------------------------------------------------------------
# Encoders
# ---------------------------------------------------------------------------

def encode_simple_string(s: str) -> bytes:
    """
    Encode a string as a RESP Simple String (+<string>\r\n).

    Simple strings are not binary-safe and cannot contain CR or LF characters.
    Used for status replies like '+OK\r\n'.
    """
    if "\r" in s or "\n" in s:
        raise RESPError("Simple strings cannot contain CR or LF characters.")
    return f"+{s}\r\n".encode("utf-8")


def encode_error(message: str) -> bytes:
    """
    Encode an error message as a RESP Error (-<message>\r\n).

    Used for protocol and execution error replies.
    """
    # Replace internal newlines if any to keep frame structure valid
    clean_msg = message.replace("\r", " ").replace("\n", " ")
    return f"-{clean_msg}\r\n".encode("utf-8")


def encode_integer(val: int) -> bytes:
    """
    Encode an integer as a RESP Integer (:<integer>\r\n).

    Used for commands like DEL and EXISTS that return counts or booleans.
    """
    return f":{val}\r\n".encode("utf-8")


def encode_bulk_string(val: Union[str, bytes]) -> bytes:
    """
    Encode data as a RESP Bulk String ($<length>\r\n<data>\r\n).

    Binary-safe string. The length is strictly the byte count (not character count).
    """
    if isinstance(val, str):
        raw_bytes = val.encode("utf-8")
    elif isinstance(val, (bytes, bytearray)):
        raw_bytes = bytes(val)
    else:
        raw_bytes = str(val).encode("utf-8")

    return f"${len(raw_bytes)}\r\n".encode("utf-8") + raw_bytes + b"\r\n"


def encode_null_bulk_string() -> bytes:
    """
    Encode a RESP Null Bulk String ($-1\r\n).

    Used to represent non-existent keys (nil in Redis).
    """
    return b"$-1\r\n"


def encode_array(items: List[Any]) -> bytes:
    """
    Encode a list of items as a RESP Array (*<count>\r\n...).
    """
    header = f"*{len(items)}\r\n".encode("utf-8")
    body = bytearray()
    for item in items:
        if item is None:
            body.extend(encode_null_bulk_string())
        elif isinstance(item, int):
            body.extend(encode_integer(item))
        elif isinstance(item, (str, bytes)):
            body.extend(encode_bulk_string(item))
        elif isinstance(item, list):
            body.extend(encode_array(item))
        else:
            body.extend(encode_bulk_string(str(item)))
    return header + bytes(body)


def encode_command_response(cmd_name: str, result: Optional[str]) -> bytes:
    """
    Map a CommandHandler result string to the appropriate RESP2 response bytes.

    Rules:
    - Error strings (starting with 'ERR') -> RESP Error (-ERR ...\r\n)
    - SET -> RESP Simple String (+OK\r\n)
    - GET:
        - If missing ('(nil)') -> RESP Null Bulk String ($-1\r\n)
        - If value present -> RESP Bulk String ($<len>\r\n<val>\r\n)
    - DEL / EXISTS -> RESP Integer (:1\r\n or :0\r\n)
    """
    if result is None:
        return encode_null_bulk_string()
    if result.startswith("ERR"):
        return encode_error(result)

    cmd = cmd_name.upper()

    if cmd == "PING":
        if result == "PONG":
            return encode_simple_string("PONG")
        return encode_bulk_string(result)

    if cmd == "SET" and result == "OK":
        return encode_simple_string("OK")

    if cmd == "GET":
        if result == "(nil)":
            return encode_null_bulk_string()
        return encode_bulk_string(result)

    if cmd in (
        "DEL", "EXISTS", "EXPIRE", "TTL", "DBSIZE", "PUBLISH", "LPUSH", "RPUSH", "LLEN", "SADD",
        "SREM", "SISMEMBER", "SCARD", "HSET", "HDEL", "HLEN",
    ):
        try:
            return encode_integer(int(result))
        except ValueError:
            return encode_integer(1 if result == "1" else 0)

    if cmd == "MSET" and result == "OK":
        return encode_simple_string("OK")

    # General fallback
    if result == "(nil)":
        return encode_null_bulk_string()
    elif result == "OK":
        return encode_simple_string("OK")
    return encode_bulk_string(result)


# ---------------------------------------------------------------------------
# Decoders / Parser
# ---------------------------------------------------------------------------

def parse_frame(buffer: bytes) -> Tuple[Optional[Any], int]:
    """
    Parse a single complete RESP frame from a byte buffer.

    Args:
        buffer: Bytes slice to parse.

    Returns:
        A tuple of (value, bytes_consumed).
        If the buffer contains only a partial/incomplete frame, returns (None, 0).

    Raises:
        RESPError: If the frame is malformed or invalid according to RESP2 syntax.
    """
    if not buffer:
        return None, 0

    prefix = buffer[0:1]

    # Simple String: +<string>\r\n
    if prefix == b"+":
        crlf_pos = buffer.find(b"\r\n")
        if crlf_pos == -1:
            return None, 0
        payload = buffer[1:crlf_pos].decode("utf-8", errors="replace")
        return payload, crlf_pos + 2

    # Error: -<error>\r\n
    elif prefix == b"-":
        crlf_pos = buffer.find(b"\r\n")
        if crlf_pos == -1:
            return None, 0
        payload = buffer[1:crlf_pos].decode("utf-8", errors="replace")
        return payload, crlf_pos + 2

    # Integer: :<integer>\r\n
    elif prefix == b":":
        crlf_pos = buffer.find(b"\r\n")
        if crlf_pos == -1:
            return None, 0
        raw_int = buffer[1:crlf_pos]
        try:
            val = int(raw_int.decode("utf-8"))
        except ValueError:
            raise RESPError(f"Invalid RESP integer value: {raw_int!r}")
        return val, crlf_pos + 2

    # Bulk String: $<length>\r\n<data>\r\n
    elif prefix == b"$":
        crlf_pos = buffer.find(b"\r\n")
        if crlf_pos == -1:
            return None, 0
        raw_len = buffer[1:crlf_pos]
        try:
            bulk_len = int(raw_len.decode("utf-8"))
        except ValueError:
            raise RESPError(f"Invalid RESP bulk string length: {raw_len!r}")

        # Null Bulk String ($-1\r\n)
        if bulk_len == -1:
            return None, crlf_pos + 2
        if bulk_len < -1:
            raise RESPError(f"Negative bulk string length invalid: {bulk_len}")

        # Data starts right after header CRLF
        data_start = crlf_pos + 2
        data_end = data_start + bulk_len
        expected_total_len = data_end + 2  # Including trailing \r\n

        if len(buffer) < expected_total_len:
            # Full bulk string payload has not arrived yet
            return None, 0

        # Validate trailing CRLF
        if buffer[data_end:expected_total_len] != b"\r\n":
            raise RESPError("Malformed bulk string: missing trailing CRLF")

        data_bytes = buffer[data_start:data_end]
        data_str = data_bytes.decode("utf-8", errors="replace")
        return data_str, expected_total_len

    # Array: *<count>\r\n<elem1>...<elemN>
    elif prefix == b"*":
        crlf_pos = buffer.find(b"\r\n")
        if crlf_pos == -1:
            return None, 0
        raw_count = buffer[1:crlf_pos]
        try:
            count = int(raw_count.decode("utf-8"))
        except ValueError:
            raise RESPError(f"Invalid RESP array count: {raw_count!r}")

        if count == -1:
            return None, crlf_pos + 2
        if count < -1:
            raise RESPError(f"Negative array count invalid: {count}")

        elements: List[Any] = []
        offset = crlf_pos + 2

        for _ in range(count):
            if offset >= len(buffer):
                # More elements expected, buffer exhausted
                return None, 0
            elem, consumed = parse_frame(buffer[offset:])
            if consumed == 0:
                # Incomplete child element
                return None, 0
            elements.append(elem)
            offset += consumed

        return elements, offset

    else:
        raise RESPError(f"Unknown RESP prefix byte: {prefix!r}")


class RESPParser:
    """
    Incremental byte buffer and parser for RESP stream processing.

    Buffers chunks read from a TCP socket and yields parsed values as soon
    as complete frames arrive.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> None:
        """Append incoming bytes to the read buffer."""
        self._buffer.extend(chunk)

    def next_frame(self) -> Optional[Any]:
        """
        Attempt to parse the next frame from the current buffer.

        Returns:
            The parsed frame value if a full frame is available, or None if
            more data is required.

        Raises:
            RESPError: If data in the buffer violates the RESP specification.
        """
        if not self._buffer:
            return None

        val, consumed = parse_frame(bytes(self._buffer))
        if consumed > 0:
            del self._buffer[:consumed]
            return val
        return None

    def clear(self) -> None:
        """Clear the internal buffer."""
        self._buffer.clear()

    @property
    def buffer_bytes(self) -> bytes:
        """Return the current unparsed buffer content."""
        return bytes(self._buffer)
