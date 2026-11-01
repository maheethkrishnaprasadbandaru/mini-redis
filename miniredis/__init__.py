"""
Mini Redis - A lightweight, educational in-memory key-value database built from scratch.
"""

__version__ = "0.14.0"

from miniredis.store import MiniRedisStore
from miniredis.commands import CommandHandler
from miniredis import persistence
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

__all__ = [
    "MiniRedisStore",
    "CommandHandler",
    "MiniRedisServer",
    "persistence",
    "RESPError",
    "RESPParser",
    "parse_frame",
    "encode_simple_string",
    "encode_error",
    "encode_integer",
    "encode_bulk_string",
    "encode_null_bulk_string",
    "encode_array",
    "encode_command_response",
]


def __getattr__(name):
    """Load the server lazily to keep ``python -m miniredis.server`` clean."""
    if name == "MiniRedisServer":
        from miniredis.server import MiniRedisServer
        return MiniRedisServer
    raise AttributeError(name)
