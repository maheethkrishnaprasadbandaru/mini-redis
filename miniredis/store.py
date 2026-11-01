"""
In-memory key-value store module for Mini Redis with TTL / expiration support.

This module provides the core storage engine (MiniRedisStore) which manages
in-memory key-value mappings and key lifetimes using monotonic time.
"""

import math
import time
import threading
from functools import wraps
from typing import Dict, List, Optional, Set, Tuple, Union


class WrongTypeError(TypeError):
    """Raised when an operation targets a key holding another data type."""


def _synchronized(method):
    """Serialize store operations while allowing safe nested store calls."""
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapped


class MiniRedisStore:
    """
    An in-memory key-value store modeled after Redis.

    Internally, data is stored in a Python dictionary (hash table), providing
    average O(1) time complexity for lookup, insertion, and deletion.

    Supports key expiration (TTL) using passive (lazy) eviction driven by
    time.monotonic().
    """

    def __init__(self) -> None:
        """Initialize an empty in-memory store."""
        self._lock = threading.RLock()
        self._data: Dict[str, str] = {}
        self._lists: Dict[str, List[str]] = {}
        self._sets: Dict[str, Set[str]] = {}
        self._hashes: Dict[str, Dict[str, str]] = {}
        self._expires: Dict[str, float] = {}

    @_synchronized
    def _raw_type(self, key: str) -> Optional[str]:
        if key in self._data:
            return "string"
        if key in self._lists:
            return "list"
        if key in self._sets:
            return "set"
        if key in self._hashes:
            return "hash"
        return None

    @_synchronized
    def type_of(self, key: str) -> Optional[str]:
        """Return a key's type, treating expired keys as missing."""
        if self._is_expired(key):
            return None
        return self._raw_type(key)

    @_synchronized
    def _drop_key(self, key: str) -> None:
        self._data.pop(key, None)
        self._lists.pop(key, None)
        self._sets.pop(key, None)
        self._hashes.pop(key, None)
        self._expires.pop(key, None)

    @_synchronized
    def _collection(self, key: str, kind: str, create: bool = False):
        self._is_expired(key)
        current = self._raw_type(key)
        if current is not None and current != kind:
            raise WrongTypeError(key)
        collection = {"list": self._lists, "set": self._sets, "hash": self._hashes}[kind]
        if current is None and create:
            collection[key] = [] if kind == "list" else set() if kind == "set" else {}
        return collection.get(key)

    @_synchronized
    def _is_expired(self, key: str) -> bool:
        """
        Check if key has expired according to time.monotonic().
        If expired, lazily purges the key and returns True.
        """
        if key in self._expires:
            if time.monotonic() >= self._expires[key]:
                self._drop_key(key)
                return True
        return False

    @_synchronized
    def set(self, key: str, value: str, ex: Optional[Union[int, float]] = None) -> bool:
        """
        Set key to hold the string value, with optional expiration in seconds.

        If the key already holds a value, it is overwritten. If ex is None,
        any existing expiration on the key is discarded.

        Args:
            key: The identifier for the stored entry.
            value: The string data to associate with the key.
            ex: Optional time-to-live in seconds.

        Returns:
            True indicating successful storage.
        """
        self._drop_key(key)
        self._data[key] = str(value)
        if ex is not None:
            self._expires[key] = time.monotonic() + ex
        else:
            self._expires.pop(key, None)
        return True

    @_synchronized
    def get(self, key: str) -> Optional[str]:
        """
        Get the value of key.

        Expired keys behave as missing keys and return None.

        Args:
            key: The identifier to look up.

        Returns:
            The string value associated with key, or None if the key does not exist or has expired.
        """
        if self._is_expired(key):
            return None
        if self._raw_type(key) not in (None, "string"):
            raise WrongTypeError(key)
        return self._data.get(key)

    @_synchronized
    def delete(self, key: str) -> bool:
        """
        Delete a key from the store.

        Expired keys behave as missing keys and return False.

        Args:
            key: The identifier to delete.

        Returns:
            True if the key existed and was deleted, False if the key did not exist or was expired.
        """
        if self._is_expired(key):
            return False
        if self._raw_type(key) is not None:
            self._drop_key(key)
            return True
        return False

    @_synchronized
    def exists(self, key: str) -> bool:
        """
        Determine if a key exists in the store.

        Expired keys return False.

        Args:
            key: The identifier to check.

        Returns:
            True if key exists and is not expired, False otherwise.
        """
        if self._is_expired(key):
            return False
        return self._raw_type(key) is not None

    @_synchronized
    def expire(self, key: str, seconds: Union[int, float]) -> bool:
        """
        Set a timeout on key. After the timeout has expired, the key will automatically be deleted.

        Args:
            key: The identifier to set timeout on.
            seconds: Timeout in seconds. If <= 0, the key is immediately deleted.

        Returns:
            True if timeout was set (or key deleted when seconds <= 0),
            False if key does not exist or has already expired.
        """
        if self._is_expired(key) or self._raw_type(key) is None:
            return False

        if seconds <= 0:
            self._drop_key(key)
            return True

        self._expires[key] = time.monotonic() + seconds
        return True

    @_synchronized
    def ttl(self, key: str) -> int:
        """
        Return the remaining time to live of a key that has a timeout.

        Returns:
            -2 if the key does not exist or has expired.
            -1 if the key exists but has no associated expire.
            Integer >= 0 representing remaining seconds if the key has an associated expire.
        """
        if self._is_expired(key) or self._raw_type(key) is None:
            return -2

        if key not in self._expires:
            return -1

        remaining = self._expires[key] - time.monotonic()
        if remaining <= 0:
            self._drop_key(key)
            return -2

        return max(0, math.ceil(remaining))

    @_synchronized
    def _purge_expired(self) -> None:
        """Helper to evict all expired keys."""
        now = time.monotonic()
        expired_keys = [k for k, exp in self._expires.items() if now >= exp]
        for k in expired_keys:
            self._drop_key(k)

    @_synchronized
    def mset(self, pairs: List[Tuple[str, str]]) -> bool:
        """
        Set multiple key-value pairs in a single call.

        Semantics mirror Redis MSET: all pairs are applied unconditionally.
        Any existing expiration on an overwritten key is discarded.

        Args:
            pairs: A list of (key, value) tuples to store.

        Returns:
            Always True — MSET never fails once arguments are validated.
        """
        for key, value in pairs:
            self._drop_key(key)
            self._data[key] = str(value)
        return True

    @_synchronized
    def mget(self, keys: List[str]) -> List[Optional[str]]:
        """
        Return the values for all specified keys, in request order.

        Expired or missing keys contribute None in their position.
        None is serialised by the server as a RESP2 Null Bulk String ($-1\\r\\n).

        Args:
            keys: A list of key names to look up.

        Returns:
            A list where each element is the stored string value, or None.
        """
        values: List[Optional[str]] = []
        for key in keys:
            if self.type_of(key) not in (None, "string"):
                values.append(None)
            else:
                values.append(self.get(key))
        return values

    @_synchronized
    def keys(self) -> List[str]:
        """
        Return a list of all non-expired key names.

        Performs an eager purge of expired keys before listing so that the
        returned names are guaranteed to be live at the moment of the call.

        Returns:
            A list of key names in lexicographic order for deterministic output.
        """
        self._purge_expired()
        return sorted(set(self._data) | set(self._lists) | set(self._sets) | set(self._hashes))

    @_synchronized
    def size(self) -> int:
        """Return the total number of unexpired keys currently stored."""
        self._purge_expired()
        return len(self._data) + len(self._lists) + len(self._sets) + len(self._hashes)

    @_synchronized
    def clear(self) -> None:
        """Remove all keys and values from the store."""
        self._data.clear()
        self._lists.clear()
        self._sets.clear()
        self._hashes.clear()
        self._expires.clear()

    # List operations
    @_synchronized
    def list_push(self, key: str, values: List[str], left: bool) -> int:
        items = self._collection(key, "list", create=True)
        if left:
            items[0:0] = list(reversed(values))
        else:
            items.extend(values)
        return len(items)

    @_synchronized
    def list_pop(self, key: str, left: bool) -> Optional[str]:
        items = self._collection(key, "list")
        if not items:
            return None
        value = items.pop(0 if left else -1)
        if not items:
            self._drop_key(key)
        return value

    @_synchronized
    def list_range(self, key: str, start: int, stop: int) -> List[str]:
        items = self._collection(key, "list")
        if not items:
            return []
        length = len(items)
        first = start if start >= 0 else length + start
        last = stop if stop >= 0 else length + stop
        first = max(first, 0)
        last = min(last, length - 1)
        if first > last or first >= length or last < 0:
            return []
        return list(items[first:last + 1])

    @_synchronized
    def list_length(self, key: str) -> int:
        items = self._collection(key, "list")
        return len(items) if items is not None else 0

    # Set operations
    @_synchronized
    def set_add(self, key: str, members: List[str]) -> int:
        values = self._collection(key, "set", create=True)
        before = len(values)
        values.update(members)
        return len(values) - before

    @_synchronized
    def set_remove(self, key: str, members: List[str]) -> int:
        values = self._collection(key, "set")
        if values is None:
            return 0
        before = len(values)
        values.difference_update(members)
        removed = before - len(values)
        if not values:
            self._drop_key(key)
        return removed

    @_synchronized
    def set_members(self, key: str) -> List[str]:
        values = self._collection(key, "set")
        return sorted(values) if values is not None else []

    @_synchronized
    def set_contains(self, key: str, member: str) -> bool:
        values = self._collection(key, "set")
        return member in values if values is not None else False

    @_synchronized
    def set_cardinality(self, key: str) -> int:
        values = self._collection(key, "set")
        return len(values) if values is not None else 0

    # Hash operations
    @_synchronized
    def hash_set(self, key: str, pairs: List[Tuple[str, str]]) -> int:
        fields = self._collection(key, "hash", create=True)
        added = 0
        for field, value in pairs:
            if field not in fields:
                added += 1
            fields[field] = value
        return added

    @_synchronized
    def hash_get(self, key: str, field: str) -> Optional[str]:
        fields = self._collection(key, "hash")
        return fields.get(field) if fields is not None else None

    @_synchronized
    def hash_getall(self, key: str) -> List[str]:
        fields = self._collection(key, "hash")
        if fields is None:
            return []
        return [item for field in sorted(fields) for item in (field, fields[field])]

    @_synchronized
    def hash_delete(self, key: str, fields_to_delete: List[str]) -> int:
        fields = self._collection(key, "hash")
        if fields is None:
            return 0
        removed = 0
        for field in fields_to_delete:
            if field in fields:
                del fields[field]
                removed += 1
        if not fields:
            self._drop_key(key)
        return removed

    @_synchronized
    def hash_length(self, key: str) -> int:
        fields = self._collection(key, "hash")
        return len(fields) if fields is not None else 0
