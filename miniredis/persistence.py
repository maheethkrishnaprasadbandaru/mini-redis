"""
Persistence module for Mini Redis (V6).

Provides save() and load() functions that serialize and deserialize the
store's in-memory state (key-value pairs + TTL deadlines) to/from a JSON
file on disk.

Design decisions:
- JSON format: human-readable, easy to inspect and debug.
- Atomic writes: data is written to a temp file first, then os.replace()
  swaps it in — guaranteeing the on-disk file is always either the old
  complete snapshot or the new complete snapshot, never a partial write.
- Monotonic-to-wall-clock conversion: time.monotonic() deadlines are
  converted to UNIX wall-clock timestamps (time.time()) before saving,
  and converted back after loading, so TTLs survive process restarts.
- Safety: a missing file is treated as an empty store (not an error).
  A malformed/corrupt file is logged as a warning and treated as an
  empty store; the corrupt file is left in place (not overwritten).
"""

import json
import math
import os
import time
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from miniredis.store import MiniRedisStore

# Persistence format version. Increment if the schema changes.
_FORMAT_VERSION = 2


def save(store: "MiniRedisStore", filepath: str) -> None:
    """
    Serialize the store's current state to a JSON file atomically.

    Expired keys are purged before saving so the file stays clean.
    Each key's remaining TTL is converted to a UNIX wall-clock timestamp
    so it survives a process restart (monotonic time cannot be persisted).

    Args:
        store:    The MiniRedisStore instance to serialize.
        filepath: Path to the target JSON file (will be created if absent).

    Raises:
        OSError: If the directory is not writable.
    """
    with store._lock:
        # Evict any keys that have already expired before we snapshot
        store._purge_expired()

        now_monotonic = time.monotonic()
        now_wall = time.time()

        typed_values = {key: ("string", value) for key, value in store._data.items()}
        typed_values.update({key: ("list", value) for key, value in store._lists.items()})
        typed_values.update({key: ("set", sorted(value)) for key, value in store._sets.items()})
        typed_values.update({key: ("hash", value) for key, value in store._hashes.items()})

        entries: Dict[str, Any] = {}
        for key, (kind, value) in typed_values.items():
            if key in store._expires:
                # Convert monotonic deadline → remaining seconds → UNIX timestamp
                remaining = store._expires[key] - now_monotonic
                if remaining <= 0:
                    # Key expired between _purge_expired() and now; skip it
                    continue
                unix_expiry: Optional[float] = now_wall + remaining
            else:
                unix_expiry = None  # No expiration

            entry = {
                "value": value,
                "expires_at": unix_expiry,  # null → no TTL; float → UNIX timestamp
            }
            if kind != "string":
                entry["type"] = kind
            entries[key] = entry

        payload = {
            "version": _FORMAT_VERSION,
            "saved_at": now_wall,
            "entries": entries,
        }

        # Atomic write: write to .tmp file then replace
        tmp_path = filepath + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            os.replace(tmp_path, filepath)
        except Exception:
            # Clean up the temp file if replace failed
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise


def load(filepath: str) -> Dict[str, Any]:
    """
    Deserialize a previously saved JSON snapshot.

    Returns a dict with two keys:
        "data"    : Dict[str, str]   — legacy string keys.
        "lists"   : Dict[str, List[str]], "sets": Dict[str, set[str]],
        "hashes"  : Dict[str, Dict[str, str]] — V8 collection values.
        "expires" : Dict[str, float] — key → monotonic deadline for keys with TTL.

    Handles these edge cases safely:
        - File does not exist       → returns empty dicts (not an error).
        - File is empty or corrupt  → logs a warning, returns empty dicts.
        - Keys already expired      → filtered out (not restored).

    Args:
        filepath: Path to the JSON file written by save().

    Returns:
        A dict {"data": {...}, "expires": {...}}.
    """
    empty = {"data": {}, "expires": {}, "lists": {}, "sets": {}, "hashes": {}}

    if not os.path.exists(filepath):
        return empty

    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        warnings.warn(
            f"[persistence] Failed to load '{filepath}': {exc}. "
            "Starting with an empty store. The file has NOT been deleted.",
            stacklevel=2,
        )
        return empty

    # Basic schema validation
    if not isinstance(payload, dict) or "entries" not in payload:
        warnings.warn(
            f"[persistence] '{filepath}' has an unrecognised format. "
            "Starting with an empty store.",
            stacklevel=2,
        )
        return empty

    entries = payload.get("entries", {})
    if not isinstance(entries, dict):
        warnings.warn(
            f"[persistence] 'entries' field in '{filepath}' is not a dict. "
            "Starting with an empty store.",
            stacklevel=2,
        )
        return empty

    now_wall = time.time()
    now_monotonic = time.monotonic()

    data: Dict[str, str] = {}
    lists: Dict[str, List[str]] = {}
    sets: Dict[str, set[str]] = {}
    hashes: Dict[str, Dict[str, str]] = {}
    expires: Dict[str, float] = {}

    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue

        kind = entry.get("type", "string")
        value = entry.get("value")
        if kind == "string" and isinstance(value, str):
            parsed_value = value
        elif kind == "list" and value and isinstance(value, list) and all(isinstance(item, str) for item in value):
            parsed_value = value
        elif kind == "set" and value and isinstance(value, list) and all(isinstance(item, str) for item in value):
            parsed_value = set(value)
        elif kind == "hash" and value and isinstance(value, dict) and all(
            isinstance(field, str) and isinstance(item, str) for field, item in value.items()
        ):
            parsed_value = value
        else:
            continue

        unix_expiry = entry.get("expires_at")  # None or float

        if unix_expiry is not None:
            # Check if the key has already expired
            remaining = unix_expiry - now_wall
            if remaining <= 0:
                # Expired while the server was down — skip it
                continue
            # Convert remaining wall-clock seconds back to a monotonic deadline
            expires[key] = now_monotonic + remaining

        if kind == "string":
            data[key] = parsed_value
        elif kind == "list":
            lists[key] = parsed_value
        elif kind == "set":
            sets[key] = parsed_value
        else:
            hashes[key] = parsed_value

    return {"data": data, "expires": expires, "lists": lists, "sets": sets, "hashes": hashes}
