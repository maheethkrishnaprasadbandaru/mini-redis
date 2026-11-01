"""
Command handler module for Mini Redis.

This module provides the CommandHandler class which parses user commands,
validates syntax/arguments, and dispatches operations to the underlying
MiniRedisStore.
"""

import shlex
from typing import List, Optional, Union
from miniredis.store import MiniRedisStore, WrongTypeError


_WRONGTYPE = "ERR WRONGTYPE Operation against a key holding the wrong kind of value"
CommandResult = Union[str, List[Optional[str]], None]


class CommandHandler:
    """
    Interprets and executes Mini Redis commands.

    Translates textual commands (such as SET, GET, DEL, EXISTS) into
    operations on an underlying MiniRedisStore and returns formatted
    responses or error messages.
    """

    def __init__(self, store: MiniRedisStore) -> None:
        """
        Initialize the CommandHandler with a storage engine instance.

        Args:
            store: An instance of MiniRedisStore.
        """
        self.store = store

    def execute(self, command_input: Union[str, List[str]]) -> CommandResult:
        """Execute a command, mapping concurrent type races to WRONGTYPE."""
        try:
            return self._execute(command_input)
        except WrongTypeError:
            return _WRONGTYPE

    def _execute(self, command_input: Union[str, List[str]]) -> CommandResult:
        """
        Parse and execute a command string or list of tokens.

        Args:
            command_input: A raw command string (e.g. 'SET name Maheeth')
                           or a list of string tokens (e.g. ['SET', 'name', 'Maheeth']).

        Returns:
            A string response: result value, 'OK', '(nil)', '1', '0',
            or an error string starting with 'ERR'.
            For MGET and KEYS, returns a list of strings/None instead of a
            plain string so the server can encode it as a RESP2 Array.
        """
        if isinstance(command_input, str):
            trimmed = command_input.strip()
            if not trimmed:
                return "ERR empty command"
            # Extract the command name first to determine how to parse arguments
            parts = trimmed.split(maxsplit=1)
            cmd_name = parts[0].upper()
            raw_args = parts[1] if len(parts) > 1 else ""
        elif isinstance(command_input, list):
            if not command_input:
                return "ERR empty command"
            cmd_name = str(command_input[0]).strip().upper()
            if not cmd_name:
                return "ERR empty command"
            raw_args = command_input[1:]
        else:
            return "ERR invalid command format"

        # Dispatch based on command name
        if cmd_name == "SET":
            return self._handle_set(raw_args)
        elif cmd_name == "GET":
            return self._handle_get(raw_args)
        elif cmd_name == "DEL":
            return self._handle_del(raw_args)
        elif cmd_name == "EXISTS":
            return self._handle_exists(raw_args)
        elif cmd_name == "PING":
            return self._handle_ping(raw_args)
        elif cmd_name == "EXPIRE":
            return self._handle_expire(raw_args)
        elif cmd_name == "TTL":
            return self._handle_ttl(raw_args)
        elif cmd_name == "MSET":
            return self._handle_mset(raw_args)
        elif cmd_name == "MGET":
            return self._handle_mget(raw_args)
        elif cmd_name == "DBSIZE":
            return self._handle_dbsize(raw_args)
        elif cmd_name == "KEYS":
            return self._handle_keys(raw_args)
        elif cmd_name in ("LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN"):
            return self._handle_list_command(cmd_name, raw_args)
        elif cmd_name in ("SADD", "SREM", "SMEMBERS", "SISMEMBER", "SCARD"):
            return self._handle_set_command(cmd_name, raw_args)
        elif cmd_name in ("HSET", "HGET", "HGETALL", "HDEL", "HLEN"):
            return self._handle_hash_command(cmd_name, raw_args)
        else:
            return f"ERR unknown command '{parts[0] if isinstance(command_input, str) else command_input[0]}'"

    def _handle_set(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the SET command.

        Syntax: SET key value [EX seconds]
        Supports values containing spaces (e.g., SET message hello world).
        """
        if isinstance(raw_args, str):
            trimmed = raw_args.strip()
            if not trimmed:
                return "ERR wrong number of arguments for 'set' command"
            tokens = trimmed.split()
            if len(tokens) < 2:
                return "ERR wrong number of arguments for 'set' command"

            # Check for trailing EX option
            if len(tokens) >= 2 and tokens[-1].upper() == "EX":
                return "ERR syntax error"
            elif len(tokens) >= 3 and tokens[1].upper() == "EX":
                return "ERR syntax error"
            elif len(tokens) >= 4 and tokens[-2].upper() == "EX":
                try:
                    ex_val = int(tokens[-1])
                except ValueError:
                    return "ERR value is not an integer or out of range"
                if ex_val <= 0:
                    return "ERR invalid expire time in 'set' command"
                key = tokens[0]
                value = " ".join(tokens[1:-2])
                self.store.set(key, value, ex=ex_val)
                return "OK"
            else:
                pair = trimmed.split(maxsplit=1)
                key, value = pair[0], pair[1]
                self.store.set(key, value)
                return "OK"
        else:
            # List of args
            if len(raw_args) < 2:
                return "ERR wrong number of arguments for 'set' command"

            if len(raw_args) >= 2 and str(raw_args[-1]).upper() == "EX":
                return "ERR syntax error"
            elif len(raw_args) >= 3 and str(raw_args[1]).upper() == "EX":
                return "ERR syntax error"
            elif len(raw_args) >= 4 and str(raw_args[-2]).upper() == "EX":
                try:
                    ex_val = int(raw_args[-1])
                except ValueError:
                    return "ERR value is not an integer or out of range"
                if ex_val <= 0:
                    return "ERR invalid expire time in 'set' command"
                key = str(raw_args[0])
                val_items = raw_args[1:-2]
                if len(val_items) == 1:
                    value = str(val_items[0])
                else:
                    value = " ".join(str(item) for item in val_items)
                self.store.set(key, value, ex=ex_val)
                return "OK"
            else:
                key = str(raw_args[0])
                if len(raw_args) == 2:
                    value = str(raw_args[1])
                else:
                    value = " ".join(str(item) for item in raw_args[1:])
                self.store.set(key, value)
                return "OK"

    def _handle_get(self, raw_args: Union[str, List[str]]) -> Union[str, None]:
        """
        Execute the GET command.

        Syntax: GET key
        """
        if isinstance(raw_args, str):
            tokens = raw_args.split()
        else:
            tokens = raw_args

        if len(tokens) != 1:
            return "ERR wrong number of arguments for 'get' command"

        key = str(tokens[0])
        try:
            val = self.store.get(key)
        except WrongTypeError:
            return _WRONGTYPE
        if val is None:
            return "(nil)"
        return val

    def _handle_del(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the DEL command.

        Syntax: DEL key
        """
        if isinstance(raw_args, str):
            tokens = raw_args.split()
        else:
            tokens = raw_args

        if len(tokens) != 1:
            return "ERR wrong number of arguments for 'del' command"

        key = str(tokens[0])
        deleted = self.store.delete(key)
        return "1" if deleted else "0"

    def _handle_exists(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the EXISTS command.

        Syntax: EXISTS key
        """
        if isinstance(raw_args, str):
            tokens = raw_args.split()
        else:
            tokens = raw_args

        if len(tokens) != 1:
            return "ERR wrong number of arguments for 'exists' command"

        key = str(tokens[0])
        present = self.store.exists(key)
        return "1" if present else "0"

    def _handle_ping(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the PING command.

        Syntax: PING [message]
        Returns 'PONG' if no argument is provided, or echoes the message argument.
        """
        if isinstance(raw_args, str):
            trimmed = raw_args.strip()
            if not trimmed:
                return "PONG"
            tokens = trimmed.split()
            if len(tokens) > 1:
                return "ERR wrong number of arguments for 'ping' command"
            return tokens[0]
        else:
            if len(raw_args) == 0:
                return "PONG"
            elif len(raw_args) == 1:
                return str(raw_args[0])
            else:
                return "ERR wrong number of arguments for 'ping' command"

    def _handle_expire(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the EXPIRE command.

        Syntax: EXPIRE key seconds
        """
        if isinstance(raw_args, str):
            tokens = raw_args.strip().split()
        else:
            tokens = raw_args

        if len(tokens) != 2:
            return "ERR wrong number of arguments for 'expire' command"

        key = str(tokens[0])
        try:
            seconds = int(tokens[1])
        except ValueError:
            return "ERR value is not an integer or out of range"

        res = self.store.expire(key, seconds)
        return "1" if res else "0"

    def _handle_ttl(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the TTL command.

        Syntax: TTL key
        """
        if isinstance(raw_args, str):
            tokens = raw_args.strip().split()
        else:
            tokens = raw_args

        if len(tokens) != 1:
            return "ERR wrong number of arguments for 'ttl' command"

        key = str(tokens[0])
        ttl_val = self.store.ttl(key)
        return str(ttl_val)

    # ------------------------------------------------------------------
    # V7 Multi-key and statistics handlers
    # ------------------------------------------------------------------

    def _handle_mset(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the MSET command.

        Syntax: MSET key value [key value ...]

        Requires at least one key-value pair and an even number of arguments.
        All pairs are written atomically (no partial writes on validation error).
        In inline text commands, quote values containing spaces, for example
        MSET greeting "hello world" city "New York". RESP array arguments
        already preserve spaces and do not need quoting. SET keeps its existing
        inline convention, where everything after the key is the value.
        """
        if isinstance(raw_args, str):
            try:
                tokens: List[str] = shlex.split(raw_args)
            except ValueError:
                return "ERR syntax error in 'mset' command"
        else:
            tokens = [str(a) for a in raw_args]

        if len(tokens) < 2:
            return "ERR wrong number of arguments for 'mset' command"
        if len(tokens) % 2 != 0:
            return "ERR wrong number of arguments for 'mset' command"

        pairs = [(tokens[i], tokens[i + 1]) for i in range(0, len(tokens), 2)]
        self.store.mset(pairs)
        return "OK"

    def _handle_mget(self, raw_args: Union[str, List[str]]) -> Union[str, List[Optional[str]]]:
        """
        Execute the MGET command.

        Syntax: MGET key [key ...]

        Returns a list where each element is the value for the corresponding key,
        or None if the key is missing or expired.  The server encodes None entries
        as RESP2 Null Bulk Strings ($-1\\r\\n).
        """
        if isinstance(raw_args, str):
            tokens: List[str] = raw_args.split()
        else:
            tokens = [str(a) for a in raw_args]

        if len(tokens) < 1:
            return "ERR wrong number of arguments for 'mget' command"

        return self.store.mget(tokens)

    def _handle_dbsize(self, raw_args: Union[str, List[str]]) -> str:
        """
        Execute the DBSIZE command.

        Syntax: DBSIZE

        Returns the number of non-expired keys currently in the store.
        Expired keys are eagerly purged before counting.
        """
        if isinstance(raw_args, str):
            tokens: List[str] = raw_args.split() if raw_args.strip() else []
        else:
            tokens = list(raw_args)

        if len(tokens) != 0:
            return "ERR wrong number of arguments for 'dbsize' command"

        return str(self.store.size())

    def _handle_keys(self, raw_args: Union[str, List[str]]) -> Union[str, List[Optional[str]]]:
        """
        Execute the KEYS command.

        Syntax: KEYS pattern

        Only the '*' pattern (match all keys) is supported in this milestone.
        Any other pattern is rejected with a clear error message.
        Returns a list of all non-expired key names as a RESP2 Array.
        """
        if isinstance(raw_args, str):
            tokens: List[str] = raw_args.split() if raw_args.strip() else []
        else:
            tokens = [str(a) for a in raw_args]

        if len(tokens) != 1:
            return "ERR wrong number of arguments for 'keys' command"

        pattern = tokens[0]
        if pattern != "*":
            return (
                f"ERR unsupported pattern '{pattern}': "
                "only '*' (match all) is supported in this version"
            )

        return self.store.keys()

    @staticmethod
    def _collection_tokens(raw_args: Union[str, List[str]]) -> List[str]:
        if isinstance(raw_args, str):
            return shlex.split(raw_args)
        return [str(arg) for arg in raw_args]

    def _handle_list_command(self, command: str, raw_args: Union[str, List[str]]):
        try:
            args = self._collection_tokens(raw_args)
        except ValueError:
            return "ERR syntax error"

        minimum = 2 if command in ("LPUSH", "RPUSH") else 3 if command == "LRANGE" else 1
        maximum = 0 if command in ("LPUSH", "RPUSH") else 3 if command == "LRANGE" else 1
        if len(args) < minimum or (maximum and len(args) > maximum):
            return f"ERR wrong number of arguments for '{command.lower()}' command"
        key = args[0]
        if command in ("LPUSH", "RPUSH"):
            expected = self._check_type(key, "list")
            if expected:
                return expected
            return str(self.store.list_push(key, args[1:], left=command == "LPUSH"))
        expected = self._check_type(key, "list")
        if expected:
            return expected
        if command in ("LPOP", "RPOP"):
            try:
                return self.store.list_pop(key, left=command == "LPOP")
            except WrongTypeError:
                return _WRONGTYPE
        if command == "LLEN":
            return str(self.store.list_length(key))
        try:
            start, stop = int(args[1]), int(args[2])
        except ValueError:
            return "ERR value is not an integer or out of range"
        return self.store.list_range(key, start, stop)

    def _handle_set_command(self, command: str, raw_args: Union[str, List[str]]):
        try:
            args = self._collection_tokens(raw_args)
        except ValueError:
            return "ERR syntax error"
        minimum = 2 if command in ("SISMEMBER", "SADD", "SREM") else 1
        maximum = 0 if command in ("SADD", "SREM") else 2 if command == "SISMEMBER" else 1
        if len(args) < minimum or (maximum and len(args) > maximum):
            return f"ERR wrong number of arguments for '{command.lower()}' command"
        key = args[0]
        expected = self._check_type(key, "set")
        if expected:
            return expected
        if command == "SADD":
            return str(self.store.set_add(key, args[1:]))
        if command == "SREM":
            return str(self.store.set_remove(key, args[1:]))
        if command == "SMEMBERS":
            return self.store.set_members(key)
        if command == "SISMEMBER":
            return "1" if self.store.set_contains(key, args[1]) else "0"
        return str(self.store.set_cardinality(key))

    def _handle_hash_command(self, command: str, raw_args: Union[str, List[str]]):
        try:
            args = self._collection_tokens(raw_args)
        except ValueError:
            return "ERR syntax error"
        minimum = 3 if command == "HSET" else 2 if command in ("HGET", "HDEL") else 1
        maximum = 0 if command in ("HSET", "HDEL") else 2 if command == "HGET" else 1
        if len(args) < minimum or (maximum and len(args) > maximum):
            return f"ERR wrong number of arguments for '{command.lower()}' command"
        if command == "HSET" and len(args[1:]) % 2:
            return "ERR wrong number of arguments for 'hset' command"
        key = args[0]
        expected = self._check_type(key, "hash")
        if expected:
            return expected
        if command == "HSET":
            pairs = [(args[i], args[i + 1]) for i in range(1, len(args), 2)]
            return str(self.store.hash_set(key, pairs))
        if command == "HGET":
            return self.store.hash_get(key, args[1])
        if command == "HGETALL":
            return self.store.hash_getall(key)
        if command == "HDEL":
            return str(self.store.hash_delete(key, args[1:]))
        return str(self.store.hash_length(key))

    def _check_type(self, key: str, expected: str) -> Optional[str]:
        current = self.store.type_of(key)
        if current is not None and current != expected:
            return _WRONGTYPE
        return None
