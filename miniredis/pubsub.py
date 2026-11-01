"""Small thread-safe channel registry used by the TCP server."""

import queue
import threading
from dataclasses import dataclass, field
from typing import Dict, Iterable, Set, Tuple


@dataclass(eq=False)
class PubSubClient:
    """A connection's subscriptions and pending RESP2 message frames."""
    messages: "queue.Queue[Tuple[str, str]]" = field(default_factory=queue.Queue)
    channels: Set[str] = field(default_factory=set)


class PubSubManager:
    """Thread-safe channel subscription registry."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._channels: Dict[str, Set[PubSubClient]] = {}

    def subscribe(self, client: PubSubClient, channels: Iterable[str]) -> int:
        with self._lock:
            for channel in channels:
                self._channels.setdefault(channel, set()).add(client)
                client.channels.add(channel)
            return len(client.channels)

    def unsubscribe(self, client: PubSubClient, channels: Iterable[str] = ()) -> Tuple[int, Tuple[str, ...]]:
        with self._lock:
            selected = tuple(channels)
            if not selected:
                selected = tuple(sorted(client.channels))
            for channel in selected:
                subscribers = self._channels.get(channel)
                if subscribers is not None:
                    subscribers.discard(client)
                    if not subscribers:
                        del self._channels[channel]
                client.channels.discard(channel)
            return len(client.channels), selected

    def publish(self, channel: str, message: str) -> int:
        with self._lock:
            subscribers = tuple(self._channels.get(channel, ()))
        for client in subscribers:
            client.messages.put((channel, message))
        return len(subscribers)

    def cleanup(self, client: PubSubClient) -> None:
        self.unsubscribe(client)

    @property
    def channel_count(self) -> int:
        with self._lock:
            return len(self._channels)
