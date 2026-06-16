from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable, Awaitable

from .registry import Instance

logger = logging.getLogger(__name__)


class ChangeType(str, Enum):
    REGISTER = "REGISTER"
    DEREGISTER = "DEREGISTER"
    STATUS_CHANGE = "STATUS_CHANGE"


@dataclass
class ChangeEvent:
    change_type: ChangeType
    service_name: str
    instance: Instance
    timestamp: float = field(default_factory=lambda: asyncio.get_event_loop().time())

    def to_dict(self) -> dict:
        return {
            "change_type": self.change_type.value,
            "service_name": self.service_name,
            "instance": self.instance.to_dict(),
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ChangeEvent:
        data = dict(data)
        data["change_type"] = ChangeType(data["change_type"])
        data["instance"] = Instance.from_dict(data["instance"])
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


SubscriberCallback = Callable[[ChangeEvent], Awaitable[None]]


@dataclass
class Subscriber:
    subscriber_id: str
    service_name: str
    callback: Optional[SubscriberCallback] = None
    queue: Optional[asyncio.Queue] = None
    filters: set[str] = field(default_factory=set)

    def __post_init__(self):
        if self.queue is None:
            self.queue = asyncio.Queue(maxsize=1000)


class Watcher:
    def __init__(self, max_queue_size: int = 1000):
        self.max_queue_size = max_queue_size
        self._subscribers: dict[str, dict[str, Subscriber]] = {}
        self._all_subscribers: dict[str, Subscriber] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self,
        service_name: str,
        subscriber_id: Optional[str] = None,
        callback: Optional[SubscriberCallback] = None,
        filters: Optional[set[str]] = None,
    ) -> Subscriber:
        async with self._lock:
            if subscriber_id is None:
                import uuid
                subscriber_id = uuid.uuid4().hex[:12]

            subscriber = Subscriber(
                subscriber_id=subscriber_id,
                service_name=service_name,
                callback=callback,
                filters=filters or set(),
            )

            if service_name not in self._subscribers:
                self._subscribers[service_name] = {}
            self._subscribers[service_name][subscriber_id] = subscriber
            self._all_subscribers[subscriber_id] = subscriber

        return subscriber

    async def subscribe_all(
        self,
        subscriber_id: Optional[str] = None,
        callback: Optional[SubscriberCallback] = None,
    ) -> Subscriber:
        return await self.subscribe("__all__", subscriber_id, callback)

    async def unsubscribe(self, subscriber_id: str) -> bool:
        async with self._lock:
            subscriber = self._all_subscribers.pop(subscriber_id, None)
            if not subscriber:
                return False
            svc_subs = self._subscribers.get(subscriber.service_name, {})
            svc_subs.pop(subscriber_id, None)
            if not svc_subs and subscriber.service_name in self._subscribers:
                del self._subscribers[subscriber.service_name]
        return True

    async def notify(self, service_name: str, instance: Instance, action: str):
        try:
            change_type = ChangeType(action)
        except ValueError:
            return

        event = ChangeEvent(
            change_type=change_type,
            service_name=service_name,
            instance=instance,
        )

        targets = []

        svc_subs = self._subscribers.get(service_name, {})
        targets.extend(svc_subs.values())

        all_subs = self._subscribers.get("__all__", {})
        targets.extend(all_subs.values())

        for subscriber in targets:
            try:
                if subscriber.callback:
                    await subscriber.callback(event)
                if subscriber.queue:
                    await subscriber.queue.put(event)
            except asyncio.QueueFull:
                logger.warning(
                    "Subscriber %s queue full, dropping event", subscriber.subscriber_id
                )
            except Exception as e:
                logger.error(
                    "Error notifying subscriber %s: %s", subscriber.subscriber_id, e
                )

    async def get_events(self, subscriber_id: str, timeout: float = 5.0) -> list[dict]:
        subscriber = self._all_subscribers.get(subscriber_id)
        if not subscriber or not subscriber.queue:
            return []

        events = []
        try:
            event = await asyncio.wait_for(subscriber.queue.get(), timeout=timeout)
            events.append(event.to_dict())
            while not subscriber.queue.empty():
                event = subscriber.queue.get_nowait()
                events.append(event.to_dict())
        except asyncio.TimeoutError:
            pass
        return events

    def get_subscriber_count(self, service_name: Optional[str] = None) -> int:
        if service_name:
            return len(self._subscribers.get(service_name, {}))
        return len(self._all_subscribers)

    async def list_subscribers(self, service_name: str) -> list[str]:
        async with self._lock:
            subs = self._subscribers.get(service_name, {})
            return list(subs.keys())
