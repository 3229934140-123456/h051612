from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Awaitable, Optional


class InstanceStatus(str, Enum):
    UP = "UP"
    DOWN = "DOWN"
    STARTING = "STARTING"
    OUT_OF_SERVICE = "OUT_OF_SERVICE"


@dataclass
class Instance:
    service_name: str
    host: str
    port: int
    instance_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    status: InstanceStatus = InstanceStatus.UP
    metadata: dict = field(default_factory=dict)
    health_check_url: Optional[str] = None
    registration_time: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    last_dirty_time: float = field(default_factory=time.time)
    version: int = 1
    region: str = "default"
    zone: str = "default"

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def to_dict(self) -> dict:
        return {
            "instance_id": self.instance_id,
            "service_name": self.service_name,
            "host": self.host,
            "port": self.port,
            "status": self.status.value,
            "metadata": self.metadata,
            "health_check_url": self.health_check_url,
            "registration_time": self.registration_time,
            "last_heartbeat": self.last_heartbeat,
            "version": self.version,
            "region": self.region,
            "zone": self.zone,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Instance:
        data = dict(data)
        data["status"] = InstanceStatus(data.get("status", "UP"))
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


OnChangeCallback = Callable[[str, Instance, str], Awaitable[None]]


class Registry:
    def __init__(
        self,
        heartbeat_interval: float = 10.0,
        heartbeat_timeout: float = 30.0,
        eviction_interval: float = 10.0,
        eviction_threshold: float = 0.85,
        tombstone_ttl: float = 3600.0,
    ):
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.eviction_interval = eviction_interval
        self.eviction_threshold = eviction_threshold
        self.tombstone_ttl = tombstone_ttl

        self._instances: dict[str, dict[str, Instance]] = {}
        self._tombstones: dict[str, tuple[int, float]] = {}
        self._lock = asyncio.Lock()
        self._change_callbacks: list[OnChangeCallback] = []
        self._eviction_task: Optional[asyncio.Task] = None
        self._tombstone_task: Optional[asyncio.Task] = None
        self._should_evict = True

        self._total_heartbeat_count = 0
        self._expected_heartbeat_count = 0
        self._protection_enabled = False

    def on_change(self, callback: OnChangeCallback):
        self._change_callbacks.append(callback)

    async def _notify_change(self, action: str, instance: Instance):
        for cb in self._change_callbacks:
            try:
                await cb(instance.service_name, instance, action)
            except Exception:
                pass

    async def register(self, instance: Instance) -> Instance:
        async with self._lock:
            svc = instance.service_name
            if svc not in self._instances:
                self._instances[svc] = {}

            existing = self._instances[svc].get(instance.instance_id)
            tombstone = self._tombstones.get(instance.instance_id)

            if existing:
                if instance.version <= existing.version:
                    instance.version = existing.version + 1
                instance.registration_time = existing.registration_time
            elif tombstone:
                if instance.version <= tombstone[0]:
                    instance.version = tombstone[0] + 1
                del self._tombstones[instance.instance_id]
                self._expected_heartbeat_count += 1
            else:
                self._expected_heartbeat_count += 1

            instance.last_heartbeat = time.time()
            instance.last_dirty_time = time.time()
            self._instances[svc][instance.instance_id] = instance

        await self._notify_change("REGISTER", instance)
        return instance

    async def deregister(self, service_name: str, instance_id: str) -> bool:
        async with self._lock:
            svc_map = self._instances.get(service_name)
            if not svc_map or instance_id not in svc_map:
                existing_tombstone = self._tombstones.get(instance_id)
                if existing_tombstone:
                    new_version = existing_tombstone[0] + 1
                    self._tombstones[instance_id] = (new_version, time.time())
                return False
            instance = svc_map.pop(instance_id)
            tombstone_version = instance.version + 1
            instance.version = tombstone_version
            self._tombstones[instance_id] = (tombstone_version, time.time())
            self._expected_heartbeat_count = max(0, self._expected_heartbeat_count - 1)
            if not svc_map:
                del self._instances[service_name]

        await self._notify_change("DEREGISTER", instance)
        return True

    async def renew(self, service_name: str, instance_id: str) -> bool:
        async with self._lock:
            svc_map = self._instances.get(service_name)
            if not svc_map or instance_id not in svc_map:
                return False
            instance = svc_map[instance_id]
            instance.last_heartbeat = time.time()
            instance.last_dirty_time = time.time()
            self._total_heartbeat_count += 1

        return True

    async def get_instance(self, service_name: str, instance_id: str) -> Optional[Instance]:
        async with self._lock:
            svc_map = self._instances.get(service_name)
            if not svc_map:
                return None
            return svc_map.get(instance_id)

    async def get_service_instances(self, service_name: str) -> list[Instance]:
        async with self._lock:
            svc_map = self._instances.get(service_name, {})
            return list(svc_map.values())

    async def get_all_instances(self) -> dict[str, list[Instance]]:
        async with self._lock:
            return {svc: list(inst_map.values()) for svc, inst_map in self._instances.items()}

    async def get_all_services(self) -> list[str]:
        async with self._lock:
            return list(self._instances.keys())

    async def update_status(self, service_name: str, instance_id: str, status: InstanceStatus) -> bool:
        async with self._lock:
            svc_map = self._instances.get(service_name)
            if not svc_map or instance_id not in svc_map:
                return False
            instance = svc_map[instance_id]
            instance.status = status
            instance.last_dirty_time = time.time()
            instance.version += 1

        await self._notify_change("STATUS_CHANGE", instance)
        return True

    def get_heartbeat_stats(self) -> dict:
        return {
            "total_heartbeats": self._total_heartbeat_count,
            "expected_heartbeats": self._expected_heartbeat_count,
            "protection_enabled": self._protection_enabled,
            "registered_instances": sum(len(v) for v in self._instances.values()),
        }

    def set_protection_mode(self, enabled: bool):
        self._protection_enabled = enabled

    async def _evict_expired_instances(self):
        if self._protection_enabled:
            return

        now = time.time()
        expired: list[tuple[str, str]] = []

        async with self._lock:
            for svc_name, svc_map in self._instances.items():
                for inst_id, inst in list(svc_map.items()):
                    if now - inst.last_heartbeat > self.heartbeat_timeout:
                        if inst.status != InstanceStatus.OUT_OF_SERVICE:
                            expired.append((svc_name, inst_id))

        for svc_name, inst_id in expired:
            await self.deregister(svc_name, inst_id)

    async def _eviction_loop(self):
        while self._should_evict:
            try:
                await self._evict_expired_instances()
            except Exception:
                pass
            await asyncio.sleep(self.eviction_interval)

    async def start_eviction(self):
        if self._eviction_task is None:
            self._should_evict = True
            self._eviction_task = asyncio.create_task(self._eviction_loop())

    async def stop_eviction(self):
        self._should_evict = False
        if self._eviction_task:
            self._eviction_task.cancel()
            try:
                await self._eviction_task
            except asyncio.CancelledError:
                pass
            self._eviction_task = None

    async def apply_delta(self, instances: list[dict], action: str = "REGISTER"):
        async with self._lock:
            for inst_data in instances:
                inst = Instance.from_dict(inst_data)
                inst_id = inst.instance_id
                svc_name = inst.service_name

                if action == "REGISTER":
                    tombstone = self._tombstones.get(inst_id)
                    if tombstone and tombstone[0] >= inst.version:
                        continue

                    existing = self._instances.get(svc_name, {}).get(inst_id)
                    if existing and existing.version >= inst.version:
                        continue

                    if svc_name not in self._instances:
                        self._instances[svc_name] = {}
                    self._instances[svc_name][inst_id] = inst

                    if tombstone:
                        del self._tombstones[inst_id]

                elif action == "DEREGISTER":
                    tombstone = self._tombstones.get(inst_id)
                    if tombstone and tombstone[0] >= inst.version:
                        continue

                    svc_map = self._instances.get(svc_name)
                    if svc_map and inst_id in svc_map:
                        del svc_map[inst_id]
                        if not svc_map:
                            del self._instances[svc_name]

                    self._tombstones[inst_id] = (inst.version, time.time())

    def get_tombstones(self) -> dict[str, int]:
        return {iid: ver for iid, (ver, _) in self._tombstones.items()}

    async def _cleanup_tombstones(self):
        now = time.time()
        async with self._lock:
            expired = [
                iid for iid, (_, ts) in self._tombstones.items()
                if now - ts > self.tombstone_ttl
            ]
            for iid in expired:
                del self._tombstones[iid]

    async def _tombstone_cleanup_loop(self):
        while self._should_evict:
            try:
                await self._cleanup_tombstones()
            except Exception:
                pass
            await asyncio.sleep(max(self.tombstone_ttl / 4, 60.0))

    async def start_eviction(self):
        if self._eviction_task is None:
            self._should_evict = True
            self._eviction_task = asyncio.create_task(self._eviction_loop())
            self._tombstone_task = asyncio.create_task(self._tombstone_cleanup_loop())

    async def stop_eviction(self):
        self._should_evict = False
        for task_attr in ["_eviction_task", "_tombstone_task"]:
            task = getattr(self, task_attr, None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, task_attr, None)

    def get_snapshot(self) -> dict:
        result = {}
        for svc_name, svc_map in self._instances.items():
            result[svc_name] = [inst.to_dict() for inst in svc_map.values()]
        return result

    def get_full_state(self) -> dict:
        return {
            "snapshot": self.get_snapshot(),
            "tombstones": {iid: {"version": ver, "timestamp": ts} for iid, (ver, ts) in self._tombstones.items()},
        }
