from __future__ import annotations

import asyncio
import logging
import urllib.request
import urllib.error
from enum import Enum
from typing import Optional, Callable, Awaitable

from .registry import Registry, Instance, InstanceStatus

logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    UNKNOWN = "UNKNOWN"


HealthCheckCallback = Callable[[Instance], Awaitable[HealthStatus]]


class HealthChecker:
    def __init__(
        self,
        registry: Registry,
        check_interval: float = 15.0,
        timeout: float = 5.0,
        unhealthy_threshold: int = 3,
        healthy_threshold: int = 2,
    ):
        self.registry = registry
        self.check_interval = check_interval
        self.timeout = timeout
        self.unhealthy_threshold = unhealthy_threshold
        self.healthy_threshold = healthy_threshold

        self._health_status: dict[str, HealthStatus] = {}
        self._consecutive_failures: dict[str, int] = {}
        self._consecutive_successes: dict[str, int] = {}
        self._custom_checkers: dict[str, HealthCheckCallback] = {}
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def register_custom_checker(self, service_name: str, checker: HealthCheckCallback):
        self._custom_checkers[service_name] = checker

    def get_health(self, instance_id: str) -> HealthStatus:
        return self._health_status.get(instance_id, HealthStatus.UNKNOWN)

    async def check_instance(self, instance: Instance) -> HealthStatus:
        custom = self._custom_checkers.get(instance.service_name)
        if custom:
            try:
                return await asyncio.wait_for(custom(instance), timeout=self.timeout)
            except Exception:
                return HealthStatus.UNHEALTHY

        if not instance.health_check_url:
            return HealthStatus.UNKNOWN

        try:
            req = urllib.request.Request(
                instance.health_check_url, method="GET"
            )
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, timeout=self.timeout),
            )
            if 200 <= response.status < 300:
                return HealthStatus.HEALTHY
            return HealthStatus.UNHEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    async def _process_instance(self, instance: Instance):
        status = await self.check_instance(instance)
        inst_id = instance.instance_id

        if status == HealthStatus.HEALTHY:
            self._consecutive_failures[inst_id] = 0
            self._consecutive_successes[inst_id] = (
                self._consecutive_successes.get(inst_id, 0) + 1
            )
            if (
                self._consecutive_successes[inst_id] >= self.healthy_threshold
                and instance.status != InstanceStatus.UP
            ):
                await self.registry.update_status(
                    instance.service_name, inst_id, InstanceStatus.UP
                )
        elif status == HealthStatus.UNHEALTHY:
            self._consecutive_successes[inst_id] = 0
            self._consecutive_failures[inst_id] = (
                self._consecutive_failures.get(inst_id, 0) + 1
            )
            if self._consecutive_failures[inst_id] >= self.unhealthy_threshold:
                await self.registry.update_status(
                    instance.service_name, inst_id, InstanceStatus.OUT_OF_SERVICE
                )
                self._consecutive_failures[inst_id] = 0

        self._health_status[inst_id] = status

    async def _check_all(self):
        all_instances = await self.registry.get_all_instances()
        tasks = []
        for svc_name, instances in all_instances.items():
            for instance in instances:
                if instance.health_check_url or svc_name in self._custom_checkers:
                    tasks.append(self._process_instance(instance))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_loop(self):
        while self._running:
            try:
                await self._check_all()
            except Exception as e:
                logger.error("Health check loop error: %s", e)
            await asyncio.sleep(self.check_interval)

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._check_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
