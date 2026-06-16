from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from .registry import Registry

logger = logging.getLogger(__name__)


class SelfProtection:
    def __init__(
        self,
        registry: Registry,
        expected_heartbeat_rate: float = 0.85,
        expected_heartbeat_window: float = 60.0,
        check_interval: float = 15.0,
        enable: bool = True,
    ):
        self.registry = registry
        self.expected_heartbeat_rate = expected_heartbeat_rate
        self.expected_heartbeat_window = expected_heartbeat_window
        self.check_interval = check_interval
        self.enable = enable

        self._window_start: float = time.time()
        self._window_heartbeat_count: int = 0
        self._last_expected_count: int = 0
        self._is_protecting: bool = False
        self._task: Optional[asyncio.Task] = None
        self._running = False

    @property
    def is_protecting(self) -> bool:
        return self._is_protecting

    def get_stats(self) -> dict:
        return {
            "protection_enabled": self._is_protecting,
            "window_heartbeats": self._window_heartbeat_count,
            "expected_threshold": self._last_expected_count,
            "expected_rate": self.expected_heartbeat_rate,
        }

    def record_heartbeat(self):
        self._window_heartbeat_count += 1

    async def _check_protection(self):
        if not self.enable:
            return

        now = time.time()
        elapsed = now - self._window_start

        stats = self.registry.get_heartbeat_stats()
        expected_total = stats["expected_heartbeats"]
        registered = stats["registered_instances"]

        if elapsed >= self.expected_heartbeat_window:
            expected_in_window = expected_total * self.expected_heartbeat_rate * (
                elapsed / self.registry.heartbeat_interval
            )
            self._last_expected_count = int(expected_in_window)

            actual_rate = 0.0
            if expected_in_window > 0:
                actual_rate = self._window_heartbeat_count / expected_in_window

            should_protect = self._window_heartbeat_count < expected_in_window and registered > 0

            if should_protect and not self._is_protecting:
                self._is_protecting = True
                self.registry.set_protection_mode(True)
                logger.warning(
                    "Self-protection ACTIVATED: received %d heartbeats, expected %d. "
                    "Network partition may be occurring, evictions suspended.",
                    self._window_heartbeat_count,
                    int(expected_in_window),
                )
            elif not should_protect and self._is_protecting:
                self._is_protecting = False
                self.registry.set_protection_mode(False)
                logger.info(
                    "Self-protection DEACTIVATED: heartbeat rate normalized. "
                    "Evictions resumed."
                )

            self._window_start = now
            self._window_heartbeat_count = 0

    async def _protection_loop(self):
        while self._running:
            try:
                await self._check_protection()
            except Exception as e:
                logger.error("Self-protection check error: %s", e)
            await asyncio.sleep(self.check_interval)

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._protection_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
