from __future__ import annotations

import random
from typing import Optional

from .registry import Registry, Instance, InstanceStatus


class QueryService:
    def __init__(self, registry: Registry):
        self.registry = registry

    async def get_service(self, service_name: str) -> list[Instance]:
        instances = await self.registry.get_service_instances(service_name)
        return [i for i in instances if i.status == InstanceStatus.UP]

    async def get_all_services(self) -> list[str]:
        return await self.registry.get_all_services()

    async def get_instance(self, service_name: str, instance_id: str) -> Optional[Instance]:
        instance = await self.registry.get_instance(service_name, instance_id)
        if instance and instance.status == InstanceStatus.UP:
            return instance
        return None

    async def get_instances_by_region(
        self, service_name: str, region: str
    ) -> list[Instance]:
        instances = await self.get_service(service_name)
        return [i for i in instances if i.region == region]

    async def get_instances_by_zone(
        self, service_name: str, zone: str
    ) -> list[Instance]:
        instances = await self.get_service(service_name)
        return [i for i in instances if i.zone == zone]

    async def get_healthy_one(self, service_name: str) -> Optional[Instance]:
        instances = await self.get_service(service_name)
        if not instances:
            return None
        return random.choice(instances)

    async def get_instances_by_metadata(
        self, service_name: str, key: str, value: str
    ) -> list[Instance]:
        instances = await self.get_service(service_name)
        return [i for i in instances if i.metadata.get(key) == value]
