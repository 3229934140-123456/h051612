from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional
from urllib.parse import parse_qs, urlparse

from .registry import Registry, Instance, InstanceStatus
from .health import HealthChecker
from .query import QueryService
from .watcher import Watcher
from .gossip import GossipProtocol
from .protection import SelfProtection

logger = logging.getLogger(__name__)


class DiscoveryServer:
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8761,
        gossip_port: int = 7946,
        heartbeat_interval: float = 10.0,
        heartbeat_timeout: float = 30.0,
        enable_gossip: bool = False,
        enable_protection: bool = True,
    ):
        self.host = host
        self.port = port

        self.registry = Registry(
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
        )
        self.health_checker = HealthChecker(self.registry)
        self.query_service = QueryService(self.registry)
        self.watcher = Watcher()
        self.protection = SelfProtection(self.registry, enable=enable_protection)
        self.gossip: Optional[GossipProtocol] = None
        self._enable_gossip = enable_gossip

        if enable_gossip:
            self.gossip = GossipProtocol(
                registry=self.registry,
                self_host=host,
                self_port=port,
                protocol_port=gossip_port,
                watcher=self.watcher,
            )

        self.registry.on_change(self._on_instance_change)
        self._server: Optional[asyncio.AbstractServer] = None

    async def _on_instance_change(self, service_name: str, instance: Instance, action: str):
        await self.watcher.notify(service_name, instance, action)
        if self.gossip:
            self.gossip.propagate_instance_change(instance, action)

    async def start(self):
        await self.registry.start_eviction()
        await self.health_checker.start()
        await self.protection.start()

        if self.gossip:
            await self.gossip.start()

        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )

        logger.info("Discovery server started on %s:%d", self.host, self.port)

    async def stop(self):
        await self.registry.stop_eviction()
        await self.health_checker.stop()
        await self.protection.stop()

        if self.gossip:
            await self.gossip.stop()

        if self._server:
            self._server.close()
            await self._server.wait_closed()

        logger.info("Discovery server stopped")

    async def serve_forever(self):
        await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await asyncio.wait_for(reader.read(65536), timeout=30.0)
            request_text = data.decode("utf-8", errors="replace")

            if not request_text:
                writer.close()
                await writer.wait_closed()
                return

            lines = request_text.split("\r\n")
            if not lines:
                writer.close()
                await writer.wait_closed()
                return

            request_line = lines[0]
            parts = request_line.split(" ")
            if len(parts) < 2:
                writer.close()
                await writer.wait_closed()
                return

            method = parts[0]
            raw_path = parts[1]

            parsed = urlparse(raw_path)
            path = parsed.path
            query_params = parse_qs(parsed.query)

            body = ""
            body_start = request_text.find("\r\n\r\n")
            if body_start != -1:
                body = request_text[body_start + 4:]

            status, headers, response_body = await self._route(method, path, query_params, body)

            response = f"HTTP/1.1 {status}\r\n"
            for key, value in headers.items():
                response += f"{key}: {value}\r\n"
            response += f"Content-Length: {len(response_body.encode())}\r\n"
            response += "\r\n"
            response += response_body

            writer.write(response.encode())
            await writer.drain()

        except Exception as e:
            logger.error("Connection handler error: %s", e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _route(self, method: str, path: str, query: dict, body: str) -> tuple:
        routes = {
            ("POST", "/register"): self._handle_register,
            ("DELETE", "/deregister"): self._handle_deregister,
            ("PUT", "/renew"): self._handle_renew,
            ("GET", "/instances"): self._handle_get_instances,
            ("GET", "/instances/query"): self._handle_query_instances,
            ("GET", "/services"): self._handle_get_services,
            ("GET", "/health"): self._handle_health_check,
            ("POST", "/subscribe"): self._handle_subscribe,
            ("DELETE", "/unsubscribe"): self._handle_unsubscribe,
            ("GET", "/events"): self._handle_get_events,
            ("GET", "/status"): self._handle_status,
            ("PUT", "/instance/status"): self._handle_update_status,
            ("POST", "/gossip/join"): self._handle_gossip_join,
            ("GET", "/gossip/members"): self._handle_gossip_members,
            ("GET", "/protection"): self._handle_protection_status,
            ("GET", "/cluster/status"): self._handle_cluster_status,
        }

        handler = routes.get((method, path))
        if handler:
            return await handler(query, body)

        return (404, {"Content-Type": "application/json"}, json.dumps({"error": "Not found"}))

    async def _handle_register(self, query: dict, body: str) -> tuple:
        try:
            data = json.loads(body)
            instance = Instance.from_dict(data)
            result = await self.registry.register(instance)
            return (200, {"Content-Type": "application/json"}, json.dumps(result.to_dict()))
        except Exception as e:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_deregister(self, query: dict, body: str) -> tuple:
        try:
            data = json.loads(body)
            service_name = data["service_name"]
            instance_id = data["instance_id"]
            success = await self.registry.deregister(service_name, instance_id)
            if success:
                return (200, {"Content-Type": "application/json"}, json.dumps({"status": "ok"}))
            return (404, {"Content-Type": "application/json"}, json.dumps({"error": "Instance not found"}))
        except Exception as e:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_renew(self, query: dict, body: str) -> tuple:
        try:
            data = json.loads(body)
            service_name = data["service_name"]
            instance_id = data["instance_id"]
            success = await self.registry.renew(service_name, instance_id)
            if success:
                self.protection.record_heartbeat()
                return (200, {"Content-Type": "application/json"}, json.dumps({"status": "ok"}))
            return (404, {"Content-Type": "application/json"}, json.dumps({"error": "Instance not found"}))
        except Exception as e:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_get_instances(self, query: dict, body: str) -> tuple:
        service_name = query.get("service_name", [None])[0]
        if not service_name:
            all_instances = await self.registry.get_all_instances()
            result = {svc: [i.to_dict() for i in insts] for svc, insts in all_instances.items()}
            return (200, {"Content-Type": "application/json"}, json.dumps(result))

        instances = await self.query_service.get_service(service_name)
        result = [i.to_dict() for i in instances]
        return (200, {"Content-Type": "application/json"}, json.dumps(result))

    async def _handle_get_services(self, query: dict, body: str) -> tuple:
        services = await self.query_service.get_all_services()
        return (200, {"Content-Type": "application/json"}, json.dumps(services))

    async def _handle_health_check(self, query: dict, body: str) -> tuple:
        return (200, {"Content-Type": "application/json"}, json.dumps({"status": "UP"}))

    async def _handle_subscribe(self, query: dict, body: str) -> tuple:
        try:
            data = json.loads(body) if body else {}
            service_name = data.get("service_name", "__all__")
            subscriber_id = data.get("subscriber_id")
            subscriber = await self.watcher.subscribe(service_name, subscriber_id)
            return (200, {"Content-Type": "application/json"}, json.dumps({
                "subscriber_id": subscriber.subscriber_id,
                "service_name": subscriber.service_name,
            }))
        except Exception as e:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_unsubscribe(self, query: dict, body: str) -> tuple:
        try:
            data = json.loads(body)
            subscriber_id = data["subscriber_id"]
            success = await self.watcher.unsubscribe(subscriber_id)
            if success:
                return (200, {"Content-Type": "application/json"}, json.dumps({"status": "ok"}))
            return (404, {"Content-Type": "application/json"}, json.dumps({"error": "Subscriber not found"}))
        except Exception as e:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_get_events(self, query: dict, body: str) -> tuple:
        subscriber_id = query.get("subscriber_id", [None])[0]
        if not subscriber_id:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": "subscriber_id required"}))
        timeout = float(query.get("timeout", [5.0])[0])
        events = await self.watcher.get_events(subscriber_id, timeout=timeout)
        return (200, {"Content-Type": "application/json"}, json.dumps(events))

    async def _handle_status(self, query: dict, body: str) -> tuple:
        stats = self.registry.get_heartbeat_stats()
        stats["protection"] = self.protection.get_stats()
        stats["subscribers"] = self.watcher.get_subscriber_count()
        if self.gossip:
            all_instances = await self.registry.get_all_instances()
            alive_members = self.gossip.get_alive_members()
            stats["alive_members"] = len(alive_members)
            stats["all_members"] = len(self.gossip.get_all_members())
            stats["total_services"] = len(all_instances)
            stats["total_instances"] = sum(len(v) for v in all_instances.values())
            stats["member_id"] = self.gossip.member_id
            stats["last_sync_times"] = self.gossip._last_sync_time
            stats["sync_error_count"] = self.gossip._sync_error_count
            stats["tombstones_count"] = len(self.registry.get_tombstones())
        return (200, {"Content-Type": "application/json"}, json.dumps(stats))

    async def _handle_update_status(self, query: dict, body: str) -> tuple:
        try:
            data = json.loads(body)
            service_name = data["service_name"]
            instance_id = data["instance_id"]
            status = InstanceStatus(data["status"])
            success = await self.registry.update_status(service_name, instance_id, status)
            if success:
                return (200, {"Content-Type": "application/json"}, json.dumps({"status": "ok"}))
            return (404, {"Content-Type": "application/json"}, json.dumps({"error": "Instance not found"}))
        except Exception as e:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_gossip_join(self, query: dict, body: str) -> tuple:
        if not self.gossip:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": "Gossip not enabled"}))
        try:
            data = json.loads(body)
            host = data["host"]
            port = int(data["port"])
            success = await self.gossip.join(host, port)
            if success:
                return (200, {"Content-Type": "application/json"}, json.dumps({"status": "ok"}))
            return (500, {"Content-Type": "application/json"}, json.dumps({"error": "Failed to join"}))
        except Exception as e:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_gossip_members(self, query: dict, body: str) -> tuple:
        if not self.gossip:
            return (400, {"Content-Type": "application/json"}, json.dumps({"error": "Gossip not enabled"}))
        members = self.gossip.get_all_members()
        result = [m.to_dict() for m in members]
        return (200, {"Content-Type": "application/json"}, json.dumps(result))

    async def _handle_protection_status(self, query: dict, body: str) -> tuple:
        stats = self.protection.get_stats()
        return (200, {"Content-Type": "application/json"}, json.dumps(stats))

    async def _handle_query_instances(self, query: dict, body: str) -> tuple:
        try:
            service_name = query.get("service_name", [None])[0]
            if not service_name:
                return (400, {"Content-Type": "application/json"},
                        json.dumps({"error": "service_name required"}))

            instances = await self.query_service.get_service(service_name)

            region = query.get("region", [None])[0]
            if region:
                instances = [i for i in instances if i.region == region]

            zone = query.get("zone", [None])[0]
            if zone:
                instances = [i for i in instances if i.zone == zone]

            status = query.get("status", [None])[0]
            if status:
                instances = [i for i in instances if i.status.value == status]

            metadata_keys = [k for k in query.keys() if k.startswith("meta_")]
            for meta_key in metadata_keys:
                meta_name = meta_key[len("meta_"):]
                meta_value = query[meta_key][0]
                instances = [i for i in instances if i.metadata.get(meta_name) == meta_value]

            instances.sort(key=lambda i: (i.host, i.port, i.instance_id))

            result = [i.to_dict() for i in instances]
            return (200, {"Content-Type": "application/json"}, json.dumps(result))
        except Exception as e:
            return (500, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _handle_cluster_status(self, query: dict, body: str) -> tuple:
        try:
            local_stats = await self.gossip.get_cluster_stats() if self.gossip else {}

            members_stats = []
            if self.gossip:
                for member in self.gossip.get_alive_members():
                    if member.member_id == self.gossip.member_id:
                        members_stats.append(local_stats)
                        continue
                    try:
                        remote_stats = await self._fetch_remote_status(member)
                        if remote_stats:
                            members_stats.append(remote_stats)
                    except Exception:
                        members_stats.append({
                            "member_id": member.member_id,
                            "host": member.host,
                            "http_port": member.http_port,
                            "gossip_port": member.port,
                            "state": "UNREACHABLE",
                            "error": "Failed to fetch status",
                        })

            has_local = any(s.get("member_id") == self.gossip.member_id for s in members_stats) if self.gossip else False
            if self.gossip and not has_local:
                members_stats.insert(0, local_stats)

            total_services_list = [s.get("total_services", 0) for s in members_stats]
            total_instances_list = [s.get("total_instances", 0) for s in members_stats]

            consistent = (
                len(set(total_services_list)) <= 1
                and len(set(total_instances_list)) <= 1
                and len(members_stats) == (await self.gossip.get_cluster_stats())["alive_members"]
            ) if self.gossip else True

            result = {
                "consistent": consistent,
                "total_members": len(members_stats),
                "members": members_stats,
            }

            return (200, {"Content-Type": "application/json"}, json.dumps(result))
        except Exception as e:
            return (500, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}))

    async def _fetch_remote_status(self, member) -> Optional[dict]:
        try:
            http_port = member.http_port if hasattr(member, 'http_port') and member.http_port else member.port
            reader, writer = await asyncio.open_connection(
                member.host, http_port, limit=65536
            )
            request = (
                f"GET /status HTTP/1.1\r\n"
                f"Host: {member.host}:{http_port}\r\n"
                f"Connection: close\r\n\r\n"
            )
            writer.write(request.encode())
            await writer.drain()

            response = await reader.read(65536)
            writer.close()
            await writer.wait_closed()

            response_text = response.decode("utf-8", errors="replace")
            body_start = response_text.find("\r\n\r\n")
            if body_start == -1:
                return None
            body = response_text[body_start + 4:]
            data = json.loads(body)
            data["member_id"] = member.member_id
            data["host"] = member.host
            data["http_port"] = http_port
            data["gossip_port"] = member.port
            return data
        except Exception as e:
            logger.error("Failed to fetch status from %s:%s: %s", member.host, member.port, e)
            return None
