from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .registry import Registry, Instance

logger = logging.getLogger(__name__)


class MemberState(str, Enum):
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"
    LEFT = "LEFT"


class MessageType(str, Enum):
    PING = "PING"
    PING_REQ = "PING_REQ"
    ACK = "ACK"
    SYNC = "SYNC"
    COMPOUND = "COMPOUND"


@dataclass
class GossipMember:
    member_id: str
    host: str
    port: int
    state: MemberState = MemberState.ALIVE
    incarnation: int = 0
    last_seen: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def to_dict(self) -> dict:
        return {
            "member_id": self.member_id,
            "host": self.host,
            "port": self.port,
            "state": self.state.value,
            "incarnation": self.incarnation,
            "last_seen": self.last_seen,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> GossipMember:
        data = dict(data)
        data["state"] = MemberState(data.get("state", "ALIVE"))
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class GossipMessage:
    msg_type: MessageType
    sender_id: str
    data: dict = field(default_factory=dict)
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    timestamp: float = field(default_factory=time.time)


class GossipProtocol:
    def __init__(
        self,
        registry: Registry,
        self_host: str,
        self_port: int,
        member_id: Optional[str] = None,
        protocol_port: int = 7946,
        fanout: int = 3,
        probe_interval: float = 5.0,
        probe_timeout: float = 3.0,
        suspect_timeout: float = 15.0,
        sync_interval: float = 10.0,
        gossip_interval: float = 1.0,
    ):
        self.registry = registry
        self.self_host = self_host
        self.self_port = self_port
        self.member_id = member_id or uuid.uuid4().hex[:12]
        self.protocol_port = protocol_port
        self.fanout = fanout
        self.probe_interval = probe_interval
        self.probe_timeout = probe_timeout
        self.suspect_timeout = suspect_timeout
        self.sync_interval = sync_interval
        self.gossip_interval = gossip_interval

        self.self_member = GossipMember(
            member_id=self.member_id,
            host=self_host,
            port=protocol_port,
            state=MemberState.ALIVE,
        )

        self._members: dict[str, GossipMember] = {self.member_id: self.self_member}
        self._pending_acks: dict[str, asyncio.Future] = {}
        self._message_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._transmission_queue: list[GossipMessage] = []

        self._probe_index = 0
        self._probe_task: Optional[asyncio.Task] = None
        self._gossip_task: Optional[asyncio.Task] = None
        self._sync_task: Optional[asyncio.Task] = None
        self._suspect_task: Optional[asyncio.Task] = None
        self._server_task: Optional[asyncio.Task] = None
        self._running = False

        self._on_member_change_callbacks = []
        self._udp_transport = None

    def on_member_change(self, callback):
        self._on_member_change_callbacks.append(callback)

    async def _notify_member_change(self, member: GossipMember, change: str):
        for cb in self._on_member_change_callbacks:
            try:
                await cb(member, change)
            except Exception as e:
                logger.error("Member change callback error: %s", e)

    def get_alive_members(self) -> list[GossipMember]:
        return [m for m in self._members.values() if m.state == MemberState.ALIVE]

    def get_all_members(self) -> list[GossipMember]:
        return list(self._members.values())

    async def join(self, host: str, port: int) -> bool:
        try:
            sync_data = await self._send_sync(host, port)
            if sync_data:
                logger.info("Join successful, sync_data keys: %s", list(sync_data.keys()))
                member_id = sync_data.get("sender_id", "")
                member = GossipMember(
                    member_id=member_id,
                    host=host,
                    port=port,
                    state=MemberState.ALIVE,
                )
                if member_id and member_id != self.member_id:
                    self._members[member.member_id] = member
                await self._notify_member_change(member, "JOIN")
                logger.info("Joined cluster via %s:%d, members: %d", host, port, len(self._members))
                return True
        except Exception as e:
            logger.error("Failed to join %s:%d: %s", host, port, e)
        return False

    async def leave(self):
        self.self_member.state = MemberState.LEFT
        self.self_member.incarnation += 1
        msg = GossipMessage(
            msg_type=MessageType.COMPOUND,
            sender_id=self.member_id,
            data={"member_update": self.self_member.to_dict(), "action": "LEAVE"},
        )
        self._transmission_queue.append(msg)
        await self._broadcast_gossip()
        await self.stop()

    async def _send_sync(self, host: str, port: int) -> Optional[dict]:
        full_state = self.registry.get_full_state()
        msg = GossipMessage(
            msg_type=MessageType.SYNC,
            sender_id=self.member_id,
            data={
                "snapshot": full_state["snapshot"],
                "tombstones": full_state.get("tombstones", {}),
                "members": {mid: m.to_dict() for mid, m in self._members.items()},
            },
        )
        try:
            response = await self._send_and_receive(host, port, msg)
            if response:
                await self._apply_sync(response)
            return response
        except Exception:
            return None

    async def _apply_sync(self, sync_data: dict):
        snapshot = sync_data.get("snapshot", {})
        for svc_name, instances_data in snapshot.items():
            await self.registry.apply_delta(instances_data, action="REGISTER")

        tombstones_data = sync_data.get("tombstones", {})
        if tombstones_data:
            await self._apply_tombstones(tombstones_data, snapshot)

        members_data = sync_data.get("members", {})
        for mid, mdata in members_data.items():
            if mid == self.member_id:
                continue
            existing = self._members.get(mid)
            new_member = GossipMember.from_dict(mdata)
            if not existing or existing.incarnation < new_member.incarnation:
                self._members[mid] = new_member

    async def _apply_tombstones(self, tombstones: dict, snapshot: dict):
        snapshot_instance_ids = set()
        for svc_instances in snapshot.values():
            for inst_data in svc_instances:
                snapshot_instance_ids.add(inst_data.get("instance_id", ""))

        for iid, tomb_data in tombstones.items():
            version = tomb_data.get("version", 0) if isinstance(tomb_data, dict) else tomb_data
            if iid in snapshot_instance_ids:
                continue

            async with self.registry._lock:
                existing_tomb = self.registry._tombstones.get(iid)
                if existing_tomb and existing_tomb[0] >= version:
                    continue

                found = None
                found_svc = None
                for svc_name, svc_map in self.registry._instances.items():
                    if iid in svc_map:
                        found = svc_map[iid]
                        found_svc = svc_name
                        break

                if found and found.version < version:
                    del self.registry._instances[found_svc][iid]
                    if not self.registry._instances[found_svc]:
                        del self.registry._instances[found_svc]

                if not existing_tomb or existing_tomb[0] < version:
                    self.registry._tombstones[iid] = (version, time.time())

    async def _send_and_receive(
        self, host: str, port: int, msg: GossipMessage, timeout: float = 5.0
    ) -> Optional[dict]:
        if self._udp_transport is None:
            return None

        future = asyncio.get_event_loop().create_future()
        self._pending_acks[msg.msg_id] = future

        try:
            payload = json.dumps({
                "msg_type": msg.msg_type.value,
                "sender_id": msg.sender_id,
                "data": msg.data,
                "msg_id": msg.msg_id,
                "timestamp": msg.timestamp,
            }).encode()

            dest = (host, port)
            self._udp_transport.sendto(payload, dest)

            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending_acks.pop(msg.msg_id, None)

    async def _probe_loop(self):
        while self._running:
            try:
                alive = self.get_alive_members()
                others = [m for m in alive if m.member_id != self.member_id]
                if others:
                    target = others[self._probe_index % len(others)]
                    self._probe_index += 1

                    success = await self._probe_member(target)
                    if not success:
                        target.state = MemberState.SUSPECT
                        target.last_seen = time.time()
                        await self._notify_member_change(target, "SUSPECT")
                        await self._indirect_probe(target, others)
            except Exception as e:
                logger.error("Probe loop error: %s", e)

            await asyncio.sleep(self.probe_interval)

    async def _probe_member(self, member: GossipMember) -> bool:
        msg = GossipMessage(
            msg_type=MessageType.PING,
            sender_id=self.member_id,
        )
        result = await self._send_and_receive(
            member.host, member.port, msg, timeout=self.probe_timeout
        )
        return result is not None

    async def _indirect_probe(
        self, target: GossipMember, others: list[GossipMember]
    ) -> bool:
        proxies = [m for m in others if m.member_id != target.member_id]
        if not proxies:
            return False
        proxies = random.sample(proxies, min(self.fanout, len(proxies)))

        for proxy in proxies:
            msg = GossipMessage(
                msg_type=MessageType.PING_REQ,
                sender_id=self.member_id,
                data={"target_id": target.member_id, "target_host": target.host, "target_port": target.port},
            )
            result = await self._send_and_receive(
                proxy.host, proxy.port, msg, timeout=self.probe_timeout * 2
            )
            if result:
                target.state = MemberState.ALIVE
                target.last_seen = time.time()
                await self._notify_member_change(target, "ALIVE")
                return True
        return False

    async def _suspect_timeout_loop(self):
        while self._running:
            try:
                now = time.time()
                for mid, member in list(self._members.items()):
                    if mid == self.member_id:
                        continue
                    if member.state == MemberState.SUSPECT:
                        if now - member.last_seen > self.suspect_timeout:
                            member.state = MemberState.DEAD
                            await self._notify_member_change(member, "DEAD")
                            logger.warning("Member %s declared DEAD", mid)
                    elif member.state == MemberState.DEAD:
                        if now - member.last_seen > self.suspect_timeout * 3:
                            del self._members[mid]
            except Exception as e:
                logger.error("Suspect timeout loop error: %s", e)

            await asyncio.sleep(self.probe_interval)

    async def _gossip_loop(self):
        while self._running:
            try:
                await self._broadcast_gossip()
            except Exception as e:
                logger.error("Gossip loop error: %s", e)
            await asyncio.sleep(self.gossip_interval)

    async def _broadcast_gossip(self):
        if not self._transmission_queue:
            return

        alive = self.get_alive_members()
        others = [m for m in alive if m.member_id != self.member_id]
        if not others:
            return

        targets = random.sample(others, min(self.fanout, len(others)))
        messages = self._transmission_queue[:]
        self._transmission_queue.clear()

        for target in targets:
            for msg in messages:
                try:
                    if self._udp_transport:
                        payload = json.dumps({
                            "msg_type": msg.msg_type.value,
                            "sender_id": msg.sender_id,
                            "data": msg.data,
                            "msg_id": msg.msg_id,
                            "timestamp": msg.timestamp,
                        }).encode()
                        self._udp_transport.sendto(payload, (target.host, target.port))
                except Exception as e:
                    logger.error("Gossip send error to %s: %s", target.address, e)

    async def _sync_loop(self):
        while self._running:
            try:
                alive = self.get_alive_members()
                others = [m for m in alive if m.member_id != self.member_id]
                if others:
                    target = random.choice(others)
                    await self._send_sync(target.host, target.port)
            except Exception as e:
                logger.error("Sync loop error: %s", e)
            await asyncio.sleep(self.sync_interval)

    async def _handle_datagram(self, data: bytes, addr: tuple):
        try:
            msg_data = json.loads(data.decode())
            msg_type = MessageType(msg_data["msg_type"])
            sender_id = msg_data.get("sender_id", "")
            msg_id = msg_data.get("msg_id", "")
            payload = msg_data.get("data", {})

            if sender_id in self._members:
                self._members[sender_id].last_seen = time.time()

            if msg_type == MessageType.PING:
                ack = GossipMessage(
                    msg_type=MessageType.ACK,
                    sender_id=self.member_id,
                    data={"original_msg_id": msg_id},
                    msg_id=uuid.uuid4().hex[:8],
                )
                if self._udp_transport:
                    response = json.dumps({
                        "msg_type": ack.msg_type.value,
                        "sender_id": ack.sender_id,
                        "data": ack.data,
                        "msg_id": ack.msg_id,
                        "timestamp": ack.timestamp,
                    }).encode()
                    self._udp_transport.sendto(response, addr)

            elif msg_type == MessageType.ACK:
                future = self._pending_acks.get(msg_data.get("data", {}).get("original_msg_id"))
                if future and not future.done():
                    future.set_result(payload)

            elif msg_type == MessageType.PING_REQ:
                target_id = payload.get("target_id")
                target_host = payload.get("target_host")
                target_port = payload.get("target_port")
                if target_host and target_port:
                    ping_msg = GossipMessage(
                        msg_type=MessageType.PING,
                        sender_id=self.member_id,
                    )
                    result = await self._send_and_receive(
                        target_host, int(target_port), ping_msg, timeout=self.probe_timeout
                    )
                    ack = GossipMessage(
                        msg_type=MessageType.ACK,
                        sender_id=self.member_id,
                        data={"original_msg_id": msg_id, "reachable": result is not None},
                    )
                    if self._udp_transport:
                        response = json.dumps({
                            "msg_type": ack.msg_type.value,
                            "sender_id": ack.sender_id,
                            "data": ack.data,
                            "msg_id": ack.msg_id,
                            "timestamp": ack.timestamp,
                        }).encode()
                        self._udp_transport.sendto(response, addr)

            elif msg_type == MessageType.SYNC:
                sync_data = payload
                action = sync_data.get("action")

                if action == "DEREGISTER":
                    snapshot = sync_data.get("snapshot", {})
                    for svc_name, instances_data in snapshot.items():
                        await self.registry.apply_delta(instances_data, action="DEREGISTER")
                elif "snapshot" in sync_data:
                    await self._apply_sync(sync_data)

                full_state = self.registry.get_full_state()
                response_data = {
                    "snapshot": full_state["snapshot"],
                    "tombstones": full_state.get("tombstones", {}),
                    "members": {mid: m.to_dict() for mid, m in self._members.items()},
                    "sender_id": self.member_id,
                }

                if self._udp_transport:
                    response = json.dumps({
                        "msg_type": MessageType.ACK.value,
                        "sender_id": self.member_id,
                        "data": {"original_msg_id": msg_id, **response_data},
                        "msg_id": uuid.uuid4().hex[:8],
                        "timestamp": time.time(),
                    }).encode()
                    self._udp_transport.sendto(response, addr)

            elif msg_type == MessageType.COMPOUND:
                action = payload.get("action")
                member_update = payload.get("member_update")
                if member_update:
                    updated = GossipMember.from_dict(member_update)
                    if updated.member_id != self.member_id:
                        existing = self._members.get(updated.member_id)
                        if not existing or existing.incarnation < updated.incarnation:
                            self._members[updated.member_id] = updated
                            await self._notify_member_change(updated, action or "UPDATE")

        except Exception as e:
            logger.error("Handle datagram error from %s: %s", addr, e)

    async def _start_udp_server(self):
        loop = asyncio.get_event_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _GossipProtocolHandler(self),
            local_addr=(self.self_host, self.protocol_port),
        )
        self._udp_transport = transport

    async def start(self):
        self._running = True
        await self._start_udp_server()

        self._probe_task = asyncio.create_task(self._probe_loop())
        self._gossip_task = asyncio.create_task(self._gossip_loop())
        self._sync_task = asyncio.create_task(self._sync_loop())
        self._suspect_task = asyncio.create_task(self._suspect_timeout_loop())

        logger.info("Gossip protocol started on %s:%d", self.self_host, self.protocol_port)

    async def stop(self):
        self._running = False
        for task in [self._probe_task, self._gossip_task, self._sync_task, self._suspect_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._udp_transport:
            self._udp_transport.close()
            self._udp_transport = None

        for future in self._pending_acks.values():
            if not future.done():
                future.cancel()
        self._pending_acks.clear()

    def propagate_instance_change(self, instance: Instance, action: str):
        msg = GossipMessage(
            msg_type=MessageType.SYNC,
            sender_id=self.member_id,
            data={
                "snapshot": {instance.service_name: [instance.to_dict()]},
                "action": action,
            },
        )
        self._transmission_queue.append(msg)


class _GossipProtocolHandler(asyncio.DatagramProtocol):
    def __init__(self, gossip: GossipProtocol):
        self.gossip = gossip

    def connection_made(self, transport):
        pass

    def datagram_received(self, data, addr):
        asyncio.create_task(self.gossip._handle_datagram(data, addr))

    def error_received(self, exc):
        logger.error("UDP error: %s", exc)
