from __future__ import annotations

import asyncio
import logging
import unittest
import time

from sdiscovery.registry import Registry, Instance, InstanceStatus
from sdiscovery.gossip import GossipProtocol

logging.basicConfig(level=logging.ERROR)


class TestMultiReplica(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.registry_a = Registry(
            heartbeat_interval=10.0,
            heartbeat_timeout=30.0,
        )
        self.gossip_a = GossipProtocol(
            registry=self.registry_a,
            self_host="127.0.0.1",
            self_port=8761,
            protocol_port=17001,
            fanout=2,
            probe_interval=10.0,
            probe_timeout=2.0,
            suspect_timeout=60.0,
            sync_interval=0.5,
            gossip_interval=0.2,
        )

        self.registry_b = Registry(
            heartbeat_interval=10.0,
            heartbeat_timeout=30.0,
        )
        self.gossip_b = GossipProtocol(
            registry=self.registry_b,
            self_host="127.0.0.1",
            self_port=8762,
            protocol_port=17002,
            fanout=2,
            probe_interval=10.0,
            probe_timeout=2.0,
            suspect_timeout=60.0,
            sync_interval=0.5,
            gossip_interval=0.2,
        )

        await self.gossip_a.start()
        await self.gossip_b.start()
        await asyncio.sleep(0.1)

    async def asyncTearDown(self):
        await self.gossip_a.stop()
        await self.gossip_b.stop()
        await asyncio.sleep(0.1)

    async def test_01_join_cluster(self):
        success = await self.gossip_b.join("127.0.0.1", 17001)
        self.assertTrue(success, "Node B should successfully join node A")

        members_a = self.gossip_a.get_all_members()
        members_b = self.gossip_b.get_all_members()

        self.assertEqual(len(members_a), 2, "Node A should see 2 members")
        self.assertEqual(len(members_b), 2, "Node B should see 2 members")

        member_ids_a = {m.member_id for m in members_a}
        member_ids_b = {m.member_id for m in members_b}
        self.assertEqual(member_ids_a, member_ids_b, "Both nodes should have same member set")

    async def test_02_registration_propagation(self):
        await self.gossip_b.join("127.0.0.1", 17001)
        await asyncio.sleep(0.2)

        inst = Instance(
            service_name="order-service",
            host="10.0.0.1",
            port=8080,
        )
        await self.registry_a.register(inst)

        await asyncio.sleep(1.5)

        instances_b = await self.registry_b.get_service_instances("order-service")
        self.assertGreaterEqual(len(instances_b), 1, "Node B should see the registered instance")

        found = instances_b[0]
        self.assertEqual(found.instance_id, inst.instance_id)
        self.assertEqual(found.service_name, "order-service")
        self.assertEqual(found.host, "10.0.0.1")
        self.assertEqual(found.port, 8080)
        self.assertEqual(found.status, InstanceStatus.UP)

    async def test_03_status_change_propagation(self):
        await self.gossip_b.join("127.0.0.1", 17001)
        await asyncio.sleep(0.2)

        inst = Instance(
            service_name="payment-service",
            host="10.0.0.2",
            port=9090,
        )
        initial_version = inst.version
        await self.registry_a.register(inst)
        await asyncio.sleep(1.0)

        await self.registry_a.update_status(
            "payment-service", inst.instance_id, InstanceStatus.OUT_OF_SERVICE
        )

        await asyncio.sleep(1.5)

        instances_b = await self.registry_b.get_service_instances("payment-service")
        self.assertGreaterEqual(len(instances_b), 1)

        found = instances_b[0]
        self.assertEqual(found.status, InstanceStatus.OUT_OF_SERVICE,
                         "Status change should propagate to node B")
        self.assertGreater(found.version, initial_version,
                           "Version should be incremented after status change")

    async def test_04_deregistration_propagation(self):
        await self.gossip_b.join("127.0.0.1", 17001)
        await asyncio.sleep(0.2)

        inst = Instance(
            service_name="cart-service",
            host="10.0.0.3",
            port=7070,
        )
        await self.registry_a.register(inst)
        await asyncio.sleep(1.0)

        instances_b_before = await self.registry_b.get_service_instances("cart-service")
        self.assertEqual(len(instances_b_before), 1)

        await self.registry_a.deregister("cart-service", inst.instance_id)

        await asyncio.sleep(1.5)

        instances_b_after = await self.registry_b.get_service_instances("cart-service")
        self.assertEqual(len(instances_b_after), 0,
                         "Deregistration should propagate to node B")

        tombstones_b = self.registry_b.get_tombstones()
        self.assertIn(inst.instance_id, tombstones_b,
                      "Tombstone should exist on node B to prevent resurrection")

    async def test_05_tombstone_prevents_resurrection(self):
        await self.gossip_b.join("127.0.0.1", 17001)
        await asyncio.sleep(0.2)

        inst = Instance(
            service_name="resurrect-test",
            host="10.0.0.4",
            port=6060,
        )
        inst.version = 5
        await self.registry_a.register(inst)
        await asyncio.sleep(1.0)

        await self.registry_a.deregister("resurrect-test", inst.instance_id)
        await asyncio.sleep(1.5)

        instances_b_after_del = await self.registry_b.get_service_instances("resurrect-test")
        self.assertEqual(len(instances_b_after_del), 0,
                         "Instance should be deleted on node B")

        tombstones_b = self.registry_b.get_tombstones()
        self.assertIn(inst.instance_id, tombstones_b,
                      "Tombstone should exist on node B")
        tomb_version = tombstones_b[inst.instance_id]
        self.assertGreater(tomb_version, 5,
                           "Tombstone version should be greater than original version")

        old_instance = Instance(
            service_name="resurrect-test",
            host="10.0.0.4",
            port=6060,
            instance_id=inst.instance_id,
        )
        old_instance.version = 3
        await self.registry_b.apply_delta([old_instance.to_dict()], action="REGISTER")

        instances_b = await self.registry_b.get_service_instances("resurrect-test")
        self.assertEqual(len(instances_b), 0,
                         "Old version instance from gossip should NOT resurrect")

        tombstones_b_after = self.registry_b.get_tombstones()
        self.assertIn(inst.instance_id, tombstones_b_after,
                      "Tombstone should still exist")

        new_instance = Instance(
            service_name="resurrect-test",
            host="10.0.0.4",
            port=6060,
            instance_id=inst.instance_id,
        )
        new_instance.version = tomb_version + 10
        await self.registry_b.apply_delta([new_instance.to_dict()], action="REGISTER")

        instances_b_final = await self.registry_b.get_service_instances("resurrect-test")
        self.assertEqual(len(instances_b_final), 1,
                         "Higher version instance should resurrect successfully")
        self.assertEqual(instances_b_final[0].version, tomb_version + 10)

    async def test_06_bidirectional_sync_convergence(self):
        await self.gossip_b.join("127.0.0.1", 17001)
        await asyncio.sleep(0.3)

        inst_a = Instance(
            service_name="bidir-svc",
            host="10.0.1.1",
            port=8080,
        )
        await self.registry_a.register(inst_a)

        inst_b = Instance(
            service_name="bidir-svc",
            host="10.0.1.2",
            port=8080,
        )
        await self.registry_b.register(inst_b)

        await asyncio.sleep(2.0)

        instances_a = await self.registry_a.get_service_instances("bidir-svc")
        instances_b = await self.registry_b.get_service_instances("bidir-svc")

        self.assertEqual(len(instances_a), 2,
                         "Node A should see both instances after convergence")
        self.assertEqual(len(instances_b), 2,
                         "Node B should see both instances after convergence")

        hosts_a = sorted(i.host for i in instances_a)
        hosts_b = sorted(i.host for i in instances_b)
        self.assertEqual(hosts_a, ["10.0.1.1", "10.0.1.2"])
        self.assertEqual(hosts_b, ["10.0.1.1", "10.0.1.2"])

    async def test_07_join_with_existing_data(self):
        inst1 = Instance(service_name="seed-svc", host="10.0.2.1", port=8080)
        inst2 = Instance(service_name="seed-svc", host="10.0.2.2", port=8080)
        inst3 = Instance(service_name="other-svc", host="10.0.2.3", port=9090)
        await self.registry_a.register(inst1)
        await self.registry_a.register(inst2)
        await self.registry_a.register(inst3)
        await asyncio.sleep(0.1)

        success = await self.gossip_b.join("127.0.0.1", 17001)
        self.assertTrue(success)

        await asyncio.sleep(0.5)

        seed_instances = await self.registry_b.get_service_instances("seed-svc")
        self.assertEqual(len(seed_instances), 2,
                         "New node should get all instances of seed-svc after join")

        other_instances = await self.registry_b.get_service_instances("other-svc")
        self.assertEqual(len(other_instances), 1,
                         "New node should get all instances of other-svc after join")

        services = await self.registry_b.get_all_services()
        self.assertIn("seed-svc", services)
        self.assertIn("other-svc", services)

    async def test_08_multiple_status_updates(self):
        await self.gossip_b.join("127.0.0.1", 17001)
        await asyncio.sleep(0.2)

        inst = Instance(
            service_name="multi-status",
            host="10.0.3.1",
            port=8080,
        )
        await self.registry_a.register(inst)
        await asyncio.sleep(1.0)

        statuses = [InstanceStatus.OUT_OF_SERVICE, InstanceStatus.UP,
                    InstanceStatus.OUT_OF_SERVICE, InstanceStatus.UP]

        for status in statuses:
            await self.registry_a.update_status("multi-status", inst.instance_id, status)
            await asyncio.sleep(0.5)

        await asyncio.sleep(1.5)

        instances_b = await self.registry_b.get_service_instances("multi-status")
        self.assertGreaterEqual(len(instances_b), 1)
        self.assertEqual(instances_b[0].status, InstanceStatus.UP,
                         "Final status should converge to UP")
        self.assertGreaterEqual(instances_b[0].version, 5,
                                "Version should reflect all updates")


if __name__ == "__main__":
    unittest.main()
