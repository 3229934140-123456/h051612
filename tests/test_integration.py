from __future__ import annotations

import asyncio
import json
import time
import unittest

from sdiscovery.registry import Registry, Instance, InstanceStatus
from sdiscovery.health import HealthChecker, HealthStatus
from sdiscovery.query import QueryService
from sdiscovery.watcher import Watcher, ChangeType, ChangeEvent
from sdiscovery.protection import SelfProtection


class TestRegistry(unittest.TestCase):
    def setUp(self):
        self.registry = Registry(
            heartbeat_interval=1.0,
            heartbeat_timeout=3.0,
            eviction_interval=1.0,
        )

    def test_register_instance(self):
        inst = Instance(service_name="order-service", host="10.0.1.1", port=8080)
        result = asyncio.run(self.registry.register(inst))
        self.assertEqual(result.service_name, "order-service")
        self.assertEqual(result.host, "10.0.1.1")
        self.assertEqual(result.status, InstanceStatus.UP)

    def test_register_and_get(self):
        inst = Instance(service_name="user-service", host="10.0.1.2", port=9090)
        asyncio.run(self.registry.register(inst))
        found = asyncio.run(self.registry.get_instance("user-service", inst.instance_id))
        self.assertIsNotNone(found)
        self.assertEqual(found.instance_id, inst.instance_id)

    def test_deregister_instance(self):
        inst = Instance(service_name="pay-service", host="10.0.1.3", port=7070)
        asyncio.run(self.registry.register(inst))
        success = asyncio.run(self.registry.deregister("pay-service", inst.instance_id))
        self.assertTrue(success)
        found = asyncio.run(self.registry.get_instance("pay-service", inst.instance_id))
        self.assertIsNone(found)

    def test_heartbeat_renew(self):
        inst = Instance(service_name="cart-service", host="10.0.1.4", port=6060)
        asyncio.run(self.registry.register(inst))
        old_hb = inst.last_heartbeat
        time.sleep(0.01)
        success = asyncio.run(self.registry.renew("cart-service", inst.instance_id))
        self.assertTrue(success)
        found = asyncio.run(self.registry.get_instance("cart-service", inst.instance_id))
        self.assertGreater(found.last_heartbeat, old_hb)

    def test_heartbeat_timeout_eviction(self):
        registry = Registry(
            heartbeat_interval=0.5,
            heartbeat_timeout=1.0,
            eviction_interval=0.5,
        )
        inst = Instance(service_name="evict-service", host="10.0.1.5", port=5050)
        inst.last_heartbeat = time.time() - 2.0
        asyncio.run(registry.register(inst))

        async def run_eviction():
            await registry.start_eviction()
            await asyncio.sleep(1.5)
            await registry.stop_eviction()

        asyncio.run(run_eviction())

        found = asyncio.run(registry.get_instance("evict-service", inst.instance_id))
        self.assertIsNone(found)

    def test_get_service_instances(self):
        for i in range(3):
            inst = Instance(service_name="multi-service", host=f"10.0.2.{i}", port=8080 + i)
            asyncio.run(self.registry.register(inst))
        instances = asyncio.run(self.registry.get_service_instances("multi-service"))
        self.assertEqual(len(instances), 3)

    def test_get_all_services(self):
        for svc in ["svc-a", "svc-b", "svc-c"]:
            inst = Instance(service_name=svc, host="10.0.3.1", port=8080)
            asyncio.run(self.registry.register(inst))
        services = asyncio.run(self.registry.get_all_services())
        self.assertEqual(set(services), {"svc-a", "svc-b", "svc-c"})

    def test_update_status(self):
        inst = Instance(service_name="status-service", host="10.0.1.6", port=4040)
        asyncio.run(self.registry.register(inst))
        success = asyncio.run(
            self.registry.update_status("status-service", inst.instance_id, InstanceStatus.OUT_OF_SERVICE)
        )
        self.assertTrue(success)
        found = asyncio.run(self.registry.get_instance("status-service", inst.instance_id))
        self.assertEqual(found.status, InstanceStatus.OUT_OF_SERVICE)

    def test_instance_version_increments(self):
        inst = Instance(service_name="ver-service", host="10.0.1.7", port=3030)
        asyncio.run(self.registry.register(inst))
        self.assertEqual(inst.version, 1)
        asyncio.run(self.registry.register(inst))
        found = asyncio.run(self.registry.get_instance("ver-service", inst.instance_id))
        self.assertEqual(found.version, 2)

    def test_snapshot_and_from_dict(self):
        inst = Instance(service_name="snap-service", host="10.0.1.8", port=2020)
        asyncio.run(self.registry.register(inst))
        snap = self.registry.get_snapshot()
        self.assertIn("snap-service", snap)
        self.assertEqual(len(snap["snap-service"]), 1)
        d = snap["snap-service"][0]
        restored = Instance.from_dict(d)
        self.assertEqual(restored.instance_id, inst.instance_id)
        self.assertEqual(restored.service_name, "snap-service")


class TestHealthChecker(unittest.TestCase):
    def test_custom_checker_healthy(self):
        registry = Registry()
        checker = HealthChecker(registry, check_interval=100.0)

        async def healthy_check(instance):
            return HealthStatus.HEALTHY

        checker.register_custom_checker("test-service", healthy_check)

        inst = Instance(service_name="test-service", host="10.0.4.1", port=8080)
        inst.status = InstanceStatus.OUT_OF_SERVICE
        asyncio.run(registry.register(inst))

        result = asyncio.run(checker.check_instance(inst))
        self.assertEqual(result, HealthStatus.HEALTHY)

    def test_custom_checker_unhealthy(self):
        registry = Registry()
        checker = HealthChecker(registry, check_interval=100.0)

        async def unhealthy_check(instance):
            return HealthStatus.UNHEALTHY

        checker.register_custom_checker("bad-service", unhealthy_check)

        inst = Instance(service_name="bad-service", host="10.0.4.2", port=8080)
        asyncio.run(registry.register(inst))

        result = asyncio.run(checker.check_instance(inst))
        self.assertEqual(result, HealthStatus.UNHEALTHY)

    def test_no_health_check_url(self):
        registry = Registry()
        checker = HealthChecker(registry)
        inst = Instance(service_name="no-url-service", host="10.0.4.3", port=8080)
        result = asyncio.run(checker.check_instance(inst))
        self.assertEqual(result, HealthStatus.UNKNOWN)

    def test_consecutive_failures_mark_out_of_service(self):
        registry = Registry()
        checker = HealthChecker(
            registry, check_interval=100.0, unhealthy_threshold=2, healthy_threshold=2
        )

        call_count = 0

        async def flapping_check(instance):
            nonlocal call_count
            call_count += 1
            return HealthStatus.UNHEALTHY

        checker.register_custom_checker("flap-service", flapping_check)

        inst = Instance(service_name="flap-service", host="10.0.4.4", port=8080)
        asyncio.run(registry.register(inst))

        for _ in range(3):
            asyncio.run(checker._process_instance(inst))

        found = asyncio.run(registry.get_instance("flap-service", inst.instance_id))
        self.assertEqual(found.status, InstanceStatus.OUT_OF_SERVICE)


class TestQueryService(unittest.TestCase):
    def setUp(self):
        self.registry = Registry()
        self.query = QueryService(self.registry)

    def test_get_service_only_up(self):
        up_inst = Instance(service_name="q-service", host="10.0.5.1", port=8080)
        down_inst = Instance(service_name="q-service", host="10.0.5.2", port=8081)
        down_inst.status = InstanceStatus.OUT_OF_SERVICE
        asyncio.run(self.registry.register(up_inst))
        asyncio.run(self.registry.register(down_inst))

        instances = asyncio.run(self.query.get_service("q-service"))
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0].status, InstanceStatus.UP)

    def test_get_healthy_one(self):
        for i in range(5):
            inst = Instance(service_name="random-svc", host=f"10.0.6.{i}", port=8080)
            asyncio.run(self.registry.register(inst))

        result = asyncio.run(self.query.get_healthy_one("random-svc"))
        self.assertIsNotNone(result)
        self.assertEqual(result.service_name, "random-svc")

    def test_get_healthy_one_empty(self):
        result = asyncio.run(self.query.get_healthy_one("nonexistent"))
        self.assertIsNone(result)

    def test_get_by_region(self):
        inst1 = Instance(service_name="region-svc", host="10.0.7.1", port=8080, region="us-east")
        inst2 = Instance(service_name="region-svc", host="10.0.7.2", port=8080, region="eu-west")
        asyncio.run(self.registry.register(inst1))
        asyncio.run(self.registry.register(inst2))

        us_instances = asyncio.run(self.query.get_instances_by_region("region-svc", "us-east"))
        self.assertEqual(len(us_instances), 1)
        self.assertEqual(us_instances[0].region, "us-east")

    def test_get_by_metadata(self):
        inst1 = Instance(
            service_name="meta-svc", host="10.0.8.1", port=8080,
            metadata={"version": "v2", "env": "prod"},
        )
        inst2 = Instance(
            service_name="meta-svc", host="10.0.8.2", port=8080,
            metadata={"version": "v1", "env": "staging"},
        )
        asyncio.run(self.registry.register(inst1))
        asyncio.run(self.registry.register(inst2))

        v2_instances = asyncio.run(self.query.get_instances_by_metadata("meta-svc", "version", "v2"))
        self.assertEqual(len(v2_instances), 1)


class TestWatcher(unittest.TestCase):
    def test_subscribe_and_notify(self):
        watcher = Watcher()
        received = []

        sub = asyncio.run(watcher.subscribe("order-service"))

        inst = Instance(service_name="order-service", host="10.0.9.1", port=8080)
        asyncio.run(watcher.notify("order-service", inst, "REGISTER"))

        events = asyncio.run(watcher.get_events(sub.subscriber_id, timeout=1.0))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["change_type"], "REGISTER")
        self.assertEqual(events[0]["service_name"], "order-service")

    def test_subscribe_all(self):
        watcher = Watcher()

        sub = asyncio.run(watcher.subscribe_all())

        inst = Instance(service_name="any-service", host="10.0.9.2", port=8080)
        asyncio.run(watcher.notify("any-service", inst, "DEREGISTER"))

        events = asyncio.run(watcher.get_events(sub.subscriber_id, timeout=1.0))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["change_type"], "DEREGISTER")

    def test_unsubscribe(self):
        watcher = Watcher()

        sub = asyncio.run(watcher.subscribe("svc"))
        asyncio.run(watcher.unsubscribe(sub.subscriber_id))

        self.assertEqual(watcher.get_subscriber_count("svc"), 0)

    def test_callback_notification(self):
        watcher = Watcher()
        received = []

        async def on_change(event):
            received.append(event)

        asyncio.run(watcher.subscribe("cb-service", callback=on_change))

        inst = Instance(service_name="cb-service", host="10.0.9.3", port=8080)
        asyncio.run(watcher.notify("cb-service", inst, "REGISTER"))

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].change_type, ChangeType.REGISTER)

    def test_no_cross_service_notification(self):
        watcher = Watcher()

        sub = asyncio.run(watcher.subscribe("svc-a"))

        inst_b = Instance(service_name="svc-b", host="10.0.9.4", port=8080)
        asyncio.run(watcher.notify("svc-b", inst_b, "REGISTER"))

        events = asyncio.run(watcher.get_events(sub.subscriber_id, timeout=0.5))
        self.assertEqual(len(events), 0)


class TestSelfProtection(unittest.TestCase):
    def test_protection_activates_on_low_heartbeat(self):
        registry = Registry(heartbeat_interval=1.0, heartbeat_timeout=3.0)
        protection = SelfProtection(
            registry,
            expected_heartbeat_rate=0.85,
            expected_heartbeat_window=1.0,
            check_interval=0.5,
        )

        for i in range(5):
            inst = Instance(service_name=f"prot-svc-{i}", host=f"10.0.10.{i}", port=8080)
            asyncio.run(registry.register(inst))

        self.assertFalse(protection.is_protecting)

        async def simulate_partition():
            await protection.start()
            await asyncio.sleep(1.5)
            await protection.stop()

        asyncio.run(simulate_partition())

        stats = registry.get_heartbeat_stats()
        self.assertTrue(stats["protection_enabled"])

    def test_protection_status(self):
        registry = Registry()
        protection = SelfProtection(registry, enable=True)
        stats = protection.get_stats()
        self.assertIn("protection_enabled", stats)
        self.assertFalse(stats["protection_enabled"])


class TestRegistryChangeCallback(unittest.TestCase):
    def test_register_callback_fired(self):
        registry = Registry()
        events = []

        async def on_change(svc_name, instance, action):
            events.append((svc_name, action))

        registry.on_change(on_change)

        inst = Instance(service_name="cb-svc", host="10.0.11.1", port=8080)
        asyncio.run(registry.register(inst))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0], ("cb-svc", "REGISTER"))

    def test_deregister_callback_fired(self):
        registry = Registry()
        events = []

        async def on_change(svc_name, instance, action):
            events.append((svc_name, action))

        registry.on_change(on_change)

        inst = Instance(service_name="cb-svc2", host="10.0.11.2", port=8080)
        asyncio.run(registry.register(inst))
        asyncio.run(registry.deregister("cb-svc2", inst.instance_id))

        self.assertEqual(len(events), 2)
        self.assertEqual(events[1], ("cb-svc2", "DEREGISTER"))


class TestInstanceSerialization(unittest.TestCase):
    def test_roundtrip(self):
        inst = Instance(
            service_name="serial-svc",
            host="10.0.12.1",
            port=8080,
            metadata={"env": "prod", "tier": "frontend"},
            health_check_url="http://10.0.12.1:8080/health",
            region="us-west",
            zone="us-west-2a",
        )
        d = inst.to_dict()
        restored = Instance.from_dict(d)
        self.assertEqual(restored.service_name, "serial-svc")
        self.assertEqual(restored.host, "10.0.12.1")
        self.assertEqual(restored.metadata["env"], "prod")
        self.assertEqual(restored.health_check_url, "http://10.0.12.1:8080/health")
        self.assertEqual(restored.region, "us-west")


if __name__ == "__main__":
    unittest.main()
