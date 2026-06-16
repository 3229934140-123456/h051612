from __future__ import annotations

import asyncio
import json
import logging
import unittest

from sdiscovery import DiscoveryServer

logging.basicConfig(level=logging.ERROR)


async def http_request(host: str, port: int, method: str, path: str, body=None):
    reader, writer = await asyncio.open_connection(host, port, limit=65536)

    if body is not None:
        body_str = json.dumps(body)
    else:
        body_str = ""

    request = (
        f"{method} {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body_str)}\r\n"
        f"Connection: close\r\n\r\n"
        f"{body_str}"
    )

    writer.write(request.encode())
    await writer.drain()

    response = await reader.read(655360)
    writer.close()
    await writer.wait_closed()

    response_text = response.decode("utf-8", errors="replace")
    parts = response_text.split("\r\n\r\n", 1)
    if len(parts) < 2:
        return 0, {}, response_text

    headers_part, body_part = parts
    status_line = headers_part.split("\r\n")[0]
    status_code = int(status_line.split(" ")[1])

    headers = {}
    for line in headers_part.split("\r\n")[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()

    try:
        body_json = json.loads(body_part)
    except json.JSONDecodeError:
        body_json = body_part

    return status_code, headers, body_json


class TestEndToEndHTTP(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server_a = DiscoveryServer(
            host="127.0.0.1",
            port=18081,
            gossip_port=17101,
            enable_gossip=True,
            enable_protection=True,
            heartbeat_interval=10.0,
            heartbeat_timeout=30.0,
        )
        self.server_a.gossip.gossip_interval = 0.2
        self.server_a.gossip.sync_interval = 0.3
        await self.server_a.start()

        self.server_b = DiscoveryServer(
            host="127.0.0.1",
            port=18082,
            gossip_port=17102,
            enable_gossip=True,
            enable_protection=True,
            heartbeat_interval=10.0,
            heartbeat_timeout=30.0,
        )
        self.server_b.gossip.gossip_interval = 0.2
        self.server_b.gossip.sync_interval = 0.3
        await self.server_b.start()

        await asyncio.sleep(0.2)

    async def asyncTearDown(self):
        await self.server_a.stop()
        await self.server_b.stop()
        await asyncio.sleep(0.2)

    async def test_01_join_cluster(self):
        status, _, body = await http_request(
            "127.0.0.1", 18082, "POST", "/gossip/join",
            {"host": "127.0.0.1", "port": 17101}
        )
        self.assertEqual(status, 200, f"Join failed: {body}")
        self.assertEqual(body["status"], "ok")

        await asyncio.sleep(1.0)

        status, _, body_a = await http_request(
            "127.0.0.1", 18081, "GET", "/gossip/members"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body_a), 2, "Node A should see 2 members")

        status, _, body_b = await http_request(
            "127.0.0.1", 18082, "GET", "/gossip/members"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body_b), 2, "Node B should see 2 members")

        member_ids_a = {m["member_id"] for m in body_a}
        member_ids_b = {m["member_id"] for m in body_b}
        self.assertEqual(member_ids_a, member_ids_b, "Members should be consistent")

    async def test_02_registration_propagation(self):
        await http_request(
            "127.0.0.1", 18082, "POST", "/gossip/join",
            {"host": "127.0.0.1", "port": 17101}
        )
        await asyncio.sleep(0.5)

        status, _, inst_a = await http_request(
            "127.0.0.1", 18081, "POST", "/register",
            {
                "service_name": "e2e-order-svc",
                "host": "10.10.0.1",
                "port": 8080,
                "region": "us-east",
                "zone": "us-east-1a",
                "metadata": {"version": "v1", "env": "prod"},
            }
        )
        self.assertEqual(status, 200)
        self.assertEqual(inst_a["service_name"], "e2e-order-svc")

        await asyncio.sleep(1.5)

        status, _, instances_b = await http_request(
            "127.0.0.1", 18082, "GET", "/instances?service_name=e2e-order-svc"
        )
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(instances_b), 1)

        found = instances_b[0]
        self.assertEqual(found["service_name"], "e2e-order-svc")
        self.assertEqual(found["host"], "10.10.0.1")
        self.assertEqual(found["port"], 8080)
        self.assertEqual(found["region"], "us-east")
        self.assertEqual(found["zone"], "us-east-1a")
        self.assertEqual(found["metadata"]["version"], "v1")
        self.assertEqual(found["metadata"]["env"], "prod")

    async def test_03_cross_replica_subscription(self):
        await http_request(
            "127.0.0.1", 18082, "POST", "/gossip/join",
            {"host": "127.0.0.1", "port": 17101}
        )
        await asyncio.sleep(0.5)

        status, _, sub_resp = await http_request(
            "127.0.0.1", 18082, "POST", "/subscribe",
            {"service_name": "e2e-sub-svc"}
        )
        self.assertEqual(status, 200)
        subscriber_id = sub_resp["subscriber_id"]
        self.assertIsNotNone(subscriber_id)

        status, _, inst = await http_request(
            "127.0.0.1", 18081, "POST", "/register",
            {
                "service_name": "e2e-sub-svc",
                "host": "10.10.1.1",
                "port": 9090,
            }
        )
        self.assertEqual(status, 200)
        instance_id = inst["instance_id"]

        await asyncio.sleep(1.0)

        status, _, events = await http_request(
            "127.0.0.1", 18082, "GET", f"/events?subscriber_id={subscriber_id}&timeout=2"
        )
        self.assertEqual(status, 200)

        register_events = [e for e in events if e["change_type"] == "REGISTER"]
        self.assertGreaterEqual(len(register_events), 1,
                                "Subscriber on B should receive REGISTER event from A")
        self.assertEqual(register_events[0]["service_name"], "e2e-sub-svc")
        self.assertEqual(register_events[0]["instance"]["host"], "10.10.1.1")

        status, _, _ = await http_request(
            "127.0.0.1", 18081, "DELETE", "/deregister",
            {"service_name": "e2e-sub-svc", "instance_id": instance_id}
        )
        self.assertEqual(status, 200)

        await asyncio.sleep(1.0)

        status, _, events2 = await http_request(
            "127.0.0.1", 18082, "GET", f"/events?subscriber_id={subscriber_id}&timeout=2"
        )
        self.assertEqual(status, 200)

        deregister_events = [e for e in events2 if e["change_type"] == "DEREGISTER"]
        self.assertGreaterEqual(len(deregister_events), 1,
                                "Subscriber on B should receive DEREGISTER event from A")
        self.assertEqual(deregister_events[0]["instance"]["instance_id"], instance_id)

    async def test_04_filtered_query_consistency(self):
        await http_request(
            "127.0.0.1", 18082, "POST", "/gossip/join",
            {"host": "127.0.0.1", "port": 17101}
        )
        await asyncio.sleep(0.5)

        instances = [
            {"service_name": "e2e-filter-svc", "host": "10.20.0.1", "port": 8080,
             "region": "us-east", "zone": "us-east-1a", "metadata": {"version": "v1", "env": "prod"}},
            {"service_name": "e2e-filter-svc", "host": "10.20.0.2", "port": 8080,
             "region": "us-east", "zone": "us-east-1b", "metadata": {"version": "v2", "env": "prod"}},
            {"service_name": "e2e-filter-svc", "host": "10.20.0.3", "port": 8080,
             "region": "us-west", "zone": "us-west-1a", "metadata": {"version": "v1", "env": "staging"}},
            {"service_name": "e2e-filter-svc", "host": "10.20.0.4", "port": 8080,
             "region": "us-east", "zone": "us-east-1a", "metadata": {"version": "v2", "env": "prod"}},
        ]

        for i, inst in enumerate(instances):
            port = 18081 if i % 2 == 0 else 18082
            status, _, _ = await http_request("127.0.0.1", port, "POST", "/register", inst)
            self.assertEqual(status, 200)

        await asyncio.sleep(2.0)

        query_path = "/instances/query?service_name=e2e-filter-svc&region=us-east&zone=us-east-1a&meta_version=v1"

        status, _, resp_a = await http_request("127.0.0.1", 18081, "GET", query_path)
        self.assertEqual(status, 200)

        status, _, resp_b = await http_request("127.0.0.1", 18082, "GET", query_path)
        self.assertEqual(status, 200)

        self.assertEqual(len(resp_a), len(resp_b), "Both replicas should return same number of instances")

        ids_a = sorted(i["instance_id"] for i in resp_a)
        ids_b = sorted(i["instance_id"] for i in resp_b)
        self.assertEqual(ids_a, ids_b, "Instance IDs should match across replicas")

        for inst in resp_a:
            self.assertEqual(inst["region"], "us-east")
            self.assertEqual(inst["zone"], "us-east-1a")
            self.assertEqual(inst["metadata"]["version"], "v1")
            self.assertEqual(inst["status"], "UP")

    async def test_05_status_change_propagation(self):
        await http_request(
            "127.0.0.1", 18082, "POST", "/gossip/join",
            {"host": "127.0.0.1", "port": 17101}
        )
        await asyncio.sleep(0.5)

        status, _, sub_resp = await http_request(
            "127.0.0.1", 18082, "POST", "/subscribe",
            {"service_name": "e2e-status-svc"}
        )
        self.assertEqual(status, 200)
        subscriber_id = sub_resp["subscriber_id"]

        status, _, inst = await http_request(
            "127.0.0.1", 18081, "POST", "/register",
            {"service_name": "e2e-status-svc", "host": "10.30.0.1", "port": 8080}
        )
        self.assertEqual(status, 200)
        instance_id = inst["instance_id"]

        await asyncio.sleep(1.0)

        status, _, _ = await http_request(
            "127.0.0.1", 18081, "PUT", "/instance/status",
            {
                "service_name": "e2e-status-svc",
                "instance_id": instance_id,
                "status": "OUT_OF_SERVICE"
            }
        )
        self.assertEqual(status, 200)

        await asyncio.sleep(1.5)

        status, _, events = await http_request(
            "127.0.0.1", 18082, "GET", f"/events?subscriber_id={subscriber_id}&timeout=2"
        )
        self.assertEqual(status, 200)

        status_events = [e for e in events if e["change_type"] == "STATUS_CHANGE"]
        self.assertGreaterEqual(len(status_events), 1)
        self.assertEqual(status_events[0]["instance"]["status"], "OUT_OF_SERVICE")

        status, _, instances_b = await http_request(
            "127.0.0.1", 18082, "GET", "/instances?service_name=e2e-status-svc"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(instances_b), 0,
                         "Only UP instances should be returned from /instances")

    async def test_06_cluster_status_view(self):
        await http_request(
            "127.0.0.1", 18081, "POST", "/register",
            {"service_name": "e2e-cluster-svc", "host": "10.40.0.1", "port": 8080}
        )
        await http_request(
            "127.0.0.1", 18082, "POST", "/register",
            {"service_name": "e2e-cluster-svc", "host": "10.40.0.2", "port": 8080}
        )

        await http_request(
            "127.0.0.1", 18082, "POST", "/gossip/join",
            {"host": "127.0.0.1", "port": 17101}
        )

        await asyncio.sleep(2.0)

        status, _, cluster_status = await http_request(
            "127.0.0.1", 18081, "GET", "/cluster/status"
        )
        self.assertEqual(status, 200)

        self.assertEqual(cluster_status["total_members"], 2)
        self.assertEqual(len(cluster_status["members"]), 2)

        for member in cluster_status["members"]:
            self.assertIn("total_services", member)
            self.assertIn("total_instances", member)
            self.assertIn("alive_members", member)
            self.assertIn("host", member)
            self.assertIn("http_port", member)
            self.assertIn("gossip_port", member)

        total_services_values = [m["total_services"] for m in cluster_status["members"]]
        total_instances_values = [m["total_instances"] for m in cluster_status["members"]]

        self.assertEqual(len(set(total_services_values)), 1,
                         "All replicas should have same service count after convergence")
        self.assertEqual(len(set(total_instances_values)), 1,
                         "All replicas should have same instance count after convergence")
        self.assertEqual(total_instances_values[0], 2,
                         "Each replica should have 2 instances after convergence")

        self.assertTrue(cluster_status["consistent"],
                        "Cluster should report consistent status after convergence")

    async def test_07_deregistration_propagation(self):
        await http_request(
            "127.0.0.1", 18082, "POST", "/gossip/join",
            {"host": "127.0.0.1", "port": 17101}
        )
        await asyncio.sleep(0.5)

        status, _, inst = await http_request(
            "127.0.0.1", 18081, "POST", "/register",
            {"service_name": "e2e-dereg-svc", "host": "10.50.0.1", "port": 8080}
        )
        self.assertEqual(status, 200)
        instance_id = inst["instance_id"]

        await asyncio.sleep(1.0)

        status, _, instances_b = await http_request(
            "127.0.0.1", 18082, "GET", "/instances?service_name=e2e-dereg-svc"
        )
        self.assertEqual(len(instances_b), 1)

        status, _, _ = await http_request(
            "127.0.0.1", 18081, "DELETE", "/deregister",
            {"service_name": "e2e-dereg-svc", "instance_id": instance_id}
        )
        self.assertEqual(status, 200)

        await asyncio.sleep(1.5)

        status, _, instances_a = await http_request(
            "127.0.0.1", 18081, "GET", "/instances?service_name=e2e-dereg-svc"
        )
        self.assertEqual(len(instances_a), 0, "Instance should be removed from A")

        status, _, instances_b = await http_request(
            "127.0.0.1", 18082, "GET", "/instances?service_name=e2e-dereg-svc"
        )
        self.assertEqual(len(instances_b), 0, "Instance should be removed from B")


if __name__ == "__main__":
    unittest.main()
