import asyncio
import logging
import sys

from sdiscovery import DiscoveryServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

GOSSIP_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7946
HTTP_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8761


async def main():
    server = DiscoveryServer(
        host="0.0.0.0",
        port=HTTP_PORT,
        gossip_port=GOSSIP_PORT,
        enable_gossip=True,
        enable_protection=True,
        heartbeat_interval=10.0,
        heartbeat_timeout=30.0,
    )
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
