from .config import Config, config
from .transport.tcp.conn import TCPConnectionHandler, TCPProtocolV1
from .queue import QueueManager

import logging
import asyncio

L = logging.getLogger(__name__)


async def async_main():
    c: Config = config.get()
    L.debug('using config: %r', c)
    L.info(f'local node is normal: {c.local_node.is_normal}')
    L.info(f'local node is delegate: {c.local_node.is_delegate}')

    queue = QueueManager(c.local_node, c.node_manager)
    conn = TCPConnectionHandler(
        c.local_node, c.node_manager, queue, TCPProtocolV1
    )
    if await conn.setup_and_wait_micronet(
        timeout=c.transport_parameters['tcpv1']['micronet_init_sec']
    ):
        L.info('micronet established')
        while True:
            await asyncio.sleep(10000)
        # scene = c.scene_class(**c.scene_parameters)
        # await scene.run()


def bootstrap():
    asyncio.run(async_main())
