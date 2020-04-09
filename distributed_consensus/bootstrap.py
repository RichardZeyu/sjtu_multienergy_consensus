from .config import Config, config
from .transport.tcp.conn import TCPConnectionHandler, TCPProtocolV1
from .queue import QueueManager
from .sync_adapter import QueueManagerAdapter

import logging
import asyncio
import threading

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
    loop = asyncio.get_running_loop()
    adapter = QueueManagerAdapter(queue, loop)
    done = asyncio.Event()
    if await conn.setup_and_wait_micronet(
        timeout=c.transport_parameters['tcpv1']['micronet_init_sec']
    ):
        L.info('micronet established')
        scene = c.scene_class(
            local=c.local_node,
            node_manager=c.node_manager,
            adapter=adapter,
            done_cb=lambda: loop.call_soon_threadsafe(done.set),
            **c.scene_parameters
        )
        t = threading.Thread(target=scene.run, daemon=True)
        t.start()
        await done.wait()


def bootstrap():
    # loop = asyncio.get_event_loop()
    asyncio.run(async_main())
    print('done')
