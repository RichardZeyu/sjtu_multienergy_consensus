import asyncio
import logging
import typing
from datetime import datetime, timedelta
from time import sleep

from ..queue.manager import NodeFilter, QueueManager, all_node
from ..queue.packet import QueuedPacket

L = logging.getLogger(__name__)


class QueueManagerAdapter:
    manager: QueueManager
    loop: asyncio.BaseEventLoop

    def __init__(self, manager: QueueManager, loop: asyncio.BaseEventLoop):
        self.manager = manager
        self.loop = loop

    def wait_next_pkt(
        self,
        timeout: typing.Optional[float] = None,
        run_till: typing.Optional[datetime] = None,
    ) -> typing.Optional[QueuedPacket]:

        now = datetime.utcnow()
        if timeout is not None:
            if run_till is not None:
                L.warn('both run_till and timeout specified, ignore run_till')
            run_till = now + timedelta(seconds=timeout)
        elif run_till is None and timeout is None:
            raise ValueError('either run_till or timeout should be specified')

        while now < run_till:
            timeout = (run_till - now).total_seconds()
            recv = asyncio.run_coroutine_threadsafe(
                self.manager.receive_one(timeout=timeout), self.loop
            )
            try:
                pkt = recv.result()
                if pkt is not None:
                    return pkt
            except asyncio.TimeoutError:
                break
            now = datetime.utcnow()
            if (run_till - now).total_seconds() > 0.5:
                # add a little sleep to avoid too much retrying if all other
                # nodes are reconnecting.
                # TODO use event or some other sync primitives
                sleep(0.5)
        return None

    def drop_node(self, node):
        self.loop.call_soon_threadsafe(self.manager.close, node)

    def broadcast(self, data: bytes, filter_: NodeFilter = all_node):
        asyncio.run_coroutine_threadsafe(
            self.manager.broadcast(data, filter_), self.loop
        ).result()

    def broadcast_forward(
        self, to_forward: QueuedPacket, filter_: NodeFilter = all_node
    ):
        asyncio.run_coroutine_threadsafe(
            self.manager.broadcast_forward(to_forward, filter_), self.loop
        ).result()
