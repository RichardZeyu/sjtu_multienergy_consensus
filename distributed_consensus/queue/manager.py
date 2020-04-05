import asyncio
import logging
import typing
from collections import deque
from weakref import WeakSet, WeakValueDictionary

from ..core.node import Node
from ..core.node_manager import NodeManager

L = logging.getLogger(__name__)

EOC = None


AddressedPacket = typing.Tuple[Node, typing.Optional[bytes]]


def clean_queue(queue: asyncio.Queue[bytes]):
    while not queue.empty():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break
    # cancel all coroutines waiting for pkt
    for getter in queue._getters:
        getter.cancel()


class QueueManager:
    local: Node
    _by_node: WeakValueDictionary[
        int, typing.Tuple[asyncio.Queue, asyncio.Queue]
    ]
    _buffered: typing.Deque[AddressedPacket]
    logger: logging.Logger
    node_manager: NodeManager

    def __init__(self, local: Node, node_manager: NodeManager):
        super().__init__()
        self.local = local
        self._by_node = WeakValueDictionary()
        self.logger = L.getChild(f'QueueManager-{local.id}')
        self._buffered = deque()
        self.node_manager = node_manager

    async def send_to(self, remote: Node, pkt: bytes):
        if remote.id not in self._by_node:
            self.logger.warn(
                f'remote {remote.id} not connected or already disconnected'
            )
            return

        _, egress = self._by_node[remote.id]
        self.logger.debug(f'put pkt to remote {remote.id}')
        await egress.put(pkt)

    async def broadcast(self, pkt: bytes):
        for node_id, (_, egress) in self._by_node.items():
            self.logger.debug(f'broadcast pkt to remote {node_id}')
            await egress.put(pkt)

    def receive_one_no_wait(self) -> typing.Optional[AddressedPacket]:
        if self._buffered:
            return self._buffered.popleft()
        return None

    async def _read(self, remote_id: int, ingress: asyncio.Queue[bytes]):
        remote = self.node_manager.get_node(remote_id)
        try:
            pkt = await ingress.get()
        except asyncio.CancelledError:
            # in case the connection has been closed
            return remote, EOC
        return remote, pkt

    async def receive_one(
        self, timeout: typing.Optional[float] = None
    ) -> AddressedPacket:
        addressed_pkt = self.receive_one_no_wait()
        if addressed_pkt is not None:
            return addressed_pkt

        tasks: typing.List[asyncio.Task] = {
            asyncio.create_task(
                self._read(node_id, ingress), name=f'receiving {node_id}'
            )
            for node_id, (ingress, _) in self._by_node.items()
        }
        done: typing.Set[asyncio.Task]
        pending: typing.Set[asyncio.Task]
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED, timeout=timeout
        )
        for task in pending:
            assert (
                not task.done() and not task.cancelled()
            ), f'unexpected task status {task._state}'
            task.cancel()

        for task in done:
            try:
                addressed_pkt = task.result()
                assert addressed_pkt is not None
                self._buffered.append(addressed_pkt)
            except:  # noqa
                self.logger.exception(f'exception occurred when in {task!r}')
        return self._buffered.popleft()

    def _check_dup_channel(self, remote: Node):
        if remote.id in self._by_node:
            raise RuntimeError(f'dup channel for remote node {remote.id}')

    def create_queue(
        self, remote: Node
    ) -> typing.Tuple[asyncio.Queue, asyncio.Queue]:
        self._check_dup_channel(remote)
        ingress: asyncio.Queue = asyncio.Queue()
        egress: asyncio.Queue = asyncio.Queue()
        pair = (ingress, egress)
        self._by_node[remote.id] = pair
        return pair

    def close(self, remote: Node):
        if remote.id not in self._by_node:
            return
        ingress, egress = self._by_node.pop(remote.id)

        # cancellation of ingress getters causes readers return (remote, EOC) indicators
        clean_queue(ingress)
        clean_queue(egress)
        del ingress
        del egress
