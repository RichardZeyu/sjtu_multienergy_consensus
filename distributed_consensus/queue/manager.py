import asyncio
import logging
import typing
from collections import deque
from weakref import WeakValueDictionary

from ..core.node import Node
from ..core.node_manager import NodeManager, BaseNode
from .packet import QueuedPacket

L = logging.getLogger(__name__)

EOC = None


NodeFilter = typing.Callable[[BaseNode], bool]


def all_node(n: BaseNode) -> bool:
    return True


def delegate_only(n: BaseNode) -> bool:
    return n.is_delegate


def normal_only(n: BaseNode) -> bool:
    return n.is_normal


def clean_queue(queue: asyncio.Queue):
    while not queue.empty():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break
    # cancel all coroutines waiting for pkt
    for getter in queue._getters:  # type: ignore
        getter.cancel()


if typing.TYPE_CHECKING:
    PacketQueue = asyncio.Queue[QueuedPacket]
    WeakQueueDictionary = WeakValueDictionary[
        int, typing.Tuple[PacketQueue, PacketQueue],
    ]
else:
    PacketQueue = typing.Any
    WeakQueueDictionary = typing.Any


class QueueManager:
    local: Node
    _by_node: WeakQueueDictionary
    _buffered: typing.Deque[QueuedPacket]
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

        to_queue = QueuedPacket(
            origin=self.local,
            send_to=remote,
            received_from=None,
            data=pkt,
            full_packet=None,
        )
        await egress.put(to_queue)

    async def broadcast(self, pkt: bytes, filter_: NodeFilter = all_node):
        for node_id, (_, egress) in self._by_node.items():
            remote = self.node_manager.get_node(node_id)
            assert remote is not None
            if not filter_(remote):
                self.logger.debug(f'{remote} is filtered out by {filter!r}')
                continue

            self.logger.debug(f'broadcast pkt to remote {node_id}')
            to_queue = QueuedPacket(
                origin=self.local,
                send_to=remote,
                received_from=None,
                data=pkt,
                full_packet=None,
            )
            await egress.put(to_queue)

    async def broadcast_forward(
        self, to_forward: QueuedPacket, filter_: NodeFilter = all_node
    ):
        for node_id, (_, egress) in self._by_node.items():
            remote = self.node_manager.get_node(node_id)
            assert remote is not None
            if not filter_(remote):
                self.logger.debug(f'{remote} is filtered out by {filter!r}')
                continue

            self.logger.debug(f'broadcast pkt to remote {node_id}')
            to_queue = QueuedPacket(
                origin=to_forward.origin,
                send_to=remote,
                received_from=to_forward.received_from,
                data=to_forward.data,
                full_packet=to_forward.full_packet,
            )
            await egress.put(to_queue)

    def receive_one_no_wait(self) -> typing.Optional[QueuedPacket]:
        if self._buffered:
            return self._buffered.popleft()
        return None

    async def _read(
        self, remote_id: int, ingress: PacketQueue,
    ):
        remote = self.node_manager.get_node(remote_id)
        try:
            pkt: QueuedPacket = await ingress.get()
            assert pkt is None or pkt.received_from == remote
        except asyncio.CancelledError:
            # in case the connection has been closed
            return EOC
        return pkt

    async def receive_one(
        self, timeout: typing.Optional[float] = None
    ) -> QueuedPacket:
        pkt = self.receive_one_no_wait()
        if pkt is not None:
            return pkt

        tasks: typing.Set[asyncio.Task] = {
            asyncio.create_task(  # type: ignore
                self._read(node_id, ingress), name=f'receiving {node_id}'
            )
            for node_id, (ingress, _) in self._by_node.items()
        }
        done: typing.Set[asyncio.Future]
        pending: typing.Set[asyncio.Future]
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
                pkt = task.result()
                assert pkt is not None
                self._buffered.append(pkt)
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

        # cancellation of ingress getters causes readers return EOC
        clean_queue(ingress)
        clean_queue(egress)
        del ingress
        del egress
