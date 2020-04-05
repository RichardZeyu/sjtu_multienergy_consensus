import typing
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from collections import Counter

from ..core.node import Node
from ..core.node_manager import NodeManager
from ..queue import QueueManager, QueuedPacket
from ..sync_adapter import wait_next_pkt

import logging

L = logging.getLogger(__name__)


class SceneTypeI(ABC):
    round_id: int
    node_manager: NodeManager
    queue_manager: QueueManager
    received_normal_data: typing.Dict[int, typing.Set[QueuedPacket]]
    received_delegate_data: typing.Dict[int, typing.Set[QueuedPacket]]
    seen: typing.Set[bytes]
    local: Node

    @abstractmethod
    def check_end(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def delegate_update(self):
        raise NotImplementedError()

    @abstractmethod
    def normal_update(self, data: typing.Optional[bytes]):
        raise NotImplementedError()

    @abstractmethod
    def round_timeout(self) -> float:
        raise NotImplementedError()

    @abstractmethod
    def normal_data(self) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def delegate_data(self) -> bytes:
        raise NotImplementedError()

    def drop_evil_packet(self, received, block_origin):
        evil_node_ids = [
            node_id
            for (node_id, pkt_set) in received.items()
            if len(pkt_set) != 1
        ]

        for evil in evil_node_ids:
            # TODO: logging
            del received[evil]
            if block_origin:
                self.node_manager.block(evil)

    def extract_majority(self, received) -> typing.Optional[bytes]:
        counter: typing.Counter[bytes] = Counter()
        for pkt_set in received.values():
            counter.update([pkt.data for pkt in pkt_set])
        if not counter:
            return None
        top, freq = counter.most_common(1)[0]
        return top if freq > self.node_manager._delegate_num / 2 else None

    def round(self):
        round_end = datetime().utcnow() + timedelta(
            seconds=self.round_timeout()
        )
        self.received_delegate_data.clear()
        self.received_normal_data.clear()
        self.seen.clear()

        if self.local.is_normal:
            # TODO broadcast to delegate nodes only
            self.queue_manager.broadcast(self.normal_data())

        while True:
            pkt = wait_next_pkt(self.queue_manager, run_till=round_end)
            if pkt is None:
                L.info(f'receive timeout, round over')
                break
            self.normal_node_action(pkt)
            self.delegate_node_action(pkt)

        if self.local.is_delegate:
            self.drop_evil_packet(self.received_normal_data, block_origin=False)
            self.delegate_update()
            self.queue_manager.broadcast(self.delegate_data())

        if self.local.is_normal:
            self.drop_evil_packet(self.received_delegate_data, block_origin=False)
            data = self.extract_majority(self.received_delegate_data)
            self.normal_update(data)

    def normal_node_action(self, pkt: QueuedPacket):
        if not self.local.is_normal:
            return

        if pkt.received_from is None:
            L.warning(
                "remote is None, normal node won't handle it",
            )
            return
        if not pkt.received_from.is_delegate:
            L.warning(
                "remote %s is not delegate, normal node won't handle it",
                pkt.received_from.id,
            )
            return
        if not pkt.origin.is_delegate:
            L.warning(
                "origin %s is not delegate, normal node won't handle it",
                pkt.origin.id,
            )
            return
        self.received_delegate_data.setdefault(pkt.origin.id, set()).add(pkt)

    def delegate_node_action(self, pkt: QueuedPacket):
        if not self.local.is_delegate:
            return

        if not pkt.origin.is_normal:
            L.warning(
                "origin %s is not normal, delegate node won't handle it",
                pkt.origin.id,
            )
            return

        self.received_normal_data.setdefault(pkt.origin.id, set()).add(pkt)
        if pkt.data not in self.seen:
            self.seen.add(pkt.data)
        # TODO forward to delegate nodes only
        self.queue_manager.broadcast_forward(pkt)
