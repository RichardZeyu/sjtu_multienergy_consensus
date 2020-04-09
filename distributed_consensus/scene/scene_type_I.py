import typing
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from collections import Counter
import struct

from ..core.node import Node
from ..core.node_manager import NodeManager
from ..queue import QueueManager, QueuedPacket, delegate_only, normal_only
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

    normal_phase_done: bool
    scene_end: bool

    def __init__(
        self,
        local: Node,
        node_manager: NodeManager,
        queue_manager: QueueManager,
    ):
        super().__init__()
        self.local = local
        self.node_manager = node_manager
        self.queue_manager = queue_manager
        self.round_id = 0
        self.received_delegate_data = dict()
        self.received_normal_data = dict()
        self.seen = set()
        self.logger = L.getChild(f'{self.__class__.__name__}-{self.local.id}')
        self.scene_end = False

    @abstractmethod
    def check_end(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def check_end_in_data(self, data: bytes) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def delegate_update(self):
        raise NotImplementedError()

    @abstractmethod
    def normal_initiate(self):
        raise NotImplementedError()

    @abstractmethod
    def normal_update(self, data: bytes):
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

    @abstractmethod
    def is_packet_valid(self, pkt: QueuedPacket) -> bool:
        raise NotImplementedError()

    def drop_evil_nodes(
        self, received: typing.Dict[int, typing.Set[QueuedPacket]]
    ):
        evil_node_ids = [
            (node_id, len(pkt_set))
            for (node_id, pkt_set) in received.items()
            if len(pkt_set) != 1
        ]

        for evil, pkt_num in evil_node_ids:
            self.logger.warn(f'remote node {evil} send f{pkt_num} packets')
            self.logger.warn(f'all packets from node {evil} are dropped')
            del received[evil]
            self.node_manager.block(evil)
            evil_node = self.node_manager.get_node(evil)
            if evil_node is not None and isinstance(evil_node, Node):
                self.queue_manager.close(evil_node)
            self.logger.warn(f'node {evil} has been blocked')

    def extract_majority(
        self, received: typing.Dict[int, typing.Set[QueuedPacket]]
    ) -> typing.Optional[bytes]:
        counter: typing.Counter[bytes] = Counter()
        counter.update(
            [
                list(pkts)[0].data
                for pkts in received.values()
                if len(pkts) == 1
            ]
        )
        if not counter:
            return None
        top, freq = counter.most_common(1)[0]
        return top if freq > self.node_manager.delegate_num / 2 else None

    def run(self):
        self.round_id = 1

        # round 1 starts from step 3
        self.normal_initiate()
        self.queue_manager.broadcast(self.normal_data(), filter_=delegate_only)

        while not self.scene_end:
            # step 1
            if self.round_id > 1 and self.local.is_delegate:
                self.drop_evil_packet(self.received_normal_data)
                self.delegate_update()
                self.check_end()
                self.queue_manager.broadcast(
                    self.delegate_data(), filter_=normal_only
                )
                self.received_normal_data.clear()

            self.normal_phase_done = False
            self.received_delegate_data.clear()
            self.seen.clear()
            self.round_id += 1
            now = datetime.utcnow()
            round_end = now + timedelta(seconds=self.round_timeout())
            self.logger.info(
                f'round {self.round_id} begin, will end by {round_end}'
            )

            # step 2 ~ 5, wrapped by receiving loop
            self.round(round_end)

    def round(self, round_end: datetime):
        while not self.scene_end:
            pkt = wait_next_pkt(self.queue_manager, run_till=round_end)
            if pkt is None:
                self.logger.info(
                    f'receive timeout, round {self.round_id} over'
                )
                break
            if self.is_packet_valid(pkt):
                self.logger.warn(
                    'drop invalid packet from %s origin %s',
                    pkt.received_from,
                    pkt.origin,
                )
                continue
            # step 2 & 3
            self.normal_node_action(pkt)
            # step 4
            self.delegate_forward(pkt)

    def normal_node_action(self, pkt: QueuedPacket):
        if self.normal_phase_done:
            self.logger.debug(
                f"normal data of round {self.round_id} has been finalized"
            )
            return
        if not self.local.is_normal or pkt:
            self.logger.debug("local is not normal node, abort",)
            return
        if pkt.received_from is None:
            self.logger.debug("remote is None, normal node won't handle it",)
            return
        if not pkt.received_from.is_delegate:
            self.logger.debug(
                "remote %s is not delegate, normal node won't handle it",
                pkt.received_from.id,
            )
            return
        if not pkt.origin.is_delegate:
            self.logger.debug(
                "origin %s is not delegate, normal node won't handle it",
                pkt.origin.id,
            )
            return

        self.received_delegate_data.setdefault(pkt.origin.id, set()).add(pkt)
        data = self.extract_majority(self.received_delegate_data)
        if data is not None:
            self.normal_update(data)
            self.normal_phase_done = True
            self.queue_manager.broadcast(
                self.normal_data(), filter_=delegate_only
            )
            self.scene_end = self.check_end_in_data(data)

    def delegate_forward(self, pkt: QueuedPacket):
        if not self.local.is_delegate:
            self.logger.debug("local is not delegate node, abort",)
            return
        if not pkt.origin.is_normal:
            self.logger.warning(
                "origin %s is not normal, delegate node won't forward it",
                pkt.origin.id,
            )
            return

        self.received_normal_data.setdefault(pkt.origin.id, set()).add(pkt)
        if pkt.data not in self.seen:
            self.seen.add(pkt.data)
        self.queue_manager.broadcast_forward(pkt, filter_=normal_only)


class SimpleAdd(SceneTypeI):
    final_round: int
    round_timeout_sec: float

    # packet: round ID 4B, value 4B, end flag 1B
    pkt_fmt: str = '>LLB'

    normal_value: int
    delegate_value: int

    def __init__(
        self,
        local,
        node_manager,
        queue_manager,
        final_round,
        round_timeout_sec,
    ):
        super().__init__(local, node_manager, queue_manager)
        self.final_round = final_round
        self.round_timeout_sec = round_timeout_sec
        self.normal_value = 0

    def check_end(self) -> bool:
        return self.round_id >= self.final_round

    def check_end_in_data(self, data: bytes) -> bool:
        try:
            return struct.unpack(self.pkt_fmt, data)[2] > 0
        except:
            return False

    def delegate_update(self):
        self.delegate_value = 0
        for node_id, pkts in self.received_normal_data:
            # this method is supposed to be called after clearup evil nodes
            assert len(pkts) == 1
            self.delegate_value += struct.unpack(self.pkt_fmt, pkts[0].data)[1]

    def normal_initiate(self):
        self.normal_value = 1

    def normal_update(self, data: bytes):
        self.normal_value = struct.unpack(self.pkt_fmt, data)[1] + 1

    def round_timeout(self) -> float:
        return self.round_timeout_sec

    def normal_data(self) -> bytes:
        return struct.pack(self.pkt_fmt, self.round_id, self.normal_value, 0)

    def delegate_data(self) -> bytes:
        return struct.pack(
            self.pkt_fmt,
            self.round_id,
            self.delegate_value,
            1 if self.check_end() else 0,
        )

    def is_packet_valid(self, pkt: QueuedPacket) -> bool:
        try:
            return struct.unpack(self.pkt_fmt, pkt.data)[0] == self.round_id
        except:
            return False
