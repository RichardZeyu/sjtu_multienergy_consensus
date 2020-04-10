import logging
import struct
import typing
from abc import abstractmethod
from datetime import datetime, timedelta
from time import sleep

from ..core.node import Node
from ..core.node_manager import NodeManager
from ..sync_adapter import (
    QueuedPacket,
    QueueManagerAdapter,
    delegate_only,
    normal_only,
)
from .scene import AbstractScene, DataType, NodeDataMap

L = logging.getLogger(__name__)


class SceneTypeI(AbstractScene):
    round_id: int
    received_normal_data: NodeDataMap
    received_delegate_data: NodeDataMap

    normal_phase_done: bool
    scene_end: bool

    def __init__(
        self,
        local: Node,
        node_manager: NodeManager,
        adapter: QueueManagerAdapter,
        done_cb: typing.Callable,
    ):
        super().__init__(local, node_manager, adapter, done_cb)
        self.round_id = 0
        self.received_delegate_data = NodeDataMap('received_delegate_data')
        self.received_normal_data = NodeDataMap('received_normal_data')
        self.received_normal_data.preload(
            self.node_manager, normal_only, self.local
        )
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
    def is_packet_valid(self, pkt: QueuedPacket) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def data_type(self, pkt: QueuedPacket) -> DataType:
        raise NotImplementedError()

    def scene_complete(self):
        self.logger.info('scene completed.')

    def run(self):
        self.round_id = 1
        local_delegate_ending: bool = False
        self.seen.clear()

        # round 1 starts from step 3
        self.normal_initiate()
        self.normal_send()

        while not self.scene_end and not local_delegate_ending:
            # step 1
            if self.round_id > 1 and self.local.is_delegate:
                self.received_normal_data.drop_evil_nodes(
                    self.node_manager, self.adapter
                )
                self.delegate_update()
                # does not set self.scene_end immediately to handle nodes with
                # both delegate and normal roles, in which case normal packet
                # receiving phase still need to run after local delegate
                # decides to exit
                local_delegate_ending = self.check_end()
                self.delegate_send()
                self.received_normal_data.preload(
                    self.node_manager, normal_only, self.local
                )

            # first round doesn't need normal data update
            self.normal_phase_done = self.round_id == 1
            self.received_delegate_data.clear()
            now = datetime.utcnow()
            round_end = now + timedelta(seconds=self.round_timeout())
            self.logger.info(
                f'round {self.round_id} begin, will end by {round_end}'
            )

            # step 2 ~ 5, wrapped by receiving loop
            self.round(round_end)

            # add a gap for possible timing error. otherwise quick nodes
            # will send packet for next round but slow nodes may drop them due
            # to invalid round id
            sleep(2)

            self.round_id += 1
        self.scene_complete()
        self.done_cb()

    def round(self, round_end: datetime):
        while not self.scene_end:
            pkt = self.adapter.wait_next_pkt(run_till=round_end)
            if pkt is None:
                self.logger.info(
                    f'receive timeout, round {self.round_id} over'
                )
                break
            if not self.is_packet_valid(pkt):
                self.logger.warn(f'drop invalid packet {pkt}')
                continue
            # step 2 & 3
            self.normal_node_action(pkt)
            # step 4
            self.delegate_node_action(pkt)

    def normal_node_action(self, pkt: QueuedPacket):
        if self.normal_phase_done:
            self.logger.debug(
                f"normal data of round {self.round_id} has been finalized"
            )
            return
        if not self.local.is_normal:
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
        if self.data_type(pkt) != DataType.DelegateToNormal:
            self.logger.debug("data is not in DelegateToNormal type, ignore")
            return

        self.received_delegate_data.add(pkt)
        data = self.extract_majority(self.received_delegate_data)
        if data is not None:
            self.normal_update(data)
            self.normal_phase_done = True
            self.normal_send()
            self.scene_end = self.check_end_in_data(data)

    def delegate_node_action(self, pkt: QueuedPacket):
        if not self.local.is_delegate:
            self.logger.debug("local is not delegate node, abort",)
            return
        if not pkt.origin.is_normal:
            self.logger.warning(
                "origin %s is not normal, delegate node won't forward it",
                pkt.origin.id,
            )
            return
        if pkt.origin.id not in self.received_normal_data.all.keys():
            self.logger.warn(
                'unexpected pkt %r, expecting %s',
                pkt,
                tuple(self.received_normal_data.all.keys()),
            )
            return
        if self.data_type(pkt) != DataType.NormalToDelegate:
            self.logger.debug("data is not in NormalToDelegate type, ignore")
            return

        self.received_normal_data.add(pkt)
        self.delegate_forward(pkt, delegate_only)


class SimpleAdd(SceneTypeI):
    final_round: int
    round_timeout_sec: float

    normal_value: int
    delegate_value: int

    class _Data:
        data_type: DataType
        round_id: int
        value: int
        is_end: bool

        # packet: data type 1B, round ID 4B, value 4B, end flag 1B
        pkt_fmt: str = '>BLLB'

        __slots__ = ['data_type', 'round_id', 'value', 'is_end']

        def __init__(
            self, data_type: DataType, round_id: int, value: int, is_end: bool
        ):
            self.data_type = data_type
            self.round_id = round_id
            self.value = value
            self.is_end = is_end

        def __repr__(self):
            return (
                f'<{self.data_type.name} round_id={self.round_id} '
                + f'value={self.value}{" end" if self.is_end else ""}>'
            )

        @classmethod
        def from_bytes(cls, bs: bytes) -> 'SimpleAdd._Data':
            type_, round_id, value, is_end = struct.unpack(cls.pkt_fmt, bs)
            return cls(DataType(type_), round_id, value, is_end > 0)

        def pack(self) -> bytes:
            return struct.pack(
                self.pkt_fmt,
                self.data_type.value,
                self.round_id,
                self.value,
                1 if self.is_end else 0,
            )

    def __init__(self, final_round, round_timeout_sec, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.final_round = final_round
        self.round_timeout_sec = round_timeout_sec
        self.normal_value = 0

    def check_end(self) -> bool:
        return self.round_id >= self.final_round

    def check_end_in_data(self, data: bytes) -> bool:
        try:
            return self._Data.from_bytes(data).is_end
        except:
            return False

    def delegate_update(self):
        self.delegate_value = self.normal_value
        for node_id, pkts in self.received_normal_data.all.items():
            # this method is supposed to be called after clearup evil nodes
            pkt = list(pkts)[0]
            self.delegate_value += self._Data.from_bytes(pkt.data).value
        self.logger.info(f'update delegate value to {self.delegate_value}')

    def normal_initiate(self):
        self.normal_value = 1

    def normal_update(self, data: bytes):
        self.normal_value = self._Data.from_bytes(data).value + 1
        self.logger.info(f'update normal value to {self.normal_value}')

    def round_timeout(self) -> float:
        return self.round_timeout_sec

    def normal_data(self) -> bytes:
        return self._Data(
            DataType.NormalToDelegate, self.round_id, self.normal_value, False
        ).pack()

    def delegate_data(self) -> bytes:
        return self._Data(
            DataType.DelegateToNormal,
            self.round_id,
            self.delegate_value,
            self.check_end(),
        ).pack()

    def is_packet_valid(self, pkt: QueuedPacket) -> bool:
        try:
            data = self._Data.from_bytes(pkt.data)
            self.logger.debug(
                'got scene data %r by %d via %s',
                data,
                pkt.origin.id,
                pkt.received_from.id if pkt.received_from else 'unknown',
            )
            return data.round_id == self.round_id
        except:
            self.logger.warn(f'cannot parse {pkt}', exc_info=True)
            return False

    def data_type(self, pkt: QueuedPacket) -> DataType:
        return self._Data.from_bytes(pkt.data).data_type

    def scene_complete(self):
        self.logger.info('scene complete, final values:')
        if self.local.is_delegate:
            self.logger.info(f'delegate value: {self.delegate_value}')
        if self.local.is_normal:
            self.logger.info(f'normal value: {self.normal_value}')
