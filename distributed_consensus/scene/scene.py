import logging
import typing
from abc import ABC, abstractmethod
from collections import Counter
from enum import Enum

from ..core.node import Node
from ..core.node_manager import BaseNode, NodeManager
from ..queue.manager import DataGetter
from ..sync_adapter import (
    QueuedPacket,
    QueueManagerAdapter,
    NodeFilter,
    all_node,
    delegate_only,
    normal_only,
)

L = logging.getLogger(__name__)


class NodeDataMap:
    # it seems keeping node_id-bytes map is sufficient but here I insist
    # storing the whole queued packet (with sender/forwarder) for better
    # debugging ability
    table: typing.Dict[int, typing.Set[QueuedPacket]]

    name: str
    logger: logging.Logger

    def __init__(self, name: str):
        super().__init__()
        self.table = dict()
        self.name = name
        self.logger = L.getChild(f'{self.__class__.__name__}-{self.name}')

    def add(self, pkt: QueuedPacket):
        """ add a received packet to table """
        self.table.setdefault(pkt.origin.id, set()).add(pkt)

    def preload(
        self,
        node_manager: NodeManager,
        filter_: NodeFilter,
        local: BaseNode,
        clear: bool = True,
    ):
        """ preload table with node IDs from which at least one packet is
        supposed to be received.

            local node and blacked nodes will be excluded from expecting list
            no matter filter keeps them or not
        """
        if clear:
            self.clear()
        for node in node_manager.nodes():
            if filter_(node) and node != local and not node.is_blacked:
                self.table[node.id] = set()

    def clear(self):
        self.table.clear()

    def extract_majority(
        self,
        initial: typing.Optional[typing.List[bytes]] = None,
        threshold: float = -1,
    ) -> typing.Optional[bytes]:
        counter: typing.Counter[bytes] = Counter()
        if initial is not None:
            counter.update({i: 1 for i in initial})
        counter.update(
            [
                list(pkts)[0].data
                for pkts in self.table.values()
                if len(pkts) == 1
            ]
        )
        if not counter:
            return None
        top, freq = counter.most_common(1)[0]
        return top if freq > threshold else None
    def extract_majority_adapt(self,threshold: float = -1):
        for pkts in self.table.values():
            pkt = list(pkts)[0]
            self.logger.warning(f'{pkt}')
        counter: typing.Counter[bytes] = Counter()
        counter.update(
            [
                list(pkts)[0].data
                for pkts in self.table.values()
                if len(pkts) == 1
            ]
        )
        if not counter:
            return None
        top, freq = counter.most_common(1)[0] # 出现频率最高的一个单词
        self.logger.warning(f'top = {top} freq = {freq} threshold={threshold}')
        return top if freq >= threshold else None
    # 移除作恶节点
    def drop_evil_nodes(
        self,
        node_manager: typing.Optional[NodeManager],
        adapter: typing.Optional[QueueManagerAdapter],
    ):
        self.logger.debug(f'normal data before filtering {self.table}')
        # 统计data,如果没有发送数据，那么节点是作恶节点
        node_recv_num = [
            (node_id, len({pkt.data for pkt in pkt_set}))
            for (node_id, pkt_set) in self.table.items()
        ]
        # 作恶的情况下，
        evil_node_ids = filter(lambda x: x[1] != 1, node_recv_num)

        for evil, pkt_num in evil_node_ids:
            self.logger.warn(f'remote node {evil} send {pkt_num} packets')
            self.logger.warn(f'all packets from node {evil} are dropped')
            del self.table[evil]
            if node_manager and adapter:
                node_manager.block(evil)
                evil_node = node_manager.get_node(evil)
                if evil_node is not None and isinstance(evil_node, Node):
                    adapter.drop_node(evil_node)
                self.logger.warn(f'node {evil} has been blocked')
        self.logger.debug(f'normal data after filtering {self.table}')

    @property
    def all(self) -> typing.Dict[int, typing.Set[QueuedPacket]]:
        return self.table

    def __repr__(self):
        return f'<{self.name} {self.all!r}>'


class DataType(Enum):
    Unknown = 0
    DelegateToNormal = 1
    NormalToDelegate = 2
    LeaderToDelegate = 3


class AbstractScene(ABC):
    local: Node
    node_manager: NodeManager
    adapter: QueueManagerAdapter
    done_cb: typing.Callable
    seen: typing.Set[typing.Tuple[int, bytes]]
    local_delegate_data: typing.Optional[bytes]
    logger: logging.Logger

    def __init__(
        self,
        local: Node,
        node_manager: NodeManager,
        adapter: QueueManagerAdapter,
        done_cb: typing.Callable,
    ):
        self.seen = set()
        self.local = local
        self.adapter = adapter
        self.node_manager = node_manager
        self.done_cb = done_cb  # type: ignore  # mypy issue #708
        self.local_delegate_data = None
        self.logger = L.getChild(f'{self.__class__.__name__}-{self.local.id}')

    def normal_send(self):
        data = self.normal_data()
        # 广播消息 filter_相当于一个代理方法(表达式、回调)
        self.adapter.broadcast(data, filter_=delegate_only)

    def delegate_send(self):
        data = self.delegate_data()
        self.adapter.broadcast(data, filter_=normal_only)
        # no loopback forwarding so local node will not receive this data from
        # micronet. remember data for normal phase (if local node is also a
        # normal node)
        self.local_delegate_data = data
    def delegate_send_adapt(self,getter: DataGetter):
        self.adapter.broadcast_adapt(getter, filter_=normal_only)
        # self.local_delegate_data = data
    def leader_send(self):
        data = self.leader_data()
        self.adapter.broadcast(data, filter_=all_node)
        # no loopback forwarding so local node will not receive this data from
        # micronet. remember data for normal phase (if local node is also a
        # normal node)
        self.local_delegate_data = data
    # 代表转发 
    # seen 存储已经转发过的node，如果没有转发，则转发出去。（以此来避免数据在各个代表中循环转发）
    def delegate_forward(self, pkt: QueuedPacket, filter_: NodeFilter) -> bool:
        # seen?这个不用每轮清空吗?
        if (pkt.origin.id, pkt.data) not in self.seen: 
            self.seen.add((pkt.origin.id, pkt.data))
            self.adapter.broadcast_forward(pkt, filter_=filter_)
            return True
        return False

    def extract_majority(self, received: NodeDataMap):
        # local_delegate_data 为本地代表发送的数据
        # received 为 received_delegate_data，从代表中接收到的数据
        initial: typing.Optional[typing.List[bytes]]
        self.logger.debug(f'trying to extract majority from {received}')
        if self.local.is_delegate and self.local_delegate_data is not None:
            initial = [self.local_delegate_data]
        else:
            initial = None
        return received.extract_majority(
            initial=initial, threshold=self.node_manager.delegate_num / 2,
        )
    def extract_majority_adapt(self,received: NodeDataMap):
        return received.extract_majority_adapt(
            threshold=self.node_manager.delegate_num / 2,
        )
    def is_delegate_packet(
        self, pkt: QueuedPacket, expected_data_type: typing.Optional[DataType]
    ) -> bool:
        assert expected_data_type in (
            None,
            DataType.LeaderToDelegate,
            DataType.DelegateToNormal,
        ), f'{expected_data_type} is not a valid delegate packet type'

        if pkt.received_from is None:
            self.logger.debug("receive_from is None, not from delegate",)
            return False
        if not pkt.received_from.is_delegate:
            self.logger.debug(
                "remote %s is not delegate", pkt.received_from.id,
            )
            return False
        if not pkt.origin.is_delegate:
            self.logger.debug(
                "origin %s is not delegate", pkt.origin.id,
            )
            return False
        data_type = self.data_type(pkt)
        if data_type in (DataType.NormalToDelegate, DataType.Unknown):
            self.logger.debug(f"data is of {data_type.name} type")
            return False
        if expected_data_type is not None and data_type != expected_data_type:
            self.logger.debug(f"data is not of {expected_data_type.name} type")
            return False
        return True

    def is_normal_packet(self, pkt: QueuedPacket) -> bool:
        if not pkt.origin.is_normal:
            self.logger.warning(
                "origin %s is not normal", pkt.origin.id,
            )
            return False
        if self.data_type(pkt) != DataType.NormalToDelegate:
            self.logger.debug("data is not in NormalToDelegate type")
            return False
        return True

    def broadcast_for_consensus(self, pkt: QueuedPacket) -> bool:
        if not self.local.is_delegate:
            self.logger.debug("local is not delegate node, abort",)
            return False
        if not self.is_normal_packet(pkt):
            self.logger.debug(f"{pkt} is not from normal node, ignore",)
            return False

        self.delegate_forward(pkt, delegate_only)
        return True

    @abstractmethod
    def run(self):
        raise NotImplementedError()
    
    def is_packet_valid(self, pkt: QueuedPacket) -> bool:
        raise NotImplementedError()

    def normal_data(self) -> bytes:
        raise NotImplementedError()

    def delegate_data(self) -> bytes:
        raise NotImplementedError()

    def leader_data(self) -> bytes:
        raise NotImplementedError()

    def data_type(self, pkt: QueuedPacket) -> DataType:
        raise NotImplementedError()
