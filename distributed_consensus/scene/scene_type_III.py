import typing
import struct
import pandas as pd
from abc import abstractmethod
from datetime import datetime, timedelta
from time import sleep

from .scene_type_I import SceneTypeI
from ..core.node import Node
from ..core.node_manager import NodeManager
from ..sync_adapter import QueuedPacket, QueueManagerAdapter, normal_only
from .scene import AbstractScene, DataType, NodeDataMap
from .consensus.nodeUpdate import NodeUpdate

# from .consensus.delegateCheckEnd import delegate_checkEnd
# from .consensus.delegateUpdate import delegate_update
# from .consensus.normalNodeUpdate import normal_node_update

# 场景3
class SceneTypeIII(AbstractScene):
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
    def delegate_getter(self, int)-> float:
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

    def scene_complete(self):
        self.logger.info('scene completed.')

    def run(self):
        self.round_id = 1
        local_delegate_ending: bool = False
        self.seen.clear()

        if self.local.is_normal:
            # round 1 starts from step 3
            self.normal_initiate()
            # 不用初始化数据，__init__方法里有初始化
            # 初始需求量发送
            self.normal_send()
        # delegate 代表
        while not self.scene_end and not local_delegate_ending:
            # step 1
            if self.round_id > 1 and self.local.is_delegate:
                # 第二步是代表共识，排除作恶节点
                # 更新数据并发回给普通节点
                self.received_normal_data.drop_evil_nodes(
                    self.node_manager, self.adapter
                )
                self.delegate_update()
                # does not set self.scene_end immediately to handle nodes with
                # both delegate and normal roles, in which case normal packet
                # receiving phase still need to run after local delegate
                # decides to exit
                local_delegate_ending = self.check_end()
                # self.delegate_send()
                self.delegate_send_adapt(self.delegate_getter)
                self.received_normal_data.preload(
                    self.node_manager, normal_only, self.local
                )

            # first round doesn't need normal data update

            # 第一轮

            self.normal_phase_done = self.round_id == 1
            self.received_delegate_data.clear()
            now = datetime.utcnow()
            round_end = now + timedelta(seconds=self.round_timeout())
            self.logger.info(
                f'round {self.round_id} begin, will end by {round_end}'
            )

            # step 2 ~ 5, wrapped by receiving loop

            # 获取数据

            # 第一步是代表接收转发 （直到超时跳出循环）
            # 第二步继续就是第三步，普通节点接收到代表的数据，更新再次发送

            self.round(round_end)

            # add a gap for possible timing error. otherwise quick nodes
            # will send packet for next round but slow nodes may drop them due
            # to invalid round id
            sleep(2)

            self.round_id += 1
        self.scene_complete()
        self.done_cb()
    # round 包含的逻辑：
    # 代表接收到普通节点发送的数据，转发给其他代表（包括普通节点）
    # 其他代表获取到数据，添加到缓存
    # 普通节点获取到数据，更新发送给所有代表，scene_end = true
    def round(self, round_end: datetime):
        while not self.scene_end:
            pkt = self.adapter.wait_next_pkt(run_till=round_end)
            if pkt is None:
                # 超时结束
                self.logger.info(
                    f'receive timeout, round {self.round_id} over'
                )
                break
            # 解压数据并且判断是否是当前一轮的数据
            if not self.is_packet_valid(pkt):
                # 如果不是当前一轮数据，或者数据不能unpack，弃包 继续循环
                self.logger.warn(f'drop invalid packet {pkt}')
                continue
            # step 2 & 3
            self.normal_node_action(pkt)
            # step 4 pkt包含了remote等信息，看看是否是根据这个来回传给普通节点
            self.delegate_node_action(pkt)
    # 普通节点处理，把来自代表的数据进行处理
    def normal_node_action(self, pkt: QueuedPacket):
        if self.normal_phase_done:
            self.logger.debug(
                f"normal data of round {self.round_id} has been finalized"
            )
            return
        if not self.local.is_normal:
            self.logger.debug("local is not normal node, abort",)
            return
        if not self.is_delegate_packet(pkt, DataType.DelegateToNormal):
            self.logger.debug(
                f"{pkt} not from delegate, normal node won't handle it",
            )
            return

        self.received_delegate_data.add(pkt)
        # 下面函数是用来判断
        # 如果接收到大部分的代表返回的数据，那么就更新数据再广播给代表
        data = self.extract_majority(self.received_delegate_data)
        if data is not None:
            self.normal_update(data)
            self.normal_phase_done = True
            self.normal_send()
            self.scene_end = self.check_end_in_data(data)
    # 代表节点处理，把来自普通节点的数据进行处理
    def delegate_node_action(self, pkt: QueuedPacket):
        # 广播 如果该节点已经进来过了，直接退出（无论是从代表转发或者是微网发送过来），来自代表节点也直接退出
        # 转发完成，把数据添加到received_normal_data中
        if not self.broadcast_for_consensus(pkt):
            # implying pkt is a normal packet
            return
        # 程序初始化时已经初始化received_normal_data了，所有普通节点的id都在里面了。
        if pkt.origin.id not in self.received_normal_data.all.keys():
            self.logger.warn(
                'pkt %r from unexpected normal node, expecting %s',
                pkt,
                tuple(self.received_normal_data.all.keys()),
            )
            return
        # 数据添加到缓存map中
        self.received_normal_data.add(pkt)
    
class MultiEnergyPark(SceneTypeIII):
    final_round: int
    round_timeout_sec: float

    normal_value: typing.List[float]
    delegate_value: () # 本轮的价格
    pre_delegate_value: () # 上一轮的价格
    

    node_update: NodeUpdate
    # excel文档路径
    # first_demand: str
    # demand: str
    # price_ge: str
    # hub: str
    class _Data:
        data_type: DataType
        round_id: int
        value: typing.List[float]
        is_end: bool
        
        # packet: data type 1B, round ID 4B, value 4B, end flag 1B
        pkt_fmt: str = '>BLdddB'

        __slots__ = ['data_type', 'round_id', 'value', 'is_end']


        def __init__(
            self, data_type: DataType, round_id: int , value: typing.List[float], is_end: bool
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
            values = struct.unpack(cls.pkt_fmt, bs)
            type_ = values[0]
            round_id = values[1]
            value = values[2:5]
            is_end = values[5]
            return cls(DataType(type_), round_id, value, is_end > 0)

        def pack(self) -> bytes:    
            return struct.pack(
                self.pkt_fmt,
                self.data_type.value,
                self.round_id,
                *self.value,
                1 if self.is_end else 0,
            )
    def __init__(self, final_round, round_timeout_sec,first_demand,demand,price_ge, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.final_round = final_round
        self.round_timeout_sec = round_timeout_sec
        # self.normal_value = self.demand_value(first_demand)
        self.node_update = NodeUpdate(demand,price_ge,self.local)
        # self.demand = demand
        # self.price_ge = price_ge
        # self.hub = hub
    
    # 这个可能需要改为nodeUpdate的checkend
    def check_end(self) -> bool:
        return self.node_update.delegate_checkEnd(*self.pre_delegate_value,*self.delegate_value)
        # return self.round_id >= self.final_round

    # 这个不需要改
    def check_end_in_data(self, data: bytes) -> bool:
        try:
            return self._Data.from_bytes(data).is_end
        except:
            return False
    # 这个需要更改为nodeUpdate中的delegate_update
    def delegate_update(self):
        # if self.local.is_normal:
            # pass
            # also take my own value into consideration since node won't
            # receive its own packet from micronet
            # self.delegate_value = self.normal_value
        # else:
            # self.delegate_value = 0
        self.pre_delegate_value = self.delegate_value
        gd = typing.List[float]
        ed = typing.List[float]
        hd = typing.List[float]
        for _, pkts in self.received_normal_data.all.items():
            # this method is supposed to be called after clearup evil nodes
            pkt = list(pkts)[0]
            values = self._Data.from_bytes(pkt.data).value
            gd.append(values[0])
            ed.append(values[1])
            hd.append(values[2])
            # self.delegate_value += self._Data.from_bytes(pkt.data).value
        price = self.node_update.init_price()
        gp,ep,hp = self.node_update.delegate_update(gd,ed,hd,*price,self.round_id)
        self.delegate_value = (gp,ep,hp)
        self.logger.info(f'update delegate value to {self.delegate_value}')

    def normal_initiate(self):
        # self.normal_value = self.demand_value(self.first_demand)
        self.normal_value = self.node_update.init_demand()
    def delegate_getter(self,node_id: int):
        values = self.delegate_value
        gp = values[0]
        ep = values[1]
        hp = values[2]
        index = (node_id-1)/4
        send = [gp,ep,hp[index]]
        return self._Data(
            DataType.DelegateToNormal,
            self.round_id,
            send,
            self.check_end(),
        ).pack()
    # 普通节点更新数据
    # 需要使用nodeUpdate来进行update
    def normal_update(self, data: bytes):
        value = self._Data.from_bytes(data).value
        gd,ed,hd,cost_min = self.node_update.normal_node_update(*value)
        self.normal_value = [gd,ed,hd]
        # self.normal_value = self._Data.from_bytes(data).value + 1
        self.logger.info(f'update normal value to {self.normal_value}')

    def round_timeout(self) -> float:
        return self.round_timeout_sec

    def normal_data(self) -> bytes:
        return self._Data(
            DataType.NormalToDelegate, self.round_id, self.normal_value, False
        ).pack()
        # real = self._Data.from_bytes(data)

    def delegate_data(self) -> bytes:
        return self._Data(
            DataType.DelegateToNormal,
            self.round_id,
            self.delegate_value,
            self.check_end(),
        ).pack()
    
    # 判断数据是否能unpack，并且判断是否是当前一轮数据
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
    
    def demand_value(self,demand_route) -> typing.List[int]:
        demand = pd.read_excel(demand_route, 'Sheet1')
        gasdemand = demand.loc[0,self.local.id] # 气需求
        elecdemand = demand.loc[1,self.local.id] # 电需求
        heatdemand = demand.loc[2,self.local.id] # 热需求
        return [gasdemand,elecdemand,heatdemand]
        
    
    
