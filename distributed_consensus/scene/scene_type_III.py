import typing
import struct
from .scene import AbstractScene, DataType, NodeDataMap,SceneTypeI


# 场景3
class SceneTypeIII(AbstractScene):
    def __init__(self):
        pass
    def run(self):
        pass
    
class MultiEnergyPark(SceneTypeI):
    class _Data:
        data_type: DataType
        round_id: typing.List(int)
        value: int
        is_end: bool
        
        # packet: data type 1B, round ID 4B, value 4B, end flag 1B
        pkt_fmt: str = '>BLLB'

        __slots__ = ['data_type', 'round_id', 'value', 'is_end']


        def __init__(
            self, data_type: DataType, round_id: typing.List(int), value: int, is_end: bool
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
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)
