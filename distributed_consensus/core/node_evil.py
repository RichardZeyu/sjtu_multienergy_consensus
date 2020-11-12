import typing
from typing import Dict,Set,Callable

EvilGetter = Callable[[float,float,float], typing.List[float]]

class NormalEvil():
    ignore:typing.List[int]
    ignore_all:bool
    diffrence:typing.Dict[int,EvilGetter]
    
    def __init__(self,ignore:typing.List[int],ignore_all:bool,diffrence:typing.Dict[int,EvilGetter]):
        self.ignore = ignore
        self.ignore_all = ignore_all
        self.diffrence = diffrence

    def evil_value(self,node_id:int,values: typing.List[float]):
        if self.ignore_all:
            return None
        if self.ignore and node_id in self.ignore:
            return None
        getter =None if not self.diffrence else self.diffrence.get(node_id)
        if getter:
            return getter(values[0],values[1],values[2])
        else:
            return values

class DelegateEvil():
    ignore:typing.List[int]
    ignore_all:bool
    from_nodes:typing.List[int]
    diffrence:typing.Dict[int,EvilGetter]
    
    def __init__(self,ignore:typing.List[int],ignore_all:bool,from_nodes:typing.List[int],diffrence:typing.Dict[int,EvilGetter]):
        self.ignore = ignore
        self.ignore_all = ignore_all
        self.diffrence = diffrence
        self.from_nodes = from_nodes
    def evil_value(self,node_id:int,values: typing.List[float]):
        if self.ignore_all:
            return None
        if self.ignore and node_id in self.ignore:
            return None
        return values
    def forward_evil_value(self,from_node_id:int,to_node_id:int,values: typing.List[float]):
        if not self.from_nodes or from_node_id not in self.from_nodes:
            return values
        getter =None if not self.diffrence else self.diffrence.get(to_node_id)
        if getter:
            return getter(values[0],values[1],values[2])
        return values
    