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

    def evil_value(self,node_id:int,value: typing.List[float]):
        if self.ignore_all:
            return None
        if node_id in self.ignore:
            return None
        getter = self.diffrence.get(node_id)
        if not getter:
            return getter(value[0],value[1],value[2])
        else:
            return value

class DelegateEvil():
    def __init__(self):
        super().__init__()
    