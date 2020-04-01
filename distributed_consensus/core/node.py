import typing

from .node_manager import AutoRegisterNode


class Node(AutoRegisterNode):
    id: int
    ip: str
    port: int
    public_key: typing.Any
    private_key: typing.Any

    def __init__(self, id, ip, port, public_key, private_key=None, manager=None):
        super().__init__(id, manager=manager)
        self.ip = ip
        self.port = port
        self.public_key = public_key
        self.private_key = private_key
