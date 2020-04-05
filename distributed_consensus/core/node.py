import typing

from .node_manager import AutoRegisterNode


class Node(AutoRegisterNode):
    ip: str
    port: int
    public_key: typing.Any
    private_key: typing.Any

    def __init__(
        self,
        id,
        ip,
        port,
        public_key,
        private_key=None,
        manager=None,
        is_delegate=False,
        is_normal=False,
        is_blacked=False,
    ):
        super().__init__(
            id,
            manager=manager,
            is_delegate=is_delegate,
            is_normal=is_normal,
            is_blacked=is_blacked,
        )
        self.ip = ip
        self.port = port
        self.public_key = public_key
        self.private_key = private_key
