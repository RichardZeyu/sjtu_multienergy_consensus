import typing

from .node_manager import AutoRegisterNode
from OpenSSL.crypto import PKey
from pprint import pformat
from ipaddress import ip_address


class Node(AutoRegisterNode):
    ip: str
    port: int
    public_key: typing.Union[str, PKey]
    private_key: typing.Union[str, PKey, None]

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

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            'id': self.id,
            'ip': self.ip,
            'port': self.port,
            'public_key': self.public_key,
            'private_key': self.private_key,
            'is_delegate': self.is_delegate,
            'is_normal': self.is_normal,
            'is_blacked': self.is_blacked,
        }

    def __repr__(self):
        dict_str = (
            pformat(self.to_dict(), width=10000, compact=True)
            .strip("{}")
            .replace(', ', ' ')
            .replace(': ', ':')
        )
        return f'<Node {dict_str}>'

    def sort_key(self) -> int:
        return (int(ip_address(self.ip)) << 16) | self.port

    def __lt__(self, value):
        return self.sort_key() < value.sort_key()
