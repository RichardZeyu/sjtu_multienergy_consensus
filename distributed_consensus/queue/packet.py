import typing
from ..core.node_manager import BaseNode


class QueuedPacket(typing.NamedTuple):
    origin: BaseNode
    received_from: typing.Optional[BaseNode]
    send_to: BaseNode
    data: bytes
    full_packet: typing.Optional[bytes]

    @property
    def is_forwarding(self) -> bool:
        return self.origin != self.received_from
