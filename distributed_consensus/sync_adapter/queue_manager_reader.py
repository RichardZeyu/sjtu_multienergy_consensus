import asyncio
import typing

from ..queue.manager import AddressedPacket, QueueManager


def wait_next_pkt(
    manager: QueueManager, loop: typing.Optional[asyncio.BaseEventLoop] = None,
) -> AddressedPacket:
    _loop = loop or asyncio.get_running_loop()
    recv = manager.receive_one()
    return _loop.run_until_complete(recv)
