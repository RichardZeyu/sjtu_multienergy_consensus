import asyncio
import typing
from datetime import datetime

from ..queue.manager import AddressedPacket, QueueManager


def wait_next_pkt(
    manager: QueueManager,
    timeout: typing.Optional[float] = None,
    run_till: typing.Optional[datetime] = None,
    loop: typing.Optional[asyncio.BaseEventLoop] = None,
) -> AddressedPacket:
    assert not all(
        x is not None for x in [timeout, run_till]
    ), 'timeout and run_till cannot be specified at same time'

    _loop = loop or asyncio.get_running_loop()
    if run_till is not None:
        timeout = (run_till - datetime.utcnow()).total_seconds()
    if timeout is not None and timeout <= 0:
        raise asyncio.TimeoutError()
    recv = manager.receive_one(timeout=timeout)
    return _loop.run_until_complete(recv)
