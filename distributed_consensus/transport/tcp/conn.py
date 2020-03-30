import asyncio
import typing
from binascii import b2a_hex
from functools import partial
from logging import getLogger

from ...core.node import Node
from .protocol import TCPProtocol

L = getLogger(__name__)


class TCPConnectionHandler:
    local_node: Node
    queue_manager: typing.Any
    protocol_class: typing.Type[TCPProtocol]

    def __init__(
        self,
        local_node: Node,
        queue_manager,
        protocol_class: typing.Type[TCPProtocol],
    ):
        super().__init__()
        self.local_node = local_node
        self.queue_manager = queue_manager
        self.protocol_class = protocol_class
        self.logger = L.getChild('TCPConnectionHandler').getChild(
            local_node.id
        )

    async def send_handshake(
        self, writer: asyncio.StreamWriter, protocol: TCPProtocol,
    ):
        writer.write(protocol.generate_handshake())
        try:
            await asyncio.wait_for(writer.drain(), protocol.handshake_timeout)
        except asyncio.TimeoutError:
            self.logger.error(
                'sending handshake to remote %s timeout',
                writer.get_extra_info('peername'),
            )
            return False
        return True

    async def wait_handshake(
        self, reader: asyncio.StreamReader, protocol: TCPProtocol
    ) -> typing.Optional[Node]:
        bs = b''
        try:
            bs = await asyncio.wait_for(
                reader.readexactly(protocol.handshake_recv_len),
                timeout=protocol.handshake_timeout,
            )
        except asyncio.TimeoutError:
            self.logger.error('recv handshake from remote timeout',)
            return None
        except asyncio.IncompleteReadError as ex:
            self.logger.error(
                'recv handshake receive early EOF, got: %s',
                b2a_hex(ex.partial),
            )
            return None
        remote = protocol.parse_handshake(bs)
        return remote

    async def add_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        remote: typing.Optional[Node] = None,
    ):
        """ schedule a dispatching task for given connection

            remote(optional): the remote node. if None, wait for remote to
            initiate handshake procedure to know its identity. Otherwise use
            given remote node identity and initiate handshake from local side
        """
        ok: bool = False
        protocol = self.protocol_class(self.local_node, remote)
        self.logger.info(
            'add connection from %s to %s (%s)',
            self.local_node.id,
            remote.id if remote is not None else 'unknown',
            writer.get_extra_info('peername'),
        )
        if remote is None:
            remote = await self.wait_handshake(reader, protocol)
            ok = remote is not None
        else:
            ok = await self.send_handshake(writer, protocol)
        if not ok:
            self.logger.warn('handshake failed, close connection')
            writer.close()
            await writer.wait_closed()
        else:
            asyncio.create_task(
                self.dispatch(remote, reader, writer, protocol)
            )

    async def dispatch(
        self,
        remote: Node,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        protocol: TCPProtocol,
    ):
        ingress, egress = self.queue_manager.create_queue(
            self.local_node, remote
        )
        try:
            self.data_loop(ingress, egress, reader, writer, protocol)
        except:  # noqa
            self.logger.exception('data loop exited with exception')
        finally:
            if writer.is_closing():
                writer.close()
                await writer.wait_closed()
            self.queue_manager.close(self.local_node, remote)

    async def data_loop(
        self,
        ingress: asyncio.Queue,
        egress: asyncio.Queue,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        protocol: TCPProtocol,
    ):
        outbound = asyncio.create_task(egress.get())
        inbound = asyncio.create_task(
            reader.readexactly(protocol.num_to_read())
        )
        while True:
            done, pending = await asyncio.wait(
                {outbound, inbound}, return_when=asyncio.FIRST_COMPLETED
            )
            if outbound in done:
                pkt = outbound.result()
                writer.write(protocol.encode(pkt))
                outbound = asyncio.create_task(egress.get())
            if inbound in done:
                try:
                    bs = inbound.result()
                except asyncio.IncompleteReadError as ex:
                    self.logger.error(
                        'recv handshake receive early EOF, got: %s',
                        b2a_hex(ex.partial),
                    )
                    raise
                pkt = protocol.decode(bs)
                if pkt:
                    await ingress.put(pkt)
                inbound = asyncio.create_task(
                    reader.readexactly(protocol.num_to_read())
                )


async def connect_to(node: Node, h: TCPConnectionHandler):
    reader, writer = await asyncio.open_connection(node.ip, node.port)
    await h.add_connection(reader, writer, node)


async def handle_client(
    h: TCPConnectionHandler,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
):
    await h.add_connection(reader, writer)


async def listen_from(node: Node, h: TCPConnectionHandler):
    server = await asyncio.start_server(
        partial(handle_client, h), node.ip, node.port
    )
    return server
