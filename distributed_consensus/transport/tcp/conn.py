import asyncio
import typing
from binascii import b2a_hex
from ipaddress import ip_address
from logging import getLogger

from ...core.node import Node
from ...core.node_manager import NodeManager
from ...queue.manager import QueueManager
from ...queue.packet import QueuedPacket
from .protocol import TCPProtocolV1

L = getLogger(__name__)


class TCPConnectionHandler:
    local_node: Node
    node_manager: NodeManager
    queue_manager: QueueManager
    protocol_class: typing.Type[TCPProtocolV1]
    pending_nodes: typing.Set[Node]
    nodes_incoming: asyncio.Semaphore
    server: typing.Optional[asyncio.AbstractServer]

    def __init__(
        self,
        local_node: Node,
        node_manager: NodeManager,
        queue_manager: QueueManager,
        protocol_class: typing.Type[TCPProtocolV1],
    ):
        super().__init__()
        self.local_node = local_node
        self.node_manager = node_manager
        self.queue_manager = queue_manager
        self.protocol_class = protocol_class
        self.logger = L.getChild('TCPConnectionHandler').getChild(
            f'{local_node.id}'
        )
        self.pending_nodes = set()
        self.nodes_incoming = asyncio.Semaphore(0)

    async def send_handshake(
        self, writer: asyncio.StreamWriter, protocol: TCPProtocolV1,
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
        self, reader: asyncio.StreamReader, protocol: TCPProtocolV1
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
        protocol = self.protocol_class(
            self.node_manager, self.local_node, remote
        )
        self.logger.info(
            'add connection from %s to %s (%s)',
            self.local_node.id,
            remote.id if remote is not None else 'unknown',
            writer.get_extra_info('peername'),
        )
        if remote is None:
            remote = await self.wait_handshake(reader, protocol)
            ok = remote is not None
            if remote is not None and remote.is_blacked:
                self.logger.warn(f'remote {remote.id} is blocked, disconnect')
                ok = False
        else:
            ok = await self.send_handshake(writer, protocol)
        if not ok:
            self.logger.warn('handshake failed, close connection')
            writer.close()
            await writer.wait_closed()
        else:
            assert remote is not None
            if remote in self.pending_nodes:
                self.pending_nodes.remove(remote)
                self.nodes_incoming.release()
            asyncio.create_task(
                self.dispatch(remote, reader, writer, protocol)
            )

    async def dispatch(
        self,
        remote: Node,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        protocol: TCPProtocolV1,
    ):
        ingress, egress = self.queue_manager.create_queue(remote)
        try:
            self.data_loop(ingress, egress, reader, writer, protocol)
        except asyncio.CancelledError:
            self.logger.info('data loop exited due to connection close')
        except:  # noqa
            self.logger.exception('data loop exited with exception')
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            self.queue_manager.close(remote)

    async def data_loop(
        self,
        ingress: asyncio.Queue[QueuedPacket],
        egress: asyncio.Queue[QueuedPacket],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        protocol: TCPProtocolV1,
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
                to_send = outbound.result()
                writer.write(protocol.encode(to_send))
                outbound = asyncio.create_task(egress.get())
            if inbound in done:
                try:
                    bs = inbound.result()
                except asyncio.IncompleteReadError as ex:
                    self.logger.error(
                        'recv receive early EOF, got: %s', b2a_hex(ex.partial),
                    )
                    raise
                while protocol.num_to_read() <= 0:
                    pkt = protocol.decode(bs)
                    if pkt is None:
                        assert (
                            protocol.num_to_read() > 0
                        ), 'cannot consume buffer'
                        break
                    else:
                        await ingress.put(pkt)
                inbound = asyncio.create_task(
                    reader.readexactly(protocol.num_to_read())
                )

    async def connect_to(self, node: Node):
        reader, writer = await asyncio.open_connection(node.ip, node.port)
        await self.add_connection(reader, writer, node)

    async def connect_in_background(self, node: Node) -> asyncio.Task:
        return asyncio.create_task(self.connect_to(node))

    async def listen_from(self, node: Node = None) -> asyncio.AbstractServer:
        node = self.local_node if node is None else node
        server = await asyncio.start_server(
            self.add_connection, node.ip, node.port
        )
        self.server = server
        return server

    async def wait_micronet_up(self, event: asyncio.Event):
        await self.nodes_incoming.acquire()
        if not self.pending_nodes:
            event.set()

    async def setup_micronet(self) -> asyncio.Event:
        micronet_ok = asyncio.Event()
        self.nodes_incoming = asyncio.Semaphore(0)

        def sort_key(n: Node):
            return (int(ip_address(n.ip)) << 16) | n.port

        over_local: bool = False

        await self.listen_from()

        all_nodes = [
            node for node in self.node_manager._all if isinstance(node, Node)
        ]

        for node in sorted(all_nodes, key=sort_key):
            if node is self.local_node:
                self.logger.debug(f'node {node.id} is local, continue')
                over_local = True
            elif over_local:
                self.logger.debug(f'connect to node {node.id}')
                self.connect_in_background(node)
                self.pending_nodes.add(node)
            else:
                self.logger.debug(f'expect node {node.id} to connect')
                self.pending_nodes.add(node)

        asyncio.create_task(self.wait_micronet_up(micronet_ok))

        return micronet_ok

    async def setup_and_wait_micronet(self, timeout: float) -> bool:
        event = await self.setup_micronet()
        try:
            return await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self.logger.error('setup micronet timeout')
            await self.tear_down_micronet()
        return False

    async def tear_down_micronet(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        for node in self.node_manager._all:
            if isinstance(node, Node):
                self.queue_manager.close(node)
