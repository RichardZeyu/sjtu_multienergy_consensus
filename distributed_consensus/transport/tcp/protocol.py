import struct
import typing
from binascii import b2a_hex
from datetime import datetime
from io import BytesIO, SEEK_SET, SEEK_END
from logging import getLogger

from ...core.node import Node
from ...core.node_manager import NodeManager
from ...crypto import SHA256, sign, verify

L = getLogger(__name__)


class TCPProtocolV1:
    version: int = 1

    digest: str = SHA256

    # version 1B, timestamp 8B, node ID 4B, 4096bits RSA sign 512B
    handshake_struct: str = '>BQL512s'

    handshake_timeout: int = 5

    # data len 4B
    packet_header_struct: str = '>L'

    # instance properties
    local: Node
    remote: Node
    handshake_recv_len: int
    header_recv_len: int
    signature_len: int
    buf: BytesIO
    node_manager: NodeManager

    def __init__(
        self, node_manager: NodeManager, local: Node, remote: Node = None
    ):
        super().__init__()
        self.node_manager = node_manager
        self.local = local
        self.remote = remote
        self.handshake_recv_len = struct.calcsize(self.handshake_struct)
        self.header_recv_len = struct.calcsize(self.packet_header_struct)
        self.signature_len = 4096 // 8
        self.version = 1
        self.buf = BytesIO()
        if remote:
            # local is client
            self.logger = L.getChild('TCPProtocolV1').getChild(
                f'{local.id}->{remote.id}'
            )
        else:
            # local is server, need handshake
            self.logger = L.getChild('TCPProtocolV1').getChild(
                f'{local.id}<-UNKNOWN'
            )

    def peek_header(self) -> typing.Optional[typing.Tuple]:
        buf_len = len(self.buf.getbuffer())
        if buf_len < self.header_recv_len:
            return None

        header = struct.unpack(
            self.packet_header_struct,
            bytes(self.buf.getbuffer()[: self.header_recv_len]),
        )
        return header

    def feed_buffer(self, bs: bytes):
        self.logger.debug('feeding %s', b2a_hex(bs))
        try:
            self.buf.seek(0, SEEK_END)
            self.buf.write(bs)
        finally:
            self.buf.seek(0, SEEK_SET)

    def decode(self, bs: bytes) -> typing.Optional[bytes]:
        assert self.remote is not None, 'handshake incomplete'
        self.feed_buffer(bs)
        self.logger.debug('buf before parse %s', b2a_hex(self.buf.getbuffer()))

        header = self.peek_header()
        if header is None:
            # insufficient data
            return None

        full_len = self.header_recv_len + header[0] + self.signature_len
        if len(self.buf.getbuffer()) < full_len:
            return None

        # drop header which has been peeked
        header_bs = self.buf.read(self.header_recv_len)
        pkt = self.buf.read(header[0])
        signature = self.buf.read(self.signature_len)
        if not verify(
            self.remote.public_key, signature, header_bs + pkt, self.digest
        ):
            self.logger.warn(
                'pkt signature corrupted, drop %d bytes', len(pkt)
            )
            return None

        self.logger.debug('pkt parsed & verified %s', b2a_hex(pkt))
        self.logger.debug('buf after parse %s', b2a_hex(self.buf.getbuffer()))

        return pkt

    def encode(self, pkt: bytes) -> bytes:
        self.logger.debug('pkt to encode %s', b2a_hex(pkt))
        header = struct.pack(self.packet_header_struct, len(pkt))
        self.logger.debug('header: %s', b2a_hex(header))
        bs = header + pkt
        signature = sign(self.local.private_key, bs, self.digest)
        # self.logger.debug('encoded: %s', b2a_hex(bs))
        return bs + signature

    def num_to_read(self) -> int:
        """ return minimal number of bytes needed to parse next packet """
        buf_len = len(self.buf.getbuffer())
        self.logger.debug('buf length %d', buf_len)

        header = self.peek_header()
        if header is None:
            self.logger.debug('shorter than header %d', self.header_recv_len)
            return self.header_recv_len - buf_len

        body_len = self.header_recv_len + header[0] + self.signature_len
        self.logger.debug('expected body length %d', body_len)

        return body_len - buf_len if buf_len < body_len else 0

    def generate_handshake(self):
        timestamp = int(datetime.utcnow().timestamp())
        bs = struct.pack(
            self.handshake_struct,
            self.version,
            timestamp,
            self.local.id,
            b'',  # occupier for signature
        )[: -self.signature_len]
        bs = bs + sign(self.local.private_key, bs, self.digest)
        self.logger.debug('handshake to send: %s', b2a_hex(bs))
        return bs

    def parse_handshake(self, bs: bytes) -> typing.Optional[Node]:
        assert self.remote is None, 'double handshake'
        assert len(bs) == self.handshake_recv_len, 'invalid handshake bs'

        local_timestamp = datetime.utcnow().timestamp()
        self.logger.debug('handshake received: %s', b2a_hex(bs))
        version, timestamp, node_id, signature = struct.unpack(
            self.handshake_struct, bs
        )

        if version != self.version:
            self.logger.warn('invalid version %d', version)
            return None
        if abs(timestamp - local_timestamp) > self.handshake_timeout:
            self.logger.warn(
                'time difference exceeds tolerance: R:%s/%s L:%s/%s',
                timestamp,
                datetime.utcfromtimestamp(timestamp),
                local_timestamp,
                datetime.utcfromtimestamp(local_timestamp),
            )
            return None
        node = self.node_manager.get_node(node_id)
        if node is None:
            self.logger.warn('unconfigured node ID %d', node_id)
            return None
        data, signature = bs[: -self.signature_len], bs[-self.signature_len :]
        if not verify(node.public_key, signature, data, self.digest):
            self.logger.warn('corrupted handshake packet')
            return None
        self.remote = node
        self.logger = L.getChild('TCPProtocolV1').getChild(
            f'{self.local.id}<-{self.remote.id}'
        )
        self.logger.info('handshake done, connection established')
        return node
