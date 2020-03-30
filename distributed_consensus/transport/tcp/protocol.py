import typing
from io import BytesIO
from logging import getLogger
from binascii import b2a_hex

from ...core.node import Node

L = getLogger(__name__)


class TCPProtocol:
    local: Node
    remote: Node
    handshake_timeout: int
    handshake_recv_len: int
    header_recv_len: int
    buf: BytesIO

    def __init__(self, local: Node, remote: Node = None):
        super().__init__()
        self.local = local
        self.remote = remote
        self.handshake_recv_len = 10
        self.handshake_recv_len = 1
        self.header_recv_len = 1
        self.buf = BytesIO()
        remote_id = remote.id if remote else 'unknown'
        self.logger = L.getChild('TCPProtocol').getChild(
            f'{local.id}->{remote_id}'
        )

    def decode(self, bs: bytes) -> typing.Optional[bytes]:
        self.logger.debug('feeding %s', b2a_hex(bs))
        self.logger.debug('buf before parse %s', b2a_hex(self.buf.getbuffer()))
        self.buf.write(bs)

        # TODO parse data in self.buf
        pkt = b''
        self.logger.debug('pkt parsed %s', b2a_hex(pkt))
        self.logger.debug('buf after parse %s', b2a_hex(self.buf.getbuffer()))

        return pkt

    def encode(self, pkt: bytes) -> bytes:
        self.logger.debug('pkt to encode %s', b2a_hex(pkt))
        bs = pkt
        self.logger.debug('encoded: %s', b2a_hex(bs))
        return bs

    def num_to_read(self) -> int:
        """ return minimal number of bytes needed to parse next packet """
        buf_len = len(self.buf.getbuffer())
        self.logger.debug('buf length %d', buf_len)
        if buf_len < self.header_recv_len:
            self.logger.debug('shorter than header %d', self.header_recv_len)
            return self.header_recv_len - buf_len

        # TODO extract packet length in header, then subtract buf_len from it
        body_len = 42
        self.logger.debug('expected body length %d', body_len)

        return body_len - buf_len if buf_len < body_len else 0

    def generate_handshake(self):
        bs = b''
        self.logger.debug('handshake to send: %s', b2a_hex(bs))
        return self.encode(self, bs)

    def parse_handshake(self, bs: bytes) -> typing.Optional[Node]:
        assert self.remote is None, 'double handshake'

        self.logger.debug('handshake received: %s', b2a_hex(bs))
        # TODO parse handshake
        # TODO get remote node from node registry
        node = None
        self.remote = node
        return node
