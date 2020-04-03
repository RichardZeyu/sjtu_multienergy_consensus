import unittest

from distributed_consensus.core.node import Node
from distributed_consensus.core.node_manager import NodeManager
from distributed_consensus.crypto import verify
from distributed_consensus.crypto.util import read_all_str
from distributed_consensus.transport.tcp.protocol import TCPProtocolV1
from unittest.mock import patch
from datetime import datetime


class TestTCPProtocolV1(unittest.TestCase):
    def setUp(self):
        self.manager = NodeManager()
        self.protocol = TCPProtocolV1(
            self.manager,
            Node(
                1,
                '1.1.1.1',
                1111,
                read_all_str('./tests/test_keys/node1.pub'),
                read_all_str('./tests/test_keys/node1.key'),
                self.manager,
            ),
            Node(
                2,
                '2.2.2.2',
                2222,
                read_all_str('./tests/test_keys/node2.pub'),
                '',
                self.manager,
            ),
        )

    def tearDown(self):
        self.protocol.buf.close()
        self.protocol = None

    @patch('distributed_consensus.transport.tcp.protocol.datetime')
    def test_handshake_generation(self, mock_datetime):
        mock_datetime.utcnow.return_value = datetime(2020, 4, 1, 18, 0, 0, 0)
        bs = self.protocol.generate_handshake()
        assert mock_datetime.utcnow.called
        assert len(bs) == 525
        assert bs[:13] == bytes.fromhex('01 000000005e846620 00000001')
        assert verify(
            self.protocol.local.public_key, bs[13:], bs[:13], 'SHA256',
        )
        remote_protocol = TCPProtocolV1(
            self.manager,
            Node(
                2,
                '2.2.2.2',
                2222,
                read_all_str('./tests/test_keys/node2.pub'),
                read_all_str('./tests/test_keys/node2.key'),
                self.manager,
            ),
        )
        node1 = remote_protocol.parse_handshake(bs)
        assert node1 is self.manager.get_node(1)

    def test_normal_encode_decode(self):
        bs = self.protocol.encode(b'\x01\x02\x03\x04')
        assert len(bs) == 4 + 4 + 512
        assert bs[:8] == bytes.fromhex('00000004 01020304')
        assert verify(
            self.protocol.local.public_key, bs[8:], bs[:8], 'SHA256',
        )
        remote_protocol = TCPProtocolV1(
            self.manager,
            Node(
                2,
                '2.2.2.2',
                2222,
                read_all_str('./tests/test_keys/node2.pub'),
                read_all_str('./tests/test_keys/node2.key'),
                self.manager,
            ),
            Node(
                1,
                '1.1.1.1',
                1111,
                read_all_str('./tests/test_keys/node1.pub'),
                None,
                self.manager,
            ),
        )
        data = remote_protocol.decode(bs)
        assert data == bytes.fromhex('01020304')
        assert remote_protocol.num_to_read() == 4

    def test_num_to_read(self):
        assert self.protocol.num_to_read() == 4
        assert self.protocol.decode(b'') is None
        assert self.protocol.num_to_read() == 4
        assert self.protocol.decode(b'\x00\x00') is None
        assert self.protocol.num_to_read() == 2
        assert self.protocol.decode(b'\x00\x0a') is None
        assert self.protocol.num_to_read() == 522
        assert self.protocol.decode(b'\x00' * 500) is None
        assert self.protocol.num_to_read() == 22
