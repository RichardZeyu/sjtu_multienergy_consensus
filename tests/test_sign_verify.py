
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory as TempDir

import pytest
from distributed_consensus.crypto import generate_key, sign, verify
from OpenSSL import crypto


def read_all_str(file: str):
    with open(file, 'r') as f:
        return f.read(-1)


def flattern(args):
    ans = []
    for a in args:
        if isinstance(a, (list, tuple)):
            ans.extend(flattern(a))
        else:
            ans.append(a)
    return ans


class TestSignVerify(unittest.TestCase):
    def test_sign_verify(self):
        with TempDir('pyut') as d:
            generate_key('test', d)
            pub = read_all_str(Path(d) / 'test.pub')
            pvt = read_all_str(Path(d) / 'test.key')
            data = b'012345678901234567'
            signature = sign(pvt, data)
            assert verify(pub, signature, data) is None

            with pytest.raises(crypto.Error) as exc:
                verify(pub, signature, b'0987654321') is None
            assert 'bad signature' in flattern(exc.value.args)
