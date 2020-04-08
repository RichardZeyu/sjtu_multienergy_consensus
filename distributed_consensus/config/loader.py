import importlib
import logging
import typing
from pathlib import Path
from pprint import pformat

import yaml

from ..core.node import Node
from ..core.node_manager import NodeManager, default_manager
from ..crypto import _extract_certificate, _load_private_key, _load_public_key
from ..crypto.util import read_all_str

L = logging.getLogger(__name__)


class Config:
    local_node: typing.Optional[Node]
    nodes: typing.List[Node]
    node_manager: NodeManager
    log_level: int
    scene_class: typing.Type
    scene_parameters: typing.Dict[str, typing.Any]
    cwd: Path
    transport_parameters: typing.Dict[str, typing.Dict[str, typing.Any]]

    __slots__ = [
        'local_node',
        'nodes',
        'node_manager',
        'log_level',
        'scene_class',
        'scene_parameters',
        'transport_parameters',
        'cwd',
    ]

    def __init__(
        self,
        cwd: typing.Union[Path, str],
        node_manager: typing.Optional[NodeManager] = None,
    ):
        super().__init__()
        if node_manager is None:
            node_manager = default_manager()
        self.node_manager = node_manager
        self.nodes = list()
        self.log_level = logging.INFO
        self.cwd = Path(cwd).expanduser().absolute()
        self.local_node = None
        self.transport_parameters = {}

    def create_node(
        self,
        node_id: int,
        ip: str,
        port: int,
        private_key_file: str,
        public_key_file: str,
        is_delegate: bool,
        is_normal: bool,
        is_local: bool,
        transport: str,
    ):
        if transport != 'tcpv1':
            L.fatal(f'unsupported transport {transport}')
            raise RuntimeError(f'unsupported transport {transport}')
        private_key_path = self.cwd / private_key_file
        public_key_path = self.cwd / public_key_file

        if is_local:
            if self.local_node is not None:
                L.fatal(
                    f'multiple local nodes, {self.local_node!r} and {node_id}'
                )
                raise RuntimeError('multiple local nodes')
            if not private_key_path.is_file():
                L.fatal(f'cannot open private key {private_key_file!s}')
                raise FileNotFoundError(str(private_key_path))
            try:
                private_key = _load_private_key(
                    _extract_certificate(read_all_str(private_key_path))
                )
            except:  # noqa
                L.fatal(f'invalid private key {private_key_file!s}')
                raise
        else:
            private_key = None

        if not public_key_path.is_file():
            L.fatal(f'cannot open public key {public_key_path!s}')
            raise FileNotFoundError(str(public_key_path))
        try:
            public_key = _load_public_key(
                _extract_certificate(read_all_str(public_key_path))
            )
        except:  # noqa
            L.fatal(f'invalid public key {public_key_path!s}')
            raise

        if node_id < 0 or node_id > 0xFFFFFFFF:
            raise ValueError(f'invalid node id {node_id}')

        self.nodes.append(
            Node(
                node_id,
                ip,
                port,
                public_key,
                private_key,
                is_delegate=is_delegate,
                is_normal=is_normal,
                manager=self.node_manager,
            )
        )
        if is_local:
            self.local_node = self.node_manager.get_node(node_id)

    @staticmethod
    def parse_log_level(name: str) -> int:
        level = logging.getLevelName(name.upper())
        if isinstance(level, int):
            return level
        raise ValueError(f'{name} is not a valid log level')

    @classmethod
    def from_yaml(
        cls,
        local_node_id: int,
        yaml_file: typing.Union[str, Path, typing.BinaryIO],
    ):
        if isinstance(yaml_file, str):
            yaml_file = Path(yaml_file)
        if isinstance(yaml_file, Path):
            yaml_file = open(yaml_file, 'rb')

        obj = yaml.safe_load(yaml_file)

        ins = cls(Path(yaml_file.name).parent)
        for node_id, node in obj.get('nodes', {}).items():
            ins.create_node(node_id, is_local=local_node_id == node_id, **node)

        if ins.node_manager.get_node(local_node_id) is None:
            L.fatal(f'local node {local_node_id} not defined in nodes section')
            raise RuntimeError(
                f'local node {local_node_id} not defined in nodes section'
            )

        ins.log_level = cls.parse_log_level(obj.get('log-level', 'info'))
        if 'scene' not in obj or 'class' not in obj['scene']:
            L.fatal(f'cannot find scene.class section in {yaml_file.name}')
            raise KeyError('scene.class')

        ins.transport_parameters = obj.get('transport', {})

        scene = obj['scene']
        ins.scene_parameters = obj.get('params', {})
        try:
            mod, clas = scene['class'].rsplit('.', 1)
            scene_module = importlib.import_module(mod)
            ins.scene_class = getattr(scene_module, clas)
        except:  # noqa
            L.fatal(f'{scene["class"]} is not a valid class path')
            raise

        return ins

    def __repr__(self):
        conf = (
            pformat(
                {
                    k: getattr(self, k)
                    for k in self.__dir__()
                    if not k.startswith('__')
                    and not callable(getattr(self, k))
                },
                width=10000,
                compact=True,
            )
            .replace(', ', ' ')
            .replace(': ', ':')
        )
        return f'<Config {conf}>'


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    config = Config.from_yaml(1, 'config.sample.yaml')
    print(config)
