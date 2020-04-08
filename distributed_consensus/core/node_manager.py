from typing import TYPE_CHECKING, Optional, Any
from weakref import WeakSet, WeakValueDictionary


class BaseNode:
    id: int
    is_delegate: bool
    is_blacked: bool
    is_normal: bool

    def __init__(
        self, id, is_delegate=False, is_normal=True, is_blacked=False,
    ):
        super().__init__()
        self.id = id
        self.is_delegate = is_delegate
        self.is_normal = is_normal
        self.is_blacked = is_blacked


if TYPE_CHECKING:
    WeakNodeSet = WeakSet[BaseNode]
    WeakNodeDictionary = WeakValueDictionary[int, BaseNode]
else:
    WeakNodeSet = Any
    WeakNodeDictionary = Any


class NodeManager:
    _all: WeakNodeSet
    _by_id: WeakNodeDictionary
    _all_num: int
    _delegate_num: int

    def __init__(self):
        super().__init__()
        self._all = WeakSet()
        self._by_id = WeakValueDictionary()
        self._all_num = 0
        self._delegate_num = 0

    def register(self, node: BaseNode):
        assert node not in self._all, f'duplicate node {node!r}'
        assert node not in self._by_id, f'duplicate node ID {node.id!r}'
        self._all.add(node)
        self._by_id[node.id] = node
        self._all_num += 1
        if node.is_delegate:
            self._delegate_num += 1

    def get_node(
        self, id: int, default: Optional[BaseNode] = None
    ) -> Optional[BaseNode]:
        return self._by_id.get(id, default=default)

    def get_delegates(self):
        return {node for node in self._all if node.is_delegate}

    def block(self, id: int):
        node = self.get_node(id)
        if node:
            node.is_blacked = True

    @property
    def delegate_num(self) -> int:
        return self._delegate_num

    @property
    def all_num(self) -> int:
        return self._all_num

    def __len__(self) -> int:
        return self._all_num


__node_manager_singleton = NodeManager()


def replace_default_manager(manager: NodeManager):
    global __node_manager_singleton
    prev = __node_manager_singleton
    __node_manager_singleton = manager
    return prev


def default_manager() -> NodeManager:
    global __node_manager_singleton
    return __node_manager_singleton


class AutoRegisterNode(BaseNode):
    def __init__(self, id: int, manager: NodeManager = None, *args, **kwargs):
        super().__init__(id, *args, **kwargs)
        manager = default_manager() if manager is None else manager
        manager.register(self)
