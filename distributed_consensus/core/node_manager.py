from weakref import WeakValueDictionary, WeakSet
from typing import Optional


class BaseNode:
    id: int

    def __init__(self, id):
        super().__init__()
        self.id = id


class NodeManager:
    _all: WeakSet
    _by_id: WeakValueDictionary

    def __init__(self):
        super().__init__()
        self._all = WeakSet()
        self._by_id = WeakValueDictionary()

    def register(self, node: BaseNode):
        assert node not in self._all, f'duplicate node {node!r}'
        assert node not in self._by_id, f'duplicate node ID {node.id!r}'
        self._all.add(node)
        self._by_id[node.id] = node

    def get_node(self, id: int, default: Optional[BaseNode] = None):
        return self._by_id.get(id, default=default)


__node_manager_singleton = NodeManager()


def replace_default_manager(manager: NodeManager):
    global __node_manager_singleton
    prev = __node_manager_singleton
    __node_manager_singleton = manager
    return prev


def default_manager():
    global __node_manager_singleton
    return __node_manager_singleton


class AutoRegisterNode(BaseNode):
    def __init__(self, id: int, manager: NodeManager = None):
        super().__init__(id)
        manager = default_manager() if manager is None else manager
        manager.register(self)
