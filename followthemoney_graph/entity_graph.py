import logging
import json

from followthemoney import model
from followthemoney.exc import InvalidData
from tqdm.autonotebook import tqdm

from .node import Node
from .operations import export

log = logging.getLogger(__name__)


class EntityGraph(object):
    def __init__(self):
        self._id_to_canonical = {}
        self._stub_proxies = set()

    @classmethod
    def from_file(cls, fd):
        G = cls()
        for line in tqdm(fd):
            proxy_dict = json.loads(line)
            node_id = proxy_dict.pop("profile_id", None)
            node_flags = proxy_dict.pop("flags", {})
            proxy = model.get_proxy(proxy_dict)
            try:
                node, _ = G.add_proxy(proxy, node_id=node_id)
            except InvalidData:
                print("!", end="")
            node.flags = node_flags
        return G

    def to_file(self, fd):
        return export.export_followthemoney_json(self, fd)

    def edges(self, **flags):
        yield from self._iter_edges(**flags)

    def nodes(self, **flags):
        yield from self._iter_nodes(**flags)

    def add_proxies(self, proxies):
        return [self.add_proxy(p) for p in proxies]

    def add_proxy(self, proxy, node_id=None):
        if proxy.id in self:
            cur_node = self.get_node_by_proxy(proxy)
            if proxy.id in self._stub_proxies:
                cur_node.fill_stub(proxy)
                self._stub_proxies.discard(proxy.id)
            if node_id is not None and cur_node.id != node_id:
                if self._has_node(node_id):
                    node = self.get_node(node_id)
                else:
                    node = Node(id=node_id, proxies=[proxy])
                    self._add_node(node)
                node = self.merge_nodes(node, cur_node)
                return node, True
            return cur_node, False
        node_id = node_id or proxy.id
        if self._has_node(node_id):
            node = self._get_node(node_id)
            node.add_proxy(proxy)
        else:
            node = Node(id=node_id, proxies=[proxy])
            self._add_node(node)
        self._id_to_canonical[proxy.id] = node.id
        self.connect_edges(node)
        return node, True

    def add_stub(self, proxy_id, schema="Thing"):
        stub = model.make_entity(schema)
        stub.id = proxy_id
        if proxy_id in self:
            return self.get_node_by_proxy(stub)
        node, _ = self.add_proxy(stub)
        self._stub_proxies.add(stub.id)
        return node

    def connect_edges(self, node):
        if node.has_edge:
            for edge_prop in node.edges:
                for target in node.get(edge_prop):
                    if target not in self:
                        target_node = self.add_stub(target)
                    else:
                        target_node = self.get_node_by_proxy_id(target)
                    self._add_edge(node.id, target_node.id, key=node.id, prop=edge_prop)

    def merge_proxies(self, *proxies):
        nodes = [self.get_node_by_proxy(p) for p in proxies]
        return self.merge_nodes(*nodes)

    def merge_nodes(self, left_node, *right_nodes):
        for right_node in right_nodes:
            if left_node == right_node:
                continue
            left_node.merge(right_node)
            for pid in right_node.parts:
                self._id_to_canonical[pid] = left_node.id
            for source_id, target_id, key, data in list(
                self._get_node_edges(right_node.id)
            ):
                self._add_edge(source_id, left_node.id, key=key, **data)
            self._remove_node(right_node.id)
        return left_node

    @classmethod
    def intersect(cls, *graphs):
        keep_proxy_ids = set(pid for node in graphs[0].nodes() for pid in node.parts)
        for graph in graphs[1:]:
            keep_proxy_ids.intersection_update(
                pid for node in graph.nodes() for pid in node.parts
            )
        # TODO: how to pass connection parameters in instantiating the new class
        G = cls()
        seen_ids = set()
        for keep_proxy_id in keep_proxy_ids:
            if keep_proxy_id in seen_ids:
                continue
            node = Node()
            for graph in graphs:
                cur_node = graph.get_node_by_proxy_id(keep_proxy_id)
                seen_ids.update(cur_node.parts)
                node.merge(cur_node)
            G._add_node(node)
            G._id_to_canonical.update({pid: node.id for pid in node.parts})
            G.connect_edges(node)
        return G

    def get_node_neighborhood(self, *nodes):
        seen_ids = {n.id for n in nodes}
        for node in nodes:
            for source_id, target_id, key, data in self.get_node_edges(node):
                if target_id not in seen_ids:
                    seen_ids.add(target_id)
                    yield self.get_node(target_id)
                elif source_id not in seen_ids:
                    seen_ids.add(source_id)
                    yield self.get_node(source_id)

    def get_node(self, node_id):
        return self._get_node(node_id)

    def get_node_by_proxy_id(self, pid):
        node_id = self._id_to_canonical[pid]
        return self.get_node(node_id)

    def get_node_by_proxy(self, proxy):
        return self.get_node_by_proxy_id(proxy.id)

    def get_node_edges(self, node):
        return self._get_node_edges(node.id)

    def proxies(self, **flags):
        for node in self.nodes(**flags):
            yield from node.proxies

    def ensure_flag(self, **flag_values):
        for node in self.nodes():
            node.ensure_flag(**flag_values)

    def __contains__(self, proxy_id):
        return proxy_id in self._id_to_canonical

    def __len__(self):
        return len(self._id_to_canonical)

    @property
    def n_nodes(self):
        return self._get_n_nodes()

    @property
    def n_edges(self):
        return self._get_n_edges()

    def __repr__(self):
        n_nodes = self._get_n_nodes()
        n_edges = self._get_n_edges()
        n_proxies = len(self)
        return f"<EntityGraph n_proxies={n_proxies}:n_nodes={n_nodes}:n_edges={n_edges} 0x{id(self):0x}>"
