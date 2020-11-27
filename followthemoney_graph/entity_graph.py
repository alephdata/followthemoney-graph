import logging

from followthemoney import model

from .node import Node

log = logging.getLogger(__name__)


class EntityGraph(object):
    def __init__(self):
        self._id_to_canonical = {}
        self._info_pending = set()

    def edges(self, **flags):
        yield from self._iter_edges(**flags)

    def nodes(self, **flags):
        yield from self._iter_nodes(**flags)

    def add_proxies(self, proxies):
        return [self.add_proxy(p) for p in proxies]

    def add_proxy(self, proxy, node_id=None):
        if proxy.id in self:
            cur_node = self.get_node_by_proxy(proxy)
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
        node = Node(id=node_id, proxies=[proxy])
        if node_id and self._has_node(node_id):
            node = self._get_node(node_id).merge(node)
        else:
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
        self._info_pending.add(stub)
        return node

    def connect_edges(self, node):
        if node.schema.edge:
            source_prop = node.schema.source_prop
            for source in node.get(source_prop):
                if source not in self:
                    source_node = self.add_stub(source)
                else:
                    source_node = self.get_node_by_proxy_id(source)
                self._add_edge(source_node.id, node.id, key=node.id, prop=source_prop)
            target_prop = node.schema.target_prop
            for target in node.get(target_prop):
                if target not in self:
                    target_node = self.add_stub(target)
                else:
                    target_node = self.get_node_by_proxy_id(target)
                self._add_edge(node.id, target_node.id, key=node.id, prop=target_prop)

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
            for source_id, target_id, key, data in self._get_node_in_edges(
                right_node.id
            ):
                self._add_edge(source_id, left_node.id, key=key, **data)
            for source_id, target_id, key, data in self._get_node_out_edges(
                right_node.id
            ):
                self._add_edge(left_node.id, target_id, key=key, **data)
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

    def get_node(self, node_id):
        return self._get_node(node_id)

    def get_node_by_proxy_id(self, pid):
        node_id = self._id_to_canonical[pid]
        return self.get_node(node_id)

    def get_node_by_proxy(self, proxy):
        return self.get_node_by_proxy_id(proxy.id)

    def get_node_in_edges(self, node):
        return self._get_node_in_edges(node.id)

    def get_node_out_edges(self, node):
        return self._get_node_out_edges(node.id)

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
