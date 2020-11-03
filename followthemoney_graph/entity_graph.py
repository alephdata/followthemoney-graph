import logging
import itertools as IT
from pprint import pprint  # noqa
import hashlib
from collections import defaultdict

import networkx as nx
from followthemoney import model

log = logging.getLogger(__name__)


class NodeData(list):
    def __init__(self, *proxies):
        for proxy in proxies:
            self.add_proxy(proxy)

    @property
    def proxy_ids(self):
        return tuple(d["proxy"].id for d in self)

    def filter(self, **flag_values):
        return tuple(
            d["proxy"]
            for d in self
            if all(d["flags"].get(f, None) == v for f, v in flag_values.items())
        )

    def set_proxy_metadata(self, proxy, **meta):
        for d in self:
            if d["proxy"] == proxy:
                for m, v in meta.items():
                    d["meta"][m].append(v)

    def set_proxy_flags(self, proxy, **flag_values):
        for d in self:
            if d["proxy"] == proxy:
                d["flags"].update(flag_values)

    def ensure_flag(self, key, value):
        for d in self:
            d["flags"].setdefault(key, value)

    def add_proxy(self, proxy):
        self.append({"proxy": proxy, "flags": {}, "meta": defaultdict(list)})
        return self

    def merge(self, *others):
        seen_ids = set(self.proxy_ids)
        for other in others:
            for data in other:
                eid = data["proxy"].id
                if eid not in seen_ids:
                    self.append(data)
                    seen_ids.add(eid)
        return self


class EntityGraph(object):
    name = None

    def __init__(self):
        self.network = nx.MultiDiGraph()
        self._id_to_canonical = {}

    @staticmethod
    def create_canonical_id(ids):
        hasher = hashlib.md5()
        ids = list(ids)
        ids.sort()
        for id_ in ids:
            hasher.update(str(id_).encode("utf8"))
        return hasher.hexdigest()

    @staticmethod
    def create_edge_entity_from_proxies(
        source_proxies, target_proxies, schema="UnknownLink", **data
    ):
        edge = model.make_entity(schema)
        for source in source_proxies:
            edge.add("subject", source)
        for target in target_proxies:
            edge.add("object", target)
        for k, v in data.items():
            edge.add(k, v)
        edge.make_id(schema, source_proxies, target_proxies)
        return edge

    def add_proxy(self, proxy):
        if proxy.schema.edge:
            return self.add_edge(proxy)
        else:
            return self.add_node(proxy)

    def add_proxies(self, proxies):
        return [self.add_proxy(p) for p in proxies]

    def add_node(self, proxy):
        if proxy.id not in self:
            eid = proxy.id
            self._id_to_canonical[eid] = eid
            log.debug(f"Adding node: {eid}")
            self.network.add_node(eid, data=NodeData(proxy))
            return True
        return False

    def add_edge(self, proxy):
        if proxy.id not in self:
            connected_nodes = IT.chain(
                proxy.get(proxy.schema.source_prop), proxy.get(proxy.schema.target_prop)
            )
            for connected_node in connected_nodes:
                if connected_node not in self:
                    raise KeyError(connected_node)

            eid = proxy.id
            self._id_to_canonical[eid] = eid
            for source_id, target_id in proxy.edgepairs():
                s = self.get_canonical_id(source_id)
                t = self.get_canonical_id(target_id)
                log.debug(f"Adding edge: {s} -> {t} (key={eid})")
                self.network.add_edge(s, t, key=eid, data=NodeData(proxy))
            return True
        return False

    def get_canonical_id(self, entity_id):
        return self._id_to_canonical[entity_id]

    def get_node(self, proxy):
        canon_id = self.get_canonical_id(proxy.id)
        return {
            "in_edges": self.network.in_edges(canon_id),
            "out_edges": self.network.out_edges(canon_id),
            **self.network.nodes[canon_id],
        }

    def merge_proxies(self, *proxies):
        if not proxies:
            return None
        elif len(proxies) == 1:
            return self.get_canonical_id(proxies[0].id)
        left_canon_id = self.get_canonical_id(proxies[0].id)
        seen_proxies = set([proxies[0].id])
        for p in proxies[1:]:
            eid = p.id
            if eid not in seen_proxies:
                right_canon_id = self.get_canonical_id(eid)
                left_canon_id = self.merge_nodes_canonical(
                    left_canon_id, right_canon_id
                )
                seen_proxies.add(eid)
        return left_canon_id

    def merge_nodes_canonicals(self, *canon_ids):
        if not canon_ids:
            return None
        elif len(canon_ids) == 1:
            return canon_ids[0]
        left_canon_id = canon_ids[0]
        seen_cids = set([left_canon_id])
        for canon_id in canon_ids[1:]:
            if canon_id not in seen_cids:
                left_canon_id = self.merge_nodes_canonical(left_canon_id, canon_id)
                seen_cids.add(canon_id)
        return left_canon_id

    def merge_nodes_canonical(self, left_canon_id, right_canon_id):
        if left_canon_id == right_canon_id:
            return left_canon_id
        elif left_canon_id not in self.network or right_canon_id not in self.network:
            raise KeyError

        left_data = self.network.nodes[left_canon_id]["data"]
        right_data = self.network.nodes[right_canon_id]["data"]
        data = NodeData().merge(left_data, right_data)
        eids = data.proxy_ids
        canon_id = self.create_canonical_id(eids)

        log.debug(f"Adding merge node: {canon_id}")
        self.network.add_node(canon_id, data=data)
        for eid in eids:
            self._id_to_canonical[eid] = canon_id

        in_edges = (
            self.network.in_edges(n, data=True, keys=True)
            for n in (left_canon_id, right_canon_id)
        )
        out_edges = (
            self.network.out_edges(n, data=True, keys=True)
            for n in (left_canon_id, right_canon_id)
        )
        edges = IT.chain(*in_edges, *out_edges)
        seen_edges = set()
        for (a, b, key, data) in edges:
            if key not in seen_edges:
                edge_new = [
                    canon_id if e in (left_canon_id, right_canon_id) else e
                    for e in (a, b)
                ]
                log.debug(
                    f"Adding merge edge: {edge_new[0]} -> {edge_new[1]} (key={key})"
                )
                self.network.add_edge(*edge_new, key=key, **data)
                seen_edges.add(key)
            else:
                log.debug(f"Skipping edge: {a} -> {b} (key={key})")

        log.debug(
            f"Deleting nodes due to merge: {canon_id}: {left_canon_id}, {right_canon_id}"
        )
        self.network.remove_node(left_canon_id)
        self.network.remove_node(right_canon_id)
        return canon_id

    def get_proxies(self, *canon_ids, **flag_values):
        if canon_ids:
            nodes = [(cid, self.network.nodes[cid]) for cid in canon_ids]
        else:
            nodes = self.network.nodes(data=True)
        for canon_id, data in nodes:
            for proxy in data["data"].filter(**flag_values):
                yield canon_id, proxy

    def set_proxy_metadata(self, proxy, **meta):
        canon_id = self.get_canonical_id(proxy.id)
        data = self.network.nodes[canon_id]["data"]
        data.set_proxy_metadata(proxy, **meta)

    def set_proxy_flags(self, proxy, **flag_values):
        canon_id = self.get_canonical_id(proxy.id)
        data = self.network.nodes[canon_id]["data"]
        data.set_proxy_flags(proxy, **flag_values)

    def ensure_flag(self, **flag_value):
        for canon_id, data in self.network.nodes(data=True):
            for flag, value in flag_value.items():
                data["data"].ensure_flag(flag, value)

    def id_to_canonical(self, eid):
        return self._id_to_canonical[eid]

    def canonical_to_id(self, canon_id):
        return self._canonical_to_id[canon_id]

    def __len__(self):
        return len(self._id_to_canonical)

    def __contains__(self, proxy):
        return proxy in self._id_to_canonical

    def __repr__(self):
        n_nodes = self.network.number_of_nodes()
        n_edges = self.network.number_of_edges()
        n_proxies = len(self)
        return f"<EntityGraph n_proxies={n_proxies}:n_nodes={n_nodes}:n_edges={n_edges} 0x{id(self):0x}>"
