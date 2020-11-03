import logging
from contextlib import contextmanager

from followthemoney_graph.entity_graph import EntityGraph

_marker = object()
log = logging.getLogger(__name__)


def track_node_tag(G, flag, force=False):
    G.ensure_flag(**{flag: False})
    # pre-get all the applicable nodes since we are making some in-place
    # changes
    if force:
        nodes = list(G.get_proxies())
    else:
        nodes = list(G.get_proxies(aleph_enrich_profile=False))
    log.debug(f"Processing nodes: {flag}: {len(nodes)}")
    for canon_id, proxy in nodes:
        yield canon_id, proxy
        G.set_proxy_flags(proxy, **{flag: True})


class EntityGraphTracker(EntityGraph):
    _override = ["get_changes", "merge_nodes_canonical", "add_edge", "add_node"]

    def __init__(self, G):
        self.changes = {"merge":set(), "nodes_new": [], "edges_new": []}
        self.__G = G

    def get_changes(self):
        merge = {canon_id: list(p for _, p in self.__G.get_proxies(canon_id)) for canon_id in self.changes['merge']}
        return {**self.changes, "merge": merge}

    def merge_nodes_canonical(self, left, right):
        log.info("Tracking merge")
        new_canonid = self.__G.merge_nodes_canonical(left, right)
        self.changes['merge'].discard(left)
        self.changes['merge'].discard(right)
        self.changes['merge'].add(new_canonid)
        return new_canonid

    def add_edge(self, proxy):
        log.info("Tracking new edge")
        is_new = self.__G.add_edge(proxy)
        if is_new:
            self.changes['edges_new'].append(proxy)
        return is_new

    def add_node(self, proxy):
        log.info("Tracking new node")
        is_new = self.__G.add_node(proxy)
        if is_new:
            self.changes['nodes_new'].append(proxy)
        return is_new

    def __getattr__(self, name, default=_marker):
        if name in self._override:
            return getattr(self, attr)
        else:
            # Get it from papa:
            try:
                return getattr(self.__G, name)
            except AttributeError:
                if default is _marker:
                    raise
                return default
