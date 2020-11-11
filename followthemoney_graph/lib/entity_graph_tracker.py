import logging
import time

from followthemoney_graph.entity_graph import EntityGraph


log = logging.getLogger(__name__)
_marker = object()


class EntityGraphTracker(EntityGraph):
    _override = ["get_changes", "merge_nodes_canonical", "add_edge", "add_node"]

    def __init__(self, G):
        self.changes = {"merge": set(), "nodes_new": [], "edges_new": []}
        self.start_time = time.time()
        if isinstance(G, EntityGraphTracker):
            G = G.__G
        self.__G = G

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def get_changes(self):
        cur_time = time.time()
        merge = {
            canon_id: list(p for _, p in self.__G.get_node_proxies(canon_id))
            for canon_id in self.changes["merge"]
        }
        return {
            **self.changes,
            "merge": merge,
            "time_tracking": cur_time - self.start_time,
        }

    def merge_nodes_canonical(self, left, right, *args, **kwargs):
        log.debug("Tracking merge")
        new_canonid = self.__G.merge_nodes_canonical(left, right, *args, **kwargs)
        self.changes["merge"].discard(left)
        self.changes["merge"].discard(right)
        self.changes["merge"].add(new_canonid)
        return new_canonid

    def add_edge(self, proxy, *args, **kwargs):
        log.debug("Tracking new edge")
        is_new = self.__G.add_edge(proxy, *args, **kwargs)
        if is_new:
            self.changes["edges_new"].append(proxy)
        return is_new

    def add_node(self, proxy, *args, **kwargs):
        log.debug("Tracking new node")
        is_new = self.__G.add_node(proxy, *args, **kwargs)
        if is_new:
            self.changes["nodes_new"].append(proxy)
        return is_new

    def __getattr__(self, name, default=_marker):
        if name in self._override:
            return getattr(self, name)
        else:
            # Get it from papa:
            try:
                return getattr(self.__G, name)
            except AttributeError:
                if default is _marker:
                    raise
                return default
