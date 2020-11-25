import networkx as nx

from .graph_backend import GraphBackend
from followthemoney_graph.entity_graph import EntityGraph


class NetworkxEntityGraph(GraphBackend, EntityGraph):
    def __init__(self):
        super().__init__()
        self.network = nx.MultiDiGraph()

    def _has_node(self, node_id):
        return node_id in self.network

    def _add_node(self, node):
        self.network.add_node(node.id, data=node)

    def _add_edge(self, source_id, target_id, key, **data):
        self.network.add_edge(source_id, target_id, key=key, **data)

    def _remove_node(self, node_id):
        """Deletes node and all adjacent edges"""
        self.network.remove_node(node_id)

    def _iter_edges(self, **flags):
        if flags:
            subgraph = self.network.subgraph([n.id for n in self._iter_edges(**flags)])
        else:
            subgraph = self.network
        yield from subgraph.edges(data=True, keys=True)

    def _iter_nodes(self, **flags):
        for node_id, data in self.network.nodes(data=True):
            node = data["data"]
            if node.has_flags(**flags):
                yield node

    def _get_n_nodes(self):
        return self.network.number_of_nodes()

    def _get_n_edges(self):
        return self.network.number_of_edges()

    def _get_node(self, node_id):
        return self.network.nodes[node_id]["data"]

    def _get_node_in_edges(self, node_id):
        return self.network.in_edges(node_id, data=True, keys=True)

    def _get_node_out_edges(self, node_id):
        return self.network.out_edges(node_id, data=True, keys=True)
