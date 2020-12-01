class GraphBackend:
    def _has_node(self, node_id):
        raise NotImplementedError

    def _add_node(self, node):
        raise NotImplementedError

    def _add_edge(self, source, target, key, **data):
        raise NotImplementedError

    def _remove_node(self, node_id):
        """Deletes node and all adjacent edges"""
        raise NotImplementedError

    def _iter_edges(self, **flags):
        raise NotImplementedError

    def _iter_nodes(self, **flags):
        raise NotImplementedError

    def _get_n_nodes(self):
        raise NotImplementedError

    def _get_n_edges(self):
        raise NotImplementedError

    def _get_node(self, node_id):
        raise NotImplementedError

    def _get_node_in_edges(self, node_id):
        raise NotImplementedError

    def _get_node_out_edges(self, node_id):
        raise NotImplementedError
