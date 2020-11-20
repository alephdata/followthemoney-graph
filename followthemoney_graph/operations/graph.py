import networkx as nx

from ..entity_graph import EntityGraph


def paths(G, source_filter, target_filter, directed=True):
    if directed:
        graph = g.network
    else:
        graph = g.network.to_undirected(as_view=True)
    for source_node in G.get_nodes(**source_filter):
        shortest_path = None
        for target_node in G.get_nodes(**target_filter):
            path = nx.shortest_path(graph, source_node, target_node)
            if shortest_path is None or len(path) < shortest_path:
                shortest_path = path
        yield shortest_path


def filter_kcore(G, k, copy=False):
    network = G.network
    for i in range(k):
        degree = network.degree()
        network = network.subgraph([e for e, d in degree if d >= i + 1])
    if copy:
        network = network.copy()
    return EntityGraph.from_networkx(network)


def filter_degree_range(G, degree_range, copy=False):
    degree = G.network.degree()
    network = G.network.subgraph(
        [
            e
            for e, d in degree
            if (degree_range[0] is None or d >= degree_range[0])
            and (degree_range[1] is None or d < degree_range[1])
        ]
    )
    if copy:
        network = network.copy()
    return EntityGraph.from_networkx(network)


def filter_degree_min(G, min_degree, copy=False):
    return filter_degree_range(G, (min_degree, None), copy=copy)


def filter_degree_max(G, max_degree, copy=False):
    return filter_degree_range(G, (None, max_degree), copy=copy)


def filter_component_size(G, size_range, copy=False):
    components = nx.algorithms.components.weakly_connected_components(G.network)
    nodes = set()
    for component in components:
        N = len(component)
        if (size_range[0] is None or N >= size_range[0]) and (
            size_range[1] is None or N < size_range[1]
        ):
            nodes.update(component)
    subgraph = G.network.subgraph(nodes)
    if copy:
        subgraph = subgraph.copy()
    return EntityGraph.from_networkx(subgraph)
