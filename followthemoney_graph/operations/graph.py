from ..entity_graph import EntityGraph


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
