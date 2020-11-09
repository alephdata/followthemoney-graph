from ..entity_graph import EntityGraph


def filter_kcore(G, k, copy=False):
    network = G.network
    for i in range(k):
        degree = network.degree()
        network = network.subgraph([e for e, d in degree if d >= i + 1])
    if copy:
        network = network.copy()
    return EntityGraph.from_networkx(network)

def filter_min_degree(G, min_degree, copy=False):
    degree = G.network.degree()
    network = G.network.subgraph([e for e, d in degree if d >= min_degree])
    if copy:
        network = network.copy()
    return EntityGraph.from_networkx(network)

def filter_max_degree(G, max_degree, copy=False):
    degree = G.network.degree()
    network = G.network.subgraph([e for e, d in degree if d < max_degree])
    if copy:
        network = network.copy()
    return EntityGraph.from_networkx(network)
