import networkx as nx
from tqdm.autonotebook import tqdm

from ..entity_graph import EntityGraph
from ..node import Node


def find_subgraphs_like(G, match, exact=True):
    match_fxn = nx.algorithms.isomorphism.generic_node_match(
        "data",
        Node(),
        lambda left, right: left.match(right, ignore_edges=True, exact=exact),
    )
    matcher = nx.algorithms.isomorphism.GraphMatcher(
        G.network, match.network, node_match=match_fxn
    )
    for result in matcher.subgraph_isomorphisms_iter():
        yield {mid: G.get_node(nid) for nid, mid in result.items()}


def paths(G, source_nodes, target_nodes, max_length=None):
    graph = G.network
    target_nodes = tuple(target_nodes)
    for source_node in tqdm(source_nodes):
        shortest_length = max_length
        for target_node in target_nodes:
            try:
                path = nx.shortest_path(graph, source_node.id, target_node.id)
                if shortest_length is None or len(path) < shortest_length:
                    yield [G.get_node(nid) for nid in path]
            except nx.NetworkXNoPath:
                continue


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
