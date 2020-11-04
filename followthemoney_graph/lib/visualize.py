import itertools as IT
from operator import itemgetter

from pyvis.network import Network
import matplotlib.pyplot as plt
import networkx as nx


def get_node_label(G, node, data):
    data = list(data)
    name = next(d.names[0] for d in data if d.names)
    return f"{name} [{len(data)}]"


def get_node_desc(G, node, data):
    desc = G.describe_node(node)
    return f"In Edges: {len(desc['in_edges'])}; Out Edges: {len(desc['out_edges'])}"


def get_edge_label(G, node, data):
    data = list(data)
    return f"{data[0].caption}"


def get_edge_desc(G, edge, data):
    src_node, dst_node = G.get_nodes(edge[0], edge[1])
    src_label = get_node_label(G, *src_node)
    dst_label = get_node_label(G, *dst_node)
    return f"{src_label} -> {dst_label}"


def show_entity_graph_pyvis(G):
    net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white")
    net.barnes_hut()

    for node_id, data in G.get_nodes():
        net.add_node(
            node_id,
            label=get_node_label(G, node_id, data),
            title=get_node_desc(G, node_id, data),
        )

    for (source, target, key), data in G.get_edge_nodes():
        net.add_edge(
            source,
            target,
            label=get_edge_label(G, (source, target), data),
            title=get_edge_desc(G, (source, target), data),
        )
    net.show_buttons()
    net.show("test.html")


def show_entity_graph(G):
    pos = nx.fruchterman_reingold_layout(G.network)
    fig = plt.figure()

    node_labels = {node: get_node_label(G, node, data) for node, data in G.get_nodes()}
    edge_labels = {
        edge: get_edge_label(G, edge, data) for edge, data in G.get_edge_nodes()
    }

    nx.draw(
        G.network,
        pos,
        edge_color="black",
        width=1,
        linewidths=1,
        node_size=50,
        node_color="pink",
        alpha=0.9,
        labels=node_labels,
    )

    nx.draw_networkx_edge_labels(
        G.network,
        pos,
        edge_labels=edge_labels,
        font_color="red",
    )
    plt.axis("off")
    return fig