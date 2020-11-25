from tqdm.autonotebook import tqdm

from pyvis.network import Network
import matplotlib.pyplot as plt
import networkx as nx

from .utils import get_node_label, get_node_desc
from .utils import get_edge_label, get_edge_desc
from .utils import is_notebook


def show_entity_graph_pyvis(G, filename, notebook=None):
    if notebook is None:
        notebook = is_notebook()
    net = Network(
        height="750px",
        width="100%",
        notebook=notebook,
    )
    net.barnes_hut()

    for node_id, data in tqdm(G.get_nodes(), total=G.n_nodes):
        net.add_node(
            node_id,
            label=get_node_label(G, node_id, data),
            title=get_node_desc(G, node_id, data),
        )

    for (source, target, key), data in tqdm(G.get_edge_nodes(), total=G.n_edges):
        net.add_edge(
            source,
            target,
            label=get_edge_label(G, (source, target, key), data),
            title=get_edge_desc(G, (source, target, key), data),
        )
    net.show_buttons()
    return net.show(filename)


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
