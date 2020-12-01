from tqdm.autonotebook import tqdm

from pyvis.network import Network
import matplotlib.pyplot as plt
import networkx as nx

from .utils import get_node_label, get_node_desc
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

    for node in tqdm(G.nodes(), total=G.n_nodes):
        net.add_node(
            node.id,
            label=get_node_label(G, node),
            title=get_node_desc(G, node),
        )

    for source, target, key, data in tqdm(G.edges(), total=G.n_edges):
        net.add_edge(
            source,
            target,
        )
    net.show_buttons()
    return net.show(filename)


def show_entity_graph(G):
    pos = nx.fruchterman_reingold_layout(G.network)
    fig = plt.figure()

    node_labels = {node: get_node_label(G, node) for node in G.nodes()}

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
    plt.axis("off")
    return fig
