import itertools as IT
from operator import itemgetter
from tqdm.autonotebook import tqdm

from pyvis.network import Network
import matplotlib.pyplot as plt
import networkx as nx


def get_node_label(G, node, data, maxlen=32):
    data = list(data)
    try:
        name = next(d["proxy"].names[0] for d in data if d["proxy"].names)
    except StopIteration:
        name = "N/A"
    if len(name) > maxlen:
        name = name[:maxlen] + "..."
    return f"{name}"


def get_node_desc(G, node, data):
    d = G.describe_node(node)
    desc = f"{len(data)} Proxes; In Edges: {len(d['in_edges'])}; Out Edges: {len(d['out_edges'])}<br>"
    desc += f"Schema: {data[0]['proxy'].schema.name}<br>"
    desc += "<br>".join(f"{p}: {values[0]}" for p, values in data.properties().items())
    return desc


def get_edge_label(G, node, data):
    data = list(data)
    return f"{data[0]['proxy'].caption}"


def get_edge_desc(G, edge, data):
    src_node, dst_node = G.get_nodes(edge[0], edge[1])
    src_label = get_node_label(G, *src_node)
    dst_label = get_node_label(G, *dst_node)
    desc = f"{src_label} -> {dst_label}<br>"
    desc += f"Schema: {data[0]['proxy'].schema.name}<br>"
    desc += "<br>".join(f"{p}: {values[0]}" for p, values in data.properties().items())
    return desc


def is_notebook():
    """
    From: https://stackoverflow.com/a/39662359
    """
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def show_entity_graph_pyvis(G):
    net = Network(
        height="750px",
        width="100%",
        notebook=is_notebook(),
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
    return net.show("test.html")


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
