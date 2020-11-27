import logging
import json

from tqdm.autonotebook import tqdm
import networkx as nx

from .utils import get_node_label


log = logging.getLogger(__name__)


def export_graphml(G, filename):
    H = nx.MultiDiGraph()
    for node in tqdm(G.nodes(), total=G.n_nodes):
        H.add_node(
            node.id,
            label=get_node_label(G, node),
            schema=node.schema.name,
            n_proxies=len(node.proxies),
            **{p: ", ".join(values) for p, values in node.properties.items()},
        )

    for source, target, key, data in tqdm(G.edges(), total=G.n_edges):
        H.add_edge(
            source,
            target,
            key=f"{source}-{target}-{key}",
        )
    nx.write_graphml(H, filename)


def export_followthemoney_json(G, fd):
    for node in tqdm(G.nodes(), total=G.n_nodes):
        fd.write(json.dumps(node.golden_proxy.to_dict()))
        fd.write("\n")
        for proxy in node.proxies:
            fd.write(json.dumps(proxy.to_dict()))
            fd.write("\n")
