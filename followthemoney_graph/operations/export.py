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
            **{flag: value for flag, value in node.flags.items()},
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
        for proxy in node.proxies:
            proxy_dict = proxy.to_dict()
            proxy_dict["profile_id"] = node.id
            proxy_dict["flags"] = node.flags
            fd.write(json.dumps(proxy_dict))
            fd.write("\n")
