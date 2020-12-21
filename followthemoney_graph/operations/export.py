import logging
import json

from tqdm.autonotebook import tqdm
import networkx as nx

from .utils import get_node_label


log = logging.getLogger(__name__)


def export_graphml(G, filename, exclude_schemas=None, slim=False):
    H = nx.Graph()
    skipped = 0
    for node in tqdm(G.nodes(), total=G.n_nodes):
        if exclude_schemas is not None and any(
            node.schema.is_a(s) for s in exclude_schemas
        ):
            skipped += 1
            continue
        try:
            collection_fid = ", ".join(
                c.get("foreign_id")
                for c in node.golden_proxy.context.get("collection", [])
            )
        except StopIteration:
            collection_fid = ""
        data = {
            "eid": node.parts[0],
            "address": ", ".join(node.properties.get("address") or []),
            "collection_fid": collection_fid,
            **node.flags,
        }
        if not slim:
            data.update(
                {
                    "n_proxies": len(node.proxies),
                    **{p: values[0] for p, values in node.properties.items()},
                }
            )
            data.pop("role")
        H.add_node(
            node.id,
            label=get_node_label(node),
            countries=", ".join(node.countries or []),
            role=", ".join(node.properties.get("role") or []),
            schema=node.schema.name,
            **data,
        )
    if skipped:
        print("Skipped nodes:", skipped)

    for source, target, key, data in tqdm(G.edges(), total=G.n_edges):
        if source in H and target in H:
            H.add_edge(
                source,
                target,
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
