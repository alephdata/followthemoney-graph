import logging
from collections import Counter
import json

from tqdm.autonotebook import tqdm
import networkx as nx

from followthemoney import model
from followthemoney.exc import InvalidData

from .utils import get_node_label
from .utils import get_edge_label


log = logging.getLogger(__name__)


def merge_node_data(datas):
    proxies = Counter()
    for data in datas:
        proxy_new = data["proxy"]
        for proxy in proxies:
            try:
                proxy.merge(proxy_new)
                proxies[proxy] += 1
                break
            except InvalidData:
                print(proxy.schema, proxy_new.schema)
                pass
        else:
            p = model.make_entity(proxy_new.schema)
            p.merge(proxy_new)
            proxies[p] = 1
    return proxies


def export_graphml(G, filename):
    H = nx.MultiDiGraph()
    for node_id, data in tqdm(G.get_nodes(), total=G.n_nodes):
        H.add_node(
            node_id,
            label=get_node_label(G, node_id, data),
            schema=data[0]["proxy"].schema.name,
            n_proxies=len(data),
            **{p: ", ".join(values) for p, values in data.properties().items()},
        )

    for (source, target, key), data in tqdm(G.get_edge_nodes(), total=G.n_edges):
        H.add_edge(
            source,
            target,
            key=f"{source}-{target}-{key}",
            label=get_edge_label(G, (source, target, key), data),
            schema=data[0]["proxy"].schema.name,
            n_proxies=len(data),
            **{p: ", ".join(values) for p, values in data.properties().items()},
        )
    nx.write_graphml(H, filename)


def export_followthemoney_json(G, fd):
    node_lookup = {}
    for node, datas in tqdm(G.get_nodes(), total=G.n_nodes):
        proxies = merge_node_data(datas)
        ((proxy, count),) = proxies.most_common(1)
        node_lookup[node] = proxy.make_id(node)
        log.debug(f"Selecting node proxy {proxy} with {count} members")

        ids = [d["proxy"].id for d in datas]
        for i in ids:
            proxy.add("sameAs", i)

        fd.write(json.dumps(proxy.to_dict()))
        fd.write("\n")

    for (source, target, key), datas in tqdm(G.get_edge_nodes(), total=G.n_edges):
        proxies = merge_node_data(datas)
        ((proxy, count),) = proxies.most_common(1)
        proxy.id = key
        log.debug(f"Selecting edge proxy {proxy} with {count} members")

        source_prop = proxy.schema.source_prop
        target_prop = proxy.schema.target_prop
        proxy.set(source_prop, (node_lookup[source],))
        proxy.set(target_prop, (node_lookup[target],))

        fd.write(json.dumps(proxy.to_dict()))
        fd.write("\n")
