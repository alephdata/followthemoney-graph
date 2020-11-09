import logging
from functools import lru_cache

from alephclient.api import AlephAPI, APIResultSet, AlephException
from tqdm.autonotebook import tqdm

from followthemoney import model
from ..lib import track_node_tag, EntityGraphTracker


log = logging.getLogger(__name__)

alephclient = AlephAPI()


def aleph_get(url, *args, **kwargs):
    fullurl = alephclient._make_url(url, *args, **kwargs)
    log.debug(f"Aleph fetch: {fullurl}")
    try:
        return _alephget(fullurl)
    except AlephException:
        log.exception("Error calling Aleph API")
        return {}


def aleph_get_qparts(url, q_parts, *args, **kwargs):
    while q_parts:
        size = 0
        for i in range(len(q_parts)):
            s = len(q_parts[i])
            if size + s > 512:
                break
            size += s
        else:
            i += 1
        q = " OR ".join(q_parts[:i])
        q_parts = q_parts[i:]
        yield from aleph_get(url, q=q, *args, **kwargs)


@lru_cache(2048)
def _alephget(url):
    log.debug(f"Fetching from server: {url}")
    return list(APIResultSet(alephclient, url))


def parse_edge(edge, in_prop, out_prop):
    ins = [parse_entity(s) for s in edge["properties"][in_prop]]
    outs = [parse_entity(s) for s in edge["properties"][out_prop]]
    edge_proxy = parse_entity(edge)
    return {"in_proxies": ins, "out_proxies": outs, "edge": edge_proxy}


def parse_entity(entity):
    for key, values in entity["properties"].items():
        for i, value in enumerate(values):
            if isinstance(value, dict):
                values[i] = value.get("id")
    return model.get_proxy(entity)


def add_aleph_entities(G, *entity_ids, publisher=True):
    with EntityGraphTracker(G) as G:
        for entity_id in entity_ids:
            entity = alephclient.get_entity(entity_id, publisher=publisher)
            proxy = parse_entity(entity)
            G.add_proxy(proxy)
        return G.get_changes()


def add_aleph_collection(G, foreign_key, include=None, schema=None, publisher=True):
    with EntityGraphTracker(G) as G:
        collection = alephclient.get_collection_by_foreign_id(foreign_key)
        entities = alephclient.stream_entities(
            collection, include=include, schema=schema, publisher=publisher
        )
        edges = []
        for entity in entities:
            proxy = parse_entity(entity)
            if proxy.schema.edge:
                edges.append(proxy)
            else:
                G.add_proxy(proxy)
        G.add_proxies(edges)
        return G.get_changes()


def enrich_profiles(G, force=False):
    with EntityGraphTracker(G) as G:
        for canon_id, proxy in track_node_tag(G, "aleph_enrich_profile", force=force):
            log.debug(f"Finding profiles for: {proxy}")
            url = (
                f"entities/{proxy.id}/entitysets?type=profile&filter:judgement=positive"
            )
            profiles = aleph_get(url)
            merge_proxies = [proxy]
            for profile in profiles:
                log.debug(f"Found profile: {profile['id']}")
                url = f"entitysets/{profile['id']}/entities"
                entities = aleph_get(url)
                for entity in entities:
                    entity_proxy = parse_entity(entity)
                    log.debug(f"Adding profile entity: {entity_proxy.id}")
                    G.add_proxy(entity_proxy)
                    merge_proxies.append(entity_proxy)
            G.merge_proxies(*merge_proxies)
        return G.get_changes()


def on_properties(G, properties=None, schemata="Thing", filters=None, force=False):
    properties = set(properties or [])
    filters = [("schemata", schemata), *(filters or [])]
    nodes = list(G.get_nodes())
    for canon_id, datas in tqdm(nodes):
        log.debug(f"Finding property matches for: {canon_id}")
        inv_properties = {
            k: values
            for k, values in datas.get_type_inverted().items()
            if (properties is None or k in properties) and values
        }
        if not inv_properties:
            continue
        clean_value = lambda v: v.replace('"', '\\"')
        q_parts = [
            f'{prop}:"{clean_value(value)}"'
            for prop, values in inv_properties.items()
            for value in values
        ]
        log.debug(f"Searching for: {len(q_parts)}: {filters}")
        entities = aleph_get_qparts("/entities", q_parts, limit=200, filters=filters)
        connected_proxies = []
        for entity in entities:
            try:
                entity_proxy = parse_entity(entity)
            except TypeError:
                print(entity)
                raise
            log.debug(f"Adding entity: {entity_proxy.id}")
            connected_proxies.append(entity_proxy)
        yield canon_id, datas, connected_proxies


def enrich_properties(G, *args, **kwargs):
    with EntityGraphTracker(G) as G:
        for canon_id, datas, connected_proxies in on_properties(G, *args, **kwargs):
            G.add_proxies(connected_proxies)
            # HACK: the choice of proxies[0] is just convinient...
            # ideally we could create an edge to a canonical_id.
            G.merge_proxies(datas[0]["proxy"], *connected_proxies)
        return G.get_changes()


def expand_properties(G, *args, edge_schema="UnknownLink", **kwargs):
    with EntityGraphTracker(G) as G:
        for canon_id, datas, connected_proxies in on_properties(G, *args, **kwargs):
            G.add_proxies(connected_proxies)
            for cproxy in connected_proxies:
                if not cproxy.schema.edge:
                    # HACK: the choice of proxies[0] is just convinient...
                    # ideally we could create an edge to a canonical_id.
                    edge = G.create_edge_entity_from_proxies(
                        [datas[0]["proxy"]], [cproxy], schema=edge_schema
                    )
                    G.add_proxy(edge)
        return G.get_changes()


def enrich_xref(
    G,
    collection_id,
    match_collection_ids=None,
    entity_schemata="Thing",
    match_schemata="Thing",
    min_score=0.5,
    skip_unknown_entities=True,
):
    entity_schema = model.get(entity_schemata)
    match_schema = model.get(match_schemata)
    with EntityGraphTracker(G) as G:
        xrefs = aleph_get(f"collections/{collection_id}/xref")
        for xref in tqdm(xrefs):
            match_collection_id = int(xref["match_collection"]["collection_id"])
            if match_collection_ids and match_collection_id not in match_collection_ids:
                log.debug(
                    f"Collection not wanted: {match_collection_ids}: {match_collection_id}"
                )
                continue
            if skip_unknown_entities and xref["entity"]["id"] not in G:
                log.debug(f"Entity not in graph: {xref['entity']}")
                continue
            entity_proxy = parse_entity(xref["entity"])
            match_proxy = parse_entity(xref["match"])
            if not entity_proxy.schema.is_a(entity_schema):
                log.debug(
                    f"Entity is not the right schema: {entity_schema}: {entity_proxy.schema}"
                )
                continue
            if not match_proxy.schema.is_a(match_schema):
                log.debug(
                    f"Match is not the right schema: {match_schema}: {match_proxy.schema}"
                )
                continue
            G.add_proxy(entity_proxy)
            G.add_proxy(match_proxy)
            G.merge_proxies(entity_proxy, match_proxy)
        return G.get_changes()


def expand_interval(
    G, schematas=("Interval",), in_edges=True, out_edges=True, filters=None, force=False
):
    if not (in_edges or out_edges):
        raise ValueError("At least one of in_edges or out_edges must be True")
    nodes = list(G.get_nodes())
    if isinstance(schematas, str):
        schematas = [schematas]
    with EntityGraphTracker(G) as G:
        for schemata in tqdm(schematas):
            schema = model.get(schemata)
            in_edge_property = schema.edge_source
            out_edge_property = schema.edge_target
            if not (in_edge_property and out_edge_property):
                raise ValueError(f"Schemata has no edges: {schemata}")

            filters = [("schemata", schemata), *(filters or [])]
            for canon_id, datas in tqdm(nodes, leave=False):
                log.debug(f"Finding {schemata} edges for: {canon_id}")
                q_parts = []
                for data in datas:
                    proxy = data["proxy"]
                    if in_edges:
                        q_parts.append(f'properties.{in_edge_property}:"{proxy.id}"')
                    if out_edges:
                        q_parts.append(f'properties.{out_edge_property}:"{proxy.id}"')
                print(q_parts)
                intervals = aleph_get_qparts("/entities", q_parts, filters=filters)
                for interval in intervals:
                    edge = parse_edge(interval, in_edge_property, out_edge_property)
                    log.debug(f"Adding entity: {edge}")
                    G.add_proxies(edge["in_proxies"])
                    G.add_proxies(edge["out_proxies"])
                    G.add_proxy(edge["edge"])
        return G.get_changes()
