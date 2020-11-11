import logging
from functools import lru_cache
from copy import deepcopy

from alephclient.api import AlephAPI, APIResultSet, AlephException
from tqdm.autonotebook import tqdm

from followthemoney import model
from ..lib import track_node_tag, EntityGraphTracker


log = logging.getLogger(__name__)

alephclient = AlephAPI()
alephclient._request = lru_cache(2048)(alephclient._request)


def aleph_get(url, *args, **kwargs):
    fullurl = alephclient._make_url(url, *args, **kwargs)
    log.debug(f"Aleph fetch: {fullurl}")
    try:
        return _alephget(fullurl)
    except AlephException:
        log.exception("Error calling Aleph API")
        return {}


def aleph_get_qparts(url, q_parts, *args, max_query_length=3072, **kwargs):
    """
    We take a list of query parts to get OR'd together. The queries are chunked
    so that the length is up to 3072 characters long in order to make sure we
    don't get rejected from aleph for having too long of a URL. The limit is
    technically 4096, but we'd like to leave a buffer just in ase
    """
    join_qs = lambda qs: "({})".format(f" OR ".join(qs))
    while q_parts:
        size = 0
        for i in range(len(q_parts)):
            q_test = join_qs(q_parts[:i])
            url_test = alephclient._make_url(url, q_test, *args, **kwargs)
            url_len = len(url_test)
            if url_len > max_query_length:
                break
        else:
            i += 1
        q = join_qs(q_parts[:i])
        q_parts = q_parts[i:]
        yield from aleph_get(url, q=q, *args, **kwargs)


def clean_q_value(value):
    return value.replace('"', '\\"')


def gen_q_part(field, value):
    value = clean_q_value(value)
    return f'(_exists_:"{field}" AND {field}:"{value}")'


def _alephget(url):
    log.debug(f"Fetching from server: {url}")
    return APIResultSet(alephclient, url)


def parse_edge(edge):
    schema = model.get(edge["schema"])
    in_edge_property = schema.edge_source
    out_edge_property = schema.edge_target
    ins = [parse_entity(s) for s in edge["properties"][in_edge_property]]
    outs = [parse_entity(s) for s in edge["properties"][out_edge_property]]
    edge_proxy = parse_entity(edge)
    return {"in_proxies": ins, "out_proxies": outs, "edge": edge_proxy}


def parse_entity(entity):
    entity = deepcopy(entity)
    for key, values in entity["properties"].items():
        for i, value in enumerate(values):
            if isinstance(value, dict):
                values[i] = value.get("id")
    return model.get_proxy(entity)


def add_aleph_entities(G, *entity_ids, publisher=True):
    with EntityGraphTracker(G) as G:
        for entity_id in entity_ids:
            entity = alephclient.get_entity(entity_id, publisher=publisher)
            if entity["id"] not in G:
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
            if entity["id"] not in G:
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


def on_properties(G, properties=None, schemata="Thing", filters=None):
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
        # TODO: try doing the following instead of OR'ing everything:
        # (prop.A = i OR prop.A = j) AND (prop.B = k OR prop.B = l)
        q_parts = [
            gen_q_part(prop, value)
            for prop, values in inv_properties.items()
            for value in values
        ]
        log.debug(f"Searching for: {len(q_parts)}: {filters}")
        entities = aleph_get_qparts("entities", q_parts, limit=200, filters=filters)
        connected_proxies = []
        for entity in entities:
            try:
                entity_proxy = parse_entity(entity)
            except TypeError:
                log.exception(f"Could not parse entity: {entity}")
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
            if xref["score"] < min_score:
                log.debug(
                    f"Stoping xref enrichment due to low xref score: {xref['score']} < {min_score}"
                )
                break
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
    G, schematas=("Interval",), in_edges=True, out_edges=True, filters=None
):
    if not (in_edges or out_edges):
        raise ValueError("At least one of in_edges or out_edges must be True")
    nodes = list(G.get_nodes())
    if isinstance(schematas, str):
        schematas = [schematas]
    schemas = [model.get(s) for s in schematas]
    in_edge_properties = [s.edge_source for s in schemas]
    out_edge_properties = [s.edge_target for s in schemas]
    if not (in_edge_properties and out_edge_properties):
        raise ValueError(f"Schematas have no edges: {schematas}")
    filters = filters or []
    filters.extend(("schemata", schemata) for schemata in schematas)
    with EntityGraphTracker(G) as G:
        for canon_id, datas in tqdm(nodes, leave=False):
            log.debug(f"Finding {schematas} edges for: {canon_id}")
            q_parts = []
            for data in datas:
                proxy = data["proxy"]
                if in_edges:
                    q_parts.extend(
                        gen_q_part(f"properties.{in_edge_property}", proxy.id)
                        for in_edge_property in in_edge_properties
                    )
                if out_edges:
                    q_parts.extend(
                        gen_q_part(f"properties.{out_edge_property}", proxy.id)
                        for out_edge_property in out_edge_properties
                    )
            intervals = aleph_get_qparts("entities", q_parts, filters=filters)
            for interval in intervals:
                if interval["id"] not in G:
                    edge = parse_edge(interval)
                    log.debug(f"Adding entity: {edge}")
                    G.add_proxies(edge["in_proxies"])
                    G.add_proxies(edge["out_proxies"])
                    G.add_proxy(edge["edge"])
        return G.get_changes()
