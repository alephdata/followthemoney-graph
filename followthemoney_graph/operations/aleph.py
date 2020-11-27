import logging
from functools import lru_cache
from copy import deepcopy
from functools import partial, wraps
from concurrent.futures import ThreadPoolExecutor

from alephclient.api import AlephAPI, EntityResultSet, AlephException
from tqdm.autonotebook import tqdm

from followthemoney import model
from followthemoney.exc import InvalidData


log = logging.getLogger(__name__)

alephclient = AlephAPI(timeout=30)
alephclient._request = lru_cache(2048)(alephclient._request)


def aleph_initializer(initializer=None):
    global alephclient
    alephclient = AlephAPI(timeout=30)
    if initializer is not None:
        initializer()


@wraps(ThreadPoolExecutor)
def AlephPool(*args, **kwargs):
    kwargs["initializer"] = aleph_initializer(initializer=kwargs.get("initializer"))
    return ThreadPoolExecutor(*args, **kwargs)


def aleph_get(api, url, *args, **kwargs):
    fullurl = api._make_url(url, *args, **kwargs)
    log.debug(f"Aleph fetch: {fullurl}")
    return _alephget(api, fullurl)


def aleph_get_qparts(
    api, base_url, q_parts, *args, max_query_length=3600, merge="or", **kwargs
):
    """
    We take a list of query parts to get OR'd together. The queries are chunked
    so that the length is up to 3600 characters long in order to make sure we
    don't get rejected from aleph for having too long of a URL. The limit is
    technically 4096, but we'd like to leave a buffer just in ase
    """
    if not q_parts:
        return
    url = _make_url_qparts(api, base_url, q_parts, *args, merge=merge, **kwargs)
    if len(url) <= max_query_length:
        yield from _alephget(api, url)
    else:
        results = []
        while q_parts:
            url = None
            for i in range(len(q_parts)):
                url_test = _make_url_qparts(
                    api, base_url, q_parts[:i], *args, merge=merge, **kwargs
                )
                url_len = len(url_test)
                if url_len > max_query_length:
                    break
                url = url_test
            else:
                i += 1
            q_parts = q_parts[i:]
            if merge.lower() == "or":
                yield from _alephget(api, url)
            else:
                result = {r["id"]: r for r in _alephget(api, url)}
        if results and merge.lower() == "and":
            keys = set.intersection(*[set(r) for r in result])
            for key in keys:
                yield results[0][key]


def _make_url_qparts(api, url, q_parts, merge="or", *args, **kwargs):
    q = f" {merge.upper()} ".join(q_parts)
    return api._make_url(url, q, *args, **kwargs)


def clean_q_value(value):
    return value.replace('"', '\\"')


def gen_q_part(field, values):
    values = map(clean_q_value, values)
    value = " OR ".join(f'"{v}"' for v in values)
    return f'(_exists_:"{field}" AND {field}:({value}))'


def _alephget(api, url):
    try:
        log.debug(f"Fetching from server: {url}")
        return EntityResultSet(api, url, publisher=True)
    except AlephException as e:
        log.critical(f"Error calling Aleph API: {e}")
        return []


def parse_nested(edge):
    schema = model.get(edge["schema"])
    in_edge_property = schema.edge_source
    out_edge_property = schema.edge_target
    for p in (in_edge_property, out_edge_property):
        for item in edge["properties"].get(p, []):
            if isinstance(item, dict):
                log.debug(f"Found nested item: {item['id']}")
                yield parse_entity(item)
    yield parse_entity(edge)


def parse_entity(entity):
    try:
        entity = deepcopy(entity)
        for key, values in entity.get("properties", {}).items():
            for i, value in enumerate(values):
                if isinstance(value, dict):
                    values[i] = value.get("id")
        return model.get_proxy(entity)
    except AttributeError:
        return None


def add_aleph_entities(G, *entity_ids, publisher=True):
    N = 0
    for entity_id in entity_ids:
        entity = alephclient.get_entity(entity_id, publisher=publisher)
        if entity["id"] not in G:
            proxy = parse_entity(entity)
            G.add_proxy(proxy)
            N += 1
    return N


def add_aleph_collection(G, foreign_key, include=None, schema=None, publisher=True):
    N = 0
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
                node, is_new = G.add_proxy(proxy)
                N += int(is_new)
    N += sum(int(is_new) for node, is_new in G.add_proxies(edges))
    return N


def flag_list(G, list_id, flag):
    url = f"entitysets/{list_id}/entities"
    N = 0
    for entity in aleph_get(alephclient, url):
        if entity["id"] in G:
            proxy = parse_entity(entity)
            G.get_node_by_proxy(proxy).set_flags(**{flag: True})
            N += 1
    return N


def enrich_xref(
    G,
    foreign_id,
    match_collection_ids=None,
    entity_schemata=None,
    match_schemata=None,
    min_score=0.5,
    skip_unknown_entities=True,
):
    if entity_schemata:
        entity_schema = model.get(entity_schemata)
    if match_schemata:
        match_schema = model.get(match_schemata)
    collection = alephclient.get_collection_by_foreign_id(foreign_id)
    collection_id = collection["id"]
    xrefs = alephclient.get_collection_xref(collection_id, publisher=True)
    N = 0
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
        if entity_schemata and not entity_proxy.schema.is_a(entity_schema):
            log.debug(
                f"Entity is not the right schema: {entity_schema}: {entity_proxy.schema}"
            )
            continue
        if match_schemata and not match_proxy.schema.is_a(match_schema):
            log.debug(
                f"Match is not the right schema: {match_schema}: {match_proxy.schema}"
            )
            continue
        try:
            G.add_proxy(entity_proxy)
            G.add_proxy(match_proxy)
            G.merge_proxies(entity_proxy, match_proxy)
        except InvalidData:
            pass
        N += 1
    return N


def _enrich_similar(item, min_score):
    try:
        name, properties = item
        data = {
            "schema": name,
            "properties": properties,
        }
        matches = alephclient.match(data, publisher=True)
        data = []
        for match in matches:
            if match["score"] <= min_score:
                break
            data.append(match)
        return data
    except AlephException as e:
        log.critical(f"Aleph Exception: {e}")
        return None
    except Exception:
        log.exception("General enrich Exception")
        return None


def enrich_similar(G, min_score=120, mapper=map):
    N = 0
    nodes = list(
        node for node in G.nodes(aleph_enrich_similar=None) if node.schema.matchable
    )
    task = partial(_enrich_similar, min_score=min_score)
    task_args = ((n.schema.name, n.properties) for n in nodes)
    results = zip(nodes, mapper(task, task_args))
    for node, matches in tqdm(results, total=len(nodes)):
        if matches is None:
            continue
        for match in matches:
            match_proxy = parse_entity(match)
            try:
                node, is_new = G.add_proxy(match_proxy, node_id=node.id)
            except InvalidData:
                G.add_proxy(match_proxy)
                link = model.make_entity("UnknownLink")
                link.add("subject", node.parts[0])
                link.add("object", match_proxy.id)
                link.make_id(node.parts[0], match_proxy.id)
                _, is_new = G.add_proxy(link)
            except KeyError as e:
                raise e
            N += int(is_new)
        node.set_flags(aleph_enrich_similar=True)
    return N


def _expand(item, filters):
    try:
        pids, is_edge = item
        if is_edge:
            return []
        q_parts = [f"entities:{pid}" for pid in pids]
        edges = aleph_get_qparts(
            alephclient, "entities", q_parts, filters=filters, merge="or"
        )
        return list(edges)
    except AlephException as e:
        log.critical(f"Aleph Exception: {len(q_parts)} query parts: {e}")
        return None
    except Exception:
        log.exception("General expand exception")
        return None


def expand(G, schematas=("Interval",), filters=None, mapper=map):
    if isinstance(schematas, str):
        schematas = [schematas]
    filters = filters or []
    filters.extend(("schemata", schemata) for schemata in schematas)
    flag = f'aleph_expand_{"_".join(schematas)}'
    nodes = list(node for node in G.nodes(**{flag: None}) if not node.schema.edge)
    task = partial(_expand, filters=filters)
    task_args = ((n.parts, n.schema.edge) for n in nodes)
    N = 0
    results = zip(nodes, mapper(task, task_args))
    for node, edges in tqdm(results, total=len(nodes)):
        if edges is None:
            continue
        for edge in edges:
            if edge["id"] not in G:
                log.debug(f"Adding edge: {edge}")
                result = G.add_proxies(parse_nested(edge))
                N += sum(int(is_new) for _, is_new in result)
        node.set_flags(**{flag: True})
    return N
