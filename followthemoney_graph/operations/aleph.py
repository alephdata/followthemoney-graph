import logging
import itertools as IT

from alephclient.api import AlephAPI, APIResultSet, AlephException

from followthemoney import model
from followthemoney_graph.cache import Cache
from .utils import track_node_tag


log = logging.getLogger(__name__)

alephclient = AlephAPI()
alephcache = Cache()


def _aleph_get(url, *args, **kwargs):
    fullurl = alephclient._make_url(url, *args, **kwargs)
    data = alephcache.get(fullurl)
    if data is not None:
        return data
    try:
        log.info(f"Aleph fetch: {url}")
        result = APIResultSet(alephclient, fullurl)
        data = list(result)
        alephcache.store(url, data)
        return data
    except AlephException:
        log.exception("Error calling Aleph API")
        return {}


def parse_edge(edge):
    subjects = [model.get_proxy(s) for s in edge["properties"]["subject"]]
    objects = [model.get_proxy(s) for s in edge["properties"]["object"]]

    edge["properties"]["subject"] = [s.id for s in subjects]
    edge["properties"]["object"] = [o.id for o in objects]
    edge_proxy = model.get_proxy(edge)
    return {"subjects": subjects, "objects": objects, "edge": edge_proxy}


def add_aleph_entities(G, *entity_ids, publisher=True):
    new_proxies = []
    for entity_id in entity_ids:
        entity = alephclient.get_entity(entity_id, publisher=publisher)
        proxy = model.get_proxy(entity)
        if G.add_proxy(proxy):
            new_proxies.append(proxy)
    return new_proxies


def add_aleph_collection(G, foreign_key, include=None, schema=None, publisher=True):
    collection = alephclient.get_collection_by_foreign_key(foreign_key)
    entities = alephclient.stream_entities(
        collection, include=include, schema=schema, publisher=publisher
    )
    new_proxies = []
    for entity in entities:
        proxy = model.get_proxy(entity)
        if G.add_proxy(proxy):
            new_proxies.append(proxy)
    return new_proxies


def enrich_profiles(G, force=False):
    new_proxies = []
    for canon_id, proxy in track_node_tag(G, "aleph_enrich_profile", force=force):
        log.debug(f"Finding profiles for: {proxy}")
        url = f"entities/{proxy.id}/entitysets?type=profile&filter:judgement=positive"
        profiles = _aleph_get(url)
        merge_proxies = [proxy]
        for profile in profiles:
            log.debug(f"Found profile: {profile['id']}")
            url = f"entitysets/{profile['id']}/entities"
            entities = _aleph_get(url)
            for entity in entities:
                entity_proxy = model.get_proxy(entity)
                log.debug(f"Adding profile entity: {entity_proxy.id}")
                if G.add_proxy(entity_proxy):
                    new_proxies.append(entity_proxy)
                merge_proxies.append(entity_proxy)
        G.merge_proxies(*merge_proxies)
    return new_proxies


def enrich_properties(G, properties=None, schemata="Thing", force=False):
    properties = set(properties or [])
    new_proxies = []
    filters = [("schemata", schemata)]
    for canon_id, proxy in track_node_tag(G, "aleph_enrich_properties", force=force):
        log.debug(f"Finding property matches for: {proxy}")
        inv_properties = {
            k: v
            for k, v in proxy.get_type_inverted().items()
            if properties is None or k in properties
        }
        merge_proxies = [proxy]
        for prop, values in inv_properties.items():
            for value in values:
                log.debug(f"Searching for {prop} = {value}")
                entities = _aleph_get("/entities", filters=[(prop, value), *filters])
                for entity in entities:
                    entity_proxy = model.get_proxy(entity)
                    log.debug(f"Adding entity: {entity_proxy.id}")
                    if G.add_proxy(entity_proxy):
                        new_proxies.append(entity_proxy)
                    merge_proxies.append
        G.merge_proxies(*merge_proxies)
    return new_proxies


def expand_interest(G, schemata="Interest", force=False, in_edges=True, out_edges=True):
    new_proxies = []
    filters = [("schemata", schemata)]
    for canon_id, proxy in track_node_tag(
        G, f"aleph_expand_interest_{schemata}", force=force
    ):
        log.debug(f"Finding interest edges for: {proxy}")
        if in_edges:
            entities_object = _aleph_get(
                "/entities",
                filters=[("properties.object", proxy.id), *filters],
            )
        else:
            entities_object = []
        if out_edges:
            entities_subject = _aleph_get(
                "/entities",
                filters=[("properties.subject", proxy.id), *filters],
            )
        else:
            entities_subject = []
        for interest in IT.chain(entities_subject, entities_object):
            edge = parse_edge(interest)
            log.debug(f"Adding entity: {edge}")
            for s in edge["subjects"]:
                if G.add_proxy(s):
                    new_proxies.append(s)
            for o in edge["objects"]:
                if G.add_proxy(o):
                    new_proxies.append(o)
            if G.add_proxy(edge["edge"]):
                new_proxies.append(edge["edge"])
    return new_proxies
