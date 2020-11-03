import logging
import itertools as IT
import json
from collections import defaultdict

from followthemoney import model

from .utils import track_node_tag


log = logging.getLogger(__name__)


def add_entities(G, *entities):
    new_proxies = []
    for entity in entities:
        proxy = model.get_proxy(entity)
        if G.add_proxy(proxy):
            new_proxies.append(proxy)
    return new_proxies

def add_entities_from_file(G, fd):
    new_proxies = []
    for line in fd:
        entity = json.loads(line)
        proxy = model.get_proxy(entity)
        if G.add_proxy(proxy):
            new_proxies.append(proxy)
    return new_proxies
    

def merge_properties(G, properties=None):
    properties = set(properties or [])
    new_proxies = []
    dedupe_table = defaultdict(set)
    for canon_id, proxy in G.get_proxies():
        for prop, values in proxy.get_type_inverted().items():
            if properties and prop not in properties:
                continue
            for value in values:
                key = f"{prop}:{value}"
                dedupe_table[key].add(canon_id)
    id_change_lookup = {}
    for key, canon_ids in dedupe_table.items():
        canon_ids = {id_change_lookup.get(cid, cid) for cid in canon_ids}
        if len(canon_ids) > 1:
            log.debug(f"Merging items: {key}: {len(canon_ids)}")
            canon_id_new = G.merge_nodes_canonicals(*canon_ids)
            id_change_lookup.update({cid: canon_id_new for cid in canon_ids})
    return []

        
