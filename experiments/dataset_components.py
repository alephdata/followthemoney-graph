from tqdm import tqdm
from multiprocessing import Pool
from alephclient.api import AlephAPI, AlephException
from followthemoney import model
from collections import Counter
import json
import os

api = AlephAPI()


class Components:
    def __init__(self):
        self.data = []

    def add(self, *items):
        found = False
        merge = []
        for i, d in enumerate(self.data):
            if any(i in d for i in items):
                if not found:
                    d.update(items)
                    found = True
                merge.append(i)
        if not found:
            self.data.append(set(items))
        elif len(merge) > 1:
            merge.sort()
            d = self.data[merge[0]]
            for i in reversed(merge[1:]):
                d.update(self.data.pop(i))

    def components(self):
        return self.data

    def n_nodes(self):
        return sum(len(d) for d in self.data)


def calculate_components(collection):
    fid = collection["foreign_id"]
    n_entities = collection["count"]
    components = Components()
    links = api.stream_entities(collection, schema="Interval")
    for link in tqdm(links, total=n_entities, leave=False, desc=fid):
        proxy = model.get_proxy(link)
        sources = proxy.get(proxy.schema.source_prop)
        targets = proxy.get(proxy.schema.target_prop)
        components.add(link["id"], *sources, *targets)
    hist = Counter(len(c) for c in components.components())
    hist[1] = n_entities - components.n_nodes()
    return hist


def process_collection(collection):
    fid = collection["foreign_id"]
    fname = f"./dataset_components/{fid}.json"
    if os.path.exists(fname):
        return
    try:
        components = calculate_components(collection)
    except AlephException as e:
        print(f"Aleph Error: {fid}: {e}")
        return
    with open(fname, "w+") as fd:
        data = {
            "components_histogram": dict(components),
            "collection": collection,
        }
        fd.write(json.dumps(data))


if __name__ == "__main__":
    collections = api.filter_collections("*")
    n_collections = collections.result["total"]
    collections = tqdm(collections, total=n_collections)
    with Pool() as p:
        p.apply(process_collection, collections)