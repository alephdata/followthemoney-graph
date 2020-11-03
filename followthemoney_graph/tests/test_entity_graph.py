from followthemoney import model

import random
import logging
import string

from followthemoney_graph.entity_graph import EntityGraph

fmt = "%(name)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=fmt)


def random_proxies(schema="LegalEntity"):
    proxy = model.make_entity(schema)
    proxy.add("name", "".join(random.sample(string.ascii_letters, 8)))
    proxy.make_id(random.sample(string.ascii_letters, 8))
    return proxy


def test_create_nodes():
    proxies = [random_proxies() for _ in range(10)]
    G = EntityGraph()
    G.add_proxies(proxies)

    for p in proxies:
        assert p.id in G


def test_create_edges():
    proxies = [random_proxies() for _ in range(10)]
    edges = [
        EntityGraph.create_edge_entity_from_proxies([proxies[i]], [proxies[-i]])
        for i in range(len(proxies) // 2)
    ]

    G = EntityGraph()
    G.add_proxies(proxies)
    G.add_proxies(edges)

    for p in edges:
        assert p.id in G


def test_merge():
    proxies = [random_proxies() for _ in range(3)]
    edges = [
        EntityGraph.create_edge_entity_from_proxies([proxies[2]], [proxies[1]]),
        EntityGraph.create_edge_entity_from_proxies([proxies[0]], [proxies[2]]),
        EntityGraph.create_edge_entity_from_proxies([proxies[1]], [proxies[2]]),
    ]

    G = EntityGraph()
    G.add_proxies(proxies)
    G.add_proxies(edges)
    G.merge_proxies(proxies[0], proxies[1])
    node = G.get_node(proxies[0])

    assert len(G) == 6
    assert len(node["data"]) == 2
    assert len(node["in_edges"]) == 1
    assert len(node["out_edges"]) == 2


def test_merge_self_loop():
    proxies = [random_proxies() for _ in range(3)]
    edges = [
        EntityGraph.create_edge_entity_from_proxies([proxies[0]], [proxies[1]]),
        EntityGraph.create_edge_entity_from_proxies([proxies[0]], [proxies[2]]),
        EntityGraph.create_edge_entity_from_proxies([proxies[1]], [proxies[1]]),
        EntityGraph.create_edge_entity_from_proxies([proxies[1]], [proxies[2]]),
    ]

    G = EntityGraph()
    G.add_proxies(proxies)
    G.add_proxies(edges)
    G.merge_proxies(proxies[0], proxies[1])
    node = G.get_node(proxies[0])

    assert len(G) == 7
    assert len(node["data"]) == 2
    assert len(node["in_edges"]) == 2
    assert len(node["out_edges"]) == 4


def test_ensure_flag():
    proxies = [random_proxies() for _ in range(3)]
    G = EntityGraph()
    G.add_proxies(proxies)
    G.ensure_flag(test=True)
    for p in proxies:
        node = G.get_node(p)
        assert [d["flags"]["test"] is True for d in node["data"]]


def test_nodes():
    proxies = [random_proxies() for _ in range(10)]
    G = EntityGraph()
    G.add_proxies(proxies)
    G.ensure_flag(test=True)
    G.set_proxy_flags(proxies[0], test=False)
    G.set_proxy_flags(proxies[1], test=None)

    assert len(list(G.get_nodes())) == 10
    assert len(list(G.get_nodes(test=True))) == 8
    assert len(list(G.get_nodes(test=False))) == 1
    assert len(list(G.get_nodes(test=None))) == 1
