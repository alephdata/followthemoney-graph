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
    node = G.describe_proxy(proxies[0])

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
    node = G.describe_proxy(proxies[0])

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
        node = G.describe_proxy(p)
        assert [d["flags"]["test"] is True for d in node["data"]]


def test_get_nodes():
    proxies = [random_proxies() for _ in range(10)]
    G = EntityGraph()
    G.add_proxies(proxies)
    G.ensure_flag(test=True)
    G.set_proxy_flags(proxies[0], test=False)
    G.set_proxy_flags(proxies[1], test=None)
    G.merge_proxies(*proxies[2:])

    assert len(list(G.get_node_proxies())) == 10
    assert len(list(G.get_node_proxies(test=True))) == 8
    assert len(list(G.get_node_proxies(test=False))) == 1
    assert len(list(G.get_node_proxies(test=None))) == 1

    assert len(list(G.get_nodes())) == 3
    assert len(list(G.get_nodes(test=True))) == 1
    assert len(list(G.get_nodes(test=False))) == 1
    assert len(list(G.get_nodes(test=None))) == 1


def test_intersect_basic():
    proxies_a = [random_proxies() for _ in range(5)]
    proxies_b = [random_proxies() for _ in range(5)]
    common = [random_proxies() for _ in range(5)]

    g_a = EntityGraph()
    g_a.add_proxies(proxies_a)
    g_a.add_proxies(common)

    g_b = EntityGraph()
    g_b.add_proxies(proxies_b)
    g_b.add_proxies(common)

    g = EntityGraph.intersect(g_a, g_b)

    assert len(g) == len(common)
    assert all(p.id in g for p in common)
    assert all(p.id not in g for p in proxies_a)
    assert all(p.id not in g for p in proxies_b)


def test_intersect_merge():
    proxies_a = [random_proxies() for _ in range(5)]
    proxies_b = [random_proxies() for _ in range(5)]
    common = [random_proxies() for _ in range(5)]

    g_a = EntityGraph()
    g_a.add_proxies(proxies_a)
    g_a.add_proxies(common)

    g_a.merge_proxies(proxies_a[0], common[0])
    g_a.add_proxy(EntityGraph.create_edge_entity_from_proxies([common[3]], [common[4]]))
    assert len(g_a) == 11

    g_b = EntityGraph()
    g_b.add_proxies(proxies_b)
    g_b.add_proxies(common)
    g_b.merge_proxies(proxies_b[0], common[1])
    assert len(g_b) == 10

    g = EntityGraph.intersect(g_a, g_b)

    assert len(g) == len(common) + 2 + 1  # 2 -> merge, 1 -> edge
    assert all(p.id in g for p in common)

    assert proxies_a[0].id in g
    assert proxies_b[0].id in g

    assert len(g.get_proxy_data(proxies_a[0])) == 2
    assert len(g.get_proxy_data(proxies_b[0])) == 2

    assert all(p.id not in g for p in proxies_a[1:])
    assert all(p.id not in g for p in proxies_b[1:])

    node = g.describe_proxy(common[3])
    assert len(list(g.get_edge_nodes())) == 1
    assert len(node["out_edges"]) == 1
