import sys

import click

from followthemoney_graph import EntityGraph
from followthemoney_graph.operations import aleph, graph
from followthemoney_graph.lib import visualize



@click.command()
@click.argument('foreign_id', required=True)
@click.option('--min-score', type=float, default=0.5, help="Minimum xref score")
@click.option('--component-size-min', type=int, default=5, help="Minimum component size for visualization")
@click.option('--component-size-max', type=int, default=50, help="maximum component size for visualization")
def companies_to_employees(foreign_id, min_score, component_size_min, component_size_max):
    G = EntityGraph()
    
    print("Reading collection:", foreign_id)
    aleph.add_aleph_collection(G, foreign_id)
    print(f"\t{G}")

    print(f"Enriching with xref with score >= {min_score}")
    aleph.enrich_xref(G, foreign_id, min_score=min_score)
    print(f"\t{G}")

    print(f"Expanding to Person entities with a matching `innCode`")
    aleph.expand_properties(G, properties=['innCode'],
                        edge_schema='Membership', edge_direction='in',
                        schematas=('Person'))
    print(f"\t{G}")

    print(f"Exporting graph to {foreign_id}.graphml")
    with open(f"{foreign_id}.graphml", 'w+') as fd:
        visualize.export_graphml(G, fd)

    component_size_range = (component_size_min, component_size_max)
    print(f"Filtering to components with size in the range {component_size_range}")
    G_small = graph.filter_component_size(G, component_size_range)
    print(f"\t{G_small}")

    print(f"Creating an HTML visualization of the filtered graph in {foreign_id}.html")
    visualize.show_entity_graph_pyvis(G_small, f"{foreign_id}.html")


if __name__ == "__main__":
    companies_to_employees()
