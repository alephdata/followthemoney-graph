def get_node_label(G, node, data, maxlen=32):
    data = list(data)
    try:
        name = next(d["proxy"].names[0] for d in data if d["proxy"].names)
    except StopIteration:
        name = "N/A"
    if len(name) > maxlen:
        name = name[:maxlen] + "..."
    return f"{name}"


def get_node_desc(G, node, data):
    d = G.describe_node(node)
    desc = f"{len(data)} Proxes; In Edges: {len(d['in_edges'])}; Out Edges: {len(d['out_edges'])}<br>"
    desc += f"Schema: {data[0]['proxy'].schema.name}<br>"
    desc += "<br>".join(f"{p}: {values[0]}" for p, values in data.properties().items())
    return desc


def get_edge_label(G, node, data):
    data = list(data)
    return f"{data[0]['proxy'].schema.name}"


def get_edge_desc(G, edge, data):
    src_node, dst_node = G.get_nodes(edge[0], edge[1])
    src_label = get_node_label(G, *src_node)
    dst_label = get_node_label(G, *dst_node)
    desc = f"{src_label} -> {dst_label}<br>"
    desc += f"Schema: {data[0]['proxy'].schema.name}<br>"
    desc += "<br>".join(f"{p}: {values[0]}" for p, values in data.properties().items())
    return desc


def is_notebook():
    """
    From: https://stackoverflow.com/a/39662359
    """
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter

