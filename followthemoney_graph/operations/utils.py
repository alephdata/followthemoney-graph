def get_node_label(G, node, maxlen=32):
    if node.schema.edge:
        name = node.schema.name
    else:
        try:
            name = node.names[0]
        except IndexError:
            name = "N/A"
    if len(name) > maxlen:
        name = name[: maxlen - 3] + "..."
    return f"{name}"


def get_node_desc(G, node):
    desc = f"{len(node.proxies)} Proxes<br>"
    desc += f"Schema: {node.schema.name}<br>"
    desc += "<br>".join(f"{p}: {node[0]}" for p, values in node.properties.items())
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

