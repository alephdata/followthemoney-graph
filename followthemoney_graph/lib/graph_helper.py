import logging

from tqdm.autonotebook import tqdm

log = logging.getLogger(__name__)


def track_node_tag(G, flag, force=False):
    G.ensure_flag(**{flag: False})
    # pre-get all the applicable nodes since we are making some in-place
    # changes
    if force:
        nodes = list(G.nodes())
    else:
        nodes = list(G.nodes(**{flag: False}))
    for node in tqdm(nodes):
        yield node
        node.set_flags(**{flag: True})

