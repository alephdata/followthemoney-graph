import logging

from tqdm.autonotebook import tqdm

log = logging.getLogger(__name__)


def track_node_tag(G, flag, force=False):
    G.ensure_flag(**{flag: False})
    # pre-get all the applicable nodes since we are making some in-place
    # changes
    if force:
        nodes = list(G.get_node_proxies())
    else:
        nodes = list(G.get_node_proxies(**{flag: False}))
    log.debug(f"Processing nodes: {flag}: {len(nodes)}")
    for canon_id, proxy in tqdm(nodes):
        yield canon_id, proxy
        G.set_proxy_flags(proxy, **{flag: True})

