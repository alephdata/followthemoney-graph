from followthemoney.multi_part_proxy import MultiPartProxy


def match_dict(haystack, needle, ignore_keys=None):
    for key, value in needle.items():
        if ignore_keys and key in ignore_keys:
            continue
        try:
            target = haystack[key]
        except KeyError:
            return False
        if isinstance(target, (list, tuple, set)):
            if not any(_match(t, value) for t in target):
                return False
        else:
            if not _match(target, value):
                return False
    return True


def _match(target, value):
    if isinstance(value, dict):
        if not match_dict(target, value):
            return False
    elif isinstance(value, (tuple, set, list)):
        if not any(
            match_dict(target, v) if isinstance(v, dict) else v in target for v in value
        ):
            return False
    else:
        if not target == value:
            return False
    return True


class Node(MultiPartProxy):
    def __init__(self, *args, flags=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.flags = flags or {}

    @property
    def has_edge(self):
        return any(self.edges)

    @property
    def edges(self):
        return (
            name for name, p in self.schema.properties.items() if not p.stub and p.range
        )

    def fill_stub(self, proxy):
        if len(self.proxies) == 1:
            self.proxies = {proxy}
            self._build()
            return proxy
        for p in self.proxies:
            if p.id == proxy.id:
                p.merge(proxy)
                self._merge_new_proxy(proxy)
                return p
        raise IndexError

    def set_flags(self, **flag_values):
        self.flags.update(flag_values)

    def ensure_flag(self, **flag_values):
        for flag, value in flag_values.items():
            self.flags.setdefault(flag, value)

    def has_flags(self, **flag_values):
        return all(self.flags.get(f) == v for f, v in flag_values.items())

    def merge(self, other):
        super().merge(other)
        for flag, value in other.flags.items():
            self.flags[flag] = self.flags.get(flag, False) and value
        return self

    def match(self, other, ignore_edges=True, exact=True):
        if other.schema.name != "UnknownLink" and not self.schema.is_a(other.schema):
            return False
        if not self.has_flags(**other.flags):
            return False
        if exact:
            ignore_props = set()
            if ignore_edges:
                ignore_props.update(self.edges)
            if not match_dict(
                self.properties, other.properties, ignore_keys=ignore_props
            ):
                return False
        elif other.properties:
            if not match_dict(
                self.get_type_inverted(),
                other.get_type_inverted(),
                ignore_keys=["entities"],
            ):
                return False
        if not match_dict(self.golden_proxy.context, other.golden_proxy.context):
            return False
        return True
