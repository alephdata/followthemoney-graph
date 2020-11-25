from followthemoney.multi_part_proxy import MultiPartProxy


class Node(MultiPartProxy):
    def __init__(self, *args, flags=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.flags = flags or {}

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

