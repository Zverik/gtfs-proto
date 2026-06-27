from . import gtfs_pb2 as gtfs
from functools import cached_property


class StringCache:
    def __init__(self, source: list[str] | None = None):
        self.strings: list[str] = ['']
        self.index: dict[str, int] = {}
        if source:
            self.set(source)

    def clear(self):
        self.strings = ['']
        self.index = {}

    def set(self, source: list[str]):
        self.strings = list(source) or ['']
        self.index = {s: i for i, s in enumerate(self.strings) if s}

    def __getitem__(self, i: int) -> str:
        return self.strings[i]

    def add(self, s: str | None) -> int:
        if not s:
            return 0
        i = self.index.get(s)
        if i:
            return i
        else:
            self.strings.append(s)
            self.index[s] = len(self.strings) - 1
            return len(self.strings) - 1

    def search(self, s: str) -> int | None:
        """Looks for a string case-insensitive."""
        i = self.index.get(s)
        if i:
            return i
        s = s.lower()
        for j, v in enumerate(self.strings):
            if s == v.lower():
                return j
        return None

    def store(self) -> bytes:
        return gtfs.StringTable(
            strings=[] if len(self.strings) == 1 else self.strings).SerializeToString()


class IdReference:
    def __init__(self, source: list[str] | None = None, delta_skip: int = 0):
        self.ids: dict[str, int] = {s: i + delta_skip for i, s in enumerate(source or []) if s}
        self.last_id = 0 if not self.ids else max(self.ids.values())
        self.delta_skip = delta_skip

    def __getitem__(self, k: str) -> int:
        return self.ids[k]

    def __len__(self) -> int:
        return len(self.ids)

    def clear(self):
        self.ids = {}
        self.last_id = 0

    def copy(self):
        c = IdReference()
        c.ids = self.ids.copy()
        c.last_id = self.last_id
        c.delta_skip = self.delta_skip
        return c

    def add(self, k: str) -> int:
        if k not in self.ids:
            self.last_id += 1
            self.ids[k] = self.last_id
            # Remove reversed cache.
            if 'original' in self.__dict__:
                delattr(self, 'original')
        return self.ids[k]

    def get(self, k: str | None, misses: bool = False) -> int | None:
        if not k:
            return None
        if misses:
            return self.ids.get(k)
        return self.ids[k]

    def to_list(self) -> list[str]:
        idstrings = [''] * (self.last_id + 1)
        for s, i in self.ids.items():
            idstrings[i] = s
        return idstrings[self.delta_skip:]

    @cached_property
    def original(self) -> dict[int, str]:
        return {i: s for s, i in self.ids.items()}
