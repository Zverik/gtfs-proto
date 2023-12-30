import struct
import zstandard
from . import gtfs_pb2 as gtfs
from typing import BinaryIO


class StringCache:
    def __init__(self, source: list[str] | None = None):
        self.strings: list[str] = source or ['']
        self.index: dict[str, int] = {s: i for i, s in enumerate(self.strings) if s}

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
        st = gtfs.StringTable(strings=self.strings)
        return st.SerializeToString()


class IdReference:
    def __init__(self, source: list[str] | None = None):
        self.ids: dict[str, int] = {s: i for i, s in enumerate(source or []) if s}
        self.last_id = 0 if not self.ids else max(self.ids.values())

    def __getitem__(self, k: str) -> int:
        return self.ids[k]

    def __len__(self) -> int:
        return len(self.ids)

    def add(self, k: str) -> int:
        if k not in self.ids:
            self.last_id += 1
            self.ids[k] = self.last_id
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
        return idstrings

    def reversed(self) -> dict[int, str]:
        return {i: s for s, i in self.ids.items()}


class FeedCache:
    def __init__(self):
        self.version = 0
        self.strings = StringCache()
        self.id_store: dict[int, IdReference] = {
            b: IdReference() for b in gtfs.Block.values()}

    def load(self, fileobj: BinaryIO | None):
        if not fileobj:
            return

        header_len = struct.unpack('<h', fileobj.read(2))[0]
        header = gtfs.GtfsHeader()
        header.ParseFromString(fileobj.read(header_len))
        arch = None if not header.compressed else zstandard.ZstdDecompressor()
        self.version = header.version

        for b, size in enumerate(header.blocks):
            if b + 1 == gtfs.B_STRINGS:
                data = fileobj.read(size)
                if arch:
                    data = arch.decompress(data)
                s = gtfs.StringTable()
                s.ParseFromString(data)
                self.strings = StringCache(s.strings)
            elif b + 1 == gtfs.B_IDS:
                data = fileobj.read(size)
                if arch:
                    data = arch.decompress(data)
                store = gtfs.IdStore()
                store.ParseFromString(data)
                for idrefs in store.refs:
                    self.id_store[idrefs.block] = IdReference(idrefs.ids)
            else:
                fileobj.seek(size, 1)

    def store(self) -> bytes:
        idstore = gtfs.IdStore()
        for block, ids in self.id_store.items():
            if ids:
                idrefs = gtfs.IdReference(block=block, ids=ids.to_list())
                idstore.refs.append(idrefs)
        return idstore.SerializeToString()
