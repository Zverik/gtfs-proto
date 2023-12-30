import struct
import zstandard
from . import gtfs_pb2 as gtfs
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from csv import DictReader
from io import TextIOWrapper
from typing import TextIO, BinaryIO
from zipfile import ZipFile


class StringCache:
    def __init__(self, source: list[str] | None = None):
        self.strings: list[str] = source or ['']
        self.index: dict[str, int] = {s: i for i, s in enumerate(self.strings) if s}

    def add(self, s: str) -> int:
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


class BasePacker(ABC):
    def __init__(self, z: ZipFile, store: FeedCache):
        self.z = z
        self.id_store = store.id_store
        self.strings = store.strings

    @property
    @abstractmethod
    def block(self) -> int:
        return gtfs.B_HEADER

    @abstractmethod
    def pack(self) -> bytes:
        return b''

    def has_file(self, name_part: str) -> bool:
        return f'{name_part}.txt' in self.z.namelist()

    @contextmanager
    def open_table(self, name_part: str):
        with self.z.open(f'{name_part}.txt', 'r') as f:
            yield TextIOWrapper(f, encoding='utf-8-sig')

    @property
    def ids(self) -> IdReference:
        return self.id_store[self.block]

    def table_reader(self, fileobj: TextIO, id_column: str,
                     ids_block: int | None = None
                     ) -> Generator[tuple[dict, int, str], None, None]:
        """Iterates over CSV rows and returns (row, our_id, source_id)."""
        ids = self.id_store[ids_block or self.block]
        for row in DictReader(fileobj):
            yield (
                {k: v.strip() for k, v in row.items()},
                ids.add(row[id_column]),
                row[id_column],
            )


class GtfsBlocks:
    def __init__(self, header: gtfs.GtfsHeader | None = None, compress: bool = False):
        self.blocks: dict[gtfs.Block, bytes] = {}
        self.header = header
        self.compress = compress

    def populate_header(self, header: gtfs.GtfsHeader):
        # This version of protobuf doesn't have the "clear()" method for repeated fields.
        while header.blocks:
            header.blocks.pop()
        for b in gtfs.Block.values():
            if 0 < b and b < gtfs.B_ITINERARIES:
                header.blocks.append(len(self.blocks.get(b, b'')))

    @property
    def not_empty(self):
        return any(self.blocks.values())

    def __iter__(self):
        for b in sorted(self.blocks):
            yield self.blocks[b]

    def archive_if(self, data: bytes):
        if self.compress:
            arch = zstandard.ZstdCompressor(level=10)
            return arch.compress(data)
        return data

    def add(self, block: int, data: bytes):
        if not data:
            return
        self.blocks[block] = self.archive_if(data)

    def run(self, packer: BasePacker):
        self.add(packer.block, packer.pack())
