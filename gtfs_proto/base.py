from zipfile import ZipFile
from typing import TextIO, BinaryIO
from collections.abc import Generator
from io import TextIOWrapper
from . import gtfs_pb2 as gtfs
from abc import ABC, abstractmethod
import zstandard
from contextlib import contextmanager
from csv import DictReader


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

    def to_list(self) -> list[str]:
        idstrings = [''] * (self.last_id + 1)
        for s, i in self.ids.items():
            idstrings[i] = s
        return idstrings

    def reversed(self) -> dict[int, str]:
        return {i: s for s, i in self.ids.items()}


class FeedCache:
    def __init__(self):
        self.strings = StringCache()
        self.id_store: dict[int, IdReference] = {
            b: IdReference() for b in gtfs.Block.values()}

    def load(self, fileobj: BinaryIO | None) -> int:
        if not fileobj:
            return 0

        store = gtfs.IdStore()
        store.ParseFromString(fileobj.read())

        for idrefs in store.refs:
            self.id_store[idrefs.block] = IdReference(idrefs.ids)
        self.strings = StringCache(store.strings)

        return store.version

    def store(self, version: int) -> bytes:
        idstore = gtfs.IdStore(version=version, strings=self.strings.strings)
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
        header.agency = len(self.blocks.get(gtfs.B_AGENCY, b''))
        header.calendar = len(self.blocks.get(gtfs.B_CALENDAR, b''))
        header.shapes = len(self.blocks.get(gtfs.B_SHAPES, b''))
        header.networks = len(self.blocks.get(gtfs.B_NETWORKS, b''))
        header.areas = len(self.blocks.get(gtfs.B_AREAS, b''))
        header.strings = len(self.blocks.get(gtfs.B_STRINGS, b''))
        header.stops = len(self.blocks.get(gtfs.B_STOPS, b''))
        header.routes = len(self.blocks.get(gtfs.B_ROUTES, b''))
        header.trips = len(self.blocks.get(gtfs.B_TRIPS, b''))
        header.transfers = len(self.blocks.get(gtfs.B_TRANSFERS, b''))
        # header.fares_v1 = len(self.blocks.get(gtfs.B_FARES_V1, b''))
        # header.fares_v2 = len(self.blocks.get(gtfs.B_FARES_V2, b''))

    @property
    def not_empty(self):
        return any(self.blocks.values())

    def __iter__(self):
        for b in sorted(self.blocks):
            yield self.blocks[b]

    def archive_if(self, data: bytes):
        if self.compress:
            arch = zstandard.ZstdCompressor(level=10, write_content_size=False)
            return arch.compress(data)
        return data

    def add(self, block: int, data: bytes):
        if not data:
            return
        self.blocks[block] = self.archive_if(data)
        if self.header:
            self.populate_header(self.header)

    def run(self, packer: BasePacker):
        self.add(packer.block, packer.pack())