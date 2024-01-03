from .. import gtfs_pb2 as gtfs
from ..base import IdReference, FareLinks, StringCache
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from csv import DictReader
from io import TextIOWrapper
from typing import TextIO
from zipfile import ZipFile


__all__ = ['BasePacker', 'StringCache', 'FareLinks', 'IdReference']


class BasePacker(ABC):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        self.z = z
        self.id_store = id_store
        self.strings = strings

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
