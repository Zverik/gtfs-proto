from .. import gtfs_pb2 as gtfs
from ..base import IdReference, FareLinks, StringCache
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from csv import DictReader
from io import TextIOWrapper
from typing import TextIO, Any
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
    def pack(self) -> Any:
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

    def sequence_reader(self, fileobj: TextIO, id_column: str,
                        seq_column: str, ids_block: int | None = None,
                        max_overlapping: int = 2,
                        ) -> Generator[tuple[list[dict], int, str], None, None]:
        cur_ids: list[int] = []
        cur_lists: list[list[tuple[int, str, dict]]] = []
        seen_ids: set[int] = set()
        for row, row_id, orig_id in self.table_reader(fileobj, id_column, ids_block):
            # Find the row_id index. From the tail, because latest ids are appended there.
            idx = len(cur_ids) - 1
            while idx >= 0 and cur_ids[idx] != row_id:
                idx -= 1

            if idx < 0:
                # Not found: dump the oldest sequence and add the new one.
                if row_id in seen_ids:
                    raise ValueError(
                        f'Unsorted sequence file, {id_column} {orig_id} is in two parts')
                seen_ids.add(row_id)

                if len(cur_ids) >= max_overlapping:
                    last_id = cur_ids.pop(0)
                    last_rows = cur_lists.pop(0)
                    last_rows.sort(key=lambda r: r[0])
                    yield [r[2] for r in last_rows], last_id, last_rows[0][1]

                cur_ids.append(row_id)
                cur_lists.append([])
                idx = len(cur_ids) - 1

            cur_lists[idx].append((int(row[seq_column]), orig_id, row))

        for i, row_id in enumerate(cur_ids):
            rows = cur_lists[i]
            rows.sort(key=lambda r: r[0])
            yield [r[2] for r in rows], row_id, rows[0][1]
