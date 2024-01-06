from .base import BasePacker, StringCache, IdReference
from zipfile import ZipFile
from .. import gtfs_pb2 as gtfs
from .. import build_shape


class ShapesPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_SHAPES

    def pack(self) -> list[gtfs.Shape]:
        result: list[gtfs.Shape] = []
        with self.open_table('shapes') as f:
            for rows, shape_id, _ in self.sequence_reader(
                    f, 'shape_id', 'shape_pt_sequence', max_overlapping=1):
                if len(rows) >= 2:
                    result.append(build_shape(
                        shape_id=shape_id,
                        coords=[(float(row['shape_pt_lon']), float(row['shape_pt_lat']))
                                for row in rows],
                    ))
        return result
