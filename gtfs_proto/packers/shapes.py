from .base import BasePacker, StringCache, IdReference
from typing import TextIO
from zipfile import ZipFile
from .. import gtfs_pb2 as gtfs


class ShapesPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_SHAPES

    def pack(self):
        with self.open_table('shapes') as f:
            return self.prepare(f)

    def prepare(self, fileobj: TextIO) -> bytes:
        last_point: list[int] = [0, 0]

        def pack_coords(coords: list[float], lp_idx: int) -> list[int]:
            nonlocal last_point
            points = [round(float(c) * 100000) for c in coords]
            lp = last_point[lp_idx]
            pts: list[int] = [points[0] - lp]
            for i in range(1, len(points)):
                pts.append(points[i] - points[i-1])
            last_point[lp_idx] = points[-1]
            return pts

        sh = gtfs.Shapes()
        for rows, shape_id, _ in self.sequence_reader(
                fileobj, 'shape_id', 'shape_pt_sequence', max_overlapping=1):
            if len(rows) >= 2:
                sh.shapes.append(gtfs.Shape(
                    shape_id=shape_id,
                    longitudes=pack_coords([row['shape_pt_lon'] for row in rows], 0),
                    latitudes=pack_coords([row['shape_pt_lat'] for row in rows], 1),
                ))

        return sh.SerializeToString()
