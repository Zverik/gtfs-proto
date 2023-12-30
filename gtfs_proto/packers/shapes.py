from .base import BasePacker, FeedCache
from typing import TextIO
from zipfile import ZipFile
from csv import DictReader
from .. import gtfs_pb2 as gtfs


class ShapesPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache):
        super().__init__(z, store)

    @property
    def block(self):
        return gtfs.B_SHAPES

    def pack(self):
        with self.open_table('shapes') as f:
            return self.prepare(f)

    def prepare(self, fileobj: TextIO) -> bytes:
        last_point: tuple[int, int] = (0, 0)

        def pack_points(points: list[tuple[int, int, int]]) -> tuple[list[int], list[int]]:
            nonlocal last_point
            points.sort(key=lambda pt: pt[2])
            pts: tuple[list[int], list[int]] = (
                [points[0][0] - last_point[0]], [points[0][1] - last_point[1]])
            for i in range(1, len(points)):
                for j in (0, 1):
                    pts[j].append(points[i][j] - points[i-1][j])
            last_point = points[-1][:2]
            return pts

        # First build the points.
        shapes: list[tuple[int, list[int], list[int]]] = []
        points: list[tuple[int, int, int]] = []
        cur_shape: str | None = None
        for row in DictReader(fileobj):
            if cur_shape != row['shape_id']:
                if len(points) >= 2 and cur_shape:
                    shapes.append((self.ids.add(cur_shape), *pack_points(points)))
                cur_shape = row['shape_id']
                points = []
            points.append((
                round(float(row['shape_pt_lon']) * 100000),
                round(float(row['shape_pt_lat']) * 100000),
                int(row['shape_pt_sequence']),
            ))
        if len(points) >= 2 and cur_shape:
            shapes.append((self.ids.add(cur_shape), *pack_points(points)))

        sh = gtfs.Shapes()
        for shape in shapes:
            sh.shapes.append(gtfs.TripShape(
                shape_id=shape[0],
                longitudes=shape[1],
                latitudes=shape[2],
            ))
        return sh.SerializeToString()
