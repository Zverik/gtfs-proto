from .base import BasePacker, StringCache, IdReference
from typing import TextIO
from zipfile import ZipFile
from collections import defaultdict
from .. import gtfs_pb2 as gtfs


class StopsPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_STOPS

    def pack(self):
        areas = {}
        if self.has_file('stop_areas'):
            with self.open_table('stop_areas') as f:
                areas = self.read_stop_areas(f)
        with self.open_table('stops') as f:
            st = self.prepare(f, areas)
        return st

    def prepare(self, fileobj: TextIO, areas: dict[str, list[int]]) -> list[gtfs.Stop]:
        stops: list[gtfs.Stop] = []
        for row, stop_id, orig_stop_id in self.table_reader(fileobj, 'stop_id'):
            stop = gtfs.Stop(stop_id=stop_id)
            if row.get('stop_code'):
                stop.code = row['stop_code']
            if row['stop_name']:
                stop.name = self.strings.add(row['stop_name'])
            if row.get('stop_desc'):
                stop.desc = row['stop_desc']
            if row.get('stop_lat'):
                # Actually we don't know what happens when it's missing.
                stop.lat = round(float(row['stop_lat']) * 1e5)
                stop.lon = round(float(row['stop_lon']) * 1e5)
            if row.get('zone_id'):
                stop.zone = self.id_store[gtfs.B_ZONES].add(row['zone_id'])
            if orig_stop_id in areas:
                stop.areas = areas[orig_stop_id]
            stop.type = self.parse_location_type(row.get('location_type'))
            pstid = row.get('parent_station')
            if pstid:
                stop.parent_id = self.ids.add(pstid)
            if row.get('stop_timezone'):
                # We don't implement time zones because they are not used.
                pass
            stop.wheelchair = self.parse_accessibility(row.get('wheelchair_boarding'))
            if row.get('platform_code'):
                stop.platform_code = row['platform_code']
            stops.append(stop)
        return stops

    def read_stop_areas(self, fileobj: TextIO) -> dict[str, list[int]]:
        areas = defaultdict(list)
        for row, area_id, _ in self.table_reader(fileobj, 'area_id', gtfs.B_AREAS):
            areas[row['stop_id']].append(area_id)
        return areas

    def parse_location_type(self, value: str | None) -> int:
        if not value:
            return 0
        v = int(value)
        if v == 0:
            return gtfs.L_STOP
        if v == 1:
            return gtfs.L_STATION
        if v == 2:
            return gtfs.L_EXIT
        if v == 3:
            return gtfs.L_NODE
        if v == 4:
            return gtfs.L_BOARDING
        raise ValueError(f'Unknown location type for a stop: {v}')

    def parse_accessibility(self, value: str | None) -> int:
        if not value:
            return 0
        if value == '0':
            return gtfs.A_UNKNOWN
        if value == '1':
            return gtfs.A_SOME
        if value == '2':
            return gtfs.A_NO
        raise ValueError(f'Unknown accessibility value for a stop: {value}')
