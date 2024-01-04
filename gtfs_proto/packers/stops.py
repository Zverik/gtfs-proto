from .base import BasePacker, StringCache, IdReference, FareLinks
from typing import TextIO
from zipfile import ZipFile
from .. import gtfs_pb2 as gtfs


class StopsPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference],
                 fl: FareLinks):
        super().__init__(z, strings, id_store)
        self.fl = fl

    @property
    def block(self):
        return gtfs.B_STOPS

    def pack(self):
        with self.open_table('stops') as f:
            st = self.prepare(f)
        if self.has_file('stop_areas'):
            with self.open_table('stop_areas') as f:
                self.read_stop_areas(f)
        return st

    def prepare(self, fileobj: TextIO) -> list[gtfs.Stop]:
        stops: list[gtfs.Stop] = []
        last_lat: float = 0
        last_lon: float = 0
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
                new_lat = round(float(row['stop_lat']) * 1e5)
                new_lon = round(float(row['stop_lon']) * 1e5)
                stop.lat = new_lat - last_lat
                stop.lon = new_lon - last_lon
                last_lat, last_lon = new_lat, new_lon
            if row.get('zone_id'):
                self.fl.stop_zones[stop_id] = self.id_store[gtfs.B_ZONES].add(row['zone_id'])
            stop.type = self.parse_location_type(row.get('location_type'))
            pstid = row.get('parent_station')
            if pstid:
                stop.parent_id = self.ids.add(pstid)
            if row.get('stop_timezone'):
                raise Exception(f'Time to implement time zones! {row["stop_timezone"]}')
            stop.wheelchair = self.parse_accessibility(row.get('wheelchair_boarding'))
            if row.get('platform_code'):
                stop.platform_code = row['platform_code']
            stops.append(stop)
        return stops

    def read_stop_areas(self, fileobj: TextIO):
        for row, area_id, _ in self.table_reader(fileobj, 'area_id', gtfs.B_AREAS):
            self.fl.stop_areas[self.ids.add(row['stop_id'])] = area_id

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
