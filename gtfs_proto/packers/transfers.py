from .base import BasePacker, StringCache, IdReference
from typing import TextIO
from zipfile import ZipFile
from csv import DictReader
from math import ceil
from .. import gtfs_pb2 as gtfs


class TransfersPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_TRANSFERS

    def pack(self) -> list[gtfs.Transfer]:
        if self.has_file('transfers'):
            with self.open_table('transfers') as f:
                return self.prepare(f)
        return []

    def prepare(self, fileobj: TextIO) -> list[gtfs.Transfer]:
        transfers: list[gtfs.Transfer] = []
        id_stops = self.id_store[gtfs.B_STOPS]
        id_routes = self.id_store[gtfs.B_ROUTES]
        id_trips = self.id_store[gtfs.B_TRIPS]
        for row in DictReader(fileobj):
            t = gtfs.Transfer()

            # stops
            from_stop = row.get('from_stop_id')
            if from_stop:
                t.from_stop = id_stops.get(from_stop)
            to_stop = row.get('to_stop_id')
            if to_stop:
                t.to_stop = id_stops.get(to_stop)

            # routes
            from_route = row.get('from_route_id')
            if from_route:
                t.from_route = id_routes.get(from_route)
            to_route = row.get('to_route_id')
            if to_route:
                t.to_route = id_routes.get(to_route)

            # trips
            from_trip = row.get('from_trip_id')
            if from_trip:
                t.from_trip = id_trips.get(from_trip)
            to_trip = row.get('to_trip_id')
            if to_trip:
                t.to_trip = id_trips.get(to_trip)

            transfer_time = row.get('min_transfer_time')
            if transfer_time and transfer_time.strip():
                t.min_transfer_time = ceil(float(transfer_time.strip()) / 5)
            t.type = self.parse_transfer_type(row['transfer_type'])
            transfers.append(t)
        return transfers

    def parse_transfer_type(self, value: str | None) -> int:
        if not value:
            return 0
        v = int(value.strip())
        if v == 0:
            return gtfs.T_POSSIBLE
        if v == 1:
            return gtfs.T_DEPARTURE_WAITS
        if v == 2:
            return gtfs.T_NEEDS_TIME
        if v == 3:
            return gtfs.T_NOT_POSSIBLE
        if v == 4:
            return gtfs.T_IN_SEAT
        if v == 5:
            return gtfs.T_IN_SEAT_FORBIDDEN
        raise ValueError(f'Unknown transfer type: {v}')
