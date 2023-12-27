from .base import BasePacker, FeedCache
from typing import TextIO
from zipfile import ZipFile
from . import gtfs_pb2 as gtfs


class TripsPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache, trip_itineraries: dict[int, int]):
        super().__init__(z, store)
        self.trip_itineraries = trip_itineraries  # trip_id -> itinerary_id

    @property
    def block(self):
        return gtfs.B_TRIPS

    def pack(self):
        with self.open_table('trips') as f:
            trips = self.read_trips(f)
        if self.has_file('frequencies'):
            with self.open_table('frequencies') as f:
                self.from_frequencies(f, trips)
        with self.open_table('stop_times') as f:
            self.from_stop_times(f, trips)

        gtrips = gtfs.Trips(trips=trips.values())
        return gtrips.SerializeToString()

    def read_trips(self, fileobj: TextIO) -> dict[int, gtfs.Trip]:
        trips: dict[int, gtfs.Trip] = {}
        for row, trip_id, _ in self.table_reader(fileobj, 'trip_id'):
            # Skip trips without stops.
            if trip_id not in self.trip_itineraries:
                continue

            trips[trip_id] = gtfs.Trip(
                trip_id=trip_id,
                service_id=self.id_store[gtfs.B_CALENDAR].ids[row['service_id']],
                itinerary_id=self.trip_itineraries[trip_id],
                short_name=row.get('trip_short_name'),
                wheelchair=self.parse_accessibility(row.get('wheelchair_accessible')),
                bikes=self.parse_accessibility(row.get('bikes_allowed')),
            )
        return trips

    def from_stop_times(self, fileobj: TextIO, trips: dict[int, gtfs.Trip]):
        for row, trip_id, _ in self.table_reader(fileobj, 'trip_id'):
            pass  # TODO

    def from_frequencies(self, fileobj: TextIO, trips: dict[int, gtfs.Trip]):
        for row, trip_id, _ in self.table_reader(fileobj, 'trip_id'):
            pass  # TODO

    def parse_accessibility(self, value: str | None) -> int:
        if not value:
            return 0
        if value == '0':
            return gtfs.A_UNKNOWN
        if value == '1':
            return gtfs.A_SOME
        if value == '2':
            return gtfs.A_NO
        raise ValueError(f'Unknown accessibility value for a trip: {value}')
