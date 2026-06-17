from .base import BasePacker, StringCache, IdReference
from typing import TextIO
from zipfile import ZipFile
from collections import defaultdict
from dataclasses import dataclass
from hashlib import md5
from .. import gtfs_pb2 as gtfs


@dataclass
class StopData:
    seq_id: int
    stop_id: int
    headsign: int | None
    pickup: gtfs.PickupDropoff
    dropoff: gtfs.PickupDropoff


class Trip:
    def __init__(self, trip_id: int, row: dict[str, str],
                 stops: list[StopData], shape_id: int | None):
        self.trip_id = trip_id
        self.headsign = row.get('trip_headsign')
        self.opposite = row.get('direction_id') == '1'
        self.stops = [s.stop_id for s in stops]
        self.shape_id = shape_id
        self.headsigns = [s.headsign for s in stops]
        self.pickup_types = [s.pickup for s in stops]
        self.dropoff_types = [s.dropoff for s in stops]

        # Generate stops key.
        m = md5(usedforsecurity=False)
        m.update(row['route_id'].encode())
        for s in self.stops:
            m.update(s.to_bytes(4))
        self.stops_key = m.hexdigest()

    def __hash__(self) -> int:
        return hash(self.stops_key)

    def __eq__(self, other):
        return self.stops_key == other.stops_key


class ItineraryPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)
        # trip_id → itinerary_id
        self.trip_itineraries: dict[int, int] = {}

    @property
    def block(self):
        return gtfs.B_ITINERARIES

    def pack(self) -> list[gtfs.Itinerary]:
        with self.open_table('stop_times') as f:
            trip_stops = self.read_trip_stops(f)
        with self.open_table('trips') as f:
            itineraries, self.trip_itineraries = self.read_itineraries(f, trip_stops)
        return itineraries

    def read_itineraries(self, fileobj: TextIO, trip_stops: dict[int, list[StopData]]
                         ) -> tuple[list[gtfs.Itinerary], dict[int, int]]:
        trips: dict[str, list[Trip]] = defaultdict(list)  # route_id -> list[Trip]
        for row, trip_id, orig_trip_id in self.table_reader(fileobj, 'trip_id', gtfs.B_TRIPS):
            stops = trip_stops.get(trip_id)
            if not stops:
                continue
            shape_id = (None if not row.get('shape_id')
                        else self.id_store[gtfs.B_SHAPES].ids[row['shape_id']])
            trips[row['route_id']].append(Trip(trip_id, row, stops, shape_id))

        # Now we have a list of itinerary-type trips which we need to deduplicate.
        # Note: since we don't have original ids, we need to keep them stable.
        # Since the only thing that matters is an order of stops, we use stops' hash as the key.

        result: list[gtfs.Itinerary] = []
        trip_itineraries: dict[int, int] = {}
        ids = self.id_store[gtfs.B_ITINERARIES]

        for route_id, trip_list in trips.items():
            for trip in set(trip_list):
                headsign = self.strings.add(trip.headsign)
                headsigns = [h or headsign for h in trip.headsigns]
                itin = gtfs.Itinerary(
                    route_id=self.id_store[gtfs.B_ROUTES].ids[route_id],
                    itinerary_id=ids.add(trip.stops_key),
                    opposite_direction=trip.opposite,
                    stops=trip.stops,
                    shape_id=trip.shape_id,
                    headsigns=self.cut_last(headsigns),
                    pickup_types=self.cut_last(trip.pickup_types),
                    dropoff_types=self.cut_last(trip.dropoff_types),
                )

                result.append(itin)
                for t in trip_list:
                    if t.stops_key == trip.stops_key:
                        trip_itineraries[t.trip_id] = itin.itinerary_id

        return result, trip_itineraries

    def read_trip_stops(self, fileobj: TextIO) -> dict[int, list[StopData]]:
        trip_stops: dict[int, list[StopData]] = {}  # trip_id -> stop data
        for rows, trip_id, orig_trip_id in self.sequence_reader(
                fileobj, 'trip_id', 'stop_sequence', gtfs.B_TRIPS):
            trip_stops[trip_id] = [StopData(
                seq_id=int(row['stop_sequence']),
                # The stop should be already in the table.
                stop_id=self.id_store[gtfs.B_STOPS].ids[row['stop_id']],
                headsign=self.strings.add(row.get('stop_headsign')),
                pickup=self.parse_pickup_dropoff(row.get('continuous_pickup')),
                dropoff=self.parse_pickup_dropoff(row.get('continuous_drop_off')),
            ) for row in rows]
        return trip_stops

    def parse_pickup_dropoff(self, value: str | None) -> int:
        if not value or value == '0':
            return gtfs.PickupDropoff.PD_YES
        if value == '1':
            return gtfs.PickupDropoff.PD_NO
        if value == '2':
            return gtfs.PickupDropoff.PD_PHONE_AGENCY
        if value == '3':
            return gtfs.PickupDropoff.PD_TELL_DRIVER
        raise ValueError(f'Wrong continous pickup / drop_off value: {value}')

    def cut_last(self, values: list) -> list:
        i = len(values)
        while i > 1 and values[i - 1] == values[i - 2]:
            i -= 1
        return values[:i]
