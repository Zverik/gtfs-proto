from .base import BasePacker, StringCache, IdReference
from typing import TextIO
from zipfile import ZipFile
from dataclasses import dataclass
from .. import gtfs_pb2 as gtfs


@dataclass
class StopTime:
    seq_id: int
    arrival: int
    departure: int
    pickup: gtfs.PickupDropoff
    dropoff: gtfs.PickupDropoff
    approximate: bool


class TripsPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference],
                 trip_itineraries: dict[int, int]):
        super().__init__(z, strings, id_store)
        self.trip_itineraries = trip_itineraries  # trip_id -> itinerary_id

    @property
    def block(self):
        return gtfs.B_TRIPS

    def pack(self) -> list[gtfs.Trips]:
        with self.open_table('trips') as f:
            trips = self.read_trips(f)
        if self.has_file('frequencies'):
            with self.open_table('frequencies') as f:
                self.from_frequencies(f, trips)
        with self.open_table('stop_times') as f:
            self.from_stop_times(f, trips)
        return list(trips.values())

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
        for rows, trip_id, orig_trip_id in self.sequence_reader(
                fileobj, 'trip_id', 'stop_sequence', gtfs.B_TRIPS):
            cur_times: list[StopTime] = []
            for row in rows:
                arrival = self.parse_time(row['arrival_time']) or 0
                arrival = round(arrival / 5)
                departure = self.parse_time(row['departure_time']) or 0
                if departure:
                    departure = round(departure / 5)
                elif arrival:
                    departure = arrival
                cur_times.append(StopTime(
                    seq_id=int(row['stop_sequence']),
                    arrival=arrival,
                    departure=departure,
                    pickup=self.parse_pickup_dropoff(row.get('continuous_pickup')),
                    dropoff=self.parse_pickup_dropoff(row.get('continuous_drop_off')),
                    approximate=row.get('timepoint') == '0',
                ))
            self.fill_trip(trips[trip_id], cur_times)

    def fill_trip(self, trip: gtfs.Trip, times: list[StopTime]):
        times.sort(key=lambda t: t.seq_id)
        if trip.arrivals or trip.departures:
            raise ValueError(f'Trip was already filled: {self.ids.original[trip.trip_id]}')
        arrivals: list[int] = []
        for i in range(len(times)):
            a = times[i].arrival
            d = times[i].departure
            # Departures is the main list, arrivals is the auxillary.
            if i == 0 or d == 0:
                trip.departures.append(d)
            else:
                trip.departures.append(d - times[i-1].departure)
            # d - a >= 0 because if d == 0, we set it to arrival time in from_stop_times().
            arrivals.append(0 if not a else d - a)
        trip.arrivals.extend(self.cut_empty(arrivals, 0))
        trip.pickup_types.extend(self.cut_empty([t.pickup for t in times], 0))
        trip.dropoff_types.extend(self.cut_empty([t.dropoff for t in times], 0))
        trip.approximate = any(t.approximate for t in times)

    def cut_empty(self, values: list, zero) -> list:
        i = len(values)
        while i > 0 and values[i - 1] == zero:
            i -= 1
        return values[:i]

    def from_frequencies(self, fileobj: TextIO, trips: dict[int, gtfs.Trip]):
        for row, trip_id, _ in self.table_reader(fileobj, 'trip_id'):
            trip = trips[trip_id]  # assuming it's there
            start = self.parse_time(row['start_time']) or 0
            end = self.parse_time(row['end_time']) or 0
            trip.start_time = round(start / 60)
            trip.end_time = round(end / 60)
            trip.interval = int(row['headway_secs'])
            trip.approximate = row.get('exact_times') == '1'

    def parse_time(self, tim: str) -> int | None:
        tim = tim.strip()
        if not tim:
            return None
        if len(tim) == 7:
            tim = '0' + tim
        if len(tim) != 8:
            raise ValueError(f'Wrong time value: {tim}')
        return int(tim[:2]) * 3600 + int(tim[3:5]) * 60 + int(tim[6:])

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

    def parse_pickup_dropoff(self, value: str | None) -> int:
        if not value:
            return gtfs.PickupDropoff.PD_NO  # 0
        v = int(value)
        if v == 0:
            return gtfs.PickupDropoff.PD_YES
        if v == 1:
            return gtfs.PickupDropoff.PD_NO
        if v == 2:
            return gtfs.PickupDropoff.PD_PHONE_AGENCY
        if v == 3:
            return gtfs.PickupDropoff.PD_TELL_DRIVER
        raise ValueError(f'Wrong continous pickup / drop_off value: {v}')
