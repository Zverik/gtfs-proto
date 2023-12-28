from .base import BasePacker, FeedCache
from typing import TextIO
from zipfile import ZipFile
from csv import DictReader
from collections import defaultdict
from hashlib import sha1
from . import gtfs_pb2 as gtfs


class Trip:
    def __init__(self, trip_id: int, row: dict[str, str],
                 stops: list[int], shape_id: int | None):
        self.trip_id = trip_id
        self.headsign = row.get('trip_headsign')
        self.opposite = row.get('direction_id') == '1'
        self.stops = stops
        self.shape_id = shape_id

        # Generate stops key.
        m = sha1(usedforsecurity=False)
        for s in stops:
            m.update(s.to_bytes(4))
        self.stops_key = m.hexdigest()

    def __hash__(self) -> int:
        return hash(self.stops_key)

    def __eq__(self, other):
        return self.stops_key == other.stops_key


class RoutesPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache):
        super().__init__(z, store)
        # trip_id → itinerary_id
        self.trip_itineraries: dict[int, int] = {}

    @property
    def block(self):
        return gtfs.B_ROUTES

    def pack(self):
        route_networks: dict[str, int] = {}
        if self.has_file('route_networks'):
            with self.open_table('route_networks') as f:
                route_networks = self.read_route_networks(f)
        with self.open_table('stop_times') as f:
            trip_stops = self.read_trip_stops(f)
        with self.open_table('trips') as f:
            itineraries, self.trip_itineraries = self.read_itineraries(f, trip_stops)
        with self.open_table('routes') as f:
            return self.prepare(f, route_networks, itineraries)

    def prepare(self, fileobj: TextIO,
                route_networks: dict[str, int],
                itineraries: dict[str, list[gtfs.RouteItinerary]]) -> bytes:
        routes = gtfs.Routes()
        agency_ids = self.id_store[gtfs.B_AGENCY]
        network_ids = self.id_store[gtfs.B_NETWORKS]
        for row, route_id, orig_route_id in self.table_reader(fileobj, 'route_id'):
            # Skip routes for which we don't have any trips.
            if orig_route_id not in itineraries:
                continue

            route = gtfs.Route(route_id=route_id, itineraries=itineraries[orig_route_id])
            if row.get('agency_id'):
                route.agency_id = agency_ids[row['agency_id']]

            # Network Id can come from two sources.
            if orig_route_id in route_networks:
                route.network_id = route_networks[orig_route_id]
            elif row.get('network_id', ''):
                route.network_id = network_ids.add(row['network_id'])

            if row.get('route_short_name', ''):
                route.short_name = row['route_short_name']
            if row.get('route_long_name', ''):
                route.long_name.extend(self.parse_route_long_name(row['route_long_name']))
            if row.get('route_desc', ''):
                route.desc = row['route_desc']
            route.type = self.route_type_to_enum(int(row['route_type']))
            if row.get('route_color', '') and row['route_color'].upper() != 'FFFFFF':
                route.color = int(row['route_color'], 16)
            if row.get('route_text_color', '') and row['route_text_color'] != '000000':
                route.text_color = int(row['route_text_color'], 16)
            route.continuous_pickup = self.parse_pickup_dropoff(row.get('continuous_pickup'))
            route.continuous_dropoff = self.parse_pickup_dropoff(row.get('continuous_drop_off'))

            routes.routes.append(route)
        return routes.SerializeToString()

    def read_route_networks(self, fileobj: TextIO) -> dict[str, int]:
        result: dict[str, int] = {}
        for row, network_id, _ in self.table_reader(fileobj, 'network_id', gtfs.B_NETWORKS):
            result[row['route_id']] = network_id
        return result

    def read_itineraries(self, fileobj: TextIO, trip_stops: dict[str, list[int]]
                         ) -> tuple[dict[str, list[gtfs.RouteItinerary]], dict[int, int]]:
        trips: dict[str, list[Trip]] = defaultdict(list)  # route_id -> list[Trip]
        for row, trip_id, orig_trip_id in self.table_reader(fileobj, 'trip_id', gtfs.B_TRIPS):
            stops = trip_stops.get(orig_trip_id)
            if not stops:
                continue
            shape_id = (None if not row.get('shape_id')
                        else self.id_store[gtfs.B_SHAPES].ids[row['shape_id']])
            trips[row['route_id']].append(Trip(trip_id, row, stops, shape_id))

        # Now we have a list of itinerary-type trips which we need to deduplicate.
        # Note: since we don't have original ids, we need to keep them stable.
        # Since the only thing that matters is an order of stops, we use stops' hash as the key.

        result: dict[str, list[gtfs.RouteItinerary]] = defaultdict(list)
        trip_itineraries: dict[int, int] = {}
        ids = self.id_store[gtfs.B_ITINERARIES]

        for route_id, trip_list in trips.items():
            for trip in set(trip_list):
                itin = gtfs.RouteItinerary(
                    itinerary_id=ids.add(trip.stops_key),
                    opposite_direction=trip.opposite,
                    stops=trip.stops,
                    shape_id=trip.shape_id,
                )
                if trip.headsign:
                    itin.headsign = self.strings.add(trip.headsign)

                result[route_id].append(itin)
                for t in trip_list:
                    if t.stops_key == trip.stops_key:
                        trip_itineraries[t.trip_id] = itin.itinerary_id

        return result, trip_itineraries

    def read_trip_stops(self, fileobj: TextIO) -> dict[str, list[int]]:
        trip_stops: dict[str, list[int]] = {}  # trip_id -> int_stop_id
        cur_trip: str = ''
        cur_stops: list[tuple[int, int]] = []
        for row in DictReader(fileobj):
            if row['trip_id'] != cur_trip:
                if cur_stops and cur_trip:
                    cur_stops.sort(key=lambda s: s[0])
                    if cur_trip in trip_stops:
                        raise ValueError(
                            f'Unsorted stop_times.txt, trip {cur_trip} is in two parts')
                    trip_stops[cur_trip] = [s[1] for s in cur_stops]
                cur_trip = row['trip_id']
                cur_stops = []
            cur_stops.append((
                int(row['stop_sequence']),
                # The stop should be already in the table.
                self.id_store[gtfs.B_STOPS].ids[row['stop_id']],
            ))
        if cur_stops and cur_trip:
            cur_stops.sort(key=lambda s: s[0])
            trip_stops[cur_trip] = [s[1] for s in cur_stops]
        return trip_stops

    def route_type_to_enum(self, t: int) -> int:
        if t == 0 or t // 100 == 9:
            return gtfs.RouteType.TRAM
        if t in (1, 401, 402):
            return gtfs.RouteType.SUBWAY
        if t == 2 or t // 100 == 1:
            return gtfs.RouteType.RAIL
        if t == 3 or t // 100 == 7:
            return gtfs.RouteType.BUS
        if t == 4 or t == 1200:
            return gtfs.RouteType.FERRY
        if t == 5 or t == 1302:
            return gtfs.RouteType.CABLE_TRAM
        if t == 6 or t // 100 == 13:
            return gtfs.RouteType.AERIAL
        if t == 7 or t == 1400:
            return gtfs.RouteType.FUNICULAR
        if t == 1501:
            return gtfs.RouteType.COMMUNAL_TAXI
        if t // 100 == 2:
            return gtfs.RouteType.COACH
        if t == 11 or t == 800:
            return gtfs.RouteType.TROLLEYBUS
        if t == 12 or t == 405:
            return gtfs.RouteType.MONORAIL
        if t in (400, 403, 403):
            return gtfs.RouteType.URBAN_RAIL
        if t == 1000:
            return gtfs.RouteType.WATER
        if t == 1100:
            return gtfs.RouteType.AIR
        if t // 100 == 15:
            return gtfs.RouteType.TAXI
        if t // 100 == 17:
            return gtfs.RouteType.MISC
        raise ValueError(f'Wrong route type {t}')

    def parse_route_long_name(self, name: str) -> list[int]:
        if not name:
            return []
        name = name.replace('—', '-').replace('–', '-')
        parts = [s.strip() for s in name.split(' - ') if s.strip()]
        idx = [self.strings.search(p) for p in parts]
        if not any(idx) or len(parts) == 1:
            return [self.strings.add(name)]
        for i in range(len(idx)):
            if not idx[i]:
                idx[i] = self.strings.add(parts[i])
        # TODO: Check when parts are capitalized
        return idx  # type: ignore

    def parse_pickup_dropoff(self, value: str | None) -> int:
        if not value:
            return 0
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
