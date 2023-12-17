from .base import BasePacker, FeedCache
from typing import TextIO
from zipfile import ZipFile
from . import gtfs_pb2 as gtfs


class RoutesPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache):
        super().__init__(z, store)

    @property
    def block(self):
        return gtfs.B_ROUTES

    def pack(self):
        route_networks: dict[str, int] = {}
        if self.has_file('route_networks'):
            with self.open_table('route_networks') as f:
                route_networks = self.read_route_networks(f)
        with self.open_table('routes') as f:
            return self.prepare(f, route_networks)

    def prepare(self, fileobj: TextIO,
                route_networks: dict[str, int]) -> bytes:
        routes = gtfs.Routes()
        agency_ids = self.id_store[gtfs.B_AGENCY]
        network_ids = self.id_store[gtfs.B_NETWORKS]
        for row, route_id, orig_route_id in self.table_reader(fileobj, 'route_id'):
            route = gtfs.Route(route_id=route_id)
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

            # TODO: Itineraries empty for now.
            routes.routes.append(route)
        return routes.SerializeToString()

    def read_route_networks(self, fileobj: TextIO) -> dict[str, int]:
        result: dict[str, int] = {}
        for row, network_id, _ in self.table_reader(fileobj, 'network_id', gtfs.B_NETWORKS):
            result[row['route_id']] = network_id
        return result

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
