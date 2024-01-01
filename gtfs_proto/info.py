import argparse
import struct
import sys
import zstandard
import io
import json
from . import gtfs_pb2 as gtfs
from typing import Any


BLOCKS = {
    'header': gtfs.B_HEADER,
    'ids': gtfs.B_IDS,
    'strings': gtfs.B_STRINGS,

    'agency': gtfs.B_AGENCY,
    'calendar': gtfs.B_CALENDAR,
    'shapes': gtfs.B_SHAPES,

    'stops': gtfs.B_STOPS,
    'routes': gtfs.B_ROUTES,
    'trips': gtfs.B_TRIPS,
    'transfers': gtfs.B_TRANSFERS,

    'networks': gtfs.B_NETWORKS,
    'areas': gtfs.B_AREAS,
    'fare_links': gtfs.B_FARE_LINKS,
    'fares': gtfs.B_FARES,
}


def read_count(block: gtfs.Block, data: bytes) -> dict[str, Any]:
    COUNT = 'count'
    if block == gtfs.B_AGENCY:
        agencies = gtfs.Agencies()
        agencies.ParseFromString(data)
        return {COUNT: len(agencies.agencies)}
    elif block == gtfs.B_CALENDAR:
        calendar = gtfs.Calendar()
        calendar.ParseFromString(data)
        return {'dates': len(calendar.dates), COUNT: len(calendar.services)}
    elif block == gtfs.B_SHAPES:
        shapes = gtfs.Shapes()
        shapes.ParseFromString(data)
        return {COUNT: len(shapes.shapes)}
    elif block == gtfs.B_NETWORKS:
        networks = gtfs.Networks()
        networks.ParseFromString(data)
        return {COUNT: len(networks.networks)}
    elif block == gtfs.B_AREAS:
        areas = gtfs.Areas()
        areas.ParseFromString(data)
        return {COUNT: len(areas.areas)}
    elif block == gtfs.B_STRINGS:
        strings = gtfs.StringTable()
        strings.ParseFromString(data)
        return {COUNT: len(strings.strings)}
    elif block == gtfs.B_STOPS:
        stops = gtfs.Stops()
        stops.ParseFromString(data)
        return {COUNT: len(stops.stops)}
    elif block == gtfs.B_ROUTES:
        routes = gtfs.Routes()
        routes.ParseFromString(data)
        return {
            COUNT: len(routes.routes),
            'itineraries': sum(len(r.itineraries) for r in routes.routes)
        }
    elif block == gtfs.B_TRIPS:
        trips = gtfs.Trips()
        trips.ParseFromString(data)
        return {COUNT: len(trips.trips)}
    elif block == gtfs.B_TRANSFERS:
        tr = gtfs.Transfers()
        tr.ParseFromString(data)
        return {COUNT: len(tr.transfers)}
    elif block == gtfs.B_FARE_LINKS:
        fl = gtfs.FareLinks()
        fl.ParseFromString(data)
        return {
            'stop_zones': len(fl.stop_zone_ids),
            'stop_areas': len(fl.stop_area_ids),
            'route_networks': len(fl.route_network_ids),
        }
    return {}


def print_skip_empty(d: dict[str, Any]):
    print(json.dumps(
        {k: v for k, v in d.items() if v is not None and v != ''},
        ensure_ascii=False
    ))


def print_header(header: gtfs.GtfsHeader, fileobj: io.BytesIO):
    print_skip_empty({
        'version': header.version,
        'original_url': header.original_url,
        'compressed': header.compressed,
    })
    block_names = {v - 1: s for s, v in BLOCKS.items()}
    arch = zstandard.ZstdDecompressor()
    for i in range(len(header.blocks)):
        if header.blocks[i] > 0:
            v = {'block': block_names[i], 'size': header.blocks[i]}
            data = fileobj.read(header.blocks[i])
            if header.compressed:
                data = arch.decompress(data)
                v['compressed'] = v['size']
                v['size'] = len(data)
            v.update(read_count(i + 1, data))
            print(json.dumps(v))


def print_id(ids: gtfs.IdReference):
    block_names = {v: s for s, v in BLOCKS.items()}
    print_skip_empty({
        'block': block_names.get(ids.block, str(ids.block)),
        'ids': {i: s for i, s in enumerate(ids.ids) if i},
    })


def print_agency(a: gtfs.Agency):
    print_skip_empty({
        'agency_id': a.agency_id,
        'name': a.name,
        'url': a.url,
        'timezone': a.timezone,
        'lang': a.lang,
        'phone': a.phone,
        'fare_url': a.fare_url,
        'email': a.email,
    })


def print_calendar(c: gtfs.Calendar):
    print(json.dumps({'base_date': c.base_date}))
    for d in c.dates:
        print(json.dumps({
            'days_id': d.days_id,
            'dates': list(d.dates),
        }))
    for s in c.services:
        print_skip_empty({
            'service_id': s.service_id,
            'start_date': None if not s.start_date else s.start_date,
            'end_date': None if not s.end_date else s.end_date,
            'weekdays': f'{s.weekdays:#b}',
            'added_days': None if not s.added_days else s.added_days,
            'removed_days': None if not s.removed_days else s.removed_days,
        })


def print_shape(s: gtfs.TripShape):
    print(json.dumps({
        'shape_id': s.shape_id,
        'longitudes': list(s.longitudes),
        'latitudes': list(s.latitudes),
    }))


def print_stop(s: gtfs.Stop):
    LOC_TYPES = ['stop', 'station', 'exit', 'node', 'boarding']
    ACC_TYPES = ['unknown', 'some', 'no']
    print_skip_empty({
        'stop_id': s.stop_id,
        'code': s.code,
        'name': s.name,
        'desc': s.desc,
        'lon': s.lon,
        'lat': s.lat,
        'type': None if not s.type else LOC_TYPES[s.type],
        'parent_id': s.parent_id or None,
        'wheelchair': None if not s.wheelchair else ACC_TYPES[s.wheelchair],
        'platform_code': s.platform_code,
        'external_str_id': s.external_str_id,
        'external_int_id': s.external_int_id or None,
    })


def print_route(r: gtfs.Route):
    def prepare_itinerary(i: gtfs.RouteItinerary) -> dict[str, Any]:
        return {k: v for k, v in {
            'itinerary_id': i.itinerary_id,
            'headsign': i.headsign or None,
            'opposite_direction': i.opposite_direction or None,
            'stops': list(i.stops),
            'shape_id': i.shape_id or None,
        }.items() if v is not None}

    ROUTE_TYPES = {
        0: 'bus',
        1: 'tram',
        2: 'subway',
        3: 'rail',
        4: 'ferry',
        5: 'cable_tram',
        6: 'aerial',
        7: 'funicular',
        9: 'communal_taxi',
        10: 'coach',
        11: 'trolleybus',
        12: 'monorail',
        21: 'urban_rail',
        22: 'water',
        23: 'air',
        24: 'taxi',
        25: 'misc',
    }
    PD_TYPES = ['no', 'yes', 'phone_agency', 'tell_driver']
    print_skip_empty({
        'route_id': r.route_id,
        'agency_id': r.agency_id,
        'short_name': r.short_name,
        'long_name': list(r.long_name),
        'desc': r.desc,
        'type': ROUTE_TYPES[r.type],
        'color': None if not r.color else f'{r.color:#08x}',
        'text_color': None if not r.text_color else f'{r.text_color:#08x}',
        'continuous_pickup': None if not r.continuous_pickup else PD_TYPES[r.continuous_pickup],
        'continuous_dropoff': None if not r.continuous_dropoff else PD_TYPES[r.continuous_dropoff],
        'itineraries': [prepare_itinerary(i) for i in r.itineraries],
    })


def print_trip(t: gtfs.Trip):
    ACC_TYPES = ['unknown', 'some', 'no']
    PD_TYPES = ['no', 'yes', 'phone_agency', 'tell_driver']
    print_skip_empty({
        'trip_id': t.trip_id,
        'service_id': t.service_id,
        'itinerary_id': t.itinerary_id,
        'short_name': t.short_name,
        'wheelchair': None if not t.wheelchair else ACC_TYPES[t.wheelchair],
        'bikes': None if not t.bikes else ACC_TYPES[t.bikes],
        'approximate': t.approximate or None,
        'arrivals': list(t.arrivals) or None,
        'departures': list(t.departures) or None,
        'pickup_types': [PD_TYPES[p] for p in t.pickup_types] or None,
        'dropoff_types': [PD_TYPES[p] for p in t.dropoff_types] or None,
        'start_time': t.start_time or None,
        'end_time': t.end_time or None,
        'interval': t.interval or None,
    })


def print_transfer(t: gtfs.Transfer):
    TTYPES = ['possible', 'departure_waits', 'needs_time', 'not_possible',
              'in_seat', 'in_seat_forbidden']
    print_skip_empty({
        'from_stop': t.from_stop or None,
        'to_stop': t.to_stop or None,
        'from_route': t.from_route or None,
        'to_route': t.to_route or None,
        'from_trip': t.from_trip or None,
        'to_trip': t.to_trip or None,
        'type': None if not t.type else TTYPES[t.type],
        'min_transfer_time': t.min_transfer_time or None,
    })


def print_fare_links(fl: gtfs.FareLinks):
    print(json.dumps({
        'stop_area_ids': {i: v for i, v in enumerate(fl.stop_area_ids) if v}
    }))
    print(json.dumps({
        'stop_zone_ids': {i: v for i, v in enumerate(fl.stop_zone_ids) if v}
    }))
    print(json.dumps({
        'route_network_ids': {i: v for i, v in enumerate(fl.route_network_ids) if v}
    }))


def print_part(part: gtfs.Block, data: bytes):
    if part == gtfs.B_IDS:
        ids = gtfs.IdStore()
        ids.ParseFromString(data)
        for i in ids.refs:
            print_id(i)
    elif part == gtfs.B_AGENCY:
        agencies = gtfs.Agencies()
        agencies.ParseFromString(data)
        for a in agencies.agencies:
            print_agency(a)
    elif part == gtfs.B_CALENDAR:
        calendar = gtfs.Calendar()
        calendar.ParseFromString(data)
        print_calendar(calendar)
    elif part == gtfs.B_SHAPES:
        shapes = gtfs.Shapes()
        shapes.ParseFromString(data)
        for s in shapes.shapes:
            print_shape(s)
    elif part == gtfs.B_NETWORKS:
        networks = gtfs.Networks()
        networks.ParseFromString(data)
        print(json.dumps(dict(networks.networks), ensure_ascii=False))
    elif part == gtfs.B_AREAS:
        areas = gtfs.Areas()
        areas.ParseFromString(data)
        print(json.dumps(dict(areas.areas), ensure_ascii=False))
    elif part == gtfs.B_STRINGS:
        strings = gtfs.StringTable()
        strings.ParseFromString(data)
        print(json.dumps({i: s for i, s in enumerate(strings.strings)}, ensure_ascii=False))
    elif part == gtfs.B_STOPS:
        stops = gtfs.Stops()
        stops.ParseFromString(data)
        for s in stops.stops:
            print_stop(s)
    elif part == gtfs.B_ROUTES:
        routes = gtfs.Routes()
        routes.ParseFromString(data)
        for r in routes.routes:
            print_route(r)
    elif part == gtfs.B_TRIPS:
        trips = gtfs.Trips()
        trips.ParseFromString(data)
        for t in trips.trips:
            print_trip(t)
    elif part == gtfs.B_TRANSFERS:
        transfers = gtfs.Transfers()
        transfers.ParseFromString(data)
        for t in transfers.transfers:
            print_transfer(t)
    elif part == gtfs.B_FARE_LINKS:
        fl = gtfs.FareLinks()
        fl.ParseFromString(data)
        print_fare_links(fl)
    else:
        print('Sorry, printing blocks of this type is not implemented yet.', file=sys.stderr)


def info():
    parser = argparse.ArgumentParser(
        description='Print information and contents of a protobuf-compressed GTFS')
    parser.add_argument('input', type=argparse.FileType('rb'), help='Source file')
    parser.add_argument('-p', '--part', choices=BLOCKS.keys(),
                        help='Part to dump, header by default')
    parser.add_argument('-f', '--format', action='store_true',
                        help='Instead of single-line json, make the output readable')
    parser.add_argument('-o', '--output', help='Output file, stdout by default')
    options = parser.parse_args()

    header_len = struct.unpack('<h', options.input.read(2))[0]
    header = gtfs.GtfsHeader()
    header.ParseFromString(options.input.read(header_len))

    if not options.part:
        print_header(header, options.input)

    else:
        # First read the block
        part = BLOCKS[options.part]
        for b, size in enumerate(header.blocks):
            if b + 1 != part:
                if size > 0:
                    options.input.seek(size, 1)
            else:
                if size > 0:
                    data = options.input.read(size)
                    if header.compressed:
                        arch = zstandard.ZstdDecompressor()
                        data = arch.decompress(data)
                    print_part(b + 1, data)
                else:
                    print(f'Block {options.part} is empty.', file=sys.stderr)
