import argparse
import sys
import json
from typing import Any
from .wrapper import (
    GtfsProto, gtfs, GtfsDelta, FareLinks, is_gtfs_delta, GtfsBlocks
)


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

    # Not actual blocks.
    'version': -1,
    'date': -2,
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
        pass  # TODO
        # fl = gtfs.FareLinks()
        # fl.ParseFromString(data)
        # return {
        #     'stop_zones': len(fl.stop_zone_ids),
        #     'stop_areas': len(fl.stop_area_ids),
        #     'route_networks': len(fl.route_network_ids),
        # }
    return {}


def print_skip_empty(d: dict[str, Any]):
    print(json.dumps(
        {k: v for k, v in d.items() if v is not None and v != ''},
        ensure_ascii=False
    ))


def print_header(header: gtfs.GtfsHeader):
    print_skip_empty({
        'version': header.version,
        'date': header.date,
        'original_url': header.original_url,
        'compressed': header.compressed,
    })


def print_delta_header(header: gtfs.GtfsDeltaHeader):
    print_skip_empty({
        'old_version': header.old_version,
        'version': header.version,
        'date': header.date,
        'compressed': header.compressed,
    })


def print_blocks(blocks: GtfsBlocks, compressed: bool):
    block_names = {v: s for s, v in BLOCKS.items()}
    for b in blocks.blocks:
        data = blocks.get(b)
        v = {
            'block': block_names.get(b, str(b)),
            'size': len(data),
        }
        if compressed:
            v['compressed'] = len(blocks.blocks[b])
        v.update(read_count(b, data))
        print(json.dumps(v))


def print_id(ids: gtfs.IdReference):
    block_names = {v: s for s, v in BLOCKS.items()}
    print_skip_empty({
        'block': block_names.get(ids.block, str(ids.block)),
        'ids': {i: s for i, s in enumerate(ids.ids) if i},
    })


def print_agency(a: gtfs.Agency, oid: str | None):
    print_skip_empty({
        'agency_id': a.agency_id,
        'original_id': oid,
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
    print(json.dumps({
        'dates': {i: list(dt.dates) for i, dt in enumerate(c.dates)},
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


def print_shape(s: gtfs.Shape, oid: str | None):
    print_skip_empty({
        'shape_id': s.shape_id,
        'original_id': oid,
        'longitudes': list(s.longitudes),
        'latitudes': list(s.latitudes),
    })


def print_stop(s: gtfs.Stop, oid: str | None):
    LOC_TYPES = ['stop', 'station', 'exit', 'node', 'boarding']
    ACC_TYPES = ['unknown', 'some', 'no']
    print_skip_empty({
        'stop_id': s.stop_id,
        'original_id': oid,
        'code': s.code,
        'name': s.name or None,
        'desc': s.desc,
        'lon': s.lon,
        'lat': s.lat,
        'type': None if not s.type else LOC_TYPES[s.type],
        'parent_id': s.parent_id or None,
        'wheelchair': None if not s.wheelchair else ACC_TYPES[s.wheelchair],
        'platform_code': s.platform_code,
        'external_str_id': s.external_str_id,
        'external_int_id': s.external_int_id or None,
        'delete': s.delete,
    })


def print_route(r: gtfs.Route, oid: str | None):
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
        'original_id': oid,
        'agency_id': r.agency_id or None,
        'short_name': r.short_name,
        'long_name': list(r.long_name) or None,
        'desc': r.desc,
        'type': ROUTE_TYPES[r.type],
        'color': None if not r.color else f'{r.color:#08x}',
        'text_color': None if not r.text_color else f'{r.text_color:#08x}',
        'continuous_pickup': None if not r.continuous_pickup else PD_TYPES[r.continuous_pickup],
        'continuous_dropoff': None if not r.continuous_dropoff else PD_TYPES[r.continuous_dropoff],
        'itineraries': [prepare_itinerary(i) for i in r.itineraries] or None,
        'delete': r.delete,
    })


def print_trip(t: gtfs.Trip, oid: str | None):
    ACC_TYPES = ['unknown', 'some', 'no']
    PD_TYPES = ['no', 'yes', 'phone_agency', 'tell_driver']
    print_skip_empty({
        'trip_id': t.trip_id,
        'original_id': oid,
        'service_id': t.service_id or None,
        'itinerary_id': t.itinerary_id or None,
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


def print_fare_links(fl: FareLinks):
    print(json.dumps({'stop_area_ids': fl.stop_areas}))
    print(json.dumps({'stop_zone_ids': fl.stop_zones}))
    print(json.dumps({'route_network_ids': fl.route_networks}))


def info():
    parser = argparse.ArgumentParser(
        description='Print information and contents of a protobuf-compressed GTFS')
    parser.add_argument('input', type=argparse.FileType('rb'), help='Source file')
    parser.add_argument('-b', '--block', choices=BLOCKS.keys(),
                        help='Block to dump, header by default')
    parser.add_argument('-i', '--id', help='Just one specific id')
    options = parser.parse_args()

    if is_gtfs_delta(options.input):
        feed: GtfsProto | GtfsDelta = GtfsDelta(options.input)
    else:
        feed = GtfsProto(options.input)

    if not options.block:
        feed.read_all()
        if isinstance(feed, GtfsDelta):
            print_delta_header(feed.header)
        else:
            print_header(feed.header)
        feed.store_strings()
        feed.store_ids()
        print_blocks(feed.blocks, feed.header.compressed)

    else:
        for_id = options.id
        try:
            int_id = int(options.id or 'none')
        except ValueError:
            int_id = -1

        block = BLOCKS[options.part]

        if options.block == 'version':
            print(feed.header.version)
        elif options.block == 'date':
            print(feed.header.date)
        elif block == gtfs.B_IDS:
            block_names = {v: s for s, v in BLOCKS.items()}
            for b, ids in feed.id_store.items():
                print_skip_empty({
                    'block': block_names.get(b, str(b)),
                    'ids': {i: s for s, i in ids.ids.items()},
                })
        elif block == gtfs.B_AGENCY:
            for a in feed.agencies:
                oid = feed.id_store[block].original.get(a.agency_id)
                if not for_id or a.agency_id == int_id or oid == for_id:
                    print_agency(a, oid)
        elif block == gtfs.B_CALENDAR:
            print_calendar(feed.calendar)
        elif block == gtfs.B_SHAPES:
            for s in feed.shapes:
                oid = feed.id_store[block].original.get(s.shape_id)
                if not for_id or s.shape_id == int_id or oid == for_id:
                    print_shape(s, oid)
        elif block == gtfs.B_NETWORKS:
            print(json.dumps(feed.networks, ensure_ascii=False))
        elif block == gtfs.B_AREAS:
            print(json.dumps(feed.areas, ensure_ascii=False))
        elif block == gtfs.B_STRINGS:
            print(json.dumps(
                {i: s for i, s in enumerate(feed.strings.strings)},
                ensure_ascii=False
            ))
        elif block == gtfs.B_STOPS:
            for s in feed.stops:
                oid = feed.id_store[block].original.get(s.stop_id)
                if not for_id or s.stop_id == int_id or oid == for_id:
                    print_stop(s, oid)
        elif block == gtfs.B_ROUTES:
            for r in feed.routes:
                oid = feed.id_store[block].original.get(r.route_id)
                if not for_id or r.route_id == int_id or oid == for_id:
                    print_route(r, oid)
        elif block == gtfs.B_TRIPS:
            for t in feed.trips:
                oid = feed.id_store[block].original.get(t.trip_id)
                if not for_id or t.trip_id == int_id or oid == for_id:
                    print_trip(t, oid)
        elif block == gtfs.B_TRANSFERS:
            for t in feed.transfers:
                print_transfer(t)
        elif block == gtfs.B_FARE_LINKS:
            print_fare_links(feed.fare_links)
        else:
            print(
                'Sorry, printing blocks of this type is not implemented yet.',
                file=sys.stderr
            )
