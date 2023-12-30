import argparse
import struct
import sys
import zstandard
import io
import json
from . import gtfs_pb2 as gtfs
from typing import Any


BLOCKS = {
    'agency': gtfs.B_AGENCY,
    'calendar': gtfs.B_CALENDAR,
    'shapes': gtfs.B_SHAPES,
    'networks': gtfs.B_NETWORKS,
    'areas': gtfs.B_AREAS,
    'strings': gtfs.B_STRINGS,
    'stops': gtfs.B_STOPS,
    'routes': gtfs.B_ROUTES,
    'trips': gtfs.B_TRIPS,
    'transfers': gtfs.B_TRANSFERS,
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
    return {}


def print_header(header: gtfs.GtfsHeader, fileobj: io.BytesIO):
    print(json.dumps({
        'version': header.version,
        'original_url': header.original_url,
        'compressed': header.compressed,
    }))
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


def print_agency(a: gtfs.Agency):
    r = {
        'agency_id': a.agency_id,
        'name': a.name,
        'url': a.url,
        'timezone': a.timezone,
        'lang': a.lang,
        'phone': a.phone,
        'fare_url': a.fare_url,
        'email': a.email,
    }
    print(json.dumps({k: v for k, v in r.items() if v}, ensure_ascii=False))


def print_calendar(c: gtfs.Calendar):
    print(json.dumps({'base_date': c.base_date}))
    for d in c.dates:
        print(json.dumps({
            'days_id': d.days_id,
            'dates': list(d.dates),
        }))
    for s in c.services:
        print(json.dumps({
            'service_id': s.service_id,
            'start_date': s.start_date,
            'end_date': s.end_date,
            'weekdays': f'{s.weekdays:#b}',
            'added_days': s.added_days,
            'removed_days': s.removed_days,
        }))


def print_shapes(shapes: gtfs.Shapes):
    print(json.dumps({
        'base_longitude': shapes.base_longitude,
        'base_latitude': shapes.base_latitude,
    }))
    for s in shapes.shapes:
        print(json.dumps({
            'shape_id': s.shape_id,
            'longitudes': list(s.longitudes),
            'latitudes': list(s.latitudes),
        }))


def print_part(part: gtfs.Block, data: bytes):
    if part == gtfs.B_AGENCY:
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
        print_shapes(shapes)
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
