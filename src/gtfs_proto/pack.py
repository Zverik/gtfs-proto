import argparse
from zipfile import ZipFile
from datetime import date
from .wrapper import GtfsProto
from .packers import (
    BasePacker, AgencyPacker, NetworksPacker, AreasPacker,
    CalendarPacker, ShapesPacker, StopsPacker,
    RoutesPacker, TripsPacker, TransfersPacker,
)


def run_block(feed: GtfsProto, packer: BasePacker):
    feed.blocks.add(packer.block, packer.pack())


def pack():
    parser = argparse.ArgumentParser(
        description='Converts GTFS feed into a protobuf-compressed version')
    parser.add_argument('input', help='Input zipped gtfs file')
    parser.add_argument('-u', '--url', help='URL to the original feed')
    parser.add_argument('-d', '--date', help='Date for the original feed')
    parser.add_argument('-p', '--prev', type=argparse.FileType('rb'),
                        help='Last build for keeping ids consistent')
    parser.add_argument('-o', '--output', required=True,
                        help='Output protobuf file. Use %% for version')
    parser.add_argument('-r', '--raw', action='store_true',
                        help='Do not compress data blocks')
    options = parser.parse_args()

    feed = GtfsProto()
    if options.prev:
        prev = GtfsProto(options.prev)
        feed.strings = prev.strings
        feed.id_store = prev.id_store
        feed.header.version = prev.header.version + 1
        feed.header.original_url = prev.header.original_url
    else:
        feed.header.version = 1

    if options.date:
        d = ''.join(c for c in options.date if c.isdecimal())
        if len(d) == 6:
            d = '20' + d
        if len(d) != 8:
            raise ValueError('Expecting date in format YYYYMMDD')
        feed.header.date = int(d)
    else:
        feed.header.date = int(date.today().strftime('%Y%m%d'))

    feed.header.compressed = not options.raw
    if options.url:
        feed.header.original_url = options.url

    with ZipFile(options.input, 'r') as z:
        feed.agencies = AgencyPacker(z, feed.strings, feed.id_store).pack()
        feed.calendar = CalendarPacker(z, feed.strings, feed.id_store).pack()
        feed.shapes = ShapesPacker(z, feed.strings, feed.id_store).pack()
        feed.networks = NetworksPacker(z, feed.strings, feed.id_store).pack()
        feed.areas = AreasPacker(z, feed.strings, feed.id_store).pack()
        feed.stops = StopsPacker(z, feed.strings, feed.id_store, feed.fare_links).pack()
        r = RoutesPacker(z, feed.strings, feed.id_store, feed.fare_links)  # reads itineraries
        feed.routes = r.pack()
        feed.trips = TripsPacker(z, feed.strings, feed.id_store, r.trip_itineraries).pack()
        feed.transfers = TransfersPacker(z, feed.strings, feed.id_store).pack()

    fn = options.output.replace('%', str(feed.header.version))
    with open(fn, 'wb') as f:
        feed.write(f)
