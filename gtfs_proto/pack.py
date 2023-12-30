import argparse
import struct
from zipfile import ZipFile
from . import gtfs_pb2 as gtfs
from .base import FeedCache, GtfsBlocks
from .packers import AgencyPacker, NetworksPacker, AreasPacker
from .calendar import CalendarPacker
from .shapes import ShapesPacker
from .stops import StopsPacker
from .routes import RoutesPacker
from .trips import TripsPacker
from .transfers import TransfersPacker


def pack():
    parser = argparse.ArgumentParser(
        description='Converts GTFS feed into a protobuf-compressed version')
    parser.add_argument('input', help='Input zipped gtfs file')
    parser.add_argument('-u', '--url', help='URL to the original feed')
    parser.add_argument('-p', '--prev', type=argparse.FileType('rb'),
                        help='Last build for keeping ids consistent')
    parser.add_argument('-o', '--output', required=True,
                        help='Output protobuf file. Use %% for version')
    parser.add_argument('-r', '--raw', action='store_true',
                        help='Do not compress data blocks')
    options = parser.parse_args()

    store = FeedCache()
    store.load(options.prev)
    header = gtfs.GtfsHeader()
    header.version = store.version + 1
    header.compressed = not options.raw
    if options.url:
        header.original_url = options.url
    blocks = GtfsBlocks(header, not options.raw)

    with ZipFile(options.input, 'r') as z:
        blocks.run(AgencyPacker(z, store))
        blocks.run(CalendarPacker(z, store))
        blocks.run(ShapesPacker(z, store))
        blocks.run(NetworksPacker(z, store))
        blocks.run(AreasPacker(z, store))
        blocks.run(StopsPacker(z, store))
        r = RoutesPacker(z, store)  # reads itineraries
        blocks.run(r)
        blocks.run(TripsPacker(z, store, r.trip_itineraries))
        blocks.run(TransfersPacker(z, store))

    if blocks.not_empty:
        blocks.add(gtfs.B_STRINGS, gtfs.StringTable(
            strings=store.strings.strings).SerializeToString())
        blocks.add(gtfs.B_IDS, store.store())
        blocks.populate_header(header)

        fn = options.output.replace('%', str(header.version))
        with open(fn, 'wb') as f:
            header_data = header.SerializeToString()
            f.write(struct.pack('<h', len(header_data)))
            f.write(header_data)
            for b in blocks:
                f.write(b)
