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


def main():
    parser = argparse.ArgumentParser(
        description='Converts GTFS feed into a protobuf-compressed version, with deltas')
    parser.add_argument('input', help='Input zipped gtfs file')
    parser.add_argument('-u', '--url', help='URL to the original feed')
    parser.add_argument('-p', '--prev-id', type=argparse.FileType('rb'),
                        help='Last id storage for keeping ids consistent')
    parser.add_argument('-i', '--id',
                        help='Resuling id storage file for keeping generated ids same. '
                        'Use % for version')
    parser.add_argument('-o', '--output', required=True,
                        help='Output protobuf file. Use % for version')
    parser.add_argument('-z', '--zip', action='store_true',
                        help='Compress data blocks')
    options = parser.parse_args()

    store = FeedCache()
    version = store.load(options.prev_id)
    header = gtfs.GtfsHeader()
    header.version = version + 1
    blocks = GtfsBlocks(header, options.zip)

    with ZipFile(options.input, 'r') as z:
        blocks.run(AgencyPacker(z, store))
        blocks.run(CalendarPacker(z, store))
        blocks.run(ShapesPacker(z, store))
        blocks.run(NetworksPacker(z, store))
        blocks.run(AreasPacker(z, store))
        blocks.run(StopsPacker(z, store))
        blocks.run(RoutesPacker(z, store))

    if blocks.not_empty:
        blocks.add(gtfs.B_STRINGS, gtfs.StringTable(
            strings=store.strings.strings).SerializeToString())
        blocks.populate_header(header)
        print(header)  # TODO: remove this

        fn = options.output.replace('%', str(header.version))
        with open(fn, 'wb') as f:
            header_data = header.SerializeToString()
            f.write(struct.pack('<h', len(header_data)))
            f.write(header_data)
            for b in blocks:
                f.write(b)
        if options.id:
            fn = options.id.replace('%', str(header.version))
            with open(fn, 'wb') as f:
                f.write(store.store(header.version))


if __name__ == '__main__':
    main()
