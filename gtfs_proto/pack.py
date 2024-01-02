import argparse
import struct
import zstandard
from zipfile import ZipFile
from . import gtfs_pb2 as gtfs
from .base import FeedCache, FareLinks
from .packers import (
    BasePacker, AgencyPacker, NetworksPacker, AreasPacker,
    CalendarPacker, ShapesPacker, StopsPacker,
    RoutesPacker, TripsPacker, TransfersPacker,
)


class GtfsBlocks:
    def __init__(self, compress: bool = False):
        self.blocks: dict[gtfs.Block, bytes] = {}
        self.compress = compress

    def populate_header(self, header: gtfs.GtfsHeader):
        # This version of protobuf doesn't have the "clear()" method for repeated fields.
        while header.blocks:
            header.blocks.pop()
        for b in gtfs.Block.values():
            if 0 < b and b < gtfs.B_ITINERARIES:
                header.blocks.append(len(self.blocks.get(b, b'')))

    @property
    def not_empty(self):
        return any(self.blocks.values())

    def __iter__(self):
        for b in sorted(self.blocks):
            yield self.blocks[b]

    def archive_if(self, data: bytes):
        if self.compress:
            arch = zstandard.ZstdCompressor(level=10)
            return arch.compress(data)
        return data

    def add(self, block: int, data: bytes):
        if not data:
            return
        self.blocks[block] = self.archive_if(data)

    def run(self, packer: BasePacker):
        self.add(packer.block, packer.pack())


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
    fl = FareLinks()
    blocks = GtfsBlocks(not options.raw)

    with ZipFile(options.input, 'r') as z:
        blocks.run(AgencyPacker(z, store))
        blocks.run(CalendarPacker(z, store))
        blocks.run(ShapesPacker(z, store))
        blocks.run(NetworksPacker(z, store))
        blocks.run(AreasPacker(z, store))
        blocks.run(StopsPacker(z, store, fl))
        r = RoutesPacker(z, store, fl)  # reads itineraries
        blocks.run(r)
        blocks.run(TripsPacker(z, store, r.trip_itineraries))
        blocks.run(TransfersPacker(z, store))

    if blocks.not_empty:
        blocks.add(gtfs.B_STRINGS, store.strings.store())
        blocks.add(gtfs.B_IDS, store.store())
        blocks.add(gtfs.B_FARE_LINKS, fl.store())
        blocks.populate_header(header)

        fn = options.output.replace('%', str(header.version))
        with open(fn, 'wb') as f:
            header_data = header.SerializeToString()
            f.write(struct.pack('<h', len(header_data)))
            f.write(header_data)
            for b in blocks:
                f.write(b)
