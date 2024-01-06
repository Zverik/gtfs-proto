import struct
import zstandard
from . import gtfs_pb2 as gtfs
from .base import StringCache, FareLinks, IdReference
from typing import BinaryIO
from collections import Counter
from collections.abc import Generator
from functools import cached_property


__all__ = ['gtfs', 'GtfsBlocks', 'GtfsProto', 'GtfsDelta',
           'FareLinks', 'is_gtfs_delta', 'SHAPE_SCALE', 'STOP_SCALE']
SHAPE_SCALE = 100000
STOP_SCALE = 100000


def is_gtfs_delta(fileobj: BinaryIO) -> bool:
    fileobj.seek(0)
    header_len = struct.unpack('<H', fileobj.read(2))[0]
    fileobj.seek(0)
    return header_len & 0x8000 > 0


class GtfsBlocks:
    def __init__(self):
        self.blocks: dict[gtfs.Block, bytes] = {}
        self.arch = zstandard.ZstdCompressor(level=10)

    def clear(self):
        self.blocks = {}

    def populate_header(self, header: gtfs.GtfsHeader, compressed: bool = False):
        del header.blocks[:]
        for b in gtfs.Block.values():
            if 0 < b and b < gtfs.B_ITINERARIES:
                header.blocks.append(len(self.get(b, compressed)))

    @property
    def not_empty(self) -> bool:
        return any(self.blocks.values())

    def __iter__(self) -> Generator[bytes, None, None]:
        for b in sorted(self.blocks):
            if self.blocks[b]:
                yield self.blocks[b]

    def __contains__(self, b: gtfs.Block) -> bool:
        return bool(self.blocks.get(b))

    def __getitem__(self, b: gtfs.Block) -> bytes:
        return self.blocks[b]

    def decompressed(self) -> Generator[bytes, None, None]:
        dearch = zstandard.ZstdDecompressor()
        for b in sorted(self.blocks):
            if self.blocks[b]:
                yield dearch.decompress(self.blocks[b])

    def add(self, block: int, data: bytes, is_compressed: bool = False):
        if not data:
            return
        self.blocks[block] = data if is_compressed else self.arch.compress(data)

    def get(self, block: int, compressed: bool = False) -> bytes:
        if block not in self.blocks:
            return b''
        if compressed:
            return self.blocks[block]
        dearch = zstandard.ZstdDecompressor()
        return dearch.decompress(self.blocks[block])


class GtfsProto:
    def __init__(self, fileobj: BinaryIO | None = None, read_now: bool = False):
        self.header = gtfs.GtfsHeader()
        self.header.compressed = True
        self.strings = StringCache()
        self.id_store: dict[int, IdReference] = {
            b: IdReference() for b in gtfs.Block.values()}

        self.blocks = GtfsBlocks()
        # position, size
        self._block_pos: dict[gtfs.Block, tuple[int, int]] = {}
        self._fileobj = None if read_now else fileobj
        self._was_compressed: bool = True  # overwritten on read
        if fileobj:
            self.read(fileobj, read_now)

    def clear(self):
        self.header = gtfs.GtfsHeader()
        self.header.compressed = True
        self.strings.clear()
        for k in self.id_store:
            self.id_store[k].clear()
        self.blocks.clear()
        self._block_pos = {}

    def _read_blocks(self, fileobj: BinaryIO, read_now: bool):
        arch = None if not self.header.compressed else zstandard.ZstdDecompressor()
        filepos = 2 + self._block_pos[gtfs.B_HEADER][1]
        for b, size in enumerate(self.header.blocks):
            if not size:
                continue
            self._block_pos[b + 1] = (filepos, size)
            filepos += size

            if b + 1 == gtfs.B_STRINGS:
                data = fileobj.read(size)
                if arch:
                    data = arch.decompress(data)
                s = gtfs.StringTable()
                s.ParseFromString(data)
                self.strings = StringCache(s.strings)
            elif b + 1 == gtfs.B_IDS:
                data = fileobj.read(size)
                if arch:
                    data = arch.decompress(data)
                store = gtfs.IdStore()
                store.ParseFromString(data)
                for idrefs in store.refs:
                    self.id_store[idrefs.block] = IdReference(idrefs.ids, idrefs.delta_skip)
            elif read_now:
                data = fileobj.read(size)
                self.blocks.add(b + 1, data, self.header.compressed)
            else:
                fileobj.seek(size, 1)

    def read_all(self):
        if not self._fileobj:
            return
        for b in self._block_pos:
            if b not in (gtfs.B_STRINGS, gtfs.B_IDS, gtfs.B_HEADER):
                self._read_block(b, True)
        self._fileobj = None

    def read(self, fileobj: BinaryIO, read_now: bool = False):
        self.clear()
        header_len = struct.unpack('<H', fileobj.read(2))[0]
        if header_len & 0x8000 > 0:
            raise Exception('The file is delta, not a regular feed.')
        self.header = gtfs.GtfsHeader()
        self.header.ParseFromString(fileobj.read(header_len))
        self._block_pos = {gtfs.B_HEADER: (2, header_len)}
        self._was_compressed = self.header.compressed
        self._read_blocks(fileobj, read_now)

    def _write_blocks(self, fileobj: BinaryIO):
        if self.header.compressed:
            for b in self.blocks:
                fileobj.write(b)
        else:
            for b in self.blocks.decompressed():
                fileobj.write(b)

    def write(self, fileobj: BinaryIO, compress: bool | None = None):
        """When compress is None, using the value from the header."""
        if not self.header.version:
            raise Exception('Please set version inside the header')

        self.store_ids()
        self.store_strings()
        self.store_fare_links()
        self.store_agencies()
        self.store_calendar()
        self.store_shapes()
        self.store_stops()
        self.store_routes()
        self.store_trips()
        self.store_transfers()
        self.store_networks()
        self.store_areas()

        if compress is not None:
            self.header.compressed = compress
        self.blocks.populate_header(self.header, self.header.compressed)

        header_data = self.header.SerializeToString()
        fileobj.write(struct.pack('<H', len(header_data)))
        fileobj.write(header_data)
        self._write_blocks(fileobj)

    def store_strings(self):
        self.blocks.add(gtfs.B_STRINGS, self.strings.store())

    def store_ids(self):
        idstore = gtfs.IdStore()
        for block, ids in self.id_store.items():
            if ids:
                idrefs = gtfs.IdReference(
                    block=block,
                    ids=ids.to_list(),
                    delta_skip=ids.delta_skip,
                )
                idstore.refs.append(idrefs)
        self.blocks.add(gtfs.B_IDS, idstore.SerializeToString())

    def _read_block(self, block: gtfs.Block, compressed: bool = False) -> bytes | None:
        if block in self.blocks:
            return self.blocks.get(block, compressed)
        if not self._fileobj or block not in self._block_pos:
            return None
        bseek, bsize = self._block_pos[block]
        self._fileobj.seek(bseek)
        data = self._fileobj.read(bsize)
        self.blocks.add(block, data, self._was_compressed)
        if self._was_compressed and not compressed:
            arch = zstandard.ZstdDecompressor()
            data = arch.decompress(data)
        return data if not compressed else self.blocks.get(block, True)

    @cached_property
    def agencies(self) -> list[gtfs.Agency]:
        data = self._read_block(gtfs.B_AGENCY)
        if not data:
            return []
        ag = gtfs.Agencies()
        ag.ParseFromString(data)
        return list(ag.agencies)

    def store_agencies(self):
        if 'agencies' in self.__dict__:
            ag = gtfs.Agencies(agencies=self.agencies)
            self.blocks.add(gtfs.B_AGENCY, ag.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_AGENCY, True)

    @cached_property
    def calendar(self) -> gtfs.Calendar:
        data = self._read_block(gtfs.B_CALENDAR)
        calendar = gtfs.Calendar()
        if not data:
            return calendar
        calendar.ParseFromString(data)
        return calendar

    def store_calendar(self):
        if 'calendar' in self.__dict__:
            self.blocks.add(gtfs.B_CALENDAR, self.calendar.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_CALENDAR, True)

    def _get_shape_last(
            self, shape: gtfs.Shape,
            prev_last: tuple[int, int] = (0, 0)) -> tuple[int, int]:
        lon = shape.longitudes[0] + prev_last[0]
        lat = shape.latitudes[0] + prev_last[1]
        for i in range(1, len(shape.longitudes)):
            lon += shape.longitudes[i]
            lat += shape.latitudes[i]
        return (lon, lat)

    @cached_property
    def shapes(self) -> list[gtfs.Shape]:
        data = self._read_block(gtfs.B_SHAPES)
        if not data:
            return []
        shapes = gtfs.Shapes()
        shapes.ParseFromString(data)

        # Decouple shapes.
        result: list[gtfs.Shape] = []
        prev_last: tuple[int, int] = (0, 0)
        for s in shapes.shapes:
            if s.longitudes:
                s.longitudes[0] += prev_last[0]
                s.latitudes[0] += prev_last[1]
                prev_last = self._get_shape_last(s)
            result.append(s)
        return result

    def store_shapes(self):
        if 'shapes' in self.__dict__:
            # Make the sequence.
            shapes: list[gtfs.Shape] = []
            prev_last: tuple[int, int] = (0, 0)
            for s in sorted(self.shapes, key=lambda k: k.shape_id):
                if s.longitudes:
                    s.longitudes[0] -= prev_last[0]
                    s.latitudes[0] -= prev_last[1]
                    prev_last = self._get_shape_last(s, prev_last)
                shapes.append(s)

            # Compress the data.
            sh = gtfs.Shapes(shapes=shapes)
            self.blocks.add(gtfs.B_SHAPES, sh.SerializeToString())
        elif self._fileobj:
            # Off chance it's not read, re-read.
            self._read_block(gtfs.B_SHAPES, True)

    @cached_property
    def stops(self) -> list[gtfs.Stop]:
        data = self._read_block(gtfs.B_STOPS)
        if not data:
            return []
        stops = gtfs.Stops()
        stops.ParseFromString(data)

        # Decouple stops.
        result: list[gtfs.Stop] = []
        prev_coord: tuple[int, int] = (0, 0)
        for s in stops.stops:
            if s.lon and s.lat:
                s.lon += prev_coord[0]
                s.lat += prev_coord[1]
                prev_coord = (s.lon, s.lat)
            result.append(s)
        return result

    def store_stops(self):
        if 'stops' in self.__dict__:
            # Make the sequence.
            stops: list[gtfs.Stop] = []
            prev_coord: tuple[int, int] = (0, 0)
            for s in sorted(self.stops, key=lambda k: k.stop_id):
                if s.lon and s.lat:
                    pc = (s.lon, s.lat)
                    s.lon -= prev_coord[0]
                    s.lat -= prev_coord[1]
                    prev_coord = pc
                stops.append(s)

            # Compress the data.
            st = gtfs.Stops(stops=stops)
            self.blocks.add(gtfs.B_STOPS, st.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_STOPS, True)

    @cached_property
    def routes(self) -> list[gtfs.Route]:
        data = self._read_block(gtfs.B_ROUTES)
        if not data:
            return []
        routes = gtfs.Routes()
        routes.ParseFromString(data)
        return list(routes.routes)

    def store_routes(self):
        if 'routes' in self.__dict__:
            r = gtfs.Routes(routes=self.routes)
            self.blocks.add(gtfs.B_ROUTES, r.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_ROUTES, True)

    @cached_property
    def trips(self) -> list[gtfs.Trip]:
        data = self._read_block(gtfs.B_TRIPS)
        if not data:
            return []
        trips = gtfs.Trips()
        trips.ParseFromString(data)
        return list(trips.trips)

    def store_trips(self):
        if 'trips' in self.__dict__:
            t = gtfs.Trips(trips=self.trips)
            self.blocks.add(gtfs.B_TRIPS, t.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_TRIPS, True)

    @cached_property
    def transfers(self) -> list[gtfs.Transfer]:
        data = self._read_block(gtfs.B_TRANSFERS)
        if not data:
            return []
        tr = gtfs.Transfers()
        tr.ParseFromString(data)
        return list(tr.transfers)

    def store_transfers(self):
        if 'transfers' in self.__dict__:
            tr = gtfs.Transfers(transfers=self.transfers)
            self.blocks.add(gtfs.B_TRANSFERS, tr.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_TRANSFERS, True)

    @cached_property
    def networks(self) -> dict[int, str]:
        data = self._read_block(gtfs.B_NETWORKS)
        if not data:
            return {}
        networks = gtfs.Networks()
        networks.ParseFromString(data)
        return {k: v for k, v in networks.networks.items()}

    def store_networks(self):
        if 'networks' in self.__dict__:
            networks = gtfs.Networks(networks=self.networks)
            self.blocks.add(gtfs.B_NETWORKS, networks.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_NETWORKS)

    @cached_property
    def areas(self) -> dict[int, str]:
        data = self._read_block(gtfs.B_AREAS)
        if not data:
            return {}
        areas = gtfs.Areas()
        areas.ParseFromString(data)
        return {k: v for k, v in areas.areas.items()}

    def store_areas(self):
        if 'areas' in self.__dict__:
            areas = gtfs.Areas(areas=self.areas)
            self.blocks.add(gtfs.B_AREAS, areas.SerializeToString())
        elif self._fileobj:
            self._read_block(gtfs.B_AREAS)

    @cached_property
    def fare_links(self) -> FareLinks:
        fl = FareLinks()
        data = self._read_block(gtfs.B_FARE_LINKS)
        if data:
            f = gtfs.FareLinks()
            f.ParseFromString(data)
            fl.load(f)
        return fl

    def store_fare_links(self):
        if 'fare_links' in self.__dict__:
            self.blocks.add(gtfs.B_FARE_LINKS, self.fare_links.store())
        elif self._fileobj:
            self._read_block(gtfs.B_FARE_LINKS)

    def pack_strings(self, sort=False):
        """
        Sorts strings by popularity and deletes unused entries to save a few bytes.
        Tests have shown that this reduces compressed feed size by 0.3%, while
        complicating string index management. Hence it's not used.

        Also, sorting somehow increases compressed size of blocks, while reducing
        the raw size.
        """
        if len(self.strings.strings) <= 1:
            return

        # Count occurences.
        c: Counter[int] = Counter()
        for a in self.agencies:
            c[a.timezone] += 1
        for s in self.stops:
            c[s.name] += 1
        for r in self.routes:
            for n in r.long_name:
                c[n] += 1
            for i in r.itineraries:
                c[i.headsign] += 1
                for h in i.stop_headsigns:
                    c[h] += 1
        del c[0]

        # Build the new strings list.
        if sort:
            repl = {v[0]: i + 1 for i, v in enumerate(c.most_common())}
            repl[0] = 0
            strings = [''] + [self.strings.strings[v] for v, _ in c.most_common()]
        else:
            repl = {}
            strings = []
            for i, v in enumerate(self.strings.strings):
                if not i or i in c:
                    repl[i] = len(strings)
                    strings.append(v)
        self.strings.set(strings)

        # Replace all the references.
        for a in self.agencies:
            a.timezone = repl[a.timezone]
        for s in self.stops:
            s.name = repl[s.name]
        for r in self.routes:
            for ln in range(len(r.long_name)):
                r.long_name[ln] = repl[r.long_name[ln]]
            for i in r.itineraries:
                i.headsign = repl[i.headsign]
                for hn in range(len(i.stop_headsigns)):
                    i.stop_headsigns[hn] = repl[i.stop_headsigns[hn]]


class GtfsDelta(GtfsProto):
    def __init__(self, fileobj: BinaryIO | None = None, read_now: bool = False):
        self.header = gtfs.GtfsDeltaHeader()
        self.header.compressed = True
        self.strings = StringCache()
        self.id_store: dict[int, IdReference] = {
            b: IdReference() for b in gtfs.Block.values()}

        self.blocks = GtfsBlocks()
        # position, size
        self._block_pos: dict[gtfs.Block, tuple[int, int]] = {}
        self._fileobj = None if read_now else fileobj
        self._was_compressed: bool = True  # overwritten on read
        if fileobj:
            self.read(fileobj, read_now)

    def read(self, fileobj: BinaryIO, read_now: bool = False):
        self.clear()
        header_len = struct.unpack('<H', fileobj.read(2))[0]
        if header_len & 0x8000 == 0:
            raise Exception('The file is a regular feed, not a delta.')
        header_len &= 0x7FFF
        self.header = gtfs.GtfsDeltaHeader()
        self.header.ParseFromString(fileobj.read(header_len))
        self._block_pos = {gtfs.B_HEADER: (2, header_len)}
        self._was_compressed = self.header.compressed
        self._read_blocks(fileobj, read_now)

    def write(self, fileobj: BinaryIO, compress: bool | None = None):
        """When compress is None, using the value from the header."""
        if not self.header.old_version or not self.header.version:
            raise Exception('A version in the header is empty')

        self.store_ids()
        self.store_strings()
        self.store_fare_links()
        self.store_agencies()
        self.store_calendar()
        self.store_shapes()
        self.store_stops()
        self.store_routes()
        self.store_trips()
        self.store_transfers()
        self.store_networks()
        self.store_areas()

        if compress is not None:
            self.header.compressed = compress
        self.blocks.populate_header(self.header, self.header.compressed)

        header_data = self.header.SerializeToString()
        fileobj.write(struct.pack('<H', len(header_data) | 0x8000))
        fileobj.write(header_data)
        self._write_blocks(fileobj)

    @cached_property
    def fare_links(self) -> FareLinks:
        fl = FareLinks()
        data = self._read_block(gtfs.B_FARE_LINKS)
        if data:
            f = gtfs.FareLinksDelta()
            f.ParseFromString(data)
            fl.load_delta(f)
        return fl

    def store_fare_links(self):
        if 'fare_links' in self.__dict__:
            self.blocks.add(gtfs.B_FARE_LINKS, self.fare_links.store_delta())
        elif self._fileobj:
            self._read_block(gtfs.B_FARE_LINKS)
