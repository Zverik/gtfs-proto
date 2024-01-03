import struct
import zstandard
from . import gtfs_pb2 as gtfs
from .base import StringCache, FareLinks, IdReference
from typing import BinaryIO
from functools import cached_property


__all__ = ['gtfs', 'GtfsProto', 'GtfsDelta', 'FareLinks']


class GtfsBlocks:
    def __init__(self):
        self.blocks: dict[gtfs.Block, bytes] = {}
        self.arch = zstandard.ZstdCompressor(level=10)

    def clear(self):
        self.blocks = {}

    def populate_header(self, header: gtfs.GtfsHeader):
        del header.blocks[:]
        for b in gtfs.Block.values():
            if 0 < b and b < gtfs.B_ITINERARIES:
                header.blocks.append(len(self.blocks.get(b, b'')))

    @property
    def not_empty(self):
        return any(self.blocks.values())

    def __iter__(self):
        for b in sorted(self.blocks):
            if self.blocks[b]:
                yield self.blocks[b]

    def decompressed(self):
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
        self.fare_links = FareLinks()

        self._blocks = GtfsBlocks()
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
        self.fare_links.clear()
        self._blocks.clear()
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
                    self.id_store[idrefs.block] = IdReference(idrefs.ids)
            elif read_now:
                data = fileobj.read(size)
                self._blocks.add(b + 1, data, self.header.compressed)
            else:
                fileobj.seek(size, 1)

    def read(self, fileobj: BinaryIO, read_now: bool = False):
        self.clear()
        header_len = struct.unpack('<h', fileobj.read(2))[0]
        if header_len & 0x8000 > 0:
            raise Exception('The file is delta, not a regular feed.')
        self.header = gtfs.GtfsHeader()
        self.header.ParseFromString(fileobj.read(header_len))
        self._block_pos = {gtfs.B_HEADER: (2, header_len)}
        self._was_compressed = self.header.compressed
        self._read_blocks(fileobj, read_now)

    def _write_blocks(self, fileobj: BinaryIO):
        if self.header.compressed:
            for b in self._blocks:
                fileobj.write(b)
        else:
            for b in self._blocks.decompressed():
                fileobj.write(b)

    def write(self, fileobj: BinaryIO, compress: bool | None = None):
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

        self._blocks.populate_header(self.header)

        if compress is not None:
            self.header.compressed = compress
        header_data = self.header.SerializeToString()
        fileobj.write(struct.pack('<h', len(header_data)))
        fileobj.write(header_data)
        self._write_blocks(fileobj)

    def store_strings(self):
        self._blocks.add(gtfs.B_STRINGS, self.strings.store())

    def store_fare_links(self):
        self._blocks.add(gtfs.B_FARE_LINKS, self.fare_links.store())

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
        self._blocks.add(gtfs.B_IDS, idstore.SerializeToString())

    def read_block(self, block: gtfs.Block, compressed: bool = False) -> bytes | None:
        if block in self._blocks:
            return self._blocks.get(block, compressed)
        if not self._fileobj or block not in self._block_pos:
            return None
        bseek, bsize = self._block_pos[block]
        self._fileobj.seek(bseek)
        data = self._fileobj.read(bsize)
        self._blocks.add(block, data, self._was_compressed)
        if self._was_compressed and not compressed:
            arch = zstandard.ZstdDecompressor()
            data = arch.decompress(data)
        return data if not compressed else self._blocks.get(block, True)

    @cached_property
    def agencies(self) -> dict[int, gtfs.Agency]:
        data = self.read_block(gtfs.B_AGENCY)
        if not data:
            return {}
        ag = gtfs.Agencies()
        ag.ParseFromString(data)
        return {a.agency_id: a for a in ag.agencies}

    def store_agencies(self):
        if 'agencies' in self.__dict__:
            ag = gtfs.Agencies(agencies=self.agencies.values())
            self._blocks.add(gtfs.B_AGENCY, ag.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_AGENCY, True)

    @cached_property
    def calendar(self) -> gtfs.Calendar:
        data = self.read_block(gtfs.B_CALENDAR)
        calendar = gtfs.Calendar()
        if not data:
            return calendar
        calendar.ParseFromString(data)
        return calendar

    def store_calendar(self):
        if 'calendar' in self.__dict__:
            self._blocks.add(gtfs.B_CALENDAR, self.calendar.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_CALENDAR, True)

    @cached_property
    def shapes(self) -> dict[int, gtfs.TripShape]:
        data = self.read_block(gtfs.B_SHAPES)
        if not data:
            return {}
        shapes = gtfs.Shapes()
        shapes.ParseFromString(data)
        return {s.shape_id: s for s in shapes.shapes}

    def store_shapes(self):
        if 'shapes' in self.__dict__:
            # Compress the data.
            sh = gtfs.Shapes(shapes=self.shapes.values())
            self._blocks.add(gtfs.B_SHAPES, sh.SerializeToString())
        elif self._fileobj:
            # Off chance it's not read, re-read.
            self.read_block(gtfs.B_SHAPES, True)

    @cached_property
    def stops(self) -> dict[int, gtfs.Stop]:
        data = self.read_block(gtfs.B_STOPS)
        if not data:
            return {}
        stops = gtfs.Stops()
        stops.ParseFromString(data)
        return {s.stop_id: s for s in stops.stops}

    def store_stops(self):
        if 'stops' in self.__dict__:
            st = gtfs.Stops(stops=self.stops.values())
            self._blocks.add(gtfs.B_STOPS, st.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_STOPS, True)

    @cached_property
    def routes(self) -> dict[int, gtfs.Route]:
        data = self.read_block(gtfs.B_ROUTES)
        if not data:
            return {}
        routes = gtfs.Routes()
        routes.ParseFromString(data)
        return {r.route_id: r for r in routes.routes}

    def store_routes(self):
        if 'routes' in self.__dict__:
            r = gtfs.Routes(routes=self.routes.values())
            self._blocks.add(gtfs.B_ROUTES, r.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_ROUTES, True)

    @cached_property
    def trips(self) -> dict[int, gtfs.Trip]:
        data = self.read_block(gtfs.B_TRIPS)
        if not data:
            return {}
        trips = gtfs.Trips()
        trips.ParseFromString(data)
        return {t.trip_id: t for t in trips.trips}

    def store_trips(self):
        if 'trips' in self.__dict__:
            t = gtfs.Trips(trips=self.trips.values())
            self._blocks.add(gtfs.B_TRIPS, t.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_TRIPS, True)

    @cached_property
    def transfers(self) -> list[gtfs.Transfer]:
        data = self.read_block(gtfs.B_TRANSFERS)
        if not data:
            return []
        tr = gtfs.Transfers()
        tr.ParseFromString(data)
        return list(tr.transfers)

    def store_transfers(self):
        if 'transfers' in self.__dict__:
            tr = gtfs.Transfers(transfers=self.transfers)
            self._blocks.add(gtfs.B_TRANSFERS, tr.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_TRANSFERS, True)

    @cached_property
    def networks(self) -> dict[int, str]:
        data = self.read_block(gtfs.B_NETWORKS)
        if not data:
            return {}
        networks = gtfs.Networks()
        networks.ParseFromString(data)
        return {k: v for k, v in networks.networks.items()}

    def store_networks(self):
        if 'networks' in self.__dict__:
            networks = gtfs.Networks(networks=self.networks)
            self._blocks.add(gtfs.B_NETWORKS, networks.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_NETWORKS)

    @cached_property
    def areas(self) -> dict[int, str]:
        data = self.read_block(gtfs.B_AREAS)
        if not data:
            return {}
        areas = gtfs.Areas()
        areas.ParseFromString(data)
        return {k: v for k, v in areas.areas.items()}

    def store_areas(self):
        if 'areas' in self.__dict__:
            areas = gtfs.Areas(areas=self.areas)
            self._blocks.add(gtfs.B_AREAS, areas.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_AREAS)


class GtfsDelta(GtfsProto):
    def __init__(self, fileobj: BinaryIO | None = None, read_now: bool = False):
        self.header = gtfs.GtfsDeltaHeader()
        self.header.compressed = True
        self.strings = StringCache()
        self.id_store: dict[int, IdReference] = {
            b: IdReference() for b in gtfs.Block.values()}

        self._blocks = GtfsBlocks()
        # position, size
        self._block_pos: dict[gtfs.Block, tuple[int, int]] = {}
        self._fileobj = None if read_now else fileobj
        self._was_compressed: bool = True  # overwritten on read
        if fileobj:
            self.read(fileobj, read_now)

    def read(self, fileobj: BinaryIO, read_now: bool = False):
        self.clear()
        header_len = struct.unpack('<h', fileobj.read(2))[0]
        if header_len & 0x8000 == 0:
            raise Exception('The file is a regular feed, not a delta.')
        header_len &= 0x7FFF
        self.header = gtfs.GtfsDeltaHeader()
        self.header.ParseFromString(fileobj.read(header_len))
        self._block_pos = {gtfs.B_HEADER: (2, header_len)}
        self._read_blocks(fileobj, read_now)

    def write(self, fileobj: BinaryIO, compress: bool | None = None):
        """When compress is None, using the source flag or True."""
        if not self.header.old_version or not self.header.version:
            raise Exception('A version in the header is empty')
        if 'fare_links' in self.__dict__:
            raise Exception('You have used the fare_links field. Use fare_links_delta instead.')

        self.store_ids()
        self.store_strings()
        self.store_fare_links_delta()
        self.store_agencies()
        self.store_calendar()
        self.store_shapes()
        self.store_stops()
        self.store_routes()
        self.store_trips()
        self.store_transfers()
        self.store_networks()
        self.store_areas()

        self._blocks.populate_header(self.header)

        if compress is not None:
            self.header.compressed = compress
        header_data = self.header.SerializeToString()
        fileobj.write(struct.pack('<h', len(header_data) & 0x8000))
        fileobj.write(header_data)
        self._write_blocks(fileobj)

    @cached_property
    def fare_links_delta(self) -> gtfs.FareLinksDelta:
        data = self.read_block(gtfs.B_FARE_LINKS)
        if not data:
            return {}
        fl = gtfs.FareLinksDelta()
        fl.ParseFromString(data)
        return fl

    def store_fare_links_delta(self):
        if 'fare_links_delta' in self.__dict__:
            self._blocks.add(gtfs.B_FARE_LINKS, self.fare_links_delta.SerializeToString())
        elif self._fileobj:
            self.read_block(gtfs.B_FARE_LINKS)
