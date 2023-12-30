from zipfile import ZipFile
from .base import BasePacker, FeedCache
from .. import gtfs_pb2 as gtfs


class AgencyPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache):
        super().__init__(z, store)

    @property
    def block(self):
        return gtfs.B_AGENCY

    def pack(self):
        with self.open_table('agency') as f:
            agencies = gtfs.Agencies()
            for row, agency_id, _ in self.table_reader(f, 'agency_id'):
                agency = gtfs.Agency(
                    agency_id=agency_id,
                    name=row['agency_name'],
                    url=row['agency_url'],
                    timezone=self.strings.add(row['agency_timezone']),
                )
                for k in ('lang', 'phone', 'fare_url', 'email'):
                    if row.get(f'agency_{k}'):
                        setattr(agency, k, row[f'agency_{k}'].strip())
                agencies.agencies.append(agency)
            return agencies.SerializeToString()


class NetworksPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache):
        super().__init__(z, store)

    @property
    def block(self):
        return gtfs.B_NETWORKS

    def pack(self):
        if self.has_file('networks'):
            with self.open_table('networks') as f:
                networks = gtfs.Networks()
                for row, network_id, _ in self.table_reader(f, 'network_id'):
                    networks.networks[network_id] = row['network_name']
                return networks.SerializeToString()


class AreasPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache):
        super().__init__(z, store)

    @property
    def block(self):
        return gtfs.B_AREAS

    def pack(self):
        if self.has_file('areas'):
            with self.open_table('areas') as f:
                areas = gtfs.Areas()
                for row, area_id, _ in self.table_reader(f, 'network_id'):
                    areas.areas[area_id] = row['area_name']
                return areas.SerializeToString()
