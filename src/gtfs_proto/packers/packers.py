from zipfile import ZipFile
from .base import BasePacker, StringCache, IdReference
from .. import gtfs_pb2 as gtfs


class AgencyPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_AGENCY

    def pack(self) -> list[gtfs.Agency]:
        result: list[gtfs.Agency] = []
        with self.open_table('agency') as f:
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
                result.append(agency)
        return result


class NetworksPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_NETWORKS

    def pack(self) -> dict[int, str]:
        result: dict[int, str] = {}
        if self.has_file('networks'):
            with self.open_table('networks') as f:
                for row, network_id, _ in self.table_reader(f, 'network_id'):
                    result[network_id] = row['network_name']
        return result


class AreasPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_AREAS

    def pack(self) -> dict[int, str]:
        result: dict[int, str] = {}
        if self.has_file('areas'):
            with self.open_table('areas') as f:
                for row, area_id, _ in self.table_reader(f, 'network_id'):
                    result[area_id] = row['area_name']
        return result
