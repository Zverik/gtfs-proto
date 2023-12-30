from dataclasses import dataclass
from typing import TextIO
from datetime import date
from .base import BasePacker, FeedCache
from zipfile import ZipFile
from . import gtfs_pb2 as gtfs
from csv import DictReader


def date_diff(b: int, a: int) -> int:
    # return b - a
    aa = (a % 100) + ((a % 10000) // 100) * 31 + (a // 10000) * 31 * 12
    bb = (b % 100) + ((b % 10000) // 100) * 31 + (b // 10000) * 31 * 12
    return bb - aa


class CalendarDates:
    def __init__(self):
        self.added: dict[str, int] = {}
        self.removed: dict[str, int] = {}
        self.lists: list[list[int]] = [[]]

    def find_list(self, dates: list[int]) -> int:
        for i in range(len(self.lists)):
            if len(self.lists[i]) == len(dates):
                found = True
                for j in range(len(dates)):
                    if self.lists[i][j] != dates[j]:
                        found = False
                        break
                if found:
                    return i
        return -1

    def add(self, dates: list[int], service_id: str, added: bool):
        if not dates:
            return
        dates.sort()
        idx = self.find_list(dates)
        if idx < 0:
            idx = len(self.lists)
            self.lists.append(dates)
        if added:
            self.added[service_id] = idx
        else:
            self.removed[service_id] = idx

    def apply_base_date(self, base_date: int):
        renumber: dict[int, int | None] = {0: 0}
        last_num = 0
        for i, dates in enumerate(self.lists):
            if i == 0:
                continue
            # Delete dates before base_date.
            j = 0
            while j < len(dates) and dates[j] <= base_date:
                j += 1
            if j > 0:
                del dates[:j]

            if dates:
                # Replaces dates with difference to previous/base.
                for j in reversed(range(1, len(dates))):
                    dates[j] = date_diff(dates[j], dates[j - 1])
                dates[0] = date_diff(dates[0], base_date)

                last_similar = self.find_list(dates)
                if 0 < last_similar and last_similar < i:
                    dates.clear()
                    renumber[i] = renumber[last_similar]
                else:
                    last_num += 1
                    renumber[i] = last_num
            else:
                renumber[i] = None

        # Now renumber
        self.lists = [] + [lst for lst in self.lists if lst]
        for k in list(self.added):
            n = renumber[self.added[k]]
            if n is None:
                del self.added[k]
            else:
                self.added[k] = n
        for k in list(self.removed):
            n = renumber[self.removed[k]]
            if n is None:
                del self.removed[k]
            else:
                self.removed[k] = n


@dataclass
class CalendarRecord:
    start_date: int
    end_date: int
    weekdays: int


class CalendarPacker(BasePacker):
    def __init__(self, z: ZipFile, store: FeedCache):
        super().__init__(z, store)

    @property
    def block(self):
        return gtfs.B_CALENDAR

    def pack(self):
        dates: CalendarDates | None = None
        if self.has_file('calendar_dates'):
            with self.open_table('calendar_dates') as f:
                dates = self.read_calendar_dates(f)
        if self.has_file('calendar'):
            with self.open_table('calendar') as f:
                data = self.prepare_calendar(f, dates)
        else:
            # dates must not be empty
            data = self.prepare_calendar(None, dates)
        return data

    def read_calendar_dates(self, fileobj: TextIO) -> CalendarDates:
        dates = CalendarDates()

        # Assuming the table is grouped by service_id.
        cur_service: str = ''
        added: list[int] = []
        removed: list[int] = []
        for row in DictReader(fileobj):
            if cur_service != row['service_id']:
                if added:
                    dates.add(added, cur_service, True)
                    added = []
                if removed:
                    dates.add(removed, cur_service, False)
                    removed = []
                cur_service = row['service_id']
            value = int(row['date'].strip())
            if row['exception_type'].strip() == '1':
                added.append(value)
            else:
                removed.append(value)
        if added:
            dates.add(added, cur_service, True)
        if removed:
            dates.add(removed, cur_service, False)

        return dates

    def prepare_calendar(
            self, fileobj: TextIO | None, dates: CalendarDates | None) -> bytes:
        services: dict[int, CalendarRecord] = {}
        if fileobj:
            # First just read everything, to detect the base date.
            for row in DictReader(fileobj):
                if row['start_date']:
                    start_date = int(row['start_date'].strip())
                else:
                    start_date = 0
                if row['end_date']:
                    end_date = int(row['end_date'].strip())
                else:
                    end_date = 0
                weekdays = 0
                for i, k in enumerate((
                        'monday', 'tuesday', 'wednesday', 'thursday',
                        'friday', 'saturday', 'sunday')):
                    if row[k] == '1':
                        weekdays += 1 << i
                services[self.ids.add(row['service_id'])] = CalendarRecord(
                    start_date=start_date,
                    end_date=end_date,
                    weekdays=weekdays,
                )
        if dates:
            # Adding stubs for each service_id that's missing in the calendar.
            for sid in sorted(dates.added.keys()):
                if sid not in services:
                    services[self.ids.add(sid)] = CalendarRecord(
                        start_date=dates.lists[dates.added[sid]][0],
                        end_date=0,
                        weekdays=0,
                    )
        if not services:
            raise ValueError('Either calendar.txt or calendar_dates.txt must be present.')

        # Determine base date.
        base_date = min(s.start_date for s in services.values()) - 1
        yesterday = int(date.today().strftime('%Y%m%d')) - 1
        if base_date < yesterday:
            base_date = yesterday

        if dates:
            dates.apply_base_date(base_date)

        # Renumber all dates we got.
        for s in services.values():
            if s.start_date > 0:
                s.start_date = date_diff(s.start_date, base_date)
                if s.start_date < 0:
                    s.start_date = 0
            if s.end_date > 0:
                s.end_date = date_diff(s.end_date, base_date)
                if s.end_date < 0:
                    s.end_date = 0

        calendar = gtfs.Calendar()
        calendar.base_date = base_date

        if dates:
            for i, date_list in enumerate(dates.lists):
                if i > 0:
                    calendar.dates.append(gtfs.CalendarDates(
                        days_id=i,
                        dates=date_list,
                    ))

        sids = self.ids.reversed()
        for service_id, service in services.items():
            s = gtfs.CalendarService(
                service_id=service_id,
                start_date=service.start_date,
                end_date=service.end_date,
                weekdays=service.weekdays,
                added_days=0 if not dates else dates.added.get(sids[service_id], 0),
                removed_days=0 if not dates else dates.removed.get(sids[service_id], 0),
            )
            calendar.services.append(s)

        return calendar.SerializeToString()
