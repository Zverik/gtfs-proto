from typing import TextIO
from datetime import date
from zipfile import ZipFile
from collections import defaultdict
from . import BasePacker
from .. import StringCache, IdReference, int_to_date, gtfs, CalendarService, build_calendar


class CalendarDates:
    def __init__(self):
        self.added: dict[int, list[date]] = defaultdict(list)
        self.removed: dict[int, list[date]] = defaultdict(list)


class CalendarPacker(BasePacker):
    def __init__(self, z: ZipFile, strings: StringCache, id_store: dict[int, IdReference]):
        super().__init__(z, strings, id_store)

    @property
    def block(self):
        return gtfs.B_CALENDAR

    def pack(self):
        dates = CalendarDates()
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
        for row, service_id, _ in self.table_reader(fileobj, 'service_id'):
            value = int_to_date(int(row['date'].strip()))
            if row['exception_type'].strip() == '1':
                dates.added[service_id].append(value)
            else:
                dates.removed[service_id].append(value)
        return dates

    def prepare_calendar(
            self, fileobj: TextIO | None, dates: CalendarDates) -> bytes:
        services: list[CalendarService] = []
        seen_ids: set[int] = set()
        if fileobj:
            # First just read everything, to detect the base date.
            for row, service_id, _ in self.table_reader(fileobj, 'service_id'):
                s = CalendarService(service_id=service_id)
                if row['start_date']:
                    s.start_date = int_to_date(int(row['start_date']))
                if row['end_date']:
                    s.end_date = int_to_date(int(row['end_date']))
                weekdays: list[bool] = []
                for i, k in enumerate((
                        'monday', 'tuesday', 'wednesday', 'thursday',
                        'friday', 'saturday', 'sunday')):
                    weekdays.append(row[k] == '1')
                s.weekdays = weekdays
                s.added_days = dates.added.get(service_id, [])
                s.removed_days = dates.removed.get(service_id, [])
                services.append(s)
                seen_ids.add(service_id)

        # Adding stubs for each service_id that's missing in the calendar.
        for sid, date_list in dates.added.items():
            if sid not in seen_ids:
                services.append(CalendarService(
                    service_id=sid,
                    added_days=date_list,
                ))
        if not services:
            raise ValueError('Either calendar.txt or calendar_dates.txt must be present.')

        return build_calendar(services)
