from datetime import date, timedelta
from . import gtfs_pb2 as gtfs
from .wrapper import SHAPE_SCALE


class CalendarService:
    def __init__(self, service_id: int,
                 start_date: date | None = None,
                 end_date: date | None = None,
                 weekdays: list[bool] | None = None,
                 added_days: list[date] | None = None,
                 removed_days: list[date] | None = None):
        self.service_id = service_id
        self.start_date = start_date
        self.end_date = end_date
        self.weekdays = weekdays or [False] * 7
        self.added_days = added_days or []
        self.removed_days = removed_days or []

    def operates(self, on: date | None = None) -> bool:
        if not on:
            on = date.today()
        if self.start_date and on < self.start_date:
            return False
        if self.end_date and on > self.end_date:
            return False
        if on in self.removed_days:
            return False
        if on in self.added_days:
            return True
        return self.weekdays[on.weekday()]


def int_to_date(d: int) -> date:
    return date(d // 10000, (d % 10000) // 100, d % 100)


def parse_calendar(c: gtfs.Calendar) -> list[CalendarService]:
    def add_base_date(d: list[int], base_date: date) -> list[date]:
        result: list[date] = []
        for i, dint in enumerate(d):
            if i == 0:
                result.append(base_date + timedelta(days=dint))
            else:
                result.append(result[-1] + timedelta(days=dint))
        return result

    base_date = int_to_date(c.base_date)
    dates = {i: add_base_date(d.dates, base_date) for i, d in enumerate(c.dates)}
    result = []
    for s in c.services:
        result.append(CalendarService(
            service_id=s.service_id,
            start_date=None if not s.start_date else base_date + timedelta(days=s.start_date),
            end_date=None if not s.end_date else base_date + timedelta(days=s.end_date),
            weekdays=[s.weekdays & (1 << i) > 0 for i in range(7)],
            added_days=dates[s.added_days],
            removed_days=dates[s.removed_days],
        ))
    return result


def build_calendar(services: list[CalendarService],
                   base_date: date | None = None) -> gtfs.Calendar:
    def to_int(d: date | None) -> int:
        return 0 if not d else int(d.strftime('%Y%m%d'))

    def pack_dates(dates: list[date], base_date: date) -> list[int]:
        result: list[int] = []
        for i, d in enumerate(sorted(dates)):
            if d > base_date:
                result.append((dates[i] - (base_date if i == 0 else dates[i - 1])).days)
        return result

    def find_or_add(dates: list[int], data: list[list[int]]) -> int:
        """Modifies the data!"""
        for i, stored in enumerate(data):
            if len(stored) == len(dates):
                found = True
                for j, v in enumerate(dates):
                    if v != stored[j]:
                        found = False
                        break
                if found:
                    return i
        data.append(dates)
        return len(data) - 1

    if not base_date:
        base_date = date.today() - timedelta(days=2)

    dates: list[list[int]] = [[]]
    c = gtfs.Calendar(base_date=to_int(base_date))
    for s in services:
        if not s.end_date:
            end_date = base_date
        elif s.end_date < base_date:
            # 1 day effectively makes it end yesterday
            end_date = base_date + timedelta(days=1)
        else:
            end_date = s.end_date
        c.services.append(gtfs.CalendarService(
            service_id=s.service_id,
            start_date=(0 if not s.start_date or s.start_date < base_date
                        else (s.start_date - base_date).days),
            end_date=(end_date - base_date).days,
            weekdays=sum(1 << i for i in range(7) if s.weekdays[i]),
            added_days=find_or_add(pack_dates(s.added_days, base_date), dates),
            removed_days=find_or_add(pack_dates(s.removed_days, base_date), dates),
        ))
    for d in dates:
        c.dates.append(gtfs.CalendarDates(dates=d))
    return c


def parse_shape(shape: gtfs.Shape) -> list[tuple[float, float]]:
    last_coord = (0, 0)
    coords: list[tuple[float, float]] = []
    for i in range(len(shape.longitudes)):
        c = (shape.longitudes[i] + last_coord[0], shape.latitudes[i] + last_coord[1])
        coords.append((c[0] / SHAPE_SCALE, c[1] / SHAPE_SCALE))
        last_coord = c
    return coords


def build_shape(shape_id: int, coords: list[tuple[float, float]]) -> gtfs.Shape:
    if len(coords) < 2:
        raise Exception(f'Got {len(coords)} coords for shape {shape_id}')
    shape = gtfs.Shape(shape_id=shape_id)
    last_coord = (0, 0)
    for c in coords:
        new_coord = (round(c[0] * SHAPE_SCALE), round(c[1] * SHAPE_SCALE))
        shape.longitudes.append(new_coord[0] - last_coord[0])
        shape.latitudes.append(new_coord[1] - last_coord[1])
        last_coord = new_coord
    return shape
