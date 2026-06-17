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

    def equals(self, other, base_date: date | None = None) -> bool:
        def cut(dates: list[date], base_date: date | None) -> list[date]:
            if not base_date:
                return dates
            return [d for d in dates if d > base_date]

        def cap(d: date | None, base_date: date | None) -> date | None:
            if not d or not base_date or d > base_date:
                return d
            return base_date

        if self.service_id != other.service_id:
            return False
        if cap(self.start_date, base_date) != cap(other.start_date, base_date):
            return False
        if cap(self.end_date, base_date) != cap(other.end_date, base_date):
            return False
        if self.weekdays != other.weekdays:
            return False
        if cut(other.added_days, base_date) != cut(self.added_days, base_date):
            return False
        if cut(other.removed_days, base_date) != cut(self.removed_days, base_date):
            return False
        return True

    def __eq__(self, other) -> bool:
        return self.equals(other)


def int_to_date(d: int) -> date:
    return date(d // 10000, (d % 10000) // 100, d % 100)


def parse_calendar(c: gtfs.Services) -> list[CalendarService]:
    def add_base_date(d: list[int], base_date: date) -> list[date]:
        result: list[date] = []
        for i, dint in enumerate(d):
            if i == 0:
                result.append(base_date + timedelta(days=dint))
            else:
                result.append(result[-1] + timedelta(days=dint))
        return result

    base_date = int_to_date(c.base_date)
    result = []
    for s in c.services:
        result.append(CalendarService(
            service_id=s.service_id,
            start_date=None if not s.start_date else base_date + timedelta(days=s.start_date),
            end_date=None if not s.end_date else base_date + timedelta(days=s.end_date),
            weekdays=[s.weekdays & (1 << i) > 0 for i in range(7)],
            added_days=add_base_date(s.added_days, base_date),
            removed_days=add_base_date(s.removed_days, base_date),
        ))
    return result


def build_calendar(services: list[CalendarService],
                   base_date: date | None = None) -> gtfs.Services:
    def to_int(d: date | None) -> int:
        return 0 if not d else int(d.strftime('%Y%m%d'))

    def pack_dates(dates: list[date], base_date: date) -> list[int]:
        result: list[int] = []
        prev = base_date
        for d in sorted(dates):
            if d > base_date:
                result.append((d - prev).days)
                prev = d
        return result

    if not base_date:
        base_date = date.today() - timedelta(days=2)

    c = gtfs.Services(base_date=to_int(base_date))
    for s in services:
        if not s.end_date:
            end_date = base_date
        elif s.end_date < base_date:
            # 1 day effectively makes it end yesterday
            end_date = base_date + timedelta(days=1)
        else:
            end_date = s.end_date
        c.services.append(gtfs.Service(
            service_id=s.service_id,
            start_date=(0 if not s.start_date or s.start_date < base_date
                        else (s.start_date - base_date).days),
            end_date=(end_date - base_date).days,
            weekdays=sum(1 << i for i in range(7) if s.weekdays[i]),
            added_days=pack_dates(s.added_days, base_date),
            removed_days=pack_dates(s.removed_days, base_date),
        ))
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
