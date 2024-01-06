import argparse
import datetime as dt
from . import (
    GtfsProto, gtfs, GtfsDelta, FareLinks, StringCache,
    parse_calendar, build_calendar, CalendarService, int_to_date,
)


class DeltaMaker:
    def __init__(self, old_strings: StringCache, new_strings: StringCache,
                 delta_strings: StringCache):
        self.s_old = old_strings
        self.s_new = new_strings
        self.strings = delta_strings

    def add_string(self, sid: int) -> int:
        if sid:
            return self.strings.add(self.s_new[sid])
        return 0

    def from_old(self, old_str: int) -> int:
        if not old_str:
            return 0
        return self.s_new.index[self.s_old[old_str]]

    def if_str_changed(self, old_str: int, new_str: int) -> int | None:
        if old_str != new_str:
            return self.add_string(new_str)
        return None

    def agencies(self, a1: list[gtfs.Agency], a2: list[gtfs.Agency]) -> list[gtfs.Agency]:
        ad1 = {a.agency_id: a for a in a1}
        ad2 = {a.agency_id: a for a in a2}
        result: list[gtfs.Agency] = []
        for k, v in ad2.items():
            if k not in ad1:
                # new agency
                v.timezone = self.add_string(v.timezone)
                result.append(v)
            else:
                old = ad1[k]
                old.timezone = self.from_old(old.timezone)
                if v != old:
                    result.append(gtfs.Agency(
                        agency_id=k,
                        name=None if old.name == v.name else v.name,
                        url=None if old.url == v.url else v.url,
                        timezone=self.if_str_changed(old.timezone, v.timezone),
                        lang=None if old.lang == v.lang else v.lang,
                        phone=None if old.phone == v.phone else v.phone,
                        fare_url=None if old.fare_url == v.fare_url else v.fare_url,
                        email=None if old.email == v.email else v.email,
                    ))
        return result

    def calendar(self, c1: gtfs.Calendar, c2: gtfs.Calendar) -> gtfs.Calendar:
        def cut(dates: list[dt.date], base_date: dt.date) -> list[dt.date]:
            return [d for d in dates if d > base_date]

        cd1 = {c.service_id: c for c in parse_calendar(c1)}
        cd2 = {c.service_id: c for c in parse_calendar(c2)}
        result: list[CalendarService] = []

        for k in cd1:
            if k not in cd2:
                result.append(CalendarService(service_id=k))
        for k, v in cd2.items():
            if k not in cd1:
                result.append(v)
            else:
                bd = dt.date.today() - dt.timedelta(days=1)
                if not cd1[k].equals(v, bd):
                    result.append(v)

        return build_calendar(result, int_to_date(c2.base_date))

    def shapes(self, s1: list[gtfs.Shape], s2: list[gtfs.Shape]) -> list[gtfs.Shape]:
        sd1 = {s.shape_id: s for s in s1}
        sd2 = {s.shape_id: s for s in s2}
        result: list[gtfs.Shape] = []
        for k in sd1:
            if k not in sd2:
                result.append(gtfs.Shape(shape_id=k))
        for k, v in sd2.items():
            if k not in sd1:
                result.append(v)
            else:
                # compare
                old = sd1[k]
                if old.longitudes != v.longitudes or old.latitudes != v.latitudes:
                    result.append(v)
        return result

    def stops(self, s1: list[gtfs.Stop], s2: list[gtfs.Stop]) -> list[gtfs.Stop]:
        sd1 = {s.stop_id: s for s in s1}
        sd2 = {s.stop_id: s for s in s2}
        result: list[gtfs.Stop] = []
        for k in sd1:
            if k not in sd2:
                result.append(gtfs.Stop(stop_id=k, delete=True))
        for k, v in sd2.items():
            if k not in sd1:
                v.name = self.add_string(v.name)
                result.append(v)
            else:
                old = sd1[k]
                old.name = self.from_old(old.name)
                if old != v:
                    result.append(gtfs.Stop(
                        stop_id=k,
                        code='' if old.code == v.code else v.code,
                        name=self.if_str_changed(old.name, v.name),
                        desc='' if old.desc == v.desc else v.desc,
                        lat=0 if old.lat == v.lat and old.lon == v.lon else v.lat,
                        lon=0 if old.lat == v.lat and old.lon == v.lon else v.lon,
                        type=v.type,
                        parent_id=0 if old.parent_id == v.parent_id else v.parent_id,
                        wheelchair=v.wheelchair,
                        platform_code=('' if old.platform_code == v.platform_code
                                       else v.platform_code),
                        external_str_id='' if old.external_str_id == v.external_str_id
                        else v.external_str_id,
                        external_int_id=0 if old.external_int_id == v.external_int_id
                        else v.external_int_id,
                    ))
        return result

    def itineraries(
            self, i1: list[gtfs.RouteItinerary], i2: list[gtfs.RouteItinerary]
            ) -> list[gtfs.RouteItinerary]:
        result: list[gtfs.RouteItinerary] = []
        di1 = {i.itinerary_id: i for i in i1}
        di2 = {i.itinerary_id: i for i in i2}
        for k in di1:
            if k not in di2:
                result.append(gtfs.RouteItinerary(itinerary_id=k))
        for k, v in di2.items():
            old = di1.get(k)
            if old:
                old.headsign = self.from_old(old.headsign)
                for i in range(len(old.stop_headsigns)):
                    old.stop_headsigns[i] = self.from_old(old.stop_headsigns[i])
            if not old or old != v:
                v.headsign = self.add_string(v.headsign)
                for i in range(len(v.stop_headsigns)):
                    v.stop_headsigns[i] = self.add_string(v.stop_headsigns[i])
                result.append(v)
        return result

    def routes(self, r1: list[gtfs.Route], r2: list[gtfs.Route]) -> list[gtfs.Route]:
        rd1 = {r.route_id: r for r in r1}
        rd2 = {r.route_id: r for r in r2}
        result: list[gtfs.Route] = []
        for k in rd1:
            if k not in rd2:
                result.append(gtfs.Route(route_id=k, delete=True))
        for k, v in rd2.items():
            if k not in rd1:
                for i in range(len(v.long_name)):
                    v.long_name[i] = self.add_string(v.long_name[i])
                del v.itineraries[:]
                v.itineraries.extend(self.itineraries([], v.itineraries))
                result.append(v)
            else:
                old = rd1[k]
                for i in range(len(old.long_name)):
                    old.long_name[i] = self.from_old(old.long_name[i])
                i1 = list(sorted(old.itineraries, key=lambda it: it.itinerary_id))
                i2 = list(sorted(v.itineraries, key=lambda it: it.itinerary_id))

                if old != v:
                    # Check if anything besides itineraries has changed.
                    ni1 = gtfs.Route()
                    ni1.CopyFrom(old)
                    del ni1.itineraries[:]
                    ni2 = gtfs.Route()
                    ni2.CopyFrom(v)
                    del ni2.itineraries[:]

                    if ni1 == ni2:
                        if i1 == i2:
                            # Just unsorted itineraries.
                            continue
                        r = gtfs.Route(route_id=k)
                    else:
                        r = gtfs.Route(
                            route_id=k,
                            agency_id=0 if old.agency_id != v.agency_id else v.agency_id,
                            short_name='' if old.short_name != v.short_name else v.short_name,
                            long_name=[] if old.long_name == v.long_name else [
                                self.add_string(s) for s in v.long_name],
                            desc='' if old.desc != v.desc else v.desc,
                            type=v.type,
                            color=v.color,
                            text_color=v.text_color,
                            continuous_pickup=v.continuous_pickup,
                            continuous_dropoff=v.continuous_dropoff,
                        )

                    # Now update itineraries.
                    r.itineraries.extend(self.itineraries(old.itineraries, v.itineraries))
                    result.append(r)

        return result

    def trips(self, t1: list[gtfs.Trip], t2: list[gtfs.Trip]) -> list[gtfs.Trip]:
        td1 = {t.trip_id: t for t in t1}
        td2 = {t.trip_id: t for t in t2}
        result: list[gtfs.Trip] = []
        for k in td1:
            if k not in td2:
                result.append(gtfs.Trip(trip_id=k))
        for k, v in td2.items():
            if k not in td1:
                result.append(v)
            elif td1[k] != v:
                old = td1[k]
                arr_dep_changed = old.departures != v.departures or old.arrivals != v.arrivals
                result.append(gtfs.Trip(
                    trip_id=k,
                    service_id=0 if old.service_id == v.service_id else v.service_id,
                    itinerary_id=0 if old.itinerary_id == v.itinerary_id else v.itinerary_id,
                    short_name='' if old.short_name == v.short_name else v.short_name,
                    wheelchair=v.wheelchair,
                    bikes=v.bikes,
                    approximate=v.approximate,
                    departures=[] if not arr_dep_changed else v.departures,
                    arrivals=[] if not arr_dep_changed else v.arrivals,
                    pickup_types=[] if old.pickup_types == v.pickup_types else v.pickup_types,
                    dropoff_types=[] if old.dropoff_types == v.dropoff_types else v.dropoff_types,
                    start_time=0 if old.start_time == v.start_time else v.start_time,
                    end_time=0 if old.end_time == v.end_time else v.end_time,
                    interval=0 if old.interval == v.interval else v.interval,
                ))
        return result

    def transfers(self, t1: list[gtfs.Transfer],
                  t2: list[gtfs.Transfer]) -> list[gtfs.Transfer]:
        def transfer_key(t: gtfs.Transfer):
            return (
                t.from_stop, t.to_stop,
                t.from_route, t.to_route,
                t.from_trip, t.to_trip,
            )

        result: list[gtfs.Transfer] = []
        td1 = {transfer_key(t): t for t in t1}
        td2 = {transfer_key(t): t for t in t2}
        for k, v in td1.items():
            if k not in td2:
                result.append(gtfs.Transfer(
                    from_stop=v.from_stop,
                    to_stop=v.to_stop,
                    from_route=v.from_route,
                    to_route=v.to_route,
                    from_trip=v.from_trip,
                    to_trip=v.to_trip,
                    delete=True,
                ))
        for k, v in td2.items():
            if k not in td1 or td1[k] != v:
                result.append(v)
        return result

    def delta_dict(self, d1: dict[int, str], d2: dict[int, str]) -> dict[int, str]:
        return {k: v for k, v in d2.items() if k not in d1 or d1[k] != v}

    def fare_links(self, f1: FareLinks, f2: FareLinks) -> FareLinks:
        def delta_dict_int(d1: dict[int, int], d2: dict[int, int]) -> dict[int, int]:
            ch = {k: v for k, v in d2.items() if k not in d1 or d1[k] != v}
            ch.update({k: 0 for k in d1 if k not in d2})
            return ch

        fl = FareLinks()
        fl.stop_areas = delta_dict_int(f1.stop_areas, f2.stop_areas)
        fl.stop_zones = delta_dict_int(f1.stop_zones, f2.stop_zones)
        fl.route_networks = delta_dict_int(f1.route_networks, f2.route_networks)
        return fl


def delta():
    parser = argparse.ArgumentParser(
        description='Generates a delta between two protobuf-packed GTFS feeds')
    parser.add_argument(
        'old', type=argparse.FileType('rb'), help='The first, older feed')
    parser.add_argument(
        'new', type=argparse.FileType('rb'), help='The second, latest feed')
    parser.add_argument(
        '-o', '--output', type=argparse.FileType('wb'), required=True,
        help='Resulting delta file')
    options = parser.parse_args()

    feed1 = GtfsProto(options.old)
    feed2 = GtfsProto(options.new)

    delta = GtfsDelta()
    delta.header.old_version = feed1.header.version
    delta.header.version = feed2.header.version
    delta.header.date = feed2.header.date
    delta.header.compressed = feed2.header.compressed

    delta.id_store = feed2.id_store
    for k, v in feed1.id_store.items():
        if k in feed2.id_store:
            feed2.id_store[k].delta_skip = v.last_id

    dm = DeltaMaker(feed1.strings, feed2.strings, delta.strings)
    delta.agencies = dm.agencies(feed1.agencies, feed2.agencies)
    delta.calendar = dm.calendar(feed1.calendar, feed2.calendar)
    delta.shapes = dm.shapes(feed1.shapes, feed2.shapes)
    delta.stops = dm.stops(feed1.stops, feed2.stops)
    delta.routes = dm.routes(feed1.routes, feed2.routes)
    delta.trips = dm.trips(feed1.trips, feed2.trips)
    delta.transfers = dm.transfers(feed1.transfers, feed2.transfers)
    delta.networks = dm.delta_dict(feed1.networks, feed2.networks)
    delta.areas = dm.delta_dict(feed1.areas, feed2.areas)
    delta.fare_links = dm.fare_links(feed1.fare_links, feed2.fare_links)
    delta.write(options.output)
