import argparse
import sys
from . import (
    gtfs, GtfsDelta, FareLinks, StringCache,
    parse_calendar, build_calendar, int_to_date,
)


class DeltaMerger:
    def __init__(self, d_strings: StringCache, new_strings: StringCache):
        self.strings = d_strings
        self.s_new = new_strings

    def add_string(self, sid: int) -> int:
        if sid:
            return self.strings.add(self.s_new[sid])
        return 0

    def agencies(self, a1: list[gtfs.Agency], a2: list[gtfs.Agency]) -> list[gtfs.Agency]:
        ad1 = {a.agency_id: a for a in a1}
        for a in a2:
            if a == gtfs.Agency(a.agency_id):
                ad1[a.agency_id] = a
            elif a.agency_id not in ad1:
                # Add an agency and its strings.
                a.timezone = self.add_string(a.timezone)
                ad1[a.agency_id] = a
            else:
                # Merge two changes.
                first = ad1[a.agency_id]
                ad1[a.agency_id] = gtfs.Agency(
                    agency_id=a.agency_id,
                    name=a.name or first.name,
                    url=a.url or first.url,
                    timezone=self.add_string(a.timezone) or first.timezone,
                    lang=a.lang or first.lang,
                    phone=a.phone or first.phone,
                    fare_url=a.fare_url or first.fare_url,
                    email=a.email or first.email,
                )
        return list(ad1.values())

    def calendar(self, c1: gtfs.Calendar, c2: gtfs.Calendar) -> gtfs.Calendar:
        cd1 = {c.service_id: c for c in parse_calendar(c1)}
        for c in parse_calendar(c2):
            cd1[c.service_id] = c
        return build_calendar(list(cd1.values()), int_to_date(c2.base_date))

    def shapes(self, s1: list[gtfs.Shape], s2: list[gtfs.Shape]) -> list[gtfs.Shape]:
        sd1 = {s.shape_id: s for s in s1}
        for s in s2:
            sd1[s.shape_id] = s
        return list(sd1.values())

    def stops(self, s1: list[gtfs.Stop], s2: list[gtfs.Stop]) -> list[gtfs.Stop]:
        sd1 = {s.stop_id: s for s in s1}
        for s in s2:
            s.name = self.add_string(s.name)
            if s.delete or s.stop_id not in sd1:
                sd1[s.stop_id] = s
            else:
                first = sd1[s.stop_id]
                sd1[s.stop_id] = gtfs.Stop(
                    stop_id=s.stop_id,
                    code=first.code or s.code,
                    name=s.name or first.name,
                    desc=s.desc or first.desc,
                    lat=s.lat or first.lat,
                    lon=s.lon or first.lon,
                    type=s.type,
                    parent_id=s.parent_id or first.parent_id,
                    wheelchair=s.wheelchair,
                    platform_code=s.platform_code or first.platform_code,
                    external_str_id=s.external_str_id or first.external_str_id,
                    external_int_id=s.external_int_id or first.external_int_id,
                )
        return list(sd1.values())

    def routes(self, r1: list[gtfs.Route], r2: list[gtfs.Route]) -> list[gtfs.Route]:
        rd1 = {r.route_id: r for r in r1}
        for r in r2:
            # Update all strings.
            for i in range(len(r.long_name)):
                r.long_name[i] = self.add_string(r.long_name[i])
            for it in r.itineraries:
                it.headsign = self.add_string(it.headsign)
                for i in range(len(it.stop_headsigns)):
                    it.stop_headsigns[i] = self.add_string(it.stop_headsigns[i])

            if r.delete or r.route_id not in rd1:
                rd1[r.route_id] = r
            else:
                first = rd1[r.route_id]
                rd1[r.route_id] = gtfs.Route(
                    route_id=r.route_id,
                    agency_id=r.agency_id or first.agency_id,
                    short_name=r.short_name or first.short_name,
                    long_name=r.long_name or first.long_name,
                    desc=r.desc or first.desc,
                    type=r.type,
                    color=r.color,
                    text_color=r.text_color,
                    continuous_pickup=r.continuous_pickup,
                    continuous_dropoff=r.continuous_dropoff,
                )

                # Now for itineraries.
                it1 = {i.itinerary_id: i for i in first.itineraries}
                for it in r.itineraries:
                    it1[it.itinerary_id] = it
                rd1[r.route_id].itineraries.extend(it1.values())
        return list(rd1.values())

    def trips(self, t1: list[gtfs.Trip], t2: list[gtfs.Trip]) -> list[gtfs.Trip]:
        td1 = {t.trip_id: t for t in t1}
        for t in t2:
            if t.trip_id not in td1:
                td1[t.trip_id] = t
            elif t == gtfs.Trip(trip_id=t.trip_id):
                td1[t.trip_id] = t
            else:
                first = td1[t.trip_id]
                new_deps = t.departures or t.arrivals
                td1[t.trip_id] = gtfs.Trip(
                    trip_id=t.trip_id,
                    service_id=t.service_id or first.service_id,
                    itinerary_id=t.itinerary_id or first.itinerary_id,
                    short_name=t.short_name or first.short_name,
                    wheelchair=t.wheelchair,
                    bikes=t.bikes,
                    approximate=t.approximate,
                    departures=t.departures if new_deps else first.departures,
                    arrivals=t.arrivals if new_deps else first.arrivals,
                    pickup_types=t.pickup_types or first.pickup_types,
                    dropoff_types=t.dropoff_types or first.dropoff_types,
                    start_time=t.start_time or first.start_time,
                    end_time=t.end_time or first.end_time,
                    interval=t.interval or first.interval,
                )
        return list(td1.values())

    def transfers(self, t1: list[gtfs.Transfer],
                  t2: list[gtfs.Transfer]) -> list[gtfs.Transfer]:
        def transfer_key(t: gtfs.Transfer):
            return (
                t.from_stop, t.to_stop,
                t.from_route, t.to_route,
                t.from_trip, t.to_trip,
            )

        td1 = {transfer_key(t): t for t in t1}
        for t in t2:
            td1[transfer_key(t)] = t
        return list(td1.values())

    def fare_links(self, f1: FareLinks, f2: FareLinks):
        f1.stop_areas.update(f2.stop_areas)
        f1.stop_zones.update(f2.stop_zones)
        f1.route_networks.update(f2.route_networks)


def delta_merge():
    parser = argparse.ArgumentParser(
        description='Merges two GTFS deltas into one')
    parser.add_argument(
        'old', type=argparse.FileType('rb'), help='The first, older delta')
    parser.add_argument(
        'new', type=argparse.FileType('rb'), help='The second, latest delta')
    parser.add_argument(
        '-o', '--output', type=argparse.FileType('wb'), required=True,
        help='Resulting merged delta file')
    options = parser.parse_args()

    delta = GtfsDelta(options.old)
    second = GtfsDelta(options.new)
    if delta.header.version < second.header.old_version:
        print(
            f'Cannot fill the gap between versions {delta.header.version} '
            'and {second.header.old_version}.', file=sys.stderr)
        sys.exit(1)
    if delta.header.old_version >= second.header.old_version:
        print('The new delta already covers the scope of the old one.',
              file=sys.stderr)
        sys.exit(1)

    delta.header.version = second.header.version
    delta.header.date = second.header.date
    delta.header.compressed = second.header.compressed

    for k, v in delta.id_store.items():
        if k in second.id_store:
            # Append ids from the second delta.
            for i in range(v.last_id + 1, v.last_id + 1):
                v.ids[second.id_store[k].original[i]] = i
            v.last_id = 0 if not v.ids else max(v.ids.values())

    dm = DeltaMerger(delta.strings, second.strings)
    delta.agencies = dm.agencies(delta.agencies, second.agencies)
    delta.calendar = dm.calendar(delta.calendar, second.calendar)
    delta.shapes = dm.shapes(delta.shapes, second.shapes)
    delta.stops = dm.stops(delta.stops, second.stops)
    delta.routes = dm.routes(delta.routes, second.routes)
    delta.trips = dm.trips(delta.trips, second.trips)
    delta.transfers = dm.transfers(delta.transfers, second.transfers)
    delta.networks.update(second.networks)
    delta.areas.update(second.areas)
    dm.fare_links(delta.fare_links, second.fare_links)
    delta.write(options.output)
