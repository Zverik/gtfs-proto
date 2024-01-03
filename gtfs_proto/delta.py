import argparse
from .wrapper import GtfsProto, gtfs, GtfsDelta, FareLinks


def delta_agencies(
        a1: dict[int, gtfs.Agency], a2: dict[int, gtfs.Agency]
        ) -> dict[int, gtfs.Agency]:
    result: list[gtfs.Agency] = []
    for k, v in a2.items():
        if k not in a1:
            # new agency
            result.append(v)
        elif v != a1[k]:
            # wonder if comparing works
            old = a1[k]
            result.append(gtfs.Agency(
                agency_id=k,
                name=None if old.name == v.name else v.name,
                url=None if old.url == v.url else v.url,
                timezone=None if old.timezone == v.timezone else v.timezone,
                lang=None if old.lang == v.lang else v.lang,
                phone=None if old.phone == v.phone else v.phone,
                fare_url=None if old.fare_url == v.fare_url else v.fare_url,
                email=None if old.email == v.email else v.email,
            ))
    return {a.agency_id: a for a in result}


def delta_calendar(c1: gtfs.Calendar, c2: gtfs.Calendar) -> gtfs.Calendar:
    def add_base_date(d: list[int], base_date: int) -> list[int]:
        if not d:
            return d
        result: list[int] = []
        for i, date in enumerate(d):
            if i == 0:
                result.append(date + base_date)
            else:
                result.append(date + result[i - 1])
        return result

    c = gtfs.Calendar(base_date=c2.base_date)

    # Dates
    # d1 = {d.days_id: add_base_date(d.dates, c1.base_date) for d in c1.dates}
    # d2 = {d.days_id: add_base_date(d.dates, c2.base_date) for d in c2.dates}
    # TODO

    # Services
    # TODO

    return c


def delta_shapes(
        s1: dict[int, gtfs.Shape], s2: dict[int, gtfs.Shape]
        ) -> dict[int, gtfs.Shape]:
    result: list[gtfs.Shape] = []
    for k in s1:
        if k not in s2:
            result.append(gtfs.Shape(shape_id=k))
    for k, v in s2.items():
        if k not in s1:
            result.append(v)
        else:
            # compare
            old = s1[k]
            if old.longitudes != v.longitudes or old.latitudes != v.latitudes:
                result.append(v)
    return {s.shape_id: s for s in result}


def delta_stops(
        s1: dict[int, gtfs.Stop], s2: dict[int, gtfs.Stop]
        ) -> dict[int, gtfs.Stop]:
    result: list[gtfs.Stop] = []
    for k in s1:
        if k not in s2:
            result.append(gtfs.Stop(stop_id=k))
    for k, v in s2.items():
        if k not in s1:
            result.append(v)
        elif s1[k] != v:
            old = s1[k]
            result.append(gtfs.Stop(
                stop_id=k,
                code='' if old.code == v.code else v.code,
                name=0 if old.name == v.name else v.name,
                desc='' if old.desc == v.desc else v.desc,
                lat=0 if old.lat == v.lat else v.lat,
                lon=0 if old.lon == v.lon else v.lon,
                type=v.type,
                parent_id=0 if old.parent_id == v.parent_id else v.parent_id,
                wheelchair=v.wheelchair,
                platform_code='' if old.platform_code == v.platform_code else v.platform_code,
                external_str_id='' if old.external_str_id == v.external_str_id
                else v.external_str_id,
                external_int_id=0 if old.external_int_id == v.external_int_id
                else v.external_int_id,
            ))
    return {s.stop_id: s for s in result}


def delta_itineraries(
        i1: list[gtfs.RouteItinerary], i2: list[gtfs.RouteItinerary]
        ) -> list[gtfs.RouteItinerary]:
    result: list[gtfs.RouteItinerary] = []
    di1 = {i.itinerary_id: i for i in i1}
    di2 = {i.itinerary_id: i for i in i2}
    for k in di1:
        if k not in di2:
            result.append(gtfs.RouteItinerary(itinerary_id=k))
    for k, v in di2.items():
        if k not in di1 or di1[k] != v:
            result.append(v)
    return result


def delta_routes(
        r1: dict[int, gtfs.Route], r2: dict[int, gtfs.Route]
        ) -> dict[int, gtfs.Route]:
    result: list[gtfs.Route] = []
    for k in r1:
        if k not in r2:
            result.append(gtfs.Route(route_id=k))
    for k, v in r2.items():
        if k not in r1:
            result.append(v)
        elif r1[k] != v:  # TODO: does it register itinerary changes?
            old = r1[k]

            # Check if anything besides itineraries has changed.
            ni1 = gtfs.Route()
            ni1.CopyFrom(old)
            del ni1.itineraries[:]
            ni2 = gtfs.Route()
            ni2.CopyFrom(v)
            del ni2.itineraries[:]

            if ni1 == ni2:
                r = gtfs.Route(route_id=k)
            else:
                r = gtfs.Route(
                    route_id=k,
                    agency_id=0 if old.agency_id != v.agency_id else v.agency_id,
                    short_name='' if old.short_name != v.short_name else v.short_name,
                    long_name=[] if old.long_name != v.long_name else v.long_name,
                    desc='' if old.desc != v.desc else v.desc,
                    type=v.type,
                    color=v.color,
                    text_color=v.text_color,
                    continuous_pickup=v.continuous_pickup,
                    continuous_dropoff=v.continuous_dropoff,
                )

            # Now update itineraries.
            r.itineraries.extend(delta_itineraries(old.itineraries, v.itineraries))
            result.append(r)

    return {r.route_id: r for r in result}


def delta_trips(
        t1: dict[int, gtfs.Trip], t2: dict[int, gtfs.Trip]
        ) -> dict[int, gtfs.Trip]:
    result: list[gtfs.Trip] = []
    for k in t1:
        if k not in t2:
            result.append(gtfs.Trip(trip_id=k))
    for k, v in t2.items():
        if k not in t1:
            result.append(v)
        elif t1[k] != v:
            old = t1[k]
            arr_dep_changed = old.departures != v.departures or old.arrivals != v.arrivals
            result.append(gtfs.Trip(
                trip_id=k,
                service_id=0 if old.service_id == v.service_id else v.service_id,
                itinerary_id=0 if old.itinerary_id == v.itinerary_id else v.itinerary_id,
                short_name='' if old.short_name == v.short_name else v.short_name,
                wheelchair=v.wheelchair,
                bikes=v.bikes,
                approximate=0 if old.approximate == v.approximate else v.approximate,
                departures=[] if not arr_dep_changed else v.departures,
                arrivals=[] if not arr_dep_changed else v.arrivals,
                pickup_types=[] if old.pickup_types == v.pickup_types else v.pickup_types,
                dropoff_types=[] if old.dropoff_types == v.dropoff_types else v.dropoff_types,
                start_time=0 if old.start_time == v.start_time else v.start_time,
                end_time=0 if old.end_time == v.end_time else v.end_time,
                interval=0 if old.interval == v.interval else v.interval,
            ))
    return {r.trip_id: r for r in result}


def delta_transfers(t1: list[gtfs.Transfer], t2: list[gtfs.Transfer]) -> list[gtfs.Transfer]:
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


def delta_dict(d1: dict[int, str], d2: dict[int, str]) -> dict[int, str]:
    return {k: v for k, v in d2.items() if k not in d1 or d1[k] != v}


def delta_dict_int(d1: dict[int, int], d2: dict[int, int]) -> dict[int, int]:
    return {k: v for k, v in d2.items() if k not in d1 or d1[k] != v}


def delta_fare_links(f1: FareLinks, f2: FareLinks) -> gtfs.FareLinksDelta:
    result = gtfs.FareLinksDelta(
        stop_area_ids=delta_dict_int(f1.stop_areas, f2.stop_areas),
        stop_zone_ids=delta_dict_int(f1.stop_zones, f2.stop_zones),
        route_network_ids=delta_dict_int(f1.route_networks, f2.route_networks),
    )
    return result


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
    delta.header.compressed = feed2.header.compressed
    delta.strings = feed2.strings
    delta.strings.delta_skip = len(feed1.strings.strings)
    delta.id_store = feed2.id_store
    for k, v in feed1.id_store.items():
        if k in feed2.id_store:
            feed2.id_store[k].delta_skip = v.last_id
    delta.agencies = delta_agencies(feed1.agencies, feed2.agencies)
    delta.calendar = delta_calendar(feed1.calendar, feed2.calendar)
    delta.shapes = delta_shapes(feed1.shapes, feed2.shapes)
    delta.stops = delta_stops(feed1.stops, feed2.stops)
    delta.routes = delta_routes(feed1.routes, feed2.routes)
    delta.trips = delta_trips(feed1.trips, feed2.trips)
    delta.transfers = delta_transfers(feed1.transfers, feed2.transfers)
    delta.networks = delta_dict(feed1.networks, feed2.networks)
    delta.areas = delta_dict(feed1.areas, feed2.areas)
    delta.fare_links_delta = delta_fare_links(feed1.fare_links, feed2.fare_links)
    delta.write(options.output)
