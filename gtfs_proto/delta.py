import argparse
from .wrapper import GtfsProto, gtfs, GtfsDelta


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
    c = gtfs.Calendar(base_date=c2.base_date)
    # TODO: dates
    # TODO: services
    return c


def delta_shapes(
        s1: dict[int, gtfs.TripShape], s2: dict[int, gtfs.TripShape]
        ) -> dict[int, gtfs.TripShape]:
    result: list[gtfs.TripShape] = []
    for k in s1:
        if k not in s2:
            result.append(gtfs.TripShape(shape_id=k))
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
        # TODO: changes routes and itineraries!
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


def delta_dict(d1: dict[int, str], d2: dict[int, str]) -> dict[int, str]:
    return {k: v for k, v in d2.items() if k not in d1 or d1[k] != v}


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
    # delta.transfers = delta_transfers(feed1.transfers, feed2.transfers)
    delta.networks = delta_dict(feed1.networks, feed2.networks)
    delta.areas = delta_dict(feed1.areas, feed2.areas)
    # delta.fare_links = delta_fare_links(feed1.fare_links, feed2.fare_links)
    delta.write(options.output)
    print(delta.header)
