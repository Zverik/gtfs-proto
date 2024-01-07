#!.venv/bin/python
import argparse
import csv
from gtfs_proto import GtfsProto, gtfs


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Removes extra data from Estonia GTFS feed')
    parser.add_argument('input', type=argparse.FileType('rb'))
    parser.add_argument('stops', type=argparse.FileType('r'))
    parser.add_argument('-s', action='store_true', help='Skip shapes')
    parser.add_argument('output', type=argparse.FileType('wb'))
    options = parser.parse_args()

    siri_ids: dict[str, int] = {}
    for row in csv.reader(options.stops, delimiter=';'):
        if row[1] and row[1] != 'SiriID':
            siri_ids[row[0]] = int(row[1])

    feed = GtfsProto(options.input)
    out = GtfsProto()
    out.header.version = feed.header.version
    out.strings = feed.strings
    out.calendar = feed.calendar
    out.trips = feed.trips

    if not options.s:
        out.shapes = feed.shapes

    out.stops = [gtfs.Stop(
        stop_id=s.stop_id,
        name=s.name,
        lat=s.lat,
        lon=s.lon,
        wheelchair=s.wheelchair,
        external_int_id=siri_ids.get(s.code, 0),
    ) for s in feed.stops]

    out.routes = [gtfs.Route(
        route_id=r.route_id,
        short_name=r.short_name,
        type=r.type,
        itineraries=[gtfs.RouteItinerary(
            itinerary_id=i.itinerary_id,
            headsign=i.headsign,
            stops=i.stops,
            shape_id=0 if options.s else i.shape_id,
        ) for i in r.itineraries],
    ) for r in feed.routes]

    out.write(options.output)
