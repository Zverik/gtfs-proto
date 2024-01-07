# GTFS to Protobuf Packaging

This library / command-line tool introduces a protocol buffers-based format
for packaging GTFS feeds. The reasons for this are:

1. Decrease the size of a feed to 8-10% of the original.
2. Allow for even smaller and easy to apply delta files.

The recommended file extension for packaged feeds is _gtp_.

## Differences with GTFS

The main thing missing from the packaged feed is fare information.
This is planned to be added, refer to [this ticket](https://github.com/Zverik/gtfs-proto/issues/1)
to track the implementation progress.

Packaged feed does not reflect source tables one to one. This is true
for most tables though. Here's what been deliberately skipped from
the official format:

* `stops.txt`: `tts_stop_name`, `stop_url`, `stop_timezone`, `level_id`.
* `routes.txt`: `route_url`, `route_sort_order`.
* `trips.txt`: `block_id`.
* `stop_times.txt`: `stop_sequence`, `shape_dist_travelled`.
* `shapes.txt`: `shape_pt_sequence`, `shape_dist_travelled`.

Feed files ignored are all the `fare*.txt`, `timeframes.txt`, `pathways.txt`,
`levels.txt`, `translations.txt`, `feed_info.txt`, and `attributions.txt`.
Fares are to be implemented, and for other tables it is recommended
to use the original feed.

### Binary Format

**Note that the format is to have significant changes, including renumbering
of fields, until version 1.0 is published.**

Inside the file, first thing is a little-endian two-byte size for the header
block. Then the header serialized message follows.

The header contains a list of sizes for each block. Blocks follow in the order
listed in the `Block` enum: first identifiers, then strings, then agencies,
and so on.

The same enum is used for keys in the `IdReference` message, that links
generated numeric ids from this packed feed with the original string ids.

If the feed is compressed (marked by a flag in the header), each block is
compressed using the Zstandard algorithm. It proved to be both fast and efficient,
decreasing the size by 70%.

### Location Encoding

Floating-point values are stored inefficiently, hence all longitude and latitudes
are multiplied by 100000 (10^5) and rounded. This allows for one-meter precision,
which is good enough on public transit scales.

In addition, when coordinates become lists, we store only a difference with the
last coordinate. This applies to both stops (relative to the previous stop) and
shapes: in latter, coordinates are relative to the previous coordinate, or to
the last one in the previous shape.

### Routes and Trips

The largest file in every GTFS feed is `stop_times.txt`. Here, it's missing, with
the data spread between routes and trips. The format also adds itineraries:

* An itinerary is a series of stops for a route with the same headsign and shape.
  * Note that there is no specific block for itineraries, instead they are packaged
    inside corresponding routes. But they still have unique identifiers.
* Route is the same as in GTFS.
* Trips reference an itinerary for stops, and add departure and arrival times for
  each stop (or start and end time when those are specified with `frequencies.txt`).

So to find a departure time for a given stop, you find itineraries that contain it,
and from those, routes and trips. You get a departure times list from the trip,
and use addition to get the actual time (since we store just differences with previous
times, with 5-second granularity).

### Deltas

Delta files looks the same as the original, but the header size has its last bit
set (`size & 0x8000`, note the unsigned integer). After that, `GtfsDeltaHeader`
follows, which also has version, date, compression fields, and a list of block sizes.

How the blocks are different, is explained in the [proto file](protobuf/gtfs.proto).

## Installation and Usage

Installing is simple:

    pip install gtfs-proto

### Packaging a feed

See a list of commands the tool provides by running it without arguments:

    gtfs_proto
    gtfs_proto pack --help

To package a feed, call:

    gtfs_proto pack gtfs.zip --output city.gtp

In a header, a feed stores an URL of a source zip file, and a date on which
that feed was built. You should specify those, although if the date is "today",
you can skip the argument:

    gtfs_proto pack gtfs.zip --url https://mta.org/gtfs/gtfs.zip --date 2024-03-19 -o city.gtp

When setting a pipeline to package feeds regularly, do specify the previous feed
file to keep identifiers from altering, and to keep delta file sizes to a minimum:

    gtfs_proto pack gtfs.zip --prev city_last.gtp -o city.gtp

### Deltas

Delta, a list differences between two feeds, is made with this obvious command:

    gtfs_proto delta city_last.gtp city.gtp -o city_delta.gtp

Currently it's to be decided whether a delta requires a different file extension.
Technically the format is almost the same, using the same protocol buffers definition.

If you lost an even older file and wish to keep your users updated even from very
old feeds, you can merge deltas:

    gtfs_proto dmerge city_delta_1-2.gtp city_delta_2-3.gtp -o city_delta_1-3.gtp

It's recommended to avoid merging deltas and store old feeds instead to produce
delta files with the `delta` command.

There is no command for applying deltas: it's on end users to read the file and
apply it straight to their inner database.

### Information

A packaged feed contains a header and an array of blocks, similar but not exactly mirroring
the original GTFS files. You can see the list, sizes and counts by running:

    gtfs_proto info city.gtp

Any block can be dumped into a series of one-line JSON objects by specifying
the block name:

    gtfs_proto info city.gtp --block stops

Currently the blocks are `ids`, `strings`, `agency`, `calendar`, `shapes`,
`stops`, `routes`, `trips`, `transfers`, `networks`, `areas`, and `fare_links`.

There are two additional "blocks" that print numbers from the header:
`version` and `date`. Use these to simplify automation. For example, this is
how you make a version-named copy of the lastest feed:

```sh
cp city-latest.gtp city-$(gtfs_proto info -b version).gtp
```

When applicable, you can print just the line for a given identifier,
both for the one from the original GTFS feed, and for a numeric generated one:

    gtfs_proto info city.gtp -p stops --id 45

Of course you can view contents of a delta file the same way.

## Python Library

Reading GTFS protobuf files is pretty straightforward:

```python
import gtfs_proto as gtp

feed = GtfsProto(open('city.gtp', 'rb'))
print(f'Feed built on {feed.header.date}')
for stop in feed.stops:
    print(f'Stop {stop.stop_id} named "{feed.strings[stop.name]}".')
```

The `GtfsProto` (and `GtfsDelta`) object reads the file header and lazily provides
all blocks as lists or dicts. To read all blocks instantly, use the `read_now=True`
argument for the constructor.

Parsing shapes and calendar services is not easy, so there are some service
functions, namely `parse_shape` and `parse_calendar`. The latter returns a list
of `CalendarService` with all the dates and day lists unpacked, and an `operates`
method to determine whether the line functions on a given date.

All built-in commands use this library, so refer to, for example,
[delta.py](src/gtfs_proto/delta.py) for an extended usage tutorial.

## Author and License

The format and the code were written by Ilya Zverev. The code is published under ISC License,
the the format is CC0 or in a public domain, whatever applies in your country.
