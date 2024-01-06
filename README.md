# GTFS to Protobuf Packaging

This library / command-line tool introduces a protocol buffers-based format
for packaging GTFS feeds. The reasons for this are:

1. Decrease the size of a feed to 8-10% of the original.
2. Allow for even smaller and easy to apply delta files.

The recommended file extension for packaged feeds is _gtp_.

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

    gtfs_proto info city.gtp --part stops

Currently the blocks are `ids`, `strings`, `agency`, `calendar`, `shapes`,
`stops`, `routes`, `trips`, `transfers`, `networks`, `areas`, and `fare_links`.

When applicable, you can print just the line for a given identifier,
both for the one from the original GTFS feed, and for a numeric generated one:

   gtfs_proto info city.gtp -p stops --id 45

Of course you can view contents of a delta file the same way.

## Technical

## Author and License

The format and the code were written by Ilya Zverev. The code is published under ISC License,
the the format is CC0 or in a public domain, whatever applies in your country.
