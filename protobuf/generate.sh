#!/bin/sh
REPO="$(cd "$(dirname "$0")/..";pwd)"
protoc --python_out="$REPO/src/gtfs_proto" -I"$REPO/protobuf" "$REPO/protobuf"/*.proto
