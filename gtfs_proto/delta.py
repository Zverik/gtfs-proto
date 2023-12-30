import argparse


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
