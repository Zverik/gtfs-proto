import sys
from .pack import pack
from .info import info


def print_help():
    print('GTFS in Protobuf toolkit')
    print()
    print('Usage: {} <command> <arguments>'.format(sys.argv[0]))
    print()
    print('Commands:')
    print('    pack\tPackage GTFS zip into a protobuf file')
    print('    info\tPrint information for a protobuf-packed GTFS')
    print()
    print('Run {} <command> --help to see a command help.'.format(sys.argv[0]))


if len(sys.argv) <= 1:
    print_help()
    sys.exit(1)

op = sys.argv[1].strip().lower()
sys.argv.pop(1)

if op == 'pack':
    pack()
elif op == 'info':
    info()
else:
    print_help()
    sys.exit(1)
