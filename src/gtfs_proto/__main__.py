import sys
from .pack import pack
from .info import info
from .delta import delta
from .dmerge import delta_merge


def print_help():
    print('GTFS in Protobuf toolkit')
    print()
    print('Usage: {} <command> <arguments>'.format(sys.argv[0]))
    print()
    print('Commands:')
    print('    pack\tPackage GTFS zip into a protobuf file')
    print('    info\tPrint information for a protobuf-packed GTFS')
    print('    delta\tGenerate a delta file for two packed GTFS feeds')
    print('    dmerge\nMerge two sequential delta files')
    print()
    print('Run {} <command> --help to see a command help.'.format(sys.argv[0]))


def main():
    if len(sys.argv) <= 1:
        print_help()
        sys.exit(1)

    op = sys.argv[1].strip().lower()
    sys.argv.pop(1)

    if op == 'pack':
        pack()
    elif op == 'info':
        info()
    elif op == 'delta':
        delta()
    elif op == 'dmerge':
        delta_merge()
    else:
        print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
