import argparse
from enum import Enum
import pathlib

from database_interface import DatabaseInterface
import arches_interface


class Subcommands(Enum):
    database_interface = 'database_interface'
    arches_interface = 'arches_interface'


cli_parser = argparse.ArgumentParser()
cli_parser.add_argument('subcommand', nargs='+', type=str, help='main subcommand')
cli_parser.add_argument('-i', '--input', nargs='*', type=pathlib.Path, help='input files')
cli_parser.add_argument('-o', '--origin', type=str, help='origin database')
cli_parser.add_argument('-t', '--target', type=str, help='target arches deployment')
cli_parser.add_argument('-r', '--reference', type=str, help='reference arches deployment')
cli_parser.add_argument('-y', '--yes', action='store_true', help='confirm action')

args: argparse.Namespace = cli_parser.parse_args()

DatabaseInterface.initialize(args)

match args.subcommand[0]:
    case Subcommands.arches_interface.value:
        arches_interface.arches_interface.process_subcommand(args)
    case Subcommands.database_interface.value:
        DatabaseInterface.process_subcommand(args)
    case _:
        raise ValueError(f"Unknown subcommand {args.subcommand[0]}")
