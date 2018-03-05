#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2018 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import logging
import medusa.backup


def debug_command(args):
    print("This command is not implemented yet")
    print(args)


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='config.json',
                        help='Specify config file')

    subcommand_template = argparse.ArgumentParser(add_help=False)
    subcommand_template.set_defaults(func=debug_command)

    subparsers = parser.add_subparsers(title='command', dest='command')
    backup_parser = subparsers.add_parser('backup', help='Backup Cassandra',
                                          parents=[subcommand_template])
    backup_parser.add_argument('-d', dest='delete_snapshot_if_exists',
                               default=False, action='store_true',
                               help='Delete snapshot if it already exists')
    backup_parser.add_argument('-n', '--backup_name', type=str, default=None,
                               help='Custom name for the backup')
    backup_parser.set_defaults(func=medusa.backup.main)

    restore_parser = subparsers.add_parser('restore', help='Restore Cassandra',
                                           parents=[subcommand_template])
    status_parser = subparsers.add_parser('status',
                                          help='Show status of backups',
                                          parents=[subcommand_template])
    return parser


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    parser = make_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        parser.exit(status=1, message='Please specify command')

    args.func(args)


if __name__ == '__main__':
    main()