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
import socket
import medusa.backup
import medusa.fetch_ringstate


def debug_command(args):
    print("This command is not implemented yet")
    print(args)


# Hardcoded values (must be refactored later)
BUCKET_NAME = "parmus-medusa-test"
CREDENTIALS_KEY_FILE = "medusa-test.json"
PREFIX = 'yolocassandra'


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='config.json',
                        help='Specify config file')

    subcommand_template = argparse.ArgumentParser(add_help=False)
    subcommand_template.add_argument('--bucket-name', type=str,
                                     default=BUCKET_NAME, help='Bucket name')
    subcommand_template.add_argument('--key-file', type=str,
                                     default=CREDENTIALS_KEY_FILE,
                                     help='GCP credentials key file')
    subcommand_template.add_argument('--prefix', type=str, default=PREFIX,
                                     help='Prefix for shared storage')
    subcommand_template.add_argument('--fqdn', type=str, default=None,
                                     help='Act as another host')
    subcommand_template.set_defaults(func=debug_command,
                                     fqdn=socket.gethostname())

    subparsers = parser.add_subparsers(title='command', dest='command')
    backup_parser = subparsers.add_parser('backup', help='Backup Cassandra',
                                          parents=[subcommand_template])
    backup_parser.add_argument('-d', dest='delete_snapshot_if_exists',
                               default=False, action='store_true',
                               help='Delete snapshot if it already exists')
    backup_parser.add_argument('-n', '--backup_name', type=str, default=None,
                               help='Custom name for the backup')
    backup_parser.set_defaults(func=medusa.backup.main)

    list_parser = subparsers.add_parser('list', help='List backups',
                                        parents=[subcommand_template])

    ringstate_parser = subparsers.add_parser('fetch-ringstate',
                                             help='Fetch ringstate',
                                             parents=[subcommand_template])
    ringstate_parser.add_argument('backup_name', type=str,
                                  metavar='BACKUP-NAME', help='Backup name')
    ringstate_parser.set_defaults(func=medusa.fetch_ringstate.main)

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

    logging.debug(args)
    args.func(args)


if __name__ == '__main__':
    main()