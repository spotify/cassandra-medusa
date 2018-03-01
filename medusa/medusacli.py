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
import json
import pathlib


def backup(args, config):
    pass


def restore(args, config):
    pass



def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='config.json',
                        help='Specify config file')

    subcommand_template = argparse.ArgumentParser(add_help=False)

    subparsers = parser.add_subparsers(title='command', dest='command')
    backup_parser = subparsers.add_parser('backup', help='Backup Cassandra',
                                          parents=[subcommand_template])
    backup_parser.add_argument("tag")

    restore_parser = subparsers.add_parser('restore', help='Restore Cassandra',
                                           parents=[subcommand_template])
    status_parser = subparsers.add_parser('status',
                                          help='Show status of backups',
                                          parents=[subcommand_template])
    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        parser.exit(status=1, message='Please specify command')

    print(args)


if __name__ == '__main__':
    main()