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
import pathlib
import socket
import medusa.backup
import medusa.config
import medusa.download
import medusa.fetch_tokenmap
import medusa.listing
import medusa.restore_cluster
import medusa.restore_node
import medusa.status


def debug_command(args, config):
    logging.error("This command is not implemented yet")


def make_parser():
    parser = argparse.ArgumentParser()

    subcommand_template = argparse.ArgumentParser(add_help=False)
    subcommand_template.add_argument('--config', type=pathlib.Path,
                                     default=None,
                                     help='Specify config file')
    subcommand_template.add_argument('-v', '--verbose', dest='loglevel',
                                     action='count',
                                     help='Increase verbosity')
    subcommand_template.add_argument('--bucket-name', type=str,
                                     default=None, help='Bucket name')
    subcommand_template.add_argument('--key-file', type=str, default=None,
                                     help='GCP credentials key file')
    subcommand_template.add_argument('--prefix', type=str, default=None,
                                     help='Prefix for shared storage')
    subcommand_template.add_argument('--fqdn', type=str, default=None,
                                     help='Act as another host')
    subcommand_template.set_defaults(func=debug_command,
                                     fqdn=socket.gethostname(),
                                     loglevel=0)

    subparsers = parser.add_subparsers(title='command', dest='command')
    backup_parser = subparsers.add_parser('backup', help='Backup Cassandra',
                                          parents=[subcommand_template])
    backup_parser.add_argument('-n', '--backup_name', type=str, default=None,
                               help='Custom name for the backup')
    backup_parser.set_defaults(func=medusa.backup.main)

    list_parser = subparsers.add_parser('list', help='List backups',
                                        parents=[subcommand_template])
    list_parser.set_defaults(func=medusa.listing.list)

    tokenmap_parser = subparsers.add_parser('fetch-tokenmap',
                                             help='Fetch tokenmap',
                                             parents=[subcommand_template])
    tokenmap_parser.add_argument('backup_name', type=str,
                                  metavar='BACKUP-NAME', help='Backup name')
    tokenmap_parser.set_defaults(func=medusa.fetch_tokenmap.main)

    download_parser = subparsers.add_parser('download', help='Download backup',
                                            parents=[subcommand_template])
    download_parser.add_argument('backup_name', type=str,
                                 metavar='BACKUP-NAME', help='Backup name')
    download_parser.add_argument('destination', type=pathlib.Path,
                                 metavar='DESTINATION',
                                 help='Download destination')
    download_parser.set_defaults(func=medusa.download.download_cmd)

    restore_cluster_parser = subparsers.add_parser('restore_cluster',
                                                   help='Restore Cassandra cluster',
                                                   parents=[subcommand_template])
    restore_cluster_parser.add_argument('backup_name', type=str,
                                        metavar='BACKUP-NAME', help='Backup name')
    restore_cluster_parser.add_argument('targets', type=str, nargs='+',
                                        metavar='HOSTS', help='List of target hosts')
    restore_cluster_parser.set_defaults(func=medusa.restore_cluster.orchestrate)

    restore_node_parser = subparsers.add_parser('restore_node',
                                                help='Restore single Cassandra node',
                                                parents=[subcommand_template])
    restore_node_parser.add_argument('--restore_from', type=pathlib.Path,
                                     metavar='PATH',
                                     help='Restore data from local directory')
    restore_node_parser.add_argument('--temp_dir', type=pathlib.Path,
                                     metavar='PATH',
                                     default=pathlib.Path('/tmp'),
                                     help='Directory for temporary storage')
    restore_node_parser.add_argument('backup_name', type=str,
                                     metavar='BACKUP-NAME', help='Backup name')
    restore_node_parser.set_defaults(func=medusa.restore_node.restore_node)

    status_parser = subparsers.add_parser('status',
                                          help='Show status of backups',
                                          parents=[subcommand_template])
    status_parser.add_argument('backup_name', type=str,
                               metavar='BACKUP-NAME', help='Backup name')
    status_parser.set_defaults(func=medusa.status.status)

    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()

    logging.basicConfig(level=max(3 - args.loglevel, 0) * 10,
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


    if args.command is None:
        parser.print_help()
        parser.exit(status=1, message='Please specify command')

    logging.debug(args)

    config = medusa.config.load_config(args)
    logging.debug(config)

    args.func(args, config)


if __name__ == '__main__':
    main()