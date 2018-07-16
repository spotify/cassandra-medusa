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
from collections import defaultdict

import argparse
import logging
import pathlib
import socket
import click
import traceback
from pprint import pprint

import medusa.backup
import medusa.config
import medusa.download
import medusa.listing
import medusa.restore_cluster
import medusa.restore_node
import medusa.status
import medusa.verify


class MyConfig(object):

    def __init__(self, args, config):
        # do something with config here ...
        self.args = args
        self.config = config


def debug_command(args, config):
    logging.error("This command is not implemented yet")


pass_MyConfig = click.make_pass_decorator(MyConfig)


def cli_wrapper():
    """
    Main function that runs the cli
    """
    try:
        cli()
    except Exception:
        traceback.print_exc()


@click.group()
@click.option('--config', help='Specify config file', type=click.Path(exists=True), required=True)
@click.option('-v', '--verbosity', help='Verbosity', default=0)
@click.option('--bucket-name', help='Bucket name')
@click.option('--key-file', default=None, help='GCP credentials key file')
@click.option('--prefix', default=None, help='Prefix for shared storage')
@click.option('--fqdn', default=None, help='Act as another host')
@click.pass_context
def cli(ctx, **kwargs):
    args = defaultdict(lambda: None, kwargs)
    ctx.obj = MyConfig(args=args, config=medusa.config.load_config(args))


@click.command()
@click.option('--backup_name', help='Custom name for the backup')
@pass_MyConfig
def backup(myconfig, backup_name):
    """
    Backup Cassandra
    """
#    myconfig.args.update(kwargs)
#    medusa.backup.main(myconfig.args, myconfig.config)


@click.option('--all/--no-all', default=False)
@click.command()
@pass_MyConfig
def list(myconfig, all):
    """
    List backups
    """
    medusa.listing.list(all, myconfig.args, myconfig.config)


@click.command()
def download():
    """
    Download backup
    """
    pass


@click.command()
def restore_cluster():
    """
    Restore Cassandra cluster
    """
    pass


@click.command()
def restore_node():
    """
    Restore single Cassandra node
    """
    pass


@click.command()
def status():
    """
    Show status of backups
    """
    pass


@click.command()
def verify():
    """
    Verify the integrity of a backup
    """
    pass


cli.add_command(backup)
cli.add_command(list)
cli.add_command(download)
cli.add_command(restore_cluster)
cli.add_command(restore_node)
cli.add_command(status)
cli.add_command(verify)


def make_parser():
    parser = argparse.ArgumentParser()
    parser.set_defaults(loglevel=0)

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
                                     loglevel=0,
                                     ssh_username=None,
                                     ssh_key_file=None)

    subparsers = parser.add_subparsers(title='command', dest='command')
    backup_parser = subparsers.add_parser('backup', help='Backup Cassandra',
                                          parents=[subcommand_template])
    backup_parser.add_argument('-n', '--backup_name', type=str, default=None,
                               help='Custom name for the backup')
    backup_parser.set_defaults(func=medusa.backup.main)

    list_parser = subparsers.add_parser('list', help='List backups',
                                        parents=[subcommand_template])
    list_parser.add_argument('--all', action='store_true',
                             help='List all backups in the bucket')
    list_parser.set_defaults(func=medusa.listing.list)

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
    restore_cluster_parser.add_argument('--ssh_username', type=str,
                                        default=None, help='SSH username to use')
    restore_cluster_parser.add_argument('--ssh_key_file', type=str,
                                        default=None, help='SSH keyfile to use')
    restore_cluster_parser.add_argument('backup_name', type=str,
                                        metavar='BACKUP-NAME', help='Backup name')
    restore_cluster_parser.add_argument('seed_target', type=str,
                                        metavar='HOST', help='Seed of the target hosts')
    restore_cluster_parser.add_argument('--temp_dir', type=pathlib.Path,
                                        metavar='PATH',
                                        default=pathlib.Path('/tmp'),
                                        help='Directory for temporary storage')
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

    verify_parser = subparsers.add_parser('verify',
                                          help='Verify the integrity of a backup',
                                          parents=[subcommand_template])
    verify_parser.add_argument('backup_name', type=str,
                               metavar='BACKUP-NAME', help='Backup name')
    verify_parser.set_defaults(func=medusa.verify.verify)

    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()

    loglevel = max(3 - args.loglevel, 0) * 10
    logging.basicConfig(level=loglevel,
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    if loglevel >= logging.DEBUG:
        # Disable debugging logging for external libraries
        for loggername in 'urllib3', 'google.auth.transport.requests':
            logging.getLogger(loggername).setLevel(logging.CRITICAL)

    if args.command is None:
        parser.print_help()
        parser.exit(status=1, message='Please specify command')

    logging.debug(args)

    config = medusa.config.load_config(args)
    logging.debug(config)

    args.func(args, config)


if __name__ == '__main__':
    cli_wrapper()
