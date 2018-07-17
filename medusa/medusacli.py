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
from click import pass_context
from pprint import pprint

import medusa.backup
import medusa.config
import medusa.download
import medusa.listing
import medusa.restore_cluster
import medusa.restore_node
import medusa.status
import medusa.verify


def debug_command(args, config):
    logging.error("This command is not implemented yet")


def configure_logging(verbosity):
    loglevel = max(3 - verbosity, 0) * 10
    logging.basicConfig(level=loglevel,
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    if loglevel >= logging.DEBUG:
        # Disable debugging logging for external libraries
        for loggername in 'urllib3', 'google.auth.transport.requests':
            logging.getLogger(loggername).setLevel(logging.CRITICAL)


@click.group()
@click.option('--config', help='Specify config file', type=click.Path(exists=True), required=True)
@click.option('-v', '--verbosity', help='Verbosity', default=0)
@click.option('--bucket-name', help='Bucket name')
@click.option('--key-file', default=None, help='GCP credentials key file')
@click.option('--prefix', default=None, help='Prefix for shared storage')
@click.option('--fqdn', default=None, help='Act as another host')
@click.option('--ssh-username')
@click.option('--ssh-key-file')
@click.pass_context
def cli(ctx, **kwargs):
    args = defaultdict(lambda: None, kwargs)
    configure_logging(kwargs['verbosity'])
    ctx.obj = medusa.config.load_config(args)


@cli.command()
@click.option('--backup_name', help='Custom name for the backup')
@pass_context
def backup(ctx, backup_name):
    """
    Backup Cassandra
    """
    medusa.backup.main(ctx.obj, backup_name )


@cli.command()
@click.option('--show-all/--no-show-all', default=False, help="List all backups in the bucket")
@pass_context
def list_backups(ctx, show_all):
    """
    List backups
    """
    medusa.listing.list(ctx.obj, show_all)


@cli.command()
@click.option('--backup-name', help='Custom name for the backup')
@click.option('--download-destination', help='Download destination')
@pass_context
def download(ctx, backup_name, download_destination):
    """
    Download backup
    """
    medusa.download.download_cmd(ctx.obj, backup_name, pathlib.Path(download_destination))


@cli.command()
@click.option('--backup-name')
@click.option('--seed-target')
@click.option('--temp-dir')
@pass_context
def restore_cluster(ctx, **kwargs):
    """
    Restore Cassandra cluster
    """
    medusa.restore_cluster.orchestrate(ctx.obj, **kwargs)
    pass


@cli.command()
def restore_node():
    """
    Restore single Cassandra node
    """
    pass


@cli.command()
def status():
    """
    Show status of backups
    """
    pass


@cli.command()
def verify():
    """
    Verify the integrity of a backup
    """
    pass


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
