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

import logging
import socket
import click
from pathlib import Path

import medusa.backup
import medusa.config
import medusa.download
import medusa.listing
import medusa.restore_cluster
import medusa.restore_node
import medusa.status
import medusa.verify

pass_MedusaConfig = click.make_pass_decorator(medusa.config.MedusaConfig)


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
@click.option('-v', '--verbosity', help='Verbosity', default=0, count=True)
@click.option('--config_file', help='Specify config file')
@click.option('--bucket-name', help='Bucket name')
@click.option('--key-file', help='GCP credentials key file')
@click.option('--prefix', help='Prefix for shared storage')
@click.option('--fqdn', help='Act as another host', default=socket.gethostname())
@click.option('--ssh-username')
@click.option('--ssh-key-file')
@click.pass_context
def cli(ctx, verbosity, config_file, **kwargs):
    config_file = Path(config_file) if config_file else None
    args = defaultdict(lambda: None, kwargs)
    configure_logging(verbosity)
    ctx.obj = medusa.config.load_config(args, config_file)


@cli.command()
@click.option('--backup_name', help='Custom name for the backup')
@pass_MedusaConfig
def backup(medusaconfig, backup_name):
    """
    Backup Cassandra
    """
    medusa.backup.main(medusaconfig, backup_name)


@cli.command()
@click.option('--show-all/--no-show-all', default=False, help="List all backups in the bucket")
@pass_MedusaConfig
def list_backups(medusaconfig, show_all):
    """
    List backups
    """
    medusa.listing.list(medusaconfig, show_all)


@cli.command()
@click.option('--backup-name', help='Custom name for the backup')
@click.option('--download-destination', help='Download destination', type=click.Path(exists=True))
@pass_MedusaConfig
def download(medusaconfig, backup_name, download_destination):
    """
    Download backup
    """
    medusa.download.download_cmd(medusaconfig, backup_name, download_destination)


@cli.command()
@click.option('--backup-name', help='Backup name')
@click.option('--seed-target', help='seed of the target hosts')
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@pass_MedusaConfig
def restore_cluster(medusaconfig, backup_name, seed_target, temp_dir):
    """
    Restore Cassandra cluster
    """
    medusa.restore_cluster.orchestrate(medusaconfig, backup_name, seed_target, Path(temp_dir))


@cli.command()
@click.option('--restore-from', help='Restore data from local directory')
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@click.option('--backup-name', help='Backup name')
@pass_MedusaConfig
def restore_node(medusaconfig, restore_from, temp_dir, backup_name):
    """
    Restore single Cassandra node
    """
    restore_from = Path(restore_from) if restore_from else None
    medusa.restore_node.restore_node(medusaconfig, restore_from, Path(temp_dir), backup_name)


@cli.command()
@click.option('--backup-name', help='Backup name')
@pass_MedusaConfig
def status(medusaconfig, backup_name):
    """
    Show status of backups
    """
    medusa.status.status(medusaconfig, backup_name)


@cli.command()
@click.option('--backup-name', help='Backup name')
@pass_MedusaConfig
def verify(medusaconfig, backup_name):
    """
    Verify the integrity of a backup
    """
    medusa.verify.verify(medusaconfig, backup_name)
