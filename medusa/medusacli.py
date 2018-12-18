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

import datetime
import logging
import socket
import click
from pathlib import Path

import medusa.backup
import medusa.config
import medusa.download
import medusa.listing
import medusa.report_latest
import medusa.restore_cluster
import medusa.restore_node
import medusa.status
import medusa.verify
import medusa.fetch_tokenmap

pass_MedusaConfig = click.make_pass_decorator(medusa.config.MedusaConfig)


def debug_command(args, config):
    logging.error("This command is not implemented yet")


def configure_logging(verbosity, without_log_timestamp):
    loglevel = max(2 - verbosity, 0) * 10
    if (verbosity == 0):
        loglevel = logging.INFO
    if without_log_timestamp:
        log_format = '%(levelname)s: %(message)s'
    else:
        log_format = '[%(asctime)s] %(levelname)s: %(message)s'
    logging.basicConfig(level=loglevel,
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')
    if loglevel >= logging.DEBUG:
        # Disable debugging logging for external libraries
        for loggername in 'urllib3', 'google.auth.transport.requests', 'paramiko':
            logging.getLogger(loggername).setLevel(logging.CRITICAL)


@click.group()
@click.option('-v', '--verbosity', help='Verbosity', default=0, count=True)
@click.option('--without-log-timestamp', help='Do not show timestamp in logs', default=False, is_flag=True)
@click.option('--config-file', help='Specify config file')
@click.option('--bucket-name', help='Bucket name')
@click.option('--key-file', help='GCP credentials key file')
@click.option('--prefix', help='Prefix for shared storage')
@click.option('--fqdn', help='Act as another host', default=socket.gethostname())
@click.option('--ssh-username')
@click.option('--ssh-key-file')
@click.pass_context
def cli(ctx, verbosity, without_log_timestamp, config_file, **kwargs):
    config_file = Path(config_file) if config_file else None
    args = defaultdict(lambda: None, kwargs)
    configure_logging(verbosity, without_log_timestamp)
    ctx.obj = medusa.config.load_config(args, config_file)


@cli.command()
@click.option('--backup-name', help='Custom name for the backup')
@click.option('--stagger', default=None, type=int, help='Check for staggering initial backups for duration seconds')
@pass_MedusaConfig
def backup(medusaconfig, backup_name, stagger):
    """
    Backup Cassandra
    """
    stagger_time = datetime.timedelta(seconds=stagger) if stagger else None
    medusa.backup.main(medusaconfig, backup_name, stagger_time)


@cli.command()
@click.option('--backup-name', help='backup name', required=True)
@pass_MedusaConfig
def fetch_tokenmap(medusaconfig, backup_name):
    """
    Backup Cassandra
    """
    medusa.fetch_tokenmap.main(medusaconfig, backup_name)


@cli.command()
@click.option('--show-all/--no-show-all', default=False, help="List all backups in the bucket")
@pass_MedusaConfig
def list_backups(medusaconfig, show_all):
    """
    List backups
    """
    medusa.listing.list(medusaconfig, show_all)


@cli.command()
@click.option('--backup-name', help='Custom name for the backup', required=True)
@click.option('--download-destination', help='Download destination', required=True)
@pass_MedusaConfig
def download(medusaconfig, backup_name, download_destination):
    """
    Download backup
    """
    medusa.download.download_cmd(medusaconfig, backup_name, Path(download_destination))


@cli.command()
@click.option('--backup-name', help='Backup name', required=True)
@click.option('--seed-target', help='seed of the target hosts', required=False)
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@click.option('--host-list', help='List of nodes to restore with the associated target host', required=False)
@click.option('--keep-auth', help='Keep system_auth as found on the nodes', default=False, is_flag=True)
@click.option('-y', '--bypass-checks', help='Bypasses the security check for restoring a cluster',
              default=False, is_flag=True)
@pass_MedusaConfig
def restore_cluster(medusaconfig, backup_name, seed_target, temp_dir, host_list, keep_auth, bypass_checks):
    """
    Restore Cassandra cluster
    """
    medusa.restore_cluster.orchestrate(medusaconfig,
                                       backup_name,
                                       seed_target,
                                       Path(temp_dir),
                                       host_list,
                                       keep_auth,
                                       bypass_checks)


@cli.command()
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@click.option('--backup-name', help='Backup name', required=True)
@click.option('--in-place', help='Indicates if the restore happens on the node the backup was done on.',
              default=False, is_flag=True)
@click.option('--keep-auth', help='Keep system_auth keyspace as found on the node',
              default=False, is_flag=True)
@pass_MedusaConfig
def restore_node(medusaconfig, temp_dir, backup_name, in_place, keep_auth):
    """
    Restore single Cassandra node
    """
    medusa.restore_node.restore_node(medusaconfig, Path(temp_dir), backup_name, in_place, keep_auth)


@cli.command()
@click.option('--backup-name', help='Backup name', required=True)
@pass_MedusaConfig
def status(medusaconfig, backup_name):
    """
    Show status of backups
    """
    medusa.status.status(medusaconfig, backup_name)


@cli.command()
@click.option('--backup-name', help='Backup name', required=True)
@pass_MedusaConfig
def verify(medusaconfig, backup_name):
    """
    Verify the integrity of a backup
    """
    medusa.verify.verify(medusaconfig, backup_name)


@cli.command()
@click.option('--ffwd', default=False, is_flag=True, help='Report to ffwd')
@pass_MedusaConfig
def report_last_backup(medusa_config, ffwd):
    """
    Find time since last backup
    :return:
    """
    medusa.report_latest.report_latest(medusa_config, ffwd)
