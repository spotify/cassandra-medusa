#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
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

import datetime
import logging
import socket
import click

from collections import defaultdict
from pathlib import Path

import medusa.backup
import medusa.config
import medusa.download
import medusa.index
import medusa.listing
import medusa.purge
import medusa.report_latest
import medusa.restore_cluster
import medusa.restore_node
import medusa.status
import medusa.verify
import medusa.fetch_tokenmap


pass_MedusaConfig = click.make_pass_decorator(medusa.config.MedusaConfig)


def configure_logging(verbosity, without_log_timestamp):
    loglevel = max(2 - verbosity, 0) * 10

    if verbosity == 0:
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
        for loggername in 'urllib3', 'google_cloud_storage.auth.transport.requests', 'paramiko':
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


@cli.command(name='backup')
@click.option('--backup-name', help='Custom name for the backup')
@click.option('--stagger', default=None, type=int, help='Check for staggering initial backups for duration seconds')
@click.option('--restore-verify-query', default=None)
@click.option('--mode', default="full", type=click.Choice(['full', 'incremental']))
@pass_MedusaConfig
def backup(medusaconfig, backup_name, stagger, restore_verify_query, mode):
    """
    Backup Cassandra
    """
    stagger_time = datetime.timedelta(seconds=stagger) if stagger else None
    medusa.backup.main(medusaconfig, backup_name, stagger_time, restore_verify_query, mode)


@cli.command(name='fetch-tokenmap')
@click.option('--backup-name', help='backup name', required=True)
@pass_MedusaConfig
def fetch_tokenmap(medusaconfig, backup_name):
    """
    Backup Cassandra
    """
    medusa.fetch_tokenmap.main(medusaconfig, backup_name)


@cli.command(name='list-backups')
@click.option('--show-all/--no-show-all', default=False, help="List all backups in the bucket")
@pass_MedusaConfig
def list_backups(medusaconfig, show_all):
    """
    List backups
    """
    medusa.listing.list_backups(medusaconfig, show_all)


@cli.command(name='download')
@click.option('--backup-name', help='Custom name for the backup', required=True)
@click.option('--download-destination', help='Download destination', required=True)
@pass_MedusaConfig
def download(medusaconfig, backup_name, download_destination):
    """
    Download backup
    """
    medusa.download.download_cmd(medusaconfig, backup_name, Path(download_destination))


@cli.command(name='restore-cluster')
@click.option('--backup-name', help='Backup name', required=True)
@click.option('--seed-target', help='seed of the target hosts', required=False)
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@click.option('--host-list', help='List of nodes to restore with the associated target host', required=False)
@click.option('--keep-auth/--overwrite-auth', help='Keep/overwrite system_auth as found on the nodes', default=True)
@click.option('-y', '--bypass-checks', help='Bypasses the security check for restoring a cluster',
              default=False, is_flag=True)
@click.option('--verify/--no-verify', help='Verify that the cluster is operational after the restore completes,',
              default=False)
@click.option('--use-sstableloader', help='Use the sstableloader to load the backup into the cluster',
              default=False, is_flag=True)
@pass_MedusaConfig
def restore_cluster(medusaconfig, backup_name, seed_target, temp_dir, host_list, keep_auth, bypass_checks,
                    verify, use_sstableloader):
    """
    Restore Cassandra cluster
    """
    medusa.restore_cluster.orchestrate(medusaconfig,
                                       backup_name,
                                       seed_target,
                                       Path(temp_dir),
                                       host_list,
                                       keep_auth,
                                       bypass_checks,
                                       verify,
                                       use_sstableloader)


@cli.command(name='restore-node')
@click.option('--temp-dir', help='Directory for temporary storage', default="/tmp")
@click.option('--backup-name', help='Backup name', required=True)
@click.option('--in-place', help='Indicates if the restore happens on the node the backup was done on.',
              default=False, is_flag=True)
@click.option('--keep-auth', help='Keep system_auth keyspace as found on the node',
              default=False, is_flag=True)
@click.option('--seeds', help='Nodes to wait for after downloading backup but before starting C*',
              default=None)
@click.option('--verify/--no-verify', help='Verify that the cluster is operational after the restore completes,',
              default=False)
@click.option('--use-sstableloader', help='Use the sstableloader to load the backup into the cluster',
              default=False, is_flag=True)
@pass_MedusaConfig
def restore_node(medusaconfig, temp_dir, backup_name, in_place, keep_auth, seeds, verify, use_sstableloader):
    """
    Restore single Cassandra node
    """
    medusa.restore_node.restore_node(medusaconfig, Path(temp_dir), backup_name, in_place, keep_auth, seeds,
                                     verify, use_sstableloader)


@cli.command(name='status')
@click.option('--backup-name', help='Backup name', required=True)
@pass_MedusaConfig
def status(medusaconfig, backup_name):
    """
    Show status of backups
    """
    medusa.status.status(medusaconfig, backup_name)


@cli.command(name='verify')
@click.option('--backup-name', help='Backup name', required=True)
@pass_MedusaConfig
def verify(medusaconfig, backup_name):
    """
    Verify the integrity of a backup
    """
    medusa.verify.verify(medusaconfig, backup_name)


@cli.command(name='report-last-backup')
@click.option('--push-metrics', default=False, is_flag=True, help='Also push the information via metrics')
@pass_MedusaConfig
def report_last_backup(medusa_config, push_metrics):
    """
    Find time since last backup and print it to stdout
    :return:
    """
    medusa.report_latest.report_latest(medusa_config, push_metrics)


@cli.command(name='get-last-complete-cluster-backup')
@pass_MedusaConfig
def get_last_complete_cluster_backup(medusa_config):
    """
    Pints the name of the latest complete cluster backup
    """
    backup = medusa.report_latest.get_latest_complete_cluster_backup(medusa_config)
    print(backup.name)


@cli.command(name='build-index')
@click.option('--noop', default=False, is_flag=True, help='Compute and print the index only. Do not upload')
@pass_MedusaConfig
def build_index(medusa_config, noop):
    """
    Builds indices for all present backups and prints them in logs. Might upload to buckets if asked to.
    """
    medusa.index.build_indices(medusa_config, noop)


@cli.command(name='purge')
@pass_MedusaConfig
def purge(medusaconfig):
    """
    Delete obsolete backups
    """
    medusa.purge.main(medusaconfig,
                      max_backup_age=int(medusaconfig.storage.max_backup_age),
                      max_backup_count=int(medusaconfig.storage.max_backup_count))
