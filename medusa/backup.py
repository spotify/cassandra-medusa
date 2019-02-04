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


import datetime
import ffwd
import json
import logging
import pathlib
import sys
import time
import psutil
import os

from medusa.cassandra_utils import Cassandra
from medusa.gsutil import GSUtil
from medusa.storage import Storage, format_bytes_str
from medusa.metrics.transport import MedusaTransport


def url_to_path(url):
    return url.split('/', 3)[-1]


class NodeBackupCache(object):
    NEVER_CACHED = ['manifest.json']

    def __init__(self, *, node_backup):
        if node_backup:
            self._backup_name = node_backup.name
            self._bucket_name = node_backup.storage.config.bucket_name
            self._cached_objects = {
                (section['keyspace'], section['columnfamily']): {
                    pathlib.Path(object['path']).name: object
                    for object in section['objects']
                }
                for section in json.loads(node_backup.manifest)
            }
        else:
            self._backup_name = None
            self._bucket_name = None
            self._cached_objects = {}
        self._replaced = 0

    @property
    def replaced(self):
        return self._replaced

    @property
    def backup_name(self):
        return self._backup_name

    def replace_if_cached(self, *, keyspace, columnfamily, src):
        if src.name in self.NEVER_CACHED:
            return src

        fqtn = (keyspace, columnfamily)
        cached_item = self._cached_objects.get(fqtn, {}).get(src.name)
        if cached_item is None:
            return src

        if src.stat().st_size != cached_item['size']:
            return src

        logging.debug('[cache] Replacing {} with {}'.format(src, cached_item['path']))
        self._replaced += 1
        return 'gs://{}/{}'.format(self._bucket_name, cached_item['path'])


def throttle_backup():
    """
    Makes sure to only us idle IO for backups
    """
    p = psutil.Process(os.getpid())
    p.ionice(psutil.IOPRIO_CLASS_IDLE)
    p.nice(19)
    logging.debug("Processus {} was set to use only idle IO and CPU resources".format(p))


def stagger(fqdn, storage, tokenmap):
    """
    Checks whether the previous node in the tokenmap has completed a backup.

    :param tokenmap:
    :param storage:
    :param fqdn:
    :return: True if this host has sufficiently been staggered, False otherwise.
    """
    # If we already have a backup for ourselves, bail early.
    previous_backups = storage.list_node_backups(fqdn=fqdn)
    if any(backup.finished for backup in previous_backups):
        return True

    ordered_tokenmap = sorted(tokenmap.items(), key=lambda item: item[1]['tokens'])
    index = ordered_tokenmap.index((fqdn, tokenmap[fqdn]))
    if index == 0:  # Always run if we're the first node
        return True
    previous_host = ordered_tokenmap[index - 1][0]
    previous_host_backups = storage.list_node_backups(fqdn=previous_host)
    has_backup = any(backup.finished for backup in previous_host_backups)
    if not has_backup:
        logging.info('Still waiting for {} to finish a backup.'.format(previous_host))
    return has_backup


def main(config, backup_name_arg, stagger_time):

    start = datetime.datetime.now()
    ffwd_client = ffwd.FFWD(transport=MedusaTransport)

    try:
        storage = Storage(config=config.storage)
        cassandra = Cassandra(config.cassandra)

        backup_name = backup_name_arg or start.strftime('%Y%m%d%H')
        node_backup = storage.get_node_backup(fqdn=config.storage.fqdn, name=backup_name)
        if node_backup.exists():
            raise IOError('Error: Backup {} already exists'.format(backup_name))

        # Make sure that priority remains to Cassandra/limiting backups resource usage
        throttle_backup()

        logging.info('Creating snapshot')
        logging.info('Saving tokenmap and schema')
        with cassandra.new_session() as cql_session:
            node_backup.schema = cql_session.dump_schema()
            tokenmap = cql_session.tokenmap()
            node_backup.tokenmap = json.dumps(tokenmap)

        if stagger_time:
            stagger_end = start + stagger_time
            logging.info('Staggering backup run, trying until {}'.format(stagger_end))
            while not stagger(config.storage.fqdn, storage, tokenmap):
                if datetime.datetime.now() < stagger_end:
                    logging.info('Staggering this backup run...')
                    time.sleep(60)
                else:
                    raise IOError('Backups on previous nodes did not complete'
                                  ' within our stagger time.'.format(backup_name))

        logging.info('Starting backup')
        actual_start = datetime.datetime.now()

        # Load last backup as a cache
        node_backup_cache = NodeBackupCache(
            node_backup=storage.latest_node_backup(fqdn=config.storage.fqdn)
        )

        with cassandra.create_snapshot() as snapshot:
            manifest = []
            num_files = 0
            with GSUtil(config.storage) as gsutil:
                for snapshotpath in snapshot.find_dirs():
                    srcs = [
                        node_backup_cache.replace_if_cached(
                            keyspace=snapshotpath.keyspace,
                            columnfamily=snapshotpath.columnfamily,
                            src=src
                        )
                        for src in snapshotpath.path.glob('*')
                    ]
                    num_files += len(srcs)

                    dst = 'gs://{}/{}'.format(
                        config.storage.bucket_name,
                        node_backup.datapath(keyspace=snapshotpath.keyspace, columnfamily=snapshotpath.columnfamily)
                    )
                    manifestobjects = gsutil.cp(srcs=srcs, dst=dst)
                    manifest.append({'keyspace': snapshotpath.keyspace,
                                     'columnfamily': snapshotpath.columnfamily,
                                     'objects': [{
                                         'path': url_to_path(manifestobject.path),
                                         'MD5': manifestobject.MD5,
                                         'size': manifestobject.size
                                     } for manifestobject in manifestobjects]})

        node_backup.manifest = json.dumps(manifest)
        end = datetime.datetime.now()
        actual_backup_duration = end - actual_start

        logging.info('Backup done')
        logging.info("""- Started: {:%Y-%m-%d %H:%M:%S}
                        - Started extracting data: {:%Y-%m-%d %H:%M:%S}
                        - Finished: {:%Y-%m-%d %H:%M:%S}""".format(start, actual_start, end))
        logging.info('- Real duration: {} (excludes time waiting '
                     'for other nodes)'.format(actual_backup_duration))
        logging.info('- {} files, {}'.format(
            node_backup.num_objects(),
            format_bytes_str(node_backup.size())
        ))
        logging.info('- {} files copied from host'.format(
            num_files - node_backup_cache.replaced
        ))
        if node_backup_cache.backup_name is not None:
            logging.info('- {} copied from previous backup ({})'.format(
                node_backup_cache.replaced,
                node_backup_cache.backup_name
            ))

        logging.debug('Emitting metrics')

        # Monitoring update
        backup_duration_metric = ffwd_client.metric(key='medusa-node-backup',
                                                    what='backup-duration',
                                                    backupname=backup_name)
        logging.info('actual duration: {}'.format(actual_backup_duration.seconds))
        backup_duration_metric.send(actual_backup_duration.seconds)
        backup_size_metric = ffwd_client.metric(key='medusa-node-backup',
                                                what='backup-size',
                                                backupname=backup_name)
        backup_size_metric.send(node_backup.size())
        backup_error_metric = ffwd_client.metric(key='medusa-node-backup',
                                                 what='backup-error',
                                                 backupname=backup_name)
        backup_error_metric.send(0)

        logging.debug('Done emitting metrics')

    except Exception as e:
        backup_error_metric = ffwd_client.metric(key='medusa-node-backup',
                                                 what='backup-error',
                                                 backupname=backup_name)
        backup_error_metric.send(1)
        logging.error('This error happened during the backup: {}'.format(str(e)))
        sys.exit(1)
