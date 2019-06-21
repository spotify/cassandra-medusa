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
import traceback
import psutil
import os
import base64
import hashlib
from libcloud.storage.providers import Provider
from medusa.cassandra_utils import Cassandra
from medusa.index import add_backup_start_to_index, add_backup_finish_to_index, set_latest_backup_in_index
from medusa.metrics.transport import MedusaTransport
from medusa.storage import Storage, format_bytes_str, ManifestObject


BLOCK_SIZE_BYTES = 65536


def generate_md5_hash(src, block_size=BLOCK_SIZE_BYTES):

    checksum = hashlib.md5()
    with open(str(src), 'rb') as f:
        # Incrementally read data and update the digest
        while True:
            read_data = f.read(block_size)
            if not read_data:
                break
            checksum.update(read_data)

    # Once we have all the data, compute checksum
    checksum = checksum.digest()
    # Convert into a bytes type that can be base64 encoded
    base64_md5 = base64.encodebytes(checksum).decode('UTF-8').strip()
    # Print the Base64 encoded CRC32C
    return base64_md5


class NodeBackupCache(object):
    NEVER_BACKED_UP = ['manifest.json']

    def __init__(self, *, node_backup, incremental_mode, storage_driver, storage_provider):
        if node_backup:
            self._node_backup_cache_is_incremental = node_backup.is_incremental
            self._backup_name = node_backup.name
            self._bucket_name = node_backup.storage.config.bucket_name
            self._data_path = node_backup.data_path
            self._cached_objects = {
                (section['keyspace'], section['columnfamily']): {
                    pathlib.Path(object['path']).name: object
                    for object in section['objects']
                }
                for section in json.loads(node_backup.manifest)
            }
            self._incremental_mode = incremental_mode
        else:
            self._node_backup_cache_is_incremental = False
            self._backup_name = None
            self._bucket_name = None
            self._cached_objects = {}
            self._incremental_mode = False
        self._replaced = 0
        self._storage_driver = storage_driver
        self._storage_provider = storage_provider

    @property
    def replaced(self):
        return self._replaced

    @property
    def backup_name(self):
        return self._backup_name

    def replace_or_remove_if_cached(self, *, keyspace, columnfamily, srcs):
        retained = list()
        skipped = list()
        for src in srcs:
            if src.name in self.NEVER_BACKED_UP:
                pass
            else:
                fqtn = (keyspace, columnfamily)
                cached_item = self._cached_objects.get(fqtn, {}).get(src.name)
                if cached_item is None or self.files_are_different(src, cached_item):
                    # We have no matching object in the cache matching the file
                    retained.append(src)
                else:
                    # File was already present in the previous backup
                    # In case the backup isn't incremental or the cache backup isn't incremental, copy from cache
                    if self._incremental_mode is False or self._node_backup_cache_is_incremental is False:
                        logging.debug("from cache : {}".format(cached_item['path']))
                        retained.append(
                            self._storage_driver.get_cache_path(
                                '{}{}'.format(
                                    self._storage_driver.get_path_prefix(self._data_path), cached_item['path'])))
                    else:
                        # in case the backup is incremental, we want to rule out files, not copy them from cache
                        logging.debug("skipped : {}".format(cached_item['path']))
                        manifest_object = ManifestObject('{}{}'.format(
                            self._storage_driver.get_path_prefix(self._data_path),
                            cached_item['path']),
                            cached_item['size'],
                            cached_item['MD5'])
                        skipped.append(manifest_object)
                    self._replaced += 1

        return (retained, skipped)

    def files_are_different(self, src, cached_item):
        return (src.stat().st_size != cached_item['size']
                or (generate_md5_hash(src) != cached_item['MD5'] and self._storage_provider != Provider.LOCAL))


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


def main(config, backup_name_arg, stagger_time, restore_verify_query, mode):

    start = datetime.datetime.now()
    backup_name = backup_name_arg or start.strftime('%Y%m%d%H')
    ffwd_client = ffwd.FFWD(transport=MedusaTransport)

    try:
        storage = Storage(config=config.storage)
        cassandra = Cassandra(config.cassandra)
        incremental_mode = False
        if mode == "incremental":
            incremental_mode = True
        node_backup = storage.get_node_backup(
            fqdn=config.storage.fqdn,
            name=backup_name,
            incremental_mode=incremental_mode)
        if node_backup.exists():
            raise IOError('Error: Backup {} already exists'.format(backup_name))

        # Make sure that priority remains to Cassandra/limiting backups resource usage
        try:
            throttle_backup()
        except Exception:
            logging.warning("Throttling backup impossible. It's probable that ionice is not available.")

        logging.info('Creating snapshot')
        logging.info('Saving tokenmap and schema')
        with cassandra.new_session() as cql_session:
            node_backup.schema = cql_session.dump_schema()
            tokenmap = cql_session.tokenmap()
            node_backup.tokenmap = json.dumps(tokenmap)
            add_backup_start_to_index(storage, node_backup)
            if incremental_mode is True:
                node_backup.incremental = mode

        if restore_verify_query:
            if os.path.isfile(restore_verify_query):
                with open(restore_verify_query) as restore_verify_query_file:
                    verify_query = json.load(restore_verify_query_file)
                    node_backup.restore_verify_query = json.dumps(verify_query)
            else:
                node_backup.restore_verify_query = restore_verify_query

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
            node_backup=storage.latest_node_backup(fqdn=config.storage.fqdn),
            incremental_mode=incremental_mode,
            storage_driver=storage.storage_driver,
            storage_provider=storage.storage_provider
        )

        with cassandra.create_snapshot() as snapshot:
            manifest = []
            num_files = backup_snapshots(storage, config, manifest, node_backup, node_backup_cache, snapshot, mode)

        logging.info('Updating backup index')
        node_backup.manifest = json.dumps(manifest)
        add_backup_finish_to_index(storage, node_backup)
        set_latest_backup_in_index(storage, node_backup)

        end = datetime.datetime.now()
        actual_backup_duration = end - actual_start

        logging.info('Backup done')
        logging.info("""- Started: {:%Y-%m-%d %H:%M:%S}
                        - Started extracting data: {:%Y-%m-%d %H:%M:%S}
                        - Finished: {:%Y-%m-%d %H:%M:%S}""".format(start, actual_start, end))
        logging.info('- Real duration: {} (excludes time waiting '
                     'for other nodes)'.format(actual_backup_duration))
        logging.info('- {} files, {}'.format(
            num_files,
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
        traceback.print_exc()
        backup_error_metric = ffwd_client.metric(key='medusa-node-backup',
                                                 what='backup-error',
                                                 backupname=backup_name)
        backup_error_metric.send(1)
        logging.error('This error happened during the backup: {}'.format(str(e)))
        sys.exit(1)


def backup_snapshots(storage, config, manifest, node_backup, node_backup_cache, snapshot, mode):
    num_files = 0
    for snapshotpath in snapshot.find_dirs():
        (srcs, already_backed_up) = node_backup_cache.replace_or_remove_if_cached(
            keyspace=snapshotpath.keyspace,
            columnfamily=snapshotpath.columnfamily,
            srcs=list(snapshotpath.path.glob('*')))
        num_files += len(srcs) + len(already_backed_up)
        dst = '{}'.format(
            node_backup.datapath(keyspace=snapshotpath.keyspace, columnfamily=snapshotpath.columnfamily)
        )

        manifestobjects = list()
        if len(srcs) > 0:
            manifestobjects = storage.storage_driver.upload_blobs(srcs, dst)

        # Reintroducing already backed up objects in the manifest in incremental
        for obj in already_backed_up:
            manifestobjects.append(obj)

        manifest.append({'keyspace': snapshotpath.keyspace,
                         'columnfamily': snapshotpath.columnfamily,
                         'objects': [{
                             'path': url_to_path(manifestobject.path, node_backup.fqdn),
                             'MD5': manifestobject.MD5,
                             'size': manifestobject.size
                         } for manifestobject in manifestobjects]})
    return num_files


def url_to_path(url, fqdn):
    # the path with store in the manifest starts with the fqdn, but we can get longer urls
    # depending on the storage provider and type of backup
    # Full backup path is : <fqdn>/<backup_name>/data/<keyspace>/<table>/...
    # Incremental backup path is : <fqdn>/data/<keyspace>/<table>/...
    url_parts = url.split('/')
    return '/'.join(url_parts[url_parts.index(fqdn):])
