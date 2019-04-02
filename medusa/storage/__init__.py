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


import collections
import itertools
import logging
import operator
import pathlib

from libcloud.storage.providers import Provider

from medusa.storage.cluster_backup import ClusterBackup
from medusa.storage.node_backup import NodeBackup
from medusa.storage.google_storage import GoogleStorage
from medusa.storage.local_storage import LocalStorage

ManifestObject = collections.namedtuple('ManifestObject', ['path', 'size', 'MD5'])


def format_bytes_str(value):
    for unit_shift, unit in enumerate(['B', 'KB', 'MB', 'GB', 'TB']):
        if value >> (unit_shift * 10) < 1024:
            break
    return '{:.2f} {}'.format(value / (1 << (unit_shift * 10)), unit)


class Storage(object):
    def __init__(self, *, config):
        self._config = config
        self._prefix = pathlib.Path(config.prefix or '.')
        self.storage_driver = self._connect_storage()

    def _connect_storage(self):
        if self._config.storage_provider == Provider.GOOGLE_STORAGE:
            return GoogleStorage(self._config)
        elif self._config.storage_provider == Provider.LOCAL:
            return LocalStorage(self._config)

        raise NotImplementedError("Unsupported storage provider")

    @property
    def config(self):
        return self._config

    def get_node_backup(self, *, fqdn, name):
        return NodeBackup(
            storage=self,
            name=name,
            fqdn=fqdn
        )

    @staticmethod
    def _get_node_backup_from_blob(blob):
        blob_path = pathlib.Path(blob.name)
        fqdn, name, *_ = blob_path.parts
        return (fqdn, name)

    def discover_node_backups(self, *, fqdn=None):
        """
        Discovers nodes backups by traversing data folders.
        This operation is very taxing for cloud backends and should be avoided.
        We keep it in the codebase for the sole reason of allowing the compute-backup-indices to work.
        """

        prefix_path = fqdn if fqdn else ''
        all_blobs = sorted(self.storage_driver.list_objects(path=prefix_path), key=operator.attrgetter('name'))

        for node_backup, blobs in itertools.groupby(all_blobs, key=self._get_node_backup_from_blob):
            backup_blobs = list(blobs)
            fqdn, name = node_backup
            if any(map(lambda blob: blob.name.endswith('/schema.cql'), backup_blobs)):
                yield NodeBackup(storage=self, fqdn=fqdn, name=name, preloaded_blobs=blobs)

    def list_node_backups(self, *, fqdn=None):
        """
        Lists node backups using the index.
        If there is no backup index, no backups will be found.
        Use discover_node_backups to discover backups from the data folders.
        """

        # list all backups in the index, keep only object names
        path = 'index/backup_index'
        all_backups = list(map(lambda b: b.name, self.storage_driver.list_objects(path)))

        if len(all_backups) == 0:
            logging.info('No backups found in index. Consider running "medusa build-index" if you have some backups')

        # possibly filter out backups only for given fqdn
        if fqdn is not None:
            relevant_backups = list(filter(lambda b: fqdn in b, all_backups))
        else:
            relevant_backups = all_backups

        # use the backup names and fqdns from index entries to construct NodeBackup objects
        for backup_index_entry in relevant_backups:
            _, _, backup_name, tokenmap_file = backup_index_entry.split('/')
            # tokenmap file is in format 'tokenmap_fqdn.json'
            tokenmap_fqdn = tokenmap_file.split('_')[1].replace('.json', '')
            yield NodeBackup(storage=self, fqdn=tokenmap_fqdn, name=backup_name)

    def list_cluster_backups(self):
        node_backups = sorted(self.list_node_backups(), key=lambda b: (b.name, b.started))
        for name, node_backups in itertools.groupby(node_backups, key=operator.attrgetter('name')):
            yield ClusterBackup(name, node_backups)

    def latest_node_backup(self, *, fqdn):
        index_path = 'index/latest_backup/{}/backup_name.txt'.format(fqdn)
        try:
            latest_backup_name = self.storage_driver.get_blob_content_as_string(index_path)
            return NodeBackup(storage=self, fqdn=fqdn, name=latest_backup_name)
        except Exception:
            logging.info('Node {} does not have latest backup'.format(fqdn))
            return None

    def latest_cluster_backup(self):
        """
        Get the latest backup attempted (successful or not)
        """
        last_started = max(self.list_cluster_backups(), key=operator.attrgetter('started'), default=None)
        return last_started

    def latest_complete_cluster_backup(self):
        """
        Get the latest *complete* backup (ie successful on all nodes)
        """
        finished_backups = filter(operator.attrgetter('finished'), self.list_cluster_backups())
        last_finished = max(finished_backups, key=operator.attrgetter('finished'), default=None)
        return last_finished

    def get_cluster_backup(self, backup_name):
        for cluster_backup in self.list_cluster_backups():
            if cluster_backup.name == backup_name:
                return cluster_backup
        raise KeyError('No such backup')
