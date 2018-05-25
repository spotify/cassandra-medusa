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


import google.cloud.storage
import itertools
import operator
import pathlib

from medusa.storage.cluster_backup import ClusterBackup
from medusa.storage.node_backup import NodeBackup


def format_bytes_str(value):
    for unit_shift, unit in enumerate(['B', 'KB', 'MB', 'GB', 'TB']):
        if value >> (unit_shift * 10) < 1024:
            break
    return '{:.2f} {}'.format(value / (1<<(unit_shift * 10)), unit)


class Storage(object):
    def __init__(self, *, config, client=None):
        self._config = config
        self._client = client or google.cloud.storage.Client.from_service_account_json(config.key_file)
        self._bucket = self._client.get_bucket(config.bucket_name)
        self._prefix = pathlib.Path(config.prefix or '.')
        self._meta_prefix = self._prefix / 'meta'
        self._data_prefix = self._prefix / 'data'

    @property
    def config(self):
        return self._config

    @property
    def bucket(self):
        return self._bucket

    def get_node_backup(self, *, fqdn, name):
        return NodeBackup(
            storage=self,
            name=name,
            fqdn=fqdn
        )

    def list_node_backups(self, *, fqdn=None):
        prefix = self._meta_prefix / (fqdn or '')
        return (
            self.get_node_backup(fqdn=fqdn or pathlib.Path(blob.name).parts[-3],
                                 name=pathlib.Path(blob.name).parts[-2])
            for blob in self._bucket.list_blobs(prefix=str(prefix))
            if blob.name.endswith('/schema.cql')
        )

    def list_cluster_backups(self):
        node_backups = sorted(self.list_node_backups(),
                              key=operator.attrgetter('started'))
        return (
            ClusterBackup(name, node_backups)
            for name, node_backups in itertools.groupby(node_backups,
                                                        key=operator.attrgetter('name'))
        )

    def latest_node_backup(self, *, fqdn):
        return max(filter(operator.attrgetter('finished'),
                          self.list_node_backups(fqdn=fqdn)),
                   key=operator.attrgetter('started'),
                   default=None)

    def get_cluster_backup(self, backup_name):
        for cluster_backup in self.list_cluster_backups():
            if cluster_backup.name == backup_name:
                return cluster_backup
        raise KeyError('No such backup')
