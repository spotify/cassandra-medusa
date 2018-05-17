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
import json
import operator
import pathlib


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
        return Storage.NodeBackup(
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
            if blob.name.endswith('/tokenmap.json')
        )

    def list_cluster_backups(self):
        node_backups = sorted(self.list_node_backups(),
                              key=operator.attrgetter('name'))
        return (
            Storage.ClusterBackup(name, node_backups)
            for name, node_backups in itertools.groupby(node_backups,
                                                        key=operator.attrgetter('name'))
        )

    def latest_node_backup(self, *, fqdn):
        return max(filter(operator.attrgetter('finished'),
                          self.list_node_backups(fqdn=fqdn)),
                   key=operator.attrgetter('started'),
                   default=None)

    class ClusterBackup(object):
        def __init__(self, name, node_backups):
            self._name = name
            node_backups = list(node_backups)
            self._first_nodebackup = next(iter(node_backups))
            self.node_backups = {node_backup.fqdn: node_backup
                                 for node_backup in node_backups}
            self._tokenmap = None

        def __repr__(self):
            return 'ClusterBackup(name={0.name})'.format(self)

        @property
        def name(self):
            return self._name

        @property
        def started(self):
            return min(map(operator.attrgetter('started'),
                           self.node_backups.values()))

        @property
        def finished(self):
            if any(self.missing_nodes()):
                return None

            finished_timestamps = list(map(operator.attrgetter('finished'),
                                           self.node_backups.values()))
            if all(finished_timestamps):
                return max(finished_timestamps)
            else:
                return None

        @property
        def tokenmap(self):
            if self._tokenmap is None:
                self._tokenmap = json.loads(self._first_nodebackup.tokenmap)
            return self._tokenmap

        def is_complete(self):
            return (not self.missing_nodes() and
                    all(map(operator.attrgetter('finished'),
                            self.node_backups.values())))

        def missing_nodes(self):
            return set(self.tokenmap.keys()) - set(self.node_backups.keys())

        def incomplete_nodes(self):
            return [node_backup
                    for node_backup in self.node_backups.values()
                    if node_backup.finished is None]

    class NodeBackup(object):
        def __init__(self, *, storage, name, fqdn):
            self._storage = storage
            self._fqdn = fqdn
            self._name = name
            self._meta_prefix = self._storage._meta_prefix / fqdn / name
            self._data_prefix = self._storage._data_prefix / fqdn / name
            self._tokenmap_path = self._meta_prefix / 'tokenmap.json'
            self._schema_path = self._meta_prefix / 'schema.cql'
            self._manifest_path = self._meta_prefix / 'manifest.json'

        def __repr__(self):
            return 'NodeBackup(name={0.name}, fqdn={0.fqdn})'.format(self)

        def _blob(self, path):
            return self.bucket.blob(str(path))

        @property
        def name(self):
            return self._name

        @property
        def fqdn(self):
            return self._fqdn

        @property
        def data_prefix(self):
            return self._data_prefix

        @property
        def bucket(self):
            return self._storage.bucket

        @property
        def storage(self):
            return self._storage

        @property
        def tokenmap_path(self):
            return self._tokenmap_path

        @property
        def tokenmap(self):
            tokenmap_blob = self._blob(self.tokenmap_path)
            return tokenmap_blob.download_as_string().decode('utf-8')

        @tokenmap.setter
        def tokenmap(self, tokenmap):
            tokenmap_blob = self._blob(self.tokenmap_path)
            tokenmap_blob.upload_from_string(tokenmap)

        @property
        def schema_path(self):
            return self._schema_path

        @property
        def schema(self):
            schema_blob = self._blob(self.schema_path)
            return schema_blob.download_as_string().decode('utf-8')

        @schema.setter
        def schema(self, schema):
            schema_blob = self._blob(self.schema_path)
            schema_blob.upload_from_string(schema)

        @property
        def started(self):
            schema_blob = self._blob(self.schema_path)
            if not schema_blob.exists():
                return None
            schema_blob.reload()
            return schema_blob.time_created

        @property
        def finished(self):
            manifest_blob = self._blob(self.manifest_path)
            if not manifest_blob.exists():
                return None
            manifest_blob.reload()
            return manifest_blob.time_created

        @property
        def manifest_path(self):
            return self._manifest_path

        @property
        def manifest(self):
            manifest_blob = self._blob(self.manifest_path)
            return manifest_blob.download_as_string().decode('utf-8')

        @manifest.setter
        def manifest(self, manifest):
            manifest_blob = self._blob(self.manifest_path)
            manifest_blob.upload_from_string(manifest)

        def datapath(self, *, keyspace, columnfamily):
            return self.data_prefix / keyspace / columnfamily

        def exists(self):
            return self._blob(self.schema_path).exists()
