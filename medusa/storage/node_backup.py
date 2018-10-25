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
import json
import pathlib


class NodeBackup(object):
    def __init__(self, *, storage, name, fqdn, preloaded_blobs=None):
        self._storage = storage
        self._fqdn = fqdn
        self._name = name
        self._node_backup_path = self._storage._prefix / fqdn / name
        self._meta_path = self._node_backup_path / 'meta'
        self._data_path = self._node_backup_path / 'data'
        self._tokenmap_path = self._meta_path / 'tokenmap.json'
        self._schema_path = self._meta_path / 'schema.cql'
        self._manifest_path = self._meta_path / 'manifest.json'

        if preloaded_blobs is None:
            preloaded_blobs = storage.bucket.list_blobs(
                prefix='{}/'.format(self._meta_path)
            )
        self._cached_blobs = {pathlib.Path(blob.name): blob
                              for blob in preloaded_blobs}
        self._cached_manifest = None

    def __repr__(self):
        return 'NodeBackup(name={0.name}, fqdn={0.fqdn})'.format(self)

    def _blob(self, path):
        blob = self._cached_blobs.get(path)
        if blob is None:
            blob = self.bucket.blob(str(path))
            self._cached_blobs[path] = blob
        return blob

    @property
    def name(self):
        return self._name

    @property
    def fqdn(self):
        return self._fqdn

    @property
    def data_path(self):
        return self._data_path

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
        return schema_blob.time_created if schema_blob else None

    @property
    def finished(self):
        manifest_blob = self._blob(self.manifest_path)
        return manifest_blob.time_created if manifest_blob else None

    @property
    def manifest_path(self):
        return self._manifest_path

    @property
    def manifest(self):
        if self._cached_manifest is None:
            manifest_blob = self._blob(self.manifest_path)
            self._cached_manifest = manifest_blob.download_as_string().decode('utf-8')
        return self._cached_manifest

    @manifest.setter
    def manifest(self, manifest):
        self._cached_manifest = None
        manifest_blob = self._blob(self.manifest_path)
        manifest_blob.upload_from_string(manifest)

    def datapath(self, *, keyspace, columnfamily):
        return self.data_path / keyspace / columnfamily

    def exists(self):
        return self._blob(self.schema_path).exists()

    def size(self):
        return sum(
            obj['size']
            for section in json.loads(self.manifest)
            for obj in section['objects']
        )

    def num_objects(self):
        return sum(
            len(section['objects'])
            for section in json.loads(self.manifest)
        )
