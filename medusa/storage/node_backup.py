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
    def __init__(self, *, storage, fqdn, name, preloaded_blobs=None):
        self._storage = storage
        self._fqdn = fqdn
        self._name = name
        if self._storage._prefix != '.':
            self._node_backup_path = self._storage._prefix / fqdn / name
        else:
            self._node_backup_path = fqdn / name
        self._meta_path = self._node_backup_path / 'meta'
        self._data_path = self._node_backup_path / 'data'
        self._tokenmap_path = self._meta_path / 'tokenmap.json'
        self._schema_path = self._meta_path / 'schema.cql'
        self._manifest_path = self._meta_path / 'manifest.json'

        if preloaded_blobs is None:
            preloaded_blobs = storage.storage_driver.list_objects(
                '{}/'.format(self._meta_path)
            )
        self._cached_blobs = {pathlib.Path(blob.name): blob
                              for blob in preloaded_blobs}
        self._cached_manifest = None

    def __repr__(self):
        return 'NodeBackup(name={0.name}, fqdn={0.fqdn}, schema_path={0.schema_path})'.format(self)

    def _blob(self, path):
        blob = self._cached_blobs.get(path)
        if blob is None:
            blob = self._storage.storage_driver.get_blob(str(path))
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
        return self._storage.storage_driver.get_blob_content_as_string(self.tokenmap_path)

    @tokenmap.setter
    def tokenmap(self, tokenmap):

        self._storage.storage_driver.upload_blob_from_string(self.tokenmap_path, tokenmap)

    @property
    def schema_path(self):
        return self._schema_path

    @property
    def schema(self):
        return self._storage.storage_driver.get_blob_content_as_string(self.schema_path)

    @schema.setter
    def schema(self, schema):
        self._storage.storage_driver.upload_blob_from_string(self.schema_path, schema)

    @property
    def started(self):
        schema_blob = self._storage.storage_driver.get_blob(self.schema_path)
        return self.storage.storage_driver.get_object_datetime(schema_blob) if schema_blob else None

    @property
    def finished(self):
        manifest_blob = self._storage.storage_driver.get_blob(self.manifest_path)
        return self.storage.storage_driver.get_object_datetime(manifest_blob) if manifest_blob else None

    @property
    def manifest_path(self):
        return self._manifest_path

    @property
    def manifest(self):
        if self._cached_manifest is None:
            self._cached_manifest = self._storage.storage_driver.get_blob_content_as_string(self.manifest_path)
        return self._cached_manifest

    @manifest.setter
    def manifest(self, manifest):
        self._cached_manifest = None
        self._storage.storage_driver.upload_blob_from_string(self.manifest_path, manifest)

    def datapath(self, *, keyspace, columnfamily):
        return self.data_path / keyspace / columnfamily

    def exists(self):
        return self._storage.storage_driver.get_blob(self.schema_path) is not None

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
