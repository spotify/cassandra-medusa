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

    def get_backup_item(self, *, fqdn, name):
        return Storage.Paths(
            parent=self,
            name=name,
            fqdn=fqdn
        )

    def list_backup_items(self, *, fqdn=None):
        return (
            self.get_backup_item(fqdn=fqdn or pathlib.Path(blob.name).parts[-3],
                                 name=pathlib.Path(blob.name).parts[-2])
            for blob in self._bucket.list_blobs(prefix=str(self._meta_prefix / (fqdn or '')))
            if blob.name.endswith('/tokenmap.json')
        )

    class Paths(object):
        def __init__(self, *, parent, name, fqdn):
            self._parent = parent
            self._fqdn = fqdn
            self._name = name
            self._meta_prefix = self._parent._meta_prefix / fqdn / name
            self._data_prefix = self._parent._data_prefix / fqdn / name
            self._tokenmap_path = self._meta_prefix / 'tokenmap.json'
            self._schema_path = self._meta_prefix / 'schema.cql'
            self._manifest_path = self._meta_prefix / 'manifest.json'

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
            return self._parent.bucket

        @property
        def storage(self):
            return self._parent

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

        def datapath(self, *, keyspace, columnspace):
            return self.data_prefix / keyspace / columnspace

        def exists(self):
            return self._blob(self.schema_path).exists()