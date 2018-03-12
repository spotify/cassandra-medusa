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
import google.cloud.storage
import pathlib


StorageConfig = collections.namedtuple('StorageConfig',
                                       ['bucket_name', 'key_file', 'prefix'])
StorageConfig.__new__.__defaults__ = (None,)


class Storage(object):
    def __init__(self, *, config, client=None):
        self._config = config
        self._client = client or google.cloud.storage.Client.from_service_account_json(config.key_file)
        self._bucket = self._client.get_bucket(config.bucket_name)
        self._prefix = pathlib.Path(config.prefix)
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

    def list_backup_items(self, *, fqdn):
        return (
            self.get_backup_item(fqdn=fqdn,
                                 name=pathlib.Path(blob.name).parts[-2])
            for blob in self._bucket.list_blobs(prefix=str(self._meta_prefix / fqdn))
            if blob.name.endswith('/ringstate.json')
        )

    class Paths(object):
        def __init__(self, *, parent, name, fqdn):
            self._parent = parent
            self._fqdn = fqdn
            self._name = name
            self._meta_prefix = self._parent._meta_prefix / fqdn / name
            self._data_prefix = self._parent._data_prefix / fqdn / name

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
        def ringstate(self):
            return self.bucket.blob(str(self._meta_prefix / 'ringstate.json'))

        @property
        def schema(self):
            return self.bucket.blob(str(self._meta_prefix / 'schema.cql'))

        @property
        def started(self):
            schema = self.schema
            if not schema.exists():
                return None
            schema.reload()
            return schema.time_created

        @property
        def finished(self):
            ringstate = self.ringstate
            if not ringstate.exists():
                return None
            ringstate.reload()
            return ringstate.time_created

        @property
        def manifest(self):
            return self.bucket.blob(str(self._meta_prefix / 'manifest.json'))

        def datapath(self, *, keyspace, columnspace):
            return self.data_prefix / keyspace / columnspace

        def exists(self):
            return self.schema.exists()