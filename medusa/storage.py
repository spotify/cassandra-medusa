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


import pathlib
import socket


class Storage(object):
    def __init__(self, bucket_name, client):
        self._bucket_name = bucket_name
        self._client = client
        self.bucket = client.get_bucket(bucket_name)

    def get_backup_item(self, name, fqdn=None, role=None):
        fqdn=fqdn or socket.gethostname()
        return Storage.Paths(
            parent=self,
            name=name,
            fqdn=fqdn,
            role=role or fqdn.split('-', 2)[1]
        )

    class Paths(object):
        META_PREFIX_TMPL = '{role}/meta/{fqdn}/{backup_name}'
        DATA_PREFIX_TMPL = '{role}/data/{fqdn}/{backup_name}'

        def __init__(self, *, parent, name, fqdn, role):
            self._parent = parent
            self._meta_prefix = pathlib.Path(self.META_PREFIX_TMPL.format(
                role=role,
                backup_name=name,
                fqdn=fqdn
            ))
            self._data_prefix = pathlib.Path(self.DATA_PREFIX_TMPL.format(
                role=role,
                backup_name=name,
                fqdn=fqdn
            ))

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
        def manifest(self):
            return self.bucket.blob(str(self._meta_prefix / 'manifest.json'))

        def datapath(self, *, keyspace, columnspace):
            return self.data_prefix / keyspace / columnspace

        def exists(self):
            return self.schema.exists()