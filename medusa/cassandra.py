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


import subprocess
import pathlib
import socket
import yaml


RESERVED_KEYSPACES = ['system', 'system_distributed', 'system_auth', 'system_traces']
SNAPSHOT_PATTERN = '*/*/snapshots/{}'


class Cassandra(object):
    DEFAULT_CASSANDRA_CONFIG = '/etc/cassandra/cassandra.yaml'

    def __init__(self, cassandra_config=None):
        self._root = self.get_root(cassandra_config)

    @property
    def root(self):
        return self._root

    @classmethod
    def get_root(cls, cassandra_config=None):
        config_file = pathlib.Path(cassandra_config or
                                   cls.DEFAULT_CASSANDRA_CONFIG)
        if not config_file.is_file():
            raise RuntimeError('{} is not a file'.format(config_file))
        config = yaml.load(config_file.open())
        data_file_directories = config.get('data_file_directories')
        if not data_file_directories:
            raise RuntimeError('data_file_directories must be properly configured')
        if len(data_file_directories) > 1:
            raise RuntimeError('Medusa only supports one data directory')
        return pathlib.Path(data_file_directories[0])

    class Snapshot(object):
        def __init__(self, parent, tag):
            self._parent = parent
            self._tag = tag

        @property
        def cassandra(self):
            return self._parent

        @property
        def tag(self):
            return self._tag

        @property
        def root(self):
            return self._parent.root

        def find_dirs(self):
            return [
                snapshot_dir
                for snapshot_dir in self.root.glob(
                    SNAPSHOT_PATTERN.format(self._tag)
                )
                if snapshot_dir.is_dir() and
                   snapshot_dir.parts[-4] not in RESERVED_KEYSPACES
            ]

        def delete(self):
            self._parent.delete_snapshot(self._tag)

        def __repr__(self):
            return '{}<{}>'.format(self.__class__.__qualname__, self._tag)

    def create_snapshot(self, tag):
        cmd = ['nodetool', 'snapshot', '-t', tag]
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                              universal_newlines=True)
        return Cassandra.Snapshot(self, tag)

    def delete_snapshot(self, tag):
        cmd = ['nodetool', 'clearsnapshot', '-t', tag]
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                              universal_newlines=True)

    def list_snapshotnames(self):
        return {
            snapshot.name
            for snapshot in self._root.glob('*/*/snapshots/*')
            if snapshot.is_dir()
        }

    def get_snapshot(self, tag):
        if any(self._root.glob(SNAPSHOT_PATTERN.format(tag))):
            return Cassandra.Snapshot(self, tag)

        raise KeyError('Snapshot {} does not exist'.format(tag))

    def snapshot_exists(self, tag):
        for snapshot in self._root.glob('*/*/snapshots/*'):
            if snapshot.is_dir() and snapshot.name == tag:
                return True
        return False


    def ringstate(self):
        cmd = ['spjmxproxy', 'ringstate']
        return subprocess.check_output(cmd, universal_newlines=True)


    def dump_schema(self):
        cmd = ['cqlsh', socket.gethostname(), '-e', 'DESCRIBE SCHEMA']
        return subprocess.check_output(cmd, universal_newlines=True)