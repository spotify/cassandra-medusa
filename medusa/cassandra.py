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
import logging
import pathlib
import socket
import subprocess
import uuid
import yaml

from cassandra.cluster import Cluster

SnapshotPath = collections.namedtuple('SnapshotPath',
                                      ['path', 'keyspace', 'columnfamily'])


class Cassandra(object):
    RESERVED_KEYSPACES = ['system', 'system_distributed', 'system_auth', 'system_traces']
    SNAPSHOT_PATTERN = '*/*/snapshots/{}'
    DEFAULT_CASSANDRA_CONFIG = '/etc/cassandra/cassandra.yaml'

    def __init__(self, cassandra_config=None, use_localhost=False):
        self._root = self.get_root(cassandra_config)
        self._hostname = socket.gethostname() if use_localhost else 'localhost'

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
                SnapshotPath(
                    snapshot_dir,
                    *snapshot_dir.relative_to(self.root).parts[:2]
                )
                for snapshot_dir in self.root.glob(
                    Cassandra.SNAPSHOT_PATTERN.format(self._tag)
                )
                if snapshot_dir.is_dir() and
                   snapshot_dir.parts[-4] not in Cassandra.RESERVED_KEYSPACES
            ]

        def delete(self):
            self._parent.delete_snapshot(self._tag)

        def __repr__(self):
            return '{}<{}>'.format(self.__class__.__qualname__, self._tag)

    def create_snapshot(self):
        tag = 'medusa-{}'.format(uuid.uuid4())
        cmd = ['nodetool', 'snapshot', '-t', tag]
        logging.debug(' '.join(cmd))
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                              universal_newlines=True)
        return Cassandra.Snapshot(self, tag)

    def delete_snapshot(self, tag):
        cmd = ['nodetool', 'clearsnapshot', '-t', tag]
        logging.debug(' '.join(cmd))
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                              universal_newlines=True)

    def list_snapshotnames(self):
        return {
            snapshot.name
            for snapshot in self.root.glob(self.SNAPSHOT_PATTERN.format('*'))
            if snapshot.is_dir()
        }

    def get_snapshot(self, tag):
        if any(self.root.glob(self.SNAPSHOT_PATTERN.format(tag))):
            return Cassandra.Snapshot(self, tag)

        raise KeyError('Snapshot {} does not exist'.format(tag))

    def snapshot_exists(self, tag):
        for snapshot in self.root.glob(self.SNAPSHOT_PATTERN.format('*')):
            if snapshot.is_dir() and snapshot.name == tag:
                return True
        return False


    def ringstate(self):
        cmd = ['spjmxproxy', 'ringstate']
        logging.debug(' '.join(cmd))
        return subprocess.check_output(cmd, universal_newlines=True)


    def dump_schema(self):
        cmd = ['cqlsh', self._hostname, '-e', 'DESCRIBE SCHEMA']
        logging.debug(' '.join(cmd))
        return subprocess.check_output(cmd, universal_newlines=True)

    def _columnfamily_path(self, keyspace_name, columnfamily_name, cf_id):
        root = pathlib.Path(self._root)
        keyspace_path = root / keyspace_name / columnfamily_name
        if keyspace_path.exists() and keyspace_path.is_dir():
            return keyspace_path
        else:
            # Notice: Cassandra use dashes in the cf_id in the system table,
            # but not in the directory names
            directory_postfix = str(cf_id).replace('-', '')
            return keyspace_path.with_name('{}-{}'.format(
                columnfamily_name,
                directory_postfix
            ))

    def schema_path_mapping(self):
        cluster = Cluster([self._hostname])
        session = cluster.connect('system')
        query = 'SELECT keyspace_name, columnfamily_name, cf_id FROM system.schema_columnfamilies'

        return {
            (row.keyspace_name, row.columnfamily_name):
                self._columnfamily_path(row.keyspace_name,
                                        row.columnfamily_name,
                                        row.cf_id)
            for row in session.execute(query)
            if row.keyspace_name not in self.RESERVED_KEYSPACES
        }
