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
import shlex
import socket
import subprocess
import uuid
import yaml

from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider


class CqlSessionProvider(object):
    def __init__(self, ip_address, *, username=None, password=None):
        self._ip_address = ip_address
        self._auth_provider = (PlainTextAuthProvider(username=username,
                                                     password=password)
                               if username and password else None)
        load_balancing_policy = WhiteListRoundRobinPolicy([ip_address])
        self._execution_profiles = {'local': ExecutionProfile(
            load_balancing_policy=load_balancing_policy
        )}

    def new_session(self):
        cluster = Cluster(contact_points=[self._ip_address],
                          auth_provider=self._auth_provider,
                          execution_profiles=self._execution_profiles)
        session = cluster.connect()

        return CqlSession(session)


class CqlSession(object):
    EXCLUDED_KEYSPACES = ['system_traces']

    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def shutdown(self):
        self.session.shutdown()
        self.cluster.shutdown()

    @property
    def cluster(self):
        return self._session.cluster

    @property
    def session(self):
        return self._session

    def token(self):
        listen_address = self.cluster.contact_points[0]
        token_map = self.cluster.metadata.token_map
        for token, host in token_map.token_to_host_owner.items():
            if host.address == listen_address:
                return token.value
        raise RuntimeError('Unable to current token')

    def datacenter(self):
        logging.debug('Checking datacenter...')
        listen_address = self.cluster.contact_points[0]
        token_map = self.cluster.metadata.token_map
        for host in token_map.token_to_host_owner.values():
            logging.debug('Checking host {} against {}/{}'
                          .format(host.address, listen_address, socket.gethostbyname(listen_address)))
            if host.address == listen_address or host.address == socket.gethostbyname(listen_address):
                return host.datacenter
        raise RuntimeError('Unable to current datacenter')

    def tokenmap(self):
        token_map = self.cluster.metadata.token_map
        datacenter = self.datacenter()
        logging.debug('Token map : {}'.format(token_map))
        return {
            socket.gethostbyaddr(host.address)[0]: {
                'token': token.value,
                'is_up': host.is_up
            }
            for token, host in token_map.token_to_host_owner.items()
            if host.datacenter == datacenter
        }

    def dump_schema(self):
        keyspaces = self.session.cluster.metadata.keyspaces
        return '\n\n'.join(metadata.export_as_string()
                           for keyspace, metadata in keyspaces.items()
                           if keyspace not in self.EXCLUDED_KEYSPACES)

    def schema_path_mapping(self):
        query = 'SELECT keyspace_name, columnfamily_name, cf_id FROM system.schema_columnfamilies'

        return (row for row in self.session.execute(query)
                if row.keyspace_name not in self.EXCLUDED_KEYSPACES)


SnapshotPath = collections.namedtuple('SnapshotPath',
                                      ['path', 'keyspace', 'columnfamily'])


class CassandraConfigReader(object):
    DEFAULT_CASSANDRA_CONFIG = '/etc/cassandra/cassandra.yaml'

    def __init__(self, cassandra_config=None):
        config_file = pathlib.Path(cassandra_config or self.DEFAULT_CASSANDRA_CONFIG)
        if not config_file.is_file():
            raise RuntimeError('{} is not a file'.format(config_file))
        self._config = yaml.load(config_file.open())

    @property
    def root(self):
        data_file_directories = self._config.get('data_file_directories')
        if not data_file_directories:
            raise RuntimeError('data_file_directories must be properly configured')
        if len(data_file_directories) > 1:
            raise RuntimeError('Medusa only supports one data directory')
        return pathlib.Path(data_file_directories[0])

    @property
    def commitlog_directory(self):
        commitlog_directory = self._config.get('commitlog_directory')
        if not commitlog_directory:
            raise RuntimeError('commitlog_directory must be properly configured')
        return pathlib.Path(commitlog_directory)

    @property
    def saved_caches_directory(self):
        saved_caches_directory = self._config.get('saved_caches_directory')
        if not saved_caches_directory:
            raise RuntimeError('saved_caches_directory must be properly configured')
        return pathlib.Path(saved_caches_directory)

    @property
    def listen_address(self):
        if 'listen_address' in self._config:
            if self._config['listen_address']:
                return self._config['listen_address']
            else:
                return socket.gethostname()
        else:
            return 'localhost'


class Cassandra(object):
    SNAPSHOT_PATTERN = '*/*/snapshots/{}'

    def __init__(self, cassandra_config):
        self._start_cmd = shlex.split(cassandra_config.start_cmd)
        self._stop_cmd = shlex.split(cassandra_config.stop_cmd)

        config_reader = CassandraConfigReader(cassandra_config.config_file)
        self._root = config_reader.root
        self._commitlog_path = config_reader.commitlog_directory
        self._saved_caches_path = config_reader.saved_caches_directory
        self._hostname = config_reader.listen_address
        self._cql_session_provider = CqlSessionProvider(
            self._hostname,
            username=cassandra_config.cql_username,
            password=cassandra_config.cql_password
        )

    def new_session(self):
        return self._cql_session_provider.new_session()

    @property
    def root(self):
        return self._root

    @property
    def commit_logs_path(self):
        return self._commitlog_path

    @property
    def saved_caches_path(self):
        return self._saved_caches_path

    class Snapshot(object):
        def __init__(self, parent, tag):
            self._parent = parent
            self._tag = tag

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            logging.debug('Cleaning up snapshot')
            self.delete()

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
                    pathlib.Path(snapshot_dir),
                    *snapshot_dir.relative_to(self.root).parts[:2]
                )
                for snapshot_dir in self.root.glob(
                    Cassandra.SNAPSHOT_PATTERN.format(self._tag)
                )
                if (snapshot_dir.is_dir() and snapshot_dir.parts[-4]
                    not in CqlSession.EXCLUDED_KEYSPACES)
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

    def _full_columnfamily_name(self, keyspace_name, columnfamily_name, cf_id):
        root = pathlib.Path(self._root)
        keyspace_path = root / keyspace_name / columnfamily_name
        if keyspace_path.exists() and keyspace_path.is_dir():
            return columnfamily_name
        else:
            # Notice: Cassandra use dashes in the cf_id in the system table,
            # but not in the directory names
            directory_postfix = str(cf_id).replace('-', '')
            return '{}-{}'.format(columnfamily_name, directory_postfix)

    def schema_path_mapping(self):
        with self._cql_session_provider.new_session() as session:
            return {
                (
                    row.keyspace_name,
                    self._full_columnfamily_name(
                        row.keyspace_name,
                        row.columnfamily_name,
                        row.cf_id
                    )
                ): self._columnfamily_path(
                    row.keyspace_name,
                    row.columnfamily_name,
                    row.cf_id
                )
                for row in session.schema_path_mapping()
            }

    def shutdown(self):
        subprocess.check_output(self._stop_cmd)

    def start(self):
        subprocess.check_output(self._start_cmd)
