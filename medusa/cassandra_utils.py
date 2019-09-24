# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
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
import itertools
import logging
import os
import pathlib
import shlex
import socket
import subprocess
import time
import uuid
import yaml

from subprocess import PIPE

from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider


SnapshotPath = collections.namedtuple(
    'SnapshotPath',
    ['path', 'keyspace', 'columnfamily']
)


class CqlSessionProvider(object):

    def __init__(self, ip_addresses, *, username=None, password=None):
        self._ip_addresses = ip_addresses

        auth_provider = PlainTextAuthProvider(username=username, password=password) if username and password else None
        self._auth_provider = auth_provider

        load_balancing_policy = WhiteListRoundRobinPolicy(ip_addresses)
        self._execution_profiles = {
            'local': ExecutionProfile(load_balancing_policy=load_balancing_policy)
        }

    def new_session(self, retry=False):
        """
        Creates a new CQL session. If retry is True then attempt to create a CQL session with retry logic. The max
        number of retries is currently hard coded at 5 and the delay between attempts is also hard coded at 5 sec. If
        no session can be created after the max retries is reached, an exception is raised.
         """

        cluster = Cluster(contact_points=self._ip_addresses,
                          auth_provider=self._auth_provider,
                          execution_profiles=self._execution_profiles)

        if retry:
            max_retries = 5
            attempts = 0
            delay = 5

            while attempts < max_retries:
                try:
                    session = cluster.connect()
                    return CqlSession(session)
                except Exception as e:
                    logging.debug('Failed to create session', exc_info=e)
                time.sleep(delay)
                attempts = attempts + 1
            raise Exception('Could not establish CQL session after {attempts}'.format(attempts=attempts))
        else:
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
        raise RuntimeError('Unable to get current token')

    def datacenter(self):
        logging.debug('Checking datacenter...')
        listen_address = self.cluster.contact_points[0]
        token_map = self.cluster.metadata.token_map

        for host in token_map.token_to_host_owner.values():
            socket_host = socket.gethostbyname(listen_address)
            logging.debug('Checking host {} against {}/{}'.format(host.address, listen_address, socket_host))
            if host.address == listen_address or host.address == socket_host:
                return host.datacenter

        raise RuntimeError('Unable to current datacenter')

    def tokenmap(self):
        token_map = self.cluster.metadata.token_map
        datacenter = self.datacenter()

        def get_host(host_token_pair):
            return host_token_pair[0]

        def get_host_address(host_token_pair):
            return host_token_pair[0].address

        def get_token(host_token_pair):
            return host_token_pair[1]

        host_token_pairs = sorted(
            [(host, token.value) for token, host in token_map.token_to_host_owner.items()],
            key=get_host_address
        )
        host_tokens_groups = itertools.groupby(host_token_pairs, key=get_host)
        host_tokens_pairs = [(host, list(map(get_token, tokens))) for host, tokens in host_tokens_groups]

        return {
            socket.gethostbyaddr(host.address)[0]: {
                'tokens': tokens,
                'is_up': host.is_up
            }
            for host, tokens in host_tokens_pairs
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

    def execute(self, query):
        return self.session.execute(query)


class CassandraConfigReader(object):

    DEFAULT_CASSANDRA_CONFIG = '/etc/cassandra/cassandra.yaml'

    def __init__(self, cassandra_config=None):
        config_file = pathlib.Path(cassandra_config or self.DEFAULT_CASSANDRA_CONFIG)
        if not config_file.is_file():
            raise RuntimeError('{} is not a file'.format(config_file))
        self._config = yaml.load(config_file.open(), Loader=yaml.BaseLoader)

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

    def __init__(self, cassandra_config, contact_point=None):
        self._start_cmd = shlex.split(cassandra_config.start_cmd)
        self._stop_cmd = shlex.split(cassandra_config.stop_cmd)
        self._is_ccm = int(shlex.split(cassandra_config.is_ccm)[0])
        self._os_has_systemd = self._has_systemd()
        logging.warning('is ccm : {}'.format(self._is_ccm))

        config_reader = CassandraConfigReader(cassandra_config.config_file)
        self._root = config_reader.root
        self._commitlog_path = config_reader.commitlog_directory
        self._saved_caches_path = config_reader.saved_caches_directory
        self._hostname = contact_point if contact_point is not None else config_reader.listen_address
        self._cql_session_provider = CqlSessionProvider(
            [self._hostname],
            username=cassandra_config.cql_username,
            password=cassandra_config.cql_password
        )

    def _has_systemd(self):
        try:
            result = subprocess.run(['systemctl', '--version'], stdout=PIPE, stderr=PIPE)
            logging.debug('This server has systemd: {}'.format(result.returncode == 0))
            return result.returncode == 0
        except (AttributeError, FileNotFoundError):
            # AttributeError is thrown when subprocess.run is not found, which happens on Trusty
            # Trusty doesn't have systemd, so the semantics of this code still hold
            logging.debug('This server has systemd: False')
            return False

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

        if self._is_ccm == 1:
            os.popen('ccm node1 nodetool \"snapshot -t {}\"'.format(tag)).read()
        else:
            logging.debug('Executing: {}'.format(' '.join(cmd)))
            subprocess.check_call(cmd, stdout=subprocess.DEVNULL, universal_newlines=True)

        return Cassandra.Snapshot(self, tag)

    def delete_snapshot(self, tag):
        cmd = ['nodetool', 'clearsnapshot', '-t', tag]

        if self._is_ccm == 1:
            os.popen('ccm node1 nodetool \"clearsnapshot -t {}\"'.format(tag)).read()
        else:
            logging.debug('Executing: {}'.format(' '.join(cmd)))
            subprocess.check_call(cmd, stdout=subprocess.DEVNULL, universal_newlines=True)

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

        def _full_cf_name(row):
            return self._full_columnfamily_name(row.keyspace_name, row.columnfamily_name, row.cf_id)

        def _full_cf_path(row):
            return self._columnfamily_path(row.keyspace_name, row.columnfamily_name, row.cf_id)

        with self._cql_session_provider.new_session() as session:
            return {
                (row.keyspace_name, _full_cf_name(row)): _full_cf_path(row)
                for row in session.schema_path_mapping()
            }

    def shutdown(self):
        try:
            subprocess.check_output(self._stop_cmd)
        except subprocess.CalledProcessError:
            logging.debug('Cassandra is already down on {}'.format(self._hostname))
            return

    def start_with_implicit_token(self):
        cmd = self._start_cmd
        logging.debug('Starting Cassandra with {}'.format(cmd))
        subprocess.check_output(cmd)

    def start(self, token_list):
        if self._is_ccm == 0:
            jvm_opts = '-Dcassandra.initial_token={} -Dcassandra.auto_bootstrap=false'.format(','.join(token_list))
            if self._os_has_systemd:
                tokens_env = 'sudo systemctl set-environment JVM_OPTS="{}"'.format(jvm_opts)
                cmd = '{} && {}'.format(tokens_env, ' '.join(shlex.quote(x) for x in self._start_cmd))
            else:
                tokens_env = 'sudo env JVM_OPTS="{}"'.format(jvm_opts)
                # Have to use command line as Subprocess does not handle quotes well
                # undoing 'shlex' split, back to a string in this case for '_start_cmd'
                # joining the 2 pieces of the command
                # Also, if the command to run cassandra uses sudo, we need to remove it
                # to add it as the first element
                if 'sudo' in self._start_cmd:
                    self._start_cmd.remove('sudo')
                cmd = '{} {}'.format(tokens_env, ' '.join(shlex.quote(x) for x in self._start_cmd))
            logging.debug('Starting Cassandra with {}'.format(cmd))
            # run the command using 'shell=True' option
            # to interpret the string command well
            subprocess.check_output(cmd, shell=True)
        else:
            subprocess.check_output(self._start_cmd, shell=True)


def wait_for_node_to_come_up(health_check, host, retries=10, delay=6):
    """
        Polls the node until the health check passes.

        :param health_check: The type of health check to perform, one of cql, thrift, all.
        :param host: The target host on which to run the check
        :param retries: The number of times to retry the health check. Defaults to 10
        :param delay: A delay in seconds to wait before polling again. Defaults to 6 seconds.
        :return: None when the node is determined to be up. If the retries are exhausted, an exception is raised.
        """

    logging.info('Waiting for Cassandra to come up on %s', host)

    attempts = 0
    while attempts < retries:
        if is_node_up(health_check, host):
            logging.info('Cassandra is up on %s', host)
            return None
        else:
            time.sleep(delay)
            attempts = attempts + 1

    raise CassandraNodeNotUpError(host, attempts)


def is_node_up(health_check, host):
    """
    Calls nodetool statusbinary, nodetool statusthrift or both. This function checks the output returned from nodetool
    and not the return code. There could be a normal return code of zero when the node is an unhealthy state and not
    accepting requests.

    :param health_check: Supported values are cql, thrift, and all. The latter will perform both checks. Defaults to
    cql.
    :param host: The target host on which to perform the check
    :return: True if the node is accepting requests, False otherwise. If both cql and thrift are checked, then the node
    must be ready to accept requests for both in order for the health check to be successful.
    """

    args = ['nodetool', '-h', host]

    if health_check == 'cql':
        return is_cql_up(args)
    elif health_check == 'thrift':
        return is_thrift_up(args)
    elif health_check == 'all':
        return is_cql_up(list(args)) and is_thrift_up(list(args))
    else:
        return is_cql_up(args)


def is_cql_up(args):
    try:
        args.append('statusbinary')
        output = subprocess.check_output(args, stderr=subprocess.STDOUT, universal_newlines=True)
        return output.find('running') >= 0
    except subprocess.CalledProcessError as e:
        # logging.debug('The native transport is not up yet %s', logging_suffix)
        logging.debug('The native transport is not up yet', exc_info=e)
        return False


def is_thrift_up(args):
    try:
        args.append('statusthrift')
        output = subprocess.check_output(args, stderr=subprocess.STDOUT, universal_newlines=True)
        return output.find('running') >= 0
    except subprocess.CalledProcessError:
        logging.debug('The thrift server is not up yet')
        return False


class CassandraNodeNotUpError(Exception):
    """
    Raised when it cannot be veriffied that a node is up by checking either nodetool statusbinary and/or
    nodetool statusthrift

    Attributes:
        host -- the hostname or ip address of the node
        attempts -- the number of times the check was performed
    """

    def __init(self, host, attempts):
        msg = 'Could not verify that Cassandra is up on {host} after {attempts}'.format(host=host, attempts=attempts)
        super(CassandraNodeNotUpError, self).__init__(msg)
