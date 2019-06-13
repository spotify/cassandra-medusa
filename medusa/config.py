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
import configparser
import logging
import os
import pathlib
import sys
import medusa.storage
import medusa.cassandra_utils

StorageConfig = collections.namedtuple('StorageConfig',
                                       ['bucket_name', 'key_file', 'prefix', 'fqdn',
                                        'host_file_separator', 'storage_provider',
                                        'api_key_or_username', 'api_secret_or_password', 'base_path'])
CassandraConfig = collections.namedtuple('CassandraConfig',
                                         ['start_cmd', 'stop_cmd',
                                          'config_file', 'cql_username', 'cql_password',
                                          'check_running', 'is_ccm'])
SSHConfig = collections.namedtuple('SSHConfig', ['username', 'key_file'])
RestoreConfig = collections.namedtuple('RestoreConfig', ['health_check'])
MedusaConfig = collections.namedtuple('MedusaConfig',
                                      ['storage', 'cassandra', 'ssh', 'restore'])

DEFAULT_CONFIGURATION_PATH = pathlib.Path('/etc/medusa/medusa.ini')


def load_config(args, config_file):
    config = configparser.ConfigParser(interpolation=None)

    # Set defaults
    config['storage'] = {
        'host_file_separator': ','
    }

    config['cassandra'] = {
        'config_file': medusa.cassandra_utils.CassandraConfigReader.DEFAULT_CASSANDRA_CONFIG,
        'start_cmd': 'sudo /etc/init.d/cassandra start',
        'stop_cmd': 'sudo spcassandra-stop',
        'check_running': 'sudo service cassandra status',
        'is_ccm': 0
    }

    config['ssh'] = {
        'username': os.environ.get('USER') or '',
        'key_file': ''
    }

    config['restore'] = {
        'health_check': 'cql'
    }

    if config_file:
        logging.debug('Loading configuration from {}'.format(config_file))
        config.read_file(config_file.open())
    elif DEFAULT_CONFIGURATION_PATH.exists():
        logging.debug('Loading configuration from {}'.format(DEFAULT_CONFIGURATION_PATH))
        config.read_file(DEFAULT_CONFIGURATION_PATH.open())
    else:
        logging.error("no configuration file provided via cli invocation and file not found in {}"
                      .format(DEFAULT_CONFIGURATION_PATH))
        sys.exit(1)

    config.read_dict({'storage': {
        key: value
        for key, value in zip(StorageConfig._fields,
                              (args['bucket_name'], args['key_file'],
                               args['prefix'], args['fqdn'],
                               args['host_file_separator'], args['storage_provider'],
                               args['api_key_or_username'], args['api_secret_or_password'], args['base_path']))
        if value is not None
    }})

    config.read_dict({'ssh': {
        key: value
        for key, value in zip(SSHConfig._fields,
                              (args['ssh_username'], args['ssh_key_file']))
        if value is not None
    }})

    config.read_dict({'restore': {
        key: value
        for key, value in zip(RestoreConfig._fields, ([args['health_check']]))
        if value is not None
    }})

    medusa_config = MedusaConfig(
        storage=_namedtuple_from_dict(StorageConfig, config['storage']),
        cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
        ssh=_namedtuple_from_dict(SSHConfig, config['ssh']),
        restore=_namedtuple_from_dict(RestoreConfig, config['restore'])
    )

    for field in ['bucket_name', 'storage_provider']:
        if getattr(medusa_config.storage, field) is None:
            logging.error('Required configuration "{}" is missing in [storage] section.'.format(field))
            sys.exit(2)

    for field in ['start_cmd', 'stop_cmd']:
        if getattr(medusa_config.cassandra, field) is None:
            logging.error('Required configuration "{}" is missing in [cassandra] section.'.format(field))
            sys.exit(2)

    for field in ['username', 'key_file']:
        if getattr(medusa_config.ssh, field) is None:
            logging.error('Required configuration "{}" is missing in [ssh] section.'.format(field))
            sys.exit(2)

    return medusa_config


def _namedtuple_from_dict(cls, data):
    return cls(**{
        field: data.get(field)
        for field in cls._fields
    })
