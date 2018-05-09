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
import pathlib
import sys
import medusa.storage
import medusa.cassandra

StorageConfig = collections.namedtuple('StorageConfig',
                                       ['bucket_name', 'key_file', 'prefix'])
CassandraConfig = collections.namedtuple('CassandraConfig',
                                         ['start_cmd', 'stop_cmd',
                                          'config_file',
                                          'cql_username', 'cql_password'])
MedusaConfig = collections.namedtuple('MedusaConfig', ['storage', 'cassandra'])

DEFAULT_CONFIGURATION_PATH = pathlib.Path('/etc/medusa/medusa.ini')


def load_config(args):
    config = configparser.ConfigParser(interpolation=None)

    # Set defaults
    config['storage'] = {}
    config['cassandra'] = {
        'config_file': medusa.cassandra.CassandraConfigReader.DEFAULT_CASSANDRA_CONFIG,
        'start_cmd': 'sudo spcassandra-enable-hecuba',
        'stop_cmd': 'sudo spcassandra-stop'
    }

    if args.config:
        if not args.config.exists():
            logging.error('Configuration file {} does not exist'.format(args.config))
            sys.exit(2)

        logging.debug('Loading configuration from {}'.format(args.config))
        config.read_file(args.config.open())
    elif DEFAULT_CONFIGURATION_PATH.exists():
        logging.debug('Loading configuration from {}'.format(DEFAULT_CONFIGURATION_PATH))
        config.read_file(DEFAULT_CONFIGURATION_PATH.open())

    config.read_dict({'storage': {
        key: value
        for key, value in zip(medusa.storage.StorageConfig._fields,
                              (args.bucket_name, args.key_file, args.prefix))
        if value is not None
    }})

    medusa_config = MedusaConfig(
        storage=StorageConfig(**{
            field: config['storage'].get(field)
            for field in StorageConfig._fields
        }),
        cassandra=CassandraConfig(**{
            field: config['cassandra'].get(field)
            for field in CassandraConfig._fields
        })
    )

    for field in ['bucket_name', 'key_file']:
        if getattr(medusa_config.storage, field) is None:
            logging.error('Required configuration "{}" is missing.'.format(field))
            sys.exit(2)

    for field in ['start_cmd', 'stop_cmd']:
        if getattr(medusa_config.cassandra, field) is None:
            logging.error('Required configuration "{}" is missing.'.format(field))
            sys.exit(2)

    return medusa_config