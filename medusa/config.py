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
import sys
import medusa.storage


StorageConfig = collections.namedtuple('StorageConfig',
                                       ['bucket_name', 'key_file', 'prefix'])


def load_config(args):
    config = configparser.ConfigParser(interpolation=None)

    # Set defaults
    config['storage'] = {}
    config['cassandra'] = {}

    if args.config:
        if not args.config.exists():
            logging.error('Configuration file {} does not exist'.format(args.config))
            sys.exit(2)

        logging.debug('Loading configuration from {}'.format(args.config))
        config.read_file(args.config.open())

    config.read_dict({'storage': {
        key: value
        for key, value in zip(medusa.storage.StorageConfig._fields,
                              (args.bucket_name, args.key_file, args.prefix))
        if value is not None
    }})

    storage_config = StorageConfig(**{
        field: config['storage'].get(field)
        for field in StorageConfig._fields
    })

    for field in ['bucket_name', 'key_file']:
        if getattr(storage_config, field) is None:
            logging.error('Required configuration "{}" is missing.'.format(field))
            sys.exit(2)

    return storage_config