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
import json
import pathlib
import logging
import sys
import medusa.storage


StorageConfig = collections.namedtuple('StorageConfig',
                                       ['bucket_name', 'key_file', 'prefix'])
StorageConfig.__new__.__defaults__ = (None,)


def load_config(args):
    if args.config:
        configfile = pathlib.Path(args.config)
        if not configfile.exists():
            logging.error('Configuration file {} does not exist'.format(args.config))
            sys.exit(2)

        config = json.load(configfile.open())
        storage_config = config.get('storage', {})
    else:
        storage_config = {}

    storage_config.update({
        key: value
        for key, value in zip(medusa.storage.StorageConfig._fields,
                              (args.bucket_name, args.key_file, args.prefix))
        if value is not None
    })

    return namedtuple_from_dict(cls=medusa.storage.StorageConfig,
                                data=storage_config)


def namedtuple_from_dict(*, cls, data):
    return cls(**{k:v for k, v in data.items() if k in cls._fields})