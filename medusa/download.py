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

import logging
import json
import sys

from medusa.storage import Storage


def download_data(storageconfig, backup, destination):
    storage = Storage(config=storageconfig)
    manifest = json.loads(backup.manifest)

    for section in manifest:
        dst = destination / section['keyspace'] / section['columnfamily']
        srcs = ['{}{}'.format(storage.storage_driver.get_path_prefix(backup.data_path), obj['path'])
                for obj in section['objects']]
        dst.mkdir(parents=True)
        if len(srcs) > 0:
            storage.storage_driver.download_blobs(srcs, dst)

    logging.info('Downloading the data...')
    storage.storage_driver.download_blobs(
        src=['{}'.format(path)
             for path in [backup.manifest_path,
                          backup.schema_path,
                          backup.tokenmap_path]],
        dest=destination
    )


def download_cmd(config, backup_name, download_destination):
    storage = Storage(config=config.storage)

    if not download_destination.is_dir():
        logging.error('{} is not a directory'.format(download_destination))
        sys.exit(1)

    node_backup = storage.get_node_backup(fqdn=storage.config.fqdn, name=backup_name)
    if not node_backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    download_data(config.storage, node_backup, download_destination)
