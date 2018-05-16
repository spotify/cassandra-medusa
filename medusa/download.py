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


import logging
import json
import sys

from medusa.storage import Storage
from medusa.gsutil import GSUtil


def download_data(storageconfig, backup, destination):
    manifest = json.loads(backup.manifest)

    with GSUtil(storageconfig) as gsutil:
        for section in manifest:
            dst = destination / section['keyspace'] / section['columnfamily']
            srcs = ['gs://{}/{}'.format(storageconfig.bucket_name, object['path'])
                    for object in section['objects']]
            dst.mkdir(parents=True)
            gsutil.cp(srcs=srcs, dst=dst)

        gsutil.cp(
            srcs=['gs://{}/{}'.format(storageconfig.bucket_name, path)
                  for path in [backup.manifest_path,
                               backup.schema_path,
                               backup.tokenmap_path]],
            dst=destination
        )


def download_cmd(args, config):
    storage = Storage(config=config.storage)

    if not args.destination.is_dir():
        logging.error('{} is not a directory'.format(args.destination))
        sys.exit(1)

    node_backup = storage.get_node_backup(fqdn=args.fqdn, name=args.backup_name)
    if not node_backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    download_data(config.storage, node_backup, args.destination)
