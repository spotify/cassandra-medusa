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


def download(args, storageconfig):
    storage = Storage(config=storageconfig)

    if not args.destination.is_dir():
        logging.error('{} is not a directory'.format(args.destination))
        sys.exit(1)

    backup = storage.get_backup_item(fqdn=args.fqdn, name=args.backup_name)
    if not backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    manifest_str = backup.manifest.download_as_string().decode('utf-8')
    manifest = json.loads(manifest_str)

    with GSUtil(storageconfig) as gsutil:
        for section in manifest:
            dst = args.destination / section['keyspace'] / section['columnfamily']
            srcs = [object['path'] for object in section['objects']]
            dst.mkdir(parents=True)
            gsutil.cp(srcs=srcs, dst=dst)

        gsutil.cp(
            srcs=['gs://{}/{}'.format(storageconfig.bucket_name, blob.name)
                  for blob in [backup.manifest,
                               backup.schema,
                               backup.ringstate]],
            dst=args.destination
        )