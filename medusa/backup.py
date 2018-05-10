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


import datetime
import json
import logging
import sys
from medusa.cassandra import Cassandra
from medusa.gsutil import GSUtil
from medusa.storage import Storage


def url_to_path(url):
    return url.split('/', 3)[-1]


def main(args, config):
    start = datetime.datetime.now()

    logging.info('Starting backup')
    backup_name = args.backup_name or start.strftime('%Y%m%d%H')

    storage = Storage(config=config.storage)
    # TODO: Test permission

    backup_paths = storage.get_backup_item(fqdn=args.fqdn, name=backup_name)
    if backup_paths.exists():
        logging.error('Error: Backup {} already exists'.format(backup_name))
        sys.exit(1)

    cassandra = Cassandra(config.cassandra)

    logging.info('Creating snapshot')
    snapshot = cassandra.create_snapshot()

    logging.info('Saving tokenmap and schema')
    backup_paths.schema = cassandra.dump_schema()
    backup_paths.tokenmap = json.dumps(cassandra.tokenmap())

    manifest = []
    with GSUtil(config.storage) as gsutil:
        for snapshotpath in snapshot.find_dirs():
            manifestobjects = gsutil.cp(
                srcs=snapshotpath.path,
                dst='gs://{}/{}'.format(
                    config.storage.bucket_name,
                    backup_paths.datapath(keyspace=snapshotpath.keyspace,
                                          columnspace=snapshotpath.columnfamily)))
            manifest.append({'keyspace': snapshotpath.keyspace,
                             'columnfamily': snapshotpath.columnfamily,
                             'objects': [{
                                 'path': url_to_path(manifestobject.path),
                                 'MD5': manifestobject.MD5,
                                 'size': manifestobject.size
                             } for manifestobject in manifestobjects]})

    backup_paths.manifest = json.dumps(manifest)

    logging.info('Backup done')
    end = datetime.datetime.now()

    logging.info('Cleaning up snapshot')
    snapshot.delete()