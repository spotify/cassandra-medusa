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


import base64
import datetime
import hashlib
import json
import logging
import pathlib
import sys
from medusa.cassandra import Cassandra
from medusa.gsutil import GSUtil
from medusa.storage import Storage


def url_to_path(url):
    return url.split('/', 3)[-1]


class NodeBackupCache(object):
    DEFAULT_BLOCK_SIZE = 16 * 1024 * 1024

    def __init__(self, *, node_backup, block_size=DEFAULT_BLOCK_SIZE, skip_md5=False):
        self._bucket_name = node_backup.storage.config.bucket_name
        self._block_size = block_size
        self._skip_md5 = skip_md5
        self._cached_objects = {} if node_backup is None else {
            (section['keyspace'], section['columnfamily']): {
                pathlib.Path(object['path']).name: object
                for object in section['objects']
            }
            for section in json.loads(node_backup.manifest)
        }

    def replace_if_cached(self, *, keyspace, columnfamily, src):
        fqtn = (keyspace, columnfamily)
        cached_item = self._cached_objects.get(fqtn, {}).get(src.name)
        if cached_item is None:
            return src

        if src.stat().st_size != cached_item['size']:
            return src

        if self._skip_md5 or self._calc_md5(src) == cached_item['MD5']:
            logging.debug('[cache] Replacing {} with {}'.format(src, cached_item['path']))
            return 'gs://{}/{}'.format(self._bucket_name, cached_item['path'])
        else:
            return src

    def _calc_md5(self, path):
        md5 = hashlib.md5()
        with path.open('rb') as f:
            md5.update(f.read(self._block_size))
        return base64.b64encode(md5.digest()).decode()


def main(args, config):
    start = datetime.datetime.now()

    logging.info('Starting backup')
    backup_name = args.backup_name or start.strftime('%Y%m%d%H')

    storage = Storage(config=config.storage)
    # TODO: Test permission

    node_backup_cache = NodeBackupCache(
        node_backup=storage.latest_backup(fqdn=args.fqdn)
    )

    node_backup = storage.get_node_backup(fqdn=args.fqdn, name=backup_name)
    if node_backup.exists():
        logging.error('Error: Backup {} already exists'.format(backup_name))
        sys.exit(1)

    cassandra = Cassandra(config.cassandra)

    logging.info('Creating snapshot')
    with cassandra.create_snapshot() as snapshot:
        logging.info('Saving tokenmap and schema')
        with cassandra.new_session() as cql_session:
            node_backup.schema = cql_session.dump_schema()
            node_backup.tokenmap = json.dumps(cql_session.tokenmap())

        manifest = []
        with GSUtil(config.storage) as gsutil:
            for snapshotpath in snapshot.find_dirs():
                srcs = [
                    node_backup_cache.replace_if_cached(
                        keyspace=snapshotpath.keyspace,
                        columnfamily=snapshotpath.columnfamily,
                        src=src
                    )
                    for src in snapshotpath.path.glob('*')
                ]

                dst = 'gs://{}/{}'.format(
                    config.storage.bucket_name,
                    node_backup.datapath(keyspace=snapshotpath.keyspace,
                                          columnfamily=snapshotpath.columnfamily)
                )

                manifestobjects = gsutil.cp(srcs=srcs, dst=dst)
                manifest.append({'keyspace': snapshotpath.keyspace,
                                 'columnfamily': snapshotpath.columnfamily,
                                 'objects': [{
                                     'path': url_to_path(manifestobject.path),
                                     'MD5': manifestobject.MD5,
                                     'size': manifestobject.size
                                 } for manifestobject in manifestobjects]})

        node_backup.manifest = json.dumps(manifest)

        logging.info('Backup done')
        end = datetime.datetime.now()
