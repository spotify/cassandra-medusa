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
import pathlib
import sys
from medusa.cassandra import Cassandra
from medusa.gsutil import GSUtil
from medusa.storage import Storage, format_bytes_str


def url_to_path(url):
    return url.split('/', 3)[-1]


class NodeBackupCache(object):
    NEVER_CACHED = ['manifest.json']

    def __init__(self, *, node_backup):
        if node_backup:
            self._backup_name = node_backup.name
            self._bucket_name = node_backup.storage.config.bucket_name
            self._cached_objects = {
                (section['keyspace'], section['columnfamily']): {
                    pathlib.Path(object['path']).name: object
                    for object in section['objects']
                }
                for section in json.loads(node_backup.manifest)
            }
        else:
            self._backup_name = None
            self._bucket_name = None
            self._cached_objects = {}
        self._replaced = 0

    @property
    def replaced(self):
        return self._replaced

    @property
    def backup_name(self):
        return self._backup_name

    def replace_if_cached(self, *, keyspace, columnfamily, src):
        if src.name in self.NEVER_CACHED:
            return src

        fqtn = (keyspace, columnfamily)
        cached_item = self._cached_objects.get(fqtn, {}).get(src.name)
        if cached_item is None:
            return src

        if src.stat().st_size != cached_item['size']:
            return src

        logging.debug('[cache] Replacing {} with {}'.format(src, cached_item['path']))
        self._replaced += 1
        return 'gs://{}/{}'.format(self._bucket_name, cached_item['path'])


def main(args, config):
    start = datetime.datetime.now()

    logging.info('Starting backup')
    backup_name = args.backup_name or start.strftime('%Y%m%d%H')

    storage = Storage(config=config.storage)
    # TODO: Test permission

    node_backup_cache = NodeBackupCache(
        node_backup=storage.latest_node_backup(fqdn=args.fqdn)
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
        num_files = 0
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
                num_files += len(srcs)

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
        end = datetime.datetime.now()

        logging.info('Backup done')
        logging.info('- Started: {:%Y-%m-%d %H:%M:%S}, Finished: {:%Y-%m-%d %H:%M:%S}'.format(start, end))
        logging.info('- Duration: {}'.format(end-start))
        logging.info('- {} files, {}'.format(
            node_backup.num_objects(),
            format_bytes_str(node_backup.size())
        ))
        logging.info('- {} files copied from host'.format(
            num_files - node_backup_cache.replaced
        ))
        if node_backup_cache.backup_name is not None:
            logging.info('- {} copied from previous backup ({})'.format(
                node_backup_cache.replaced,
                node_backup_cache.backup_name
            ))