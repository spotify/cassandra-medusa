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

import json
import logging
import subprocess
import sys
import uuid

from medusa.cassandra_utils import Cassandra
from medusa.download import download_data
from medusa.storage import Storage


def restore_node(config, restore_from, temp_dir, backup_name):
    storage = Storage(config=config.storage)

    node_backup = storage.get_node_backup(fqdn=config.storage.fqdn, name=backup_name)
    if not node_backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    cassandra = Cassandra(config.cassandra)

    manifest = json.loads(node_backup.manifest)
    schema_path_mapping = cassandra.schema_path_mapping()

    if restore_from:
        if not restore_from.is_dir():
            logging.error('{} is not a directory'.format(restore_from))
            sys.exit(1)
        download_dir = restore_from
        logging.info('Restoring data from {}'.format(download_dir))
    else:
        download_dir = temp_dir / 'medusa-restore-{}'.format(uuid.uuid4())
        logging.info('Downloading data from backup to {}'.format(download_dir))
        download_data(config.storage, node_backup, destination=download_dir)

    logging.info('Stopping Cassandra')
    cassandra.shutdown()

    # Clean the commitlogs, the saved cache to prevent any kind of conflict
    # especially around system tables.
    commit_logs_path = cassandra.commit_logs_path
    saved_caches_path = cassandra.saved_caches_path
    if commit_logs_path.exists():
        logging.debug('Cleaning commitlogs ({})'.format(commit_logs_path))
        subprocess.check_output(['sudo', '-u', commit_logs_path.owner(),
                                 'rm', '-rf', str(commit_logs_path)])
    if saved_caches_path.exists():
        logging.debug('Cleaning saved caches ({})'.format(saved_caches_path))
        subprocess.check_output(['sudo', '-u', saved_caches_path.owner(),
                                 'rm', '-rf', str(saved_caches_path)])

    # move backup data to Cassandra data directory according to system table
    logging.info('Moving backup data to Cassandra data directory')
    file_ownership = '{}:{}'.format(cassandra.root.owner(),
                                    cassandra.root.group())
    for section in manifest:
        src = download_dir / section['keyspace'] / section['columnfamily']
        dst = schema_path_mapping[(section['keyspace'], section['columnfamily'])]

        # restoring all tables from all backed up keyspaces except system.peers
        if dst.exists():
            logging.debug('Cleaning directory {}'.format(dst))
            subprocess.check_output(['sudo', '-u', cassandra.root.owner(),
                                     'rm', '-rf', str(dst)])
        if not (section['keyspace'] == 'system' and section['columnfamily'].startswith('peers')):
            subprocess.check_output(['sudo', 'mv', str(src), str(dst)])
            subprocess.check_output(['sudo', 'chown', '-R', file_ownership, str(dst)])

    # Start up Cassandra
    logging.info('Starting Cassandra')
    cassandra.start()
