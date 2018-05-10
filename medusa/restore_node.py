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

from medusa.cassandra import Cassandra
from medusa.download import download_data
from medusa.storage import Storage


def restore_node(args, config):
    storage = Storage(config=config.storage)

    backup = storage.get_backup_item(fqdn=args.fqdn, name=args.backup_name)
    if not backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    cassandra = Cassandra(config.cassandra)

    logging.info('Validating token')
    tokenmap = json.loads(backup.tokenmap)
    with cassandra.new_session() as session:
        # TODO: Should be store token as string?
        current_token = session.current_token()
        backup_token = tokenmap[backup.fqdn]['token']
        if  current_token != backup_token:
            logging.error('Token mismatch: Current ({}) != Backup ({})'.format(
                current_token,
                backup_token
            ))
            sys.exit(1)

    manifest = json.loads(backup.manifest)
    schema_path_mapping = cassandra.schema_path_mapping()

    # Validate existance of column families
    logging.info('Validate existance of column families')
    for section in manifest:
        if (section['keyspace'], section['columnfamily']) not in schema_path_mapping:
            logging.error("Current schema is missing ({}.{})".format(
                section['keyspace'],
                section['columnfamily']
            ))
            sys.exit(1)

    if args.restore_from:
        if not args.restore_from.is_dir():
            logging.error('{} is not a directory'.format(args.restore_from))
            sys.exit(1)
        download_dir = args.restore_from
        logging.info('Restoring data from {}'.format(download_dir))
    else:
        download_dir = args.temp_dir / 'medusa-restore-{}'.format(uuid.uuid4())
        logging.info('Downloading data from backup to {}'.format(download_dir))
        download_data(config.storage, backup, destination=download_dir)

    logging.info('Stopping Cassandra')
    cassandra.shutdown()

    # Move backup data to Cassandra data directory according to system table
    logging.info('Moving backup data to Cassandra data directory')
    file_ownership = '{}:{}'.format(cassandra.root.owner(),
                                    cassandra.root.group())
    for section in manifest:
        src = download_dir / section['keyspace'] / section['columnfamily']
        dst = schema_path_mapping[(section['keyspace'], section['columnfamily'])]
        if dst.exists():
            subprocess.check_output(['sudo', '-u', cassandra.root.owner(),
                                     'rm', '-rf', str(dst)])
        subprocess.check_output(['sudo', 'mv', str(src), str(dst)])
        subprocess.check_output(['sudo', 'chown', '-R', file_ownership, str(dst)])

    # Start up Cassandra
    logging.info('Starting Cassandra')
    cassandra.start()