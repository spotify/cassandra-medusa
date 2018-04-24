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

from medusa.cassandra import Cassandra
from medusa.download import download_data
from medusa.storage import Storage


def restore_node(args, storageconfig):
    storage = Storage(config=storageconfig)

    backup = storage.get_backup_item(fqdn=args.fqdn, name=args.backup_name)
    if not backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    if not args.destination.is_dir():
        logging.error('{} is not a directory'.format(args.destination))
        sys.exit(1)

    # TODO: Validate token

    manifest_str = backup.manifest.download_as_string().decode('utf-8')
    manifest = json.loads(manifest_str)

    cassandra = Cassandra()
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

    logging.info('Downloading data from backup')
    download_data(storageconfig, backup, destination=args.destination)

    logging.info('Stopping Cassandra')
    cassandra.shutdown()

    # Move backup data to Cassandra data directory according to system table
    logging.info('Moving backup data to Cassandra data directory')
    for section in manifest:
        src = args.destination / section['keyspace'] / section['columnfamily']
        dst = schema_path_mapping[(section['keyspace'], section['columnfamily'])]
        if dst.exists():
            subprocess.check_output(['sudo', '-u', 'cassandra', 'rm', '-rf', str(dst)])
        subprocess.check_output(['sudo', 'mv', str(src), str(dst)])
        subprocess.check_output(['sudo', 'chown', '-R', 'cassandra:', str(dst)])

    # Start up Cassandra
    logging.info('Starting Cassandra')
    cassandra.start()