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
import google.cloud.storage
from medusa.cassandra import Cassandra
from medusa.gsutil import GSUtil
from medusa.storage import Storage

# Hardcoded values (must be refactored later)
BUCKET_NAME = "parmus-medusa-test"
GCP_KEY = "medusa-test.json"


def main(args):
    start = datetime.datetime.now()

    logging.info('Starting backup')
    backup_name = args.backup_name or start.strftime('%Y%m%d%H')

    client = google.cloud.storage.Client.from_service_account_json(GCP_KEY)
    storage = Storage(BUCKET_NAME, client)
    # TODO: Test permission

    backup_paths = storage.get_backup_item(backup_name)
    if backup_paths.exists():
        print('Error: Backup {} already exists'.format(backup_name))
        sys.exit(1)

    cassandra = Cassandra()

    if cassandra.snapshot_exists(backup_name):
        if args.delete_snapshot_if_exists:
            logging.info('Deleting existing snapshot')
            cassandra.delete_snapshot(backup_name)
        else:
            print('Error: Snapshot {} already exists'.format(backup_name))
            sys.exit(1)

    logging.info('Creating snapshot')
    snapshot = cassandra.create_snapshot(backup_name)

    logging.info('Saving ringstate and schema')
    ringstate = cassandra.ringstate()
    schema = cassandra.dump_schema()

    backup_paths.schema.upload_from_string(schema)

    gsutil = GSUtil(BUCKET_NAME)

    manifest = []
    for snapshotpath in snapshot.find_dirs():
        manifestobjects = gsutil.cp(
            src=snapshotpath.path,
            dst=str(backup_paths.datapath(keyspace=snapshotpath.keyspace,
                                          columnspace=snapshotpath.columnfamily)))
        manifest.append({'keyspace': snapshotpath.keyspace,
                         'columnfamily': snapshotpath.columnfamily,
                         'objects': [o._asdict() for o in manifestobjects]})
    backup_paths.manifest.upload_from_string(json.dumps(manifest))

    backup_paths.ringstate.upload_from_string(ringstate)

    logging.info('Backup done')
    end = datetime.datetime.now()

    logging.info('Cleaning up snapshot')
    snapshot.delete()