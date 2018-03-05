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
import socket
import sys
from google.cloud import storage
from medusa.cassandra import Cassandra
from medusa.gsutil import GSUtil


META_PREFIX_TMPL = '{role}/meta/{backup_name}/{hostname}'
DATA_PREFIX_TMPL = '{role}/data/{backup_name}/{hostname}'

# Hardcoded values (must be refactored later)
BUCKET_NAME = "parmus-medusa-test"
GCP_KEY = "medusa-test.json"


def get_hostname_and_role():
    hostname = socket.gethostname().split('.' ,1)[0]
    role = hostname.split('-', 2)[1]
    return (hostname, role)


def main(args):
    start = datetime.datetime.now()

    logging.info('Starting backup')
    backup_name = args.backup_name or start.strftime('%Y%m%d%H')

    hostname, role = get_hostname_and_role()

    client = storage.Client.from_service_account_json(GCP_KEY)
    bucket = client.get_bucket(BUCKET_NAME)

    # TODO: Test permission

    meta_prefix = META_PREFIX_TMPL.format(role=role,
                                          backup_name=backup_name,
                                          hostname=hostname)
    data_prefix = DATA_PREFIX_TMPL.format(role=role,
                                          backup_name=backup_name,
                                          hostname=hostname)

    # TODO: Test if backup by that name already exists

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

    blob = bucket.blob('{}/ringstate.json'.format(meta_prefix))
    blob.upload_from_string(ringstate)

    blob = bucket.blob('{}/schema.cql'.format(meta_prefix))
    blob.upload_from_string(schema)

    gsutil = GSUtil(BUCKET_NAME)

    manifest = []
    for snapshotpath in snapshot.find_dirs():
        manifestobjects = gsutil.cp(
            src=snapshotpath.path,
            dst='{0}/{1.keyspace}/{1.columnfamily}'.format(data_prefix,
                                                           snapshotpath))
        manifest.append({'keyspace': snapshotpath.keyspace,
                         'columnfamily': snapshotpath.columnfamily,
                         'objects': [o._asdict() for o in manifestobjects]})
    blob = bucket.blob('{}/manifest.json'.format(meta_prefix))
    blob.upload_from_string(json.dumps(manifest))


    logging.info('Backup done')
    end = datetime.datetime.now()

    logging.info('Cleaning up snapshot')
    snapshot.delete()