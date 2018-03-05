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
import pathlib
import socket
import subprocess
import sys
import tempfile
from medusa.cassandra import Cassandra, ringstate


# Hardcoded values (must be refactored later)
BUCKET_NAME = "parmus-medusa-test"
GCP_KEY = "medusa-test.json"


def gsutil_cp(*, src, dst, manifest_log=None, max_retries=5):
    if manifest_log == None:
        with tempfile.NamedTemporaryFile(delete=False) as t:
            manifest_log = t.name

    cmd = ['gsutil', '-q', '-m', 'cp', '-c',
           '-L', manifest_log,
           '-r', str(src), str(dst)]

    retry = 0
    while retry < max_retries:
        if subprocess.call(cmd) == 0:
            pathlib.Path(manifest_log).unlink()
            return
        retry += 1
    raise Exception('gsutil failed: {}'.format(' '.join(cmd)))


def get_hostname_and_role():
    hostname = socket.gethostname().split('.' ,1)[0]
    role = hostname.split('-', 2)[1]
    return (hostname, role)


def main(args):
    start = datetime.datetime.now()
    backup_name = args.backup_name or start.strftime('%Y%m%d%H')

    hostname, role = get_hostname_and_role()

    cassandra = Cassandra()

    if cassandra.snapshot_exists(backup_name):
        if args.delete_snapshot_if_exists:
            cassandra.delete_snapshot(backup_name)
        else:
            print('Error: Snapshot {} already exists'.format(backup_name))
            sys.exit(1)

    snapshot = cassandra.create_snapshot(backup_name)
    state = ringstate()

    dst_format = 'gs://{bucket_name}/{role}/{backup_name}/{hostname}'
    backup_dst = dst_format.format(bucket_name=BUCKET_NAME,
                                   role=role,
                                   backup_name=backup_name,
                                   hostname=hostname)
    for snapshot in snapshot.find_dirs():
        gsutil_cp(src=snapshot,
                  dst='{}/{}/'.format(backup_dst, snapshot.relative_to(cassandra.root)))

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(state)
        ringstate_file = f.name
    gsutil_cp(src=ringstate_file, dst='{}/ringstate.json'.format(backup_dst))
    pathlib.Path(ringstate_file).unlink()

    end = datetime.datetime.now()

    snapshot.delete()