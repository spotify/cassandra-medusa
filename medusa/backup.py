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


import subprocess
import tempfile
import pathlib


def nodetool_snapshot(tag):
    cmd = ['nodetool', 'snapshot', '-t', tag]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                          universal_newlines=True)


def nodetool_clearsnapshot(tag):
    cmd = ['nodetool', 'clearsnapshot', '-t', tag]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                          universal_newlines=True)


def ringstate():
    cmd = ['spjmxproxy', 'ringstate']
    return subprocess.check_output(cmd, universal_newlines=True)


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


def backup():
    # TODO: Figure out a backup name
    bucket_name = "parmus-medusa-test"
    role = "yolo"
    hostname = "gew1-yolocassandra-a-wm87"
    backup_name = "test_backup"

    nodetool_snapshot(backup_name)
    state = ringstate()

    cassandra_root = pathlib.Path('/spotify/cassandra')
    snapshot_pattern = '*/data/*/code/snapshots/{}'
    snapshots = [
        snapshot_dir
        for snapshot_dir in cassandra_root.glob(
            snapshot_pattern.format(backup_name)
        )
        if snapshot_dir.is_dir()
    ]

    dst_format = 'gs://{bucket_name}/{role}/{backup_name}/{hostname}'
    backup_dst = dst_format.format(bucket_name=bucket_name,
                                   role=role,
                                   backup_name=backup_name,
                                   hostname=hostname)
    for snapshot in snapshots:
        gsutil_cp(src=snapshot,
                  dst='{}/{}/'.format(backup_dst, snapshot.relative_to(cassandra_root)))

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(state)
        ringstate_file = f.name
    gsutil_cp(src=ringstate_file, dst='{}/ringstate.json'.format(backup_dst))
    pathlib.Path(ringstate_file).unlink()


if __name__ == '__main__':
    backup()
