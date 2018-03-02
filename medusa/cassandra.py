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
import pathlib


RESERVED_KEYSPACES = ['system', 'system_distributed', 'system_auth', 'system_traces']
SNAPSHOT_PATTERN = '*/data/*/*/snapshots/{}'


class Cassandra(object):
    DEFAULT_CASSANDRA_ROOT = '/spotify/cassandra'

    def __init__(self, root=None):
        self._root = pathlib.Path(root or self.DEFAULT_CASSANDRA_ROOT)

    def create_snapshot(self, tag):
        cmd = ['nodetool', 'snapshot', '-t', tag]
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                              universal_newlines=True)

    def delete_snapshot(self, tag):
        cmd = ['nodetool', 'clearsnapshot', '-t', tag]
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL,
                              universal_newlines=True)

    def find_snapshotdirs(self, tag):
        cassandra_root = pathlib.Path(self._root)
        return [
            snapshot_dir
            for snapshot_dir in cassandra_root.glob(
                SNAPSHOT_PATTERN.format(tag)
            )
            if snapshot_dir.is_dir() and
               snapshot_dir.parts[-4] not in RESERVED_KEYSPACES
        ]

    def listsnapshots(self):
        cmd = ['nodetool', 'listsnapshots']
        data = subprocess.check_output(cmd, universal_newlines=True)
        return {line.strip().split(maxsplit=1)[0]
                for line in data.splitlines()[2:-2]
                if line}


def ringstate():
    cmd = ['spjmxproxy', 'ringstate']
    return subprocess.check_output(cmd, universal_newlines=True)
