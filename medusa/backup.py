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


def snapshot(tag):
    cmd = ['nodetool', 'snapshot', '-t', tag]
    cp = subprocess.run(cmd,
                        stdin=None,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        check=True,
                        universal_newlines=True)
    # TODO: Return anything?


def ringstate():
    cmd = ['spjmxproxy', 'ringstate']
    cp = subprocess.run(cmd,
                        stdin=None,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        check=True,
                        universal_newlines=True)
    return cp.stdout  # Pipe directly to file?


def backup():
    # TODO: Figure out a backup name
    backup_name = "test_backup"
    snapshot(backup_name)
    state = ringstate()
    # TODO: spjmxproxy ringstate
    # (TODO: MD5 and/or inventory)
    # TODO: Upload snapshot
    # (TODO: Upload MD5/inventory)
    # TODO: Upload state