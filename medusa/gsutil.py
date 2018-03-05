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


import pathlib
import subprocess
import tempfile


class GSUtil(object):
    def __init__(self, bucket_name):
        self._bucket_name = bucket_name

    @property
    def bucket_name(self):
        return self._bucket_name


    def cp(self, *, src, dst, manifest_log=None, max_retries=5):
        if manifest_log == None:
            with tempfile.NamedTemporaryFile(delete=False) as t:
                manifest_log = t.name

        cmd = ['gsutil', '-q', '-m', 'cp', '-c',
               '-L', manifest_log,
               '-r', str(src), 'gs://{}/{}'.format(self._bucket_name, str(dst))]

        retry = 0
        while retry < max_retries:
            if subprocess.call(cmd) == 0:
                pathlib.Path(manifest_log).unlink()
                return
            retry += 1
        raise Exception('gsutil failed: {}'.format(' '.join(cmd)))
