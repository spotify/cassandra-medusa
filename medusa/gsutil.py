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


import collections
import csv
import logging
import os
import pathlib
import subprocess
import tempfile


ManifestObject = collections.namedtuple('ManifestObject', ['path', 'size', 'MD5'])


class GSUtil(object):
    def __init__(self, bucket_name, key_file):
        self._bucket_name = bucket_name
        self._key_file = key_file

    @property
    def bucket_name(self):
        return self._bucket_name

    def __enter__(self):
        self._gcloud_config = tempfile.TemporaryDirectory()
        self._env = dict(os.environ, CLOUDSDK_CONFIG=self._gcloud_config.name)
        cmd = ['gcloud', 'auth', 'activate-service-account',
               '--key-file={}'.format(self._key_file)]
        logging.info('Authenticating gcloud with {}'.format(self._key_file))
        logging.debug(self._env)
        subprocess.check_call(cmd,
                              env=self._env,
                              stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._gcloud_config.cleanup()
        self._env = dict(os.environ)
        return False

    def cp(self, *, src, dst, manifest_log=None, max_retries=5):
        if manifest_log == None:
            with tempfile.NamedTemporaryFile(delete=False) as t:
                manifest_log = t.name

        cmd = ['gsutil', '-q', '-m', 'cp', '-c',
               '-L', manifest_log,
               '-r', str(src), 'gs://{}/{}'.format(self._bucket_name, str(dst))]

        logging.debug(' '.join(cmd))

        retry = 0
        while retry < max_retries:
            if subprocess.call(cmd, env=self._env) == 0:
                with open(manifest_log) as f:
                    manifestobjects = [ManifestObject(row['Destination'],
                                                      row['Source Size'],
                                                      row['Md5'])
                                for row in csv.DictReader(f, delimiter=',')]
                pathlib.Path(manifest_log).unlink()
                return manifestobjects
            retry += 1
        raise Exception('gsutil failed: {}'.format(' '.join(cmd)))
