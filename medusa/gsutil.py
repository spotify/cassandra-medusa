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
import sys
import os
import pathlib
import subprocess
import tempfile
import time
import uuid

ManifestObject = collections.namedtuple('ManifestObject', ['path', 'size', 'MD5'])


class GSUtil(object):
    def __init__(self, config):
        self._config = config

    @property
    def bucket_name(self):
        return self._config.bucket_name

    def __enter__(self):
        self._gcloud_config = tempfile.TemporaryDirectory()
        self._env = dict(os.environ, CLOUDSDK_CONFIG=self._gcloud_config.name)
        cmd = ['gcloud', 'auth', 'activate-service-account',
               '--key-file={}'.format(self._config.key_file)]
        logging.info('Authenticating gcloud with {}'.format(self._config.key_file))
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

    def cp(self, *, srcs, dst, max_retries=5):
        if isinstance(srcs, str) or isinstance(srcs, pathlib.Path):
            srcs = [srcs]

        # TODO: Clean up these files in production
        job_id = str(uuid.uuid4())
        manifest_log = '/tmp/gsutil_{0}.manifest'.format(job_id)
        gsutil_output = '/tmp/gsutil_{0}.output'.format(job_id)

        # TODO: Enable multi-processing and expose the following settings?
        # The problem is that trickle is not compatible with multiple porcesses
        # We need another way to throttle (ideally the disk IO directly)
        # ie. Do not run 'trickle ... gsutil -m ...'
        # https://github.com/GoogleCloudPlatform/gsutil/issues/413

        # If not using trickle here, we could use following gsutil options:
        # parallel_process_count = 4
        # parallel_thread_count = 4
        # '-o', 'GSUtil:parallel_process_count={}'.format(parallel_process_count),
        # '-o', 'GSUtil:parallel_thread_count={}'.format(parallel_thread_count),
        # '-m',
        cmd = ['trickle', '-u', str(self._config.upload_throttle_in_KBps),
               'gsutil',
               'cp', '-c',
               '-L', manifest_log, '-I', str(dst)]

        logging.debug(' '.join(cmd))

        for retry in range(max_retries):
            if retry > 0:
                time.sleep(5)  # TODO: Move this magic number
                logging.debug('Retrying ({}/{})....'.format(
                    retry + 1,
                    max_retries
                ))
            try:
                with open(gsutil_output, 'w') as output:
                    process = subprocess.Popen(cmd, env=self._env,
                                               bufsize=0,
                                               stdin=subprocess.PIPE,
                                               stdout=output,
                                               stderr=subprocess.STDOUT,
                                               universal_newlines=True)
                for src in srcs:
                    process.stdin.write(str(src) + '\n')
                process.stdin.close()
                if process.wait() == 0:
                    with open(manifest_log) as f:
                        manifestobjects = [
                            ManifestObject(row['Destination'],
                                           int(row['Source Size']),
                                           row['Md5'])
                            for row in csv.DictReader(f, delimiter=',')
                        ]
                    return manifestobjects
            except Exception as e:
                logging.debug("Exception type, message, and trace {}, {}, {}"
                              .format(type(e), type(e)(e.message), sys.exc_info()[2]))
                if isinstance(e, IOError):
                    continue
        raise IOError('gsutil failed. Max attempts ({}) exceeded'.format(max_retries))
