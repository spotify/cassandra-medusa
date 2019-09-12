# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
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

import threading
import logging
import os
import pathlib
import concurrent.futures
import medusa.storage


class UploadJob:
    def __init__(self, storage, src_files, dest, bucket):
        self.storage = storage
        self.src_files = src_files
        self.dest = dest
        self.bucket = bucket
        self.lock = threading.Lock()
        self.connection_pool = []

    def execute(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            return executor.map(self._upload_file, self.src_files)

    def _upload_file(self, src_file):
        with self.lock:
            if not self.connection_pool:
                connection = self.storage.connect_storage()
            else:
                connection = self.connection_pool.pop()
        try:
            if not isinstance(src_file, pathlib.Path):
                src_file = pathlib.Path(src_file)
            logging.info("Uploading {}".format(src_file))
            obj = connection.upload_object(
                os.fspath(src_file),
                container=self.bucket,
                object_name=str("{}/{}".format(self.dest, src_file.name))
            )
            return medusa.storage.ManifestObject(obj.name, obj.size, obj.hash)
        finally:
            with self.lock:
                self.connection_pool.append(connection)




