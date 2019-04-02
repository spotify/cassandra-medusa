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

import configparser
import datetime
import os
import shutil
import unittest

from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict
from medusa.storage import Storage
import medusa.storage.abstract_storage


class RestoreNodeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_storage_dir = "/tmp/medusa_local_storage"
        self.medusa_bucket_dir = "/tmp/medusa_test_bucket"

    def setUp(self):
        if os.path.isdir(self.local_storage_dir):
            shutil.rmtree(self.local_storage_dir)
        if os.path.isdir(self.medusa_bucket_dir):
            shutil.rmtree(self.medusa_bucket_dir)

        os.makedirs(self.local_storage_dir)
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {'host_file_separator': ',', 'bucket_name': 'medusa_test_bucket', 'key_file': '',
                             'storage_provider': 'local', 'prefix': '', 'fqdn': '127.0.0.1',
                             'api_key_or_username': '', 'api_secret_or_password': '', 'base_path': '/tmp'}
        self.config = MedusaConfig(
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            cassandra=None,
            ssh=None
        )

        self.storage = Storage(config=self.config.storage)

    def test_add_object_from_string(self):
        file_content = "content of the test file"
        self.storage.storage_driver.upload_blob_from_string("test1/file.txt", file_content)
        self.assertEquals(self.storage.storage_driver.get_blob_content_as_string("test1/file.txt"), file_content)

    def test_download_blobs(self):
        files_to_download = list()
        file1_content = "content of the test file1"
        file2_content = "content of the test file2"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        files_to_download.append("test_download_blobs1/file1.txt")
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs2/file2.txt", file2_content)
        files_to_download.append("test_download_blobs2/file2.txt")
        self.assertEquals(len(os.listdir(self.medusa_bucket_dir)), 2)
        self.storage.storage_driver.download_blobs(files_to_download, self.local_storage_dir)
        self.assertEquals(len(os.listdir(self.local_storage_dir)), 2)

    def test_list_objects(self):
        file1_content = "content of the test file1"
        file2_content = "content of the test file2"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs2/file2.txt", file2_content)
        objects = self.storage.storage_driver.list_objects()
        self.assertEquals(len(objects), 2)
        one_object = self.storage.storage_driver.list_objects("test_download_blobs2")
        self.assertEquals(len(one_object), 1)

    def test_read_blob(self):
        file1_content = "content of the test file1"
        objects = self.storage.storage_driver.list_objects("test_download_blobs1")
        object_content = self.storage.storage_driver.read_blob_as_string(objects[0])
        self.assertEquals(object_content, file1_content)

    def test_get_blob(self):
        file1_content = "content of the test file1"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        obj = self.storage.storage_driver.get_blob("test_download_blobs1/file1.txt")
        self.assertEquals(obj.name, "test_download_blobs1/file1.txt")

    def test_read_blob_as_bytes(self):
        file1_content = "content of the test file1"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        object_content = self.storage.storage_driver.get_blob_content_as_bytes("test_download_blobs1/file1.txt")
        self.assertEquals(object_content, b"content of the test file1")

    def test_verify_hash(self):
        file1_content = "content of the test file1"
        manifest = self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        obj = self.storage.storage_driver.get_blob("test_download_blobs1/file1.txt")
        self.assertEquals(manifest.MD5, obj.hash)

    def test_hashes_match(self):
        # Should match
        hash1 = "S1EAM/BVMqhbJnAUs/nWlQ=="
        hash2 = "4b510033f05532a85b267014b3f9d695"
        self.assertTrue(
            medusa.storage.abstract_storage.AbstractStorage.hashes_match(hash1, hash2)
        )

        # Should match
        hash1 = "4b510033f05532a85b267014b3f9d695"
        hash2 = "4b510033f05532a85b267014b3f9d695"
        self.assertTrue(
            medusa.storage.abstract_storage.AbstractStorage.hashes_match(hash1, hash2)
        )

        # Should not match
        hash1 = "S1EAM/BVMqhbJnAUs/nWlQsdfsdf=="
        hash2 = "4b510033f05532a85b267014b3f9d695"
        self.assertFalse(
            medusa.storage.abstract_storage.AbstractStorage.hashes_match(hash1, hash2)
        )

    def test_get_object_datetime(self):
        file1_content = "content of the test file1"
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
        obj = self.storage.storage_driver.get_blob("test_download_blobs1/file1.txt")
        self.assertEquals(
            datetime.datetime.fromtimestamp(int(obj.extra["modify_time"])),
            self.storage.storage_driver.get_object_datetime(obj)
        )


if __name__ == '__main__':
    unittest.main()
