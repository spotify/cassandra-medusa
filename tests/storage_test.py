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
from medusa.index import build_indices
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
        self.storage.storage_driver.upload_blob_from_string("test_download_blobs1/file1.txt", file1_content)
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

    def test_get_fqdn_from_backup_index_blob(self):
        blob_name = "index/backup_index/2019051307/manifest_node1.whatever.com.json"
        self.assertEquals(
            "node1.whatever.com",
            self.storage.get_fqdn_from_backup_index_blob(blob_name)
        )

        blob_name = "index/backup_index/2019051307/schema_node2.whatever.com.cql"
        self.assertEquals(
            "node2.whatever.com",
            self.storage.get_fqdn_from_backup_index_blob(blob_name)
        )

        blob_name = "index/backup_index/2019051307/schema_node3.whatever.com.txt"
        self.assertEquals(
            "node3.whatever.com",
            self.storage.get_fqdn_from_backup_index_blob(blob_name)
        )

    def test_parse_backup_index(self):
        file_content = "content of the test file"
        # SSTables for node1 and backup1
        self.storage.storage_driver.upload_blob_from_string("node1/backup1/data/ks1/sstable1.db", file_content)
        self.storage.storage_driver.upload_blob_from_string("node1/backup1/data/ks1/sstable2.db", file_content)
        # Metadata for node1 and backup1
        self.storage.storage_driver.upload_blob_from_string("node1/backup1/meta/tokenmap.json", file_content)
        self.storage.storage_driver.upload_blob_from_string("node1/backup1/meta/manifest.json", file_content)
        self.storage.storage_driver.upload_blob_from_string("node1/backup1/meta/schema.cql", file_content)
        # SSTables for node2 and backup1
        self.storage.storage_driver.upload_blob_from_string("node2/backup1/data/ks1/sstable1.db", file_content)
        self.storage.storage_driver.upload_blob_from_string("node2/backup1/data/ks1/sstable2.db", file_content)
        # Metadata for node2 and backup1
        self.storage.storage_driver.upload_blob_from_string("node2/backup1/meta/tokenmap.json", file_content)
        self.storage.storage_driver.upload_blob_from_string("node2/backup1/meta/manifest.json", file_content)
        self.storage.storage_driver.upload_blob_from_string("node2/backup1/meta/schema.cql", file_content)
        # SSTables for node1 and backup2
        self.storage.storage_driver.upload_blob_from_string("node1/backup2/data/ks1/sstable1.db", file_content)
        self.storage.storage_driver.upload_blob_from_string("node1/backup2/data/ks1/sstable2.db", file_content)
        # Metadata for node1 and backup2
        self.storage.storage_driver.upload_blob_from_string("node1/backup2/meta/tokenmap.json", file_content)
        self.storage.storage_driver.upload_blob_from_string("node1/backup2/meta/manifest.json", file_content)
        self.storage.storage_driver.upload_blob_from_string("node1/backup2/meta/schema.cql", file_content)
        build_indices(self.config, False)
        path = 'index/backup_index'
        backup_index = self.storage.storage_driver.list_objects(path)
        blobs_by_backup = self.storage.group_backup_index_by_backup_and_node(backup_index)
        self.assertTrue("backup1" in blobs_by_backup)
        self.assertTrue("backup2" in blobs_by_backup)
        self.assertTrue("node1" in blobs_by_backup["backup1"])
        self.assertTrue("node2" in blobs_by_backup["backup1"])
        self.assertTrue("node1" in blobs_by_backup["backup2"])
        self.assertFalse("node2" in blobs_by_backup["backup2"])

    def test_remove_extension(self):
        self.assertEquals(
            'localhost',
            self.storage.remove_extension('localhost.txt')
        )
        self.assertEquals(
            'localhost',
            self.storage.remove_extension('localhost.timestamp')
        )
        self.assertEquals(
            'localhost',
            self.storage.remove_extension('localhost.cql')
        )
        self.assertEquals(
            'localhost.foo',
            self.storage.remove_extension('localhost.foo')
        )

    def test_get_timestamp_from_blob_name(self):
        self.assertEquals(
            1558021519,
            self.storage.get_timestamp_from_blob_name('finished_localhost_1558021519.timestamp')
        )
        self.assertEquals(
            1558021519,
            self.storage.get_timestamp_from_blob_name('finished_some.host.net_1558021519.timestamp')
        )


if __name__ == '__main__':
    unittest.main()
