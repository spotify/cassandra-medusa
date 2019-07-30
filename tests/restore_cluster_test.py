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
import json
import unittest
from pathlib import Path

from unittest.mock import MagicMock
from unittest.mock import Mock

from medusa.restore_cluster import RestoreJob
from medusa.config import MedusaConfig, StorageConfig, _namedtuple_from_dict


class RestoreClusterTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        config = configparser.ConfigParser(interpolation=None)
        config['storage'] = {'host_file_separator': ','}
        self.config = MedusaConfig(
            storage=_namedtuple_from_dict(StorageConfig, config['storage']),
            monitoring={},
            cassandra=None,
            ssh=None,
            restore=None
        )

    # Test that we can properly associate source and target nodes for restore using a host list
    def test_populate_ringmap(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            cluster_backup = Mock()
            tokenmap = json.loads(f.read())
            cluster_backup.tokenmap.return_value = tokenmap
            host_list = "tests/resources/restore_cluster_host_list.txt"
            restoreJob = RestoreJob(cluster_backup,
                                    self.config, Path('/tmp'), host_list, None, False, False, None)
            restoreJob._populate_hostmap()

        self.assertEqual(restoreJob.host_map["node1.mydomain.net"]['target'], "node1.mydomain.net")
        self.assertEqual(restoreJob.host_map["node2.mydomain.net"]['target'], "node2.mydomain.net")
        self.assertEqual(restoreJob.host_map["node4.mydomain.net"]['target'], "node3.mydomain.net")

    # Test that we can properly associate source and target nodes for restore using a token map
    def test_populate_tokenmap(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            with open("tests/resources/restore_cluster_tokenmap.target.json", 'r') as f_target:
                tokenmap = json.loads(f.read())
                cluster_backup = MagicMock()
                restoreJob = RestoreJob(cluster_backup,
                                        self.config, Path('/tmp'), None, "node1.mydomain.net", False, False, None)

                target_tokenmap = json.loads(f_target.read())
                restoreJob._populate_ringmap(tokenmap, target_tokenmap)

        self.assertEqual(restoreJob.host_map["node1.mydomain.net"]['target'], "node4.mydomain.net")
        self.assertEqual(restoreJob.host_map["node2.mydomain.net"]['target'], "node5.mydomain.net")
        self.assertEqual(restoreJob.host_map["node3.mydomain.net"]['target'], "node6.mydomain.net")

    # Test that we can't restore the cluster if the source and target topology have different sizes
    def test_populate_tokenmap_fail(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            with open("tests/resources/restore_cluster_tokenmap.fail.json", 'r') as f_target:
                tokenmap = json.loads(f.read())
                cluster_backup = MagicMock()
                restoreJob = RestoreJob(cluster_backup,
                                        self.config, Path('/tmp'), None, "node1.mydomain.net", False, False, None)

                target_tokenmap = json.loads(f_target.read())
                with self.assertRaises(Exception) as context:
                    restoreJob._populate_ringmap(tokenmap, target_tokenmap)

                self.assertTrue('Cannot restore to a tokenmap of differing size' in str(context.exception))

    # Test that we can't restore the cluster if the source and target topology have different tokens
    def test_populate_tokenmap_fail_tokens(self):
        node_backups = list()
        node_backups.append(Mock())
        with open("tests/resources/restore_cluster_tokenmap.json", 'r') as f:
            with open("tests/resources/restore_cluster_tokenmap.fail_tokens.json", 'r') as f_target:
                tokenmap = json.loads(f.read())
                cluster_backup = MagicMock()
                restoreJob = RestoreJob(cluster_backup,
                                        self.config, Path('/tmp'), None, "node1.mydomain.net", False, False, None)

                target_tokenmap = json.loads(f_target.read())
                with self.assertRaises(Exception) as context:
                    restoreJob._populate_ringmap(tokenmap, target_tokenmap)

                self.assertTrue('Tokenmap is differently distributed' in str(context.exception))


if __name__ == '__main__':
    unittest.main()
