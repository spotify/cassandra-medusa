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
import base64
import json
import logging
from medusa.storage import Storage


def validate_manifest(storage, node_backup):
    try:
        manifest = json.loads(node_backup.manifest)
    except Exception:
        logging.error('Unable to read manifest from storage')
        return

    data_objects = {
        blob.name: blob
        for blob in storage.storage_driver.list_objects(node_backup.data_path)
    }

    objects_in_manifest = [
        obj
        for columnfamily_manifest in manifest
        for obj in columnfamily_manifest['objects']
    ]
    for obj in objects_in_manifest:
        blob = data_objects.get('{}{}'.format(storage.storage_driver.get_path_prefix(node_backup.data_path),
                                              obj['path']))
        if blob is None:
            yield("  - [{}] Doesn't exists".format(obj['path']))
            continue
        if base64.b64decode(obj['MD5']).hex() != str(blob.hash) and obj['MD5'] != str(blob.hash):
            logging.error("Expected {} got {} for {}".format(base64.b64decode(obj['MD5']).hex(),
                                                             blob.hash,
                                                             obj['path']))
            yield("  - [{}] Wrong checksum".format(obj['path']))
            continue
        if obj['size'] != blob.size:
            yield("  - [{}] Wrong file size".format(obj['path']))
            continue

    if node_backup.is_incremental is False:
        # Only for full backups as incremental backups can have more files in data dir than in manifest
        paths_in_manifest = {
            "{}{}".format(storage.storage_driver.get_path_prefix(node_backup.data_path), obj['path'])
            for obj in objects_in_manifest
        }
        paths_in_storage = set(data_objects.keys())
        for path in paths_in_storage - paths_in_manifest:
            yield("  - [{}] exists in storage, but not in manifest".format(path))


def verify(config, backup_name):
    storage = Storage(config=config.storage)

    try:
        cluster_backup = storage.get_cluster_backup(backup_name)
    except KeyError:
        logging.error('No such backup')
        raise RuntimeError("Manifest validation failed")

    print('Validating {0.name} ...'.format(cluster_backup))

    if cluster_backup.is_complete():
        print('- Completion: OK!')
    else:
        print('- Completion: Not complete!')
        for incomplete_node in cluster_backup.incomplete_nodes():
            print('  - [{0.fqdn}] Backup started at '
                  '{0.started}, but not finished yet'.format(incomplete_node))
        for fqdn in cluster_backup.missing_nodes():
            print('  - [{}] Backup missing'.format(fqdn))

    consistency_errors = [
        consistency_error
        for node_backup in cluster_backup.node_backups.values()
        for consistency_error in validate_manifest(storage, node_backup)
    ]
    if consistency_errors:
        print("- Manifest validation: Failed!")
        for error in consistency_errors:
            print(error)
        raise RuntimeError("Manifest validation failed")
    else:
        print("- Manifest validated: OK!!")
