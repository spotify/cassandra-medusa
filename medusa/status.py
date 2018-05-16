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


import json
import logging
import sys
from medusa.storage import Storage


def validate_manifest(backup):
    try:
        manifest = json.loads(backup.manifest)
    except Exception:
        logging.error('Unable to read manifest from storage')
        return

    data_objects = {
        blob.name: blob
        for blob in backup.bucket.list_blobs(prefix=str(backup.data_prefix))
    }

    objects_in_manifest = [
        obj
        for columnfamily_manifest in manifest
        for obj in columnfamily_manifest['objects']
    ]
    for obj in objects_in_manifest:
        blob = data_objects.get(obj['path'])
        if blob is None:
            yield("[{}] Doesn't exists".format(obj['path']))
            continue
        if obj['MD5'] != blob.md5_hash:
            yield("[{}] Wrong checksum".format(obj['path']))
            continue
        if obj['size'] != blob.size:
            yield("[{}] Wrong file size".format(obj['path']))
            continue

    paths_in_manifest = {obj['path'] for obj in objects_in_manifest}
    paths_in_storage = set(data_objects.keys())
    for path in paths_in_storage - paths_in_manifest:
        yield("[{}] exists in storage, but not in manifest".format(path))


def validate_completion(backup):
    tokenmap = json.loads(backup.tokenmap)
    dc = tokenmap[backup.fqdn]['dc']
    all_backups_in_set = [
        backup.storage.get_node_backup(fqdn=node, name=backup.name)
        for node, config in tokenmap.items()
        if config.get('dc') == dc
    ]
    for b in all_backups_in_set:
        if not b.exists():
            yield('[{}] Backup missing'.format(b.fqdn))
            continue
        if b.finished is None:
            yield('[{}] Backup started at {}, but not finished yet'.format(b.fqdn, b.started))
            continue


def status(args, config):
    storage = Storage(config=config.storage)
    node_backup = storage.get_node_backup(fqdn=args.fqdn, name=args.backup_name)
    if not node_backup.exists():
        logging.error('No such backup')
        sys.exit(1)

    completion_errors = validate_completion(node_backup)
    if completion_errors:
        print('Completion: Not complete!')
        for error in completion_errors:
            print(error)
    else:
        print('Completion: OK!')

    consistency_errors = list(validate_manifest(node_backup))
    if consistency_errors:
        print("Manifest validation: Failed!")
        for error in consistency_errors:
            print(error)
    else:
        print("Manifest validated: OK!!")
