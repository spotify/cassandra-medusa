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

import logging
import sys
import traceback

from medusa.storage import Storage


def update_backup_index(storage, node_backup):
    """
    Called when a backup happens, this method adds an entry about this backup to the backup index
    and sets this backups as the latest backup.
    """
    add_backup_to_index(storage, node_backup)
    set_latest_backup_in_index(storage, node_backup)


def build_indices(config, noop):
    """
    One-off function to construct the backup index.
    This function lists all complete cluster backups and all node backups in them.
    For all node backups found this way, it will find the latest one per node and update index accordingly.
    """
    try:
        storage = Storage(config=config.storage)
        all_backups = storage.discover_node_backups()
        latest_node_backups = dict()

        if noop:
            logging.info('--noop was set, will only print the indices')

        for node_backup in all_backups:
            # if we are dealing with a complete backup
            if node_backup.finished is not None:
                # check if this backup is newer than what was seen so far
                latest = latest_node_backups.get(node_backup.fqdn, node_backup)
                if node_backup.finished >= latest.finished:
                    latest_node_backups[node_backup.fqdn] = node_backup
                # if requested, add the node backup to the index
                logging.debug('Found backup {} from {}'.format(node_backup.name, node_backup.fqdn))
                if not noop:
                    add_backup_to_index(storage, node_backup)

        # once we have seen all backups, we can set the latest ones as well
        for fqdn, node_backup in latest_node_backups.items():
            logging.debug('Latest backup {} is {}'.format(fqdn, node_backup.name))
            if not noop:
                set_latest_backup_in_index(storage, node_backup)

    except Exception:
        traceback.print_exc()
        sys.exit(1)


def add_backup_to_index(storage, node_backup):
    dst = 'index/backup_index/{}/tokenmap_{}.json'.format(node_backup.name, node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.tokenmap)


def set_latest_backup_in_index(storage, node_backup):
    dst = 'index/latest_backup/{}/tokenmap.json'.format(node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.tokenmap)
    dst = 'index/latest_backup/{}/backup_name.txt'.format(node_backup.fqdn)
    storage.storage_driver.upload_blob_from_string(dst, node_backup.name)
