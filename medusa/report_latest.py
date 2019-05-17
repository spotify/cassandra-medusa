#! /usr/bin/env python
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

import datetime
import ffwd
import logging
import time
from medusa.metrics.transport import MedusaTransport
from medusa.storage import Storage


def report_latest(config, report_to_ffwd):
    MAX_RETRIES = 3
    SLEEP_TIME = 15
    retry = 0
    ffwd_client = ffwd.FFWD(transport=MedusaTransport)

    for retry in range(MAX_RETRIES):
        try:
            logging.debug('Trying to report about existing backups ({}/{})...'.format(
                retry + 1,
                MAX_RETRIES
            ))
            storage = Storage(config=config.storage)
            fqdn = config.storage.fqdn
            check_node_backup(storage, fqdn, report_to_ffwd, ffwd_client)
            check_complete_cluster_backup(storage, report_to_ffwd, ffwd_client)
            check_latest_cluster_backup(storage, report_to_ffwd, ffwd_client)
            break
        except Exception as e:
            if (retry + 1) < MAX_RETRIES:
                logging.debug('Report attempt {} failed, waiting {} seconds to retry'.format(
                    retry + 1,
                    SLEEP_TIME
                ))
                time.sleep(SLEEP_TIME)
                continue
            else:
                logging.error('This error happened during the check: {}'.format(e), exc_info=True)
                if report_to_ffwd:
                    # Set latest known complete backup to ~ 10 years ago to attract the attention
                    # of the operator on the broken monitoring.
                    long_time_flag_value = 315365400
                    logging.info("Sending a big value to 'seconds-since-backup' metric"
                                 " to trigger alerts.")
                    finished_ago_metric = ffwd_client.metric(key='medusa-cluster-backup',
                                                             what='seconds-since-backup',
                                                             backupname='TRACKING-ERROR')
                    finished_ago_metric.send(long_time_flag_value)


def check_node_backup(storage, fqdn, report_to_ffwd, ffwd_client):
    latest_node_backup = storage.latest_node_backup(fqdn=fqdn)

    if latest_node_backup is None:
        logging.info('This node has not been backed up yet')
        return

    finished = latest_node_backup.finished
    now = int(datetime.datetime.now().timestamp())
    node_backup_finished_seconds_ago = int((now - finished) / 1000)
    logging.info('Latest node backup '
                 'finished {} seconds ago'.format(node_backup_finished_seconds_ago))

    if report_to_ffwd:
        logging.debug('Sending time since last node backup to ffwd')
        finished_ago_metric = ffwd_client.metric(key='medusa-node-backup',
                                                 what='seconds-since-backup',
                                                 backupname=latest_node_backup.name)
        finished_ago_metric.send(node_backup_finished_seconds_ago)


def check_complete_cluster_backup(storage, report_to_ffwd, ffwd_client):
    latest_complete_cluster_backup = storage.latest_complete_cluster_backup()

    if latest_complete_cluster_backup is None:
        logging.info('The cluster this node belongs to has no complete backup yet')
        return

    logging.info('Latest complete backup:')
    logging.info('- Name: {}'.format(latest_complete_cluster_backup.name))

    finished = latest_complete_cluster_backup.finished
    now = int(datetime.datetime.now().timestamp())
    cluster_backup_finished_seconds_ago = int((now - finished) / 1000)
    logging.info('- Finished: {} seconds ago'.format(cluster_backup_finished_seconds_ago))

    if report_to_ffwd:
        logging.debug("Sending time since last complete cluster backup to ffwd")
        finished_ago_metric = ffwd_client.metric(key='medusa-cluster-backup',
                                                 what='seconds-since-backup',
                                                 backupname=latest_complete_cluster_backup.name)
        finished_ago_metric.send(cluster_backup_finished_seconds_ago)


def check_latest_cluster_backup(storage, report_to_ffwd, ffwd_client):
    latest_cluster_backup = storage.latest_cluster_backup()

    if latest_cluster_backup is None:
        logging.info('The cluster this node belongs to has not started a backup yet')
        return

    logging.info('Latest backup:')
    logging.info('- Name: {}'.format(latest_cluster_backup.name))
    # Boolean showing completion - ie all nodes' backup succeeded
    is_complete = latest_cluster_backup.is_complete()
    logging.info('- Finished: {}'.format(is_complete))

    logging.info('- Details - Node counts')
    # Node count for successful backups
    complete_nodes_count = len(latest_cluster_backup.complete_nodes())
    logging.info('- Complete backup: {} nodes have completed the backup'.format(complete_nodes_count))
    # Node count for incomplete backups
    incomplete_nodes_count = len(latest_cluster_backup.incomplete_nodes())
    logging.info('- Incomplete backup: {} nodes have not completed the backup yet'.format(incomplete_nodes_count))
    # Known hosts not having backups
    missing_nodes_count = len(latest_cluster_backup.missing_nodes())
    logging.info('- Missing backup: {} nodes are not running backups'.format(missing_nodes_count))

    # Total size used for this backup (all nodes sum) and the corresponding number of files
    latest_cluster_backup_size = latest_cluster_backup.size()
    readable_backup_size = human_readable_size(latest_cluster_backup_size)
    logging.info('- Total size: {}'.format(readable_backup_size))
    number_of_files = latest_cluster_backup.num_objects()
    logging.info('- Total files: {}'.format(number_of_files))

    if report_to_ffwd:
        complete_nodes_count_metric = ffwd_client.metric(key='medusa-cluster-backup',
                                                         what='complete-backups-node-count',
                                                         backupname=latest_cluster_backup.name)
        complete_nodes_count_metric.send(complete_nodes_count)
        incomplete_nodes_count_metric = ffwd_client.metric(key='medusa-cluster-backup',
                                                           what='incomplete-backups-node-count',
                                                           backupname=latest_cluster_backup.name)
        incomplete_nodes_count_metric.send(incomplete_nodes_count)
        missing_nodes_count_metric = ffwd_client.metric(key='medusa-cluster-backup',
                                                        what='missing-backups-node-count',
                                                        backupname=latest_cluster_backup.name)
        missing_nodes_count_metric.send(missing_nodes_count)
        total_backup_size_metric = ffwd_client.metric(key='medusa-cluster-backup',
                                                      what='backup-total-size',
                                                      backupname=latest_cluster_backup.name)
        total_backup_size_metric.send(latest_cluster_backup_size)
        total_number_of_files_metric = ffwd_client.metric(key='medusa-cluster-backup',
                                                          what='backup-total-file-count',
                                                          backupname=latest_cluster_backup.name)
        total_number_of_files_metric.send(number_of_files)


def human_readable_size(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "{:.2f} {}{}".format(num, unit, suffix)
        num /= 1024.0
    return "{:.2f} {}{}".format(num, 'Yi', suffix)


def get_latest_complete_cluster_backup(config):
    storage = Storage(config=config.storage)
    return storage.latest_complete_cluster_backup()
