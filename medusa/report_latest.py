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
from medusa.metrics.transport import MedusaTransport
from medusa.storage import Storage


def report_latest(config, report_to_ffwd):
    storage = Storage(config=config.storage)
    fqdn = config.storage.fqdn
    ffwd_client = ffwd.FFWD(transport=MedusaTransport)

    check_node_backup(storage, fqdn, report_to_ffwd, ffwd_client)
    check_cluster_backup(storage, report_to_ffwd, ffwd_client)


def check_node_backup(storage, fqdn, report_to_ffwd, ffwd_client):
    latest_node_backup = storage.latest_node_backup(fqdn=fqdn)

    if latest_node_backup is None:
        logging.info('This node has not been backed up yet')
        return

    finished = latest_node_backup.finished
    node_backup_finished_ago = (datetime.datetime.now(finished.tzinfo) - finished)
    logging.info('Latest node backup finished {} seconds ago'.format(node_backup_finished_ago.seconds))

    if report_to_ffwd:
        logging.debug('Sending time since last node backup to ffwd')
        finished_ago_metric = ffwd_client.metric(key='medusa-node-backup', what='seconds-since-backup')
        finished_ago_metric.send(node_backup_finished_ago.seconds)


def check_cluster_backup(storage, report_to_ffwd, ffwd_client):
    latest_cluster_backup = storage.latest_cluster_backup()

    if latest_cluster_backup is None:
        logging.info('The cluster this node belongs to has not backed up yet')
        return

    finished = latest_cluster_backup.finished
    cluster_backup_finished_ago = (datetime.datetime.now(finished.tzinfo) - finished)
    logging.info('Latest cluster backup finished {} seconds ago'.format(cluster_backup_finished_ago.seconds))

    if report_to_ffwd:
        logging.debug("Sending time since last cluster backup to ffwd")
        finished_ago_metric = ffwd_client.metric(key='medusa-cluster-backup', what='seconds-since-backup')
        finished_ago_metric.send(cluster_backup_finished_ago.seconds)
