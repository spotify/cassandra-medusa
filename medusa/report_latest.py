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

    latest_backup = storage.latest_node_backup(fqdn=config.storage.fqdn)

    if latest_backup is None:
        logging.info('This node has not been backed up yet')
        return

    finished = latest_backup.finished
    finished_ago = (datetime.datetime.now(finished.tzinfo) - finished)
    logging.info('Latest backup finished {} seconds ago'.format(finished_ago.seconds))

    if not report_to_ffwd:
        logging.debug('Not sending to ffwd')
        return

    logging.debug('Sending time since last backup to ffwd')
    ffwd_client = ffwd.FFWD(transport=MedusaTransport)
    finished_ago_metric = ffwd_client.metric(key='medusa-backup', what='seconds-since-backup')
    finished_ago_metric.send(finished_ago.seconds)
