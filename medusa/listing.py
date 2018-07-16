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


from medusa.storage import Storage

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


def list(show_all, config):
    storage = Storage(config=config.storage)

    cluster_backups = storage.list_cluster_backups()
    if not show_all:
        cluster_backups = filter(
            lambda cluster_backup: config.storage.fqdn in cluster_backup.node_backups,
            cluster_backups
        )

    for cluster_backup in cluster_backups:
        finished = cluster_backup.finished
        if finished is not None:
            finished = finished.strftime(TIMESTAMP_FORMAT)
        else:
            finished = 'Incomplete [{} of {} nodes]'.format(
                len(cluster_backup.node_backups),
                len(cluster_backup.tokenmap)
            )
        print('{} (started: {}, finished: {})'.format(
            cluster_backup.name,
            cluster_backup.started.strftime(TIMESTAMP_FORMAT),
            finished
        ))
