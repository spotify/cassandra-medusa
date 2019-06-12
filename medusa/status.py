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
from datetime import datetime

from medusa.storage import Storage, format_bytes_str


TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'


def status(config, backup_name):
    storage = Storage(config=config.storage)

    try:
        cluster_backup = storage.get_cluster_backup(backup_name)
    except KeyError:
        logging.error('No such backup')
        sys.exit(1)

    if (cluster_backup.is_complete()):
        print('{.name}'.format(cluster_backup))
    else:
        print('{.name} [Incomplete!]'.format(cluster_backup))

    started = datetime.fromtimestamp(cluster_backup.started).strftime(TIMESTAMP_FORMAT)
    if cluster_backup.finished is None:
        print('- Started: {}, '
              'Finished: never'.format(started))
    else:
        finished = datetime.fromtimestamp(cluster_backup.finished).strftime(TIMESTAMP_FORMAT)
        print('- Started: {}, '
              'Finished: {}'.format(started, finished))

    print('- {0} nodes completed, '
          '{1} nodes incomplete, '
          '{2} nodes missing'.format(len(cluster_backup.complete_nodes()), len(cluster_backup.incomplete_nodes()),
                                     len(cluster_backup.missing_nodes())
                                     ))

    print('- {} files, {}'.format(
        cluster_backup.num_objects(),
        format_bytes_str(cluster_backup.size())
    ))
