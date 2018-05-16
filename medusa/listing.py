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


def list(args, config):
    storage = Storage(config=config.storage)

    for node_backup in storage.list_node_backups(fqdn=args.fqdn):
        print('{} (started: {}, finished: {})'.format(
            node_backup.name,
            node_backup.started.strftime('%Y-%m-%d %H:%M:%S'),
            node_backup.finished.strftime('%Y-%m-%d %H:%M:%S')
        ))
