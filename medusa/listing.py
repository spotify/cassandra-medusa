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

    for backup_item in storage.list_backup_items(fqdn=args.fqdn):
        print('{} (started: {}, finished: {})'.format(
            backup_item.name,
            backup_item.started.strftime('%Y-%m-%d %H:%M:%S'),
            backup_item.finished.strftime('%Y-%m-%d %H:%M:%S')
        ))
