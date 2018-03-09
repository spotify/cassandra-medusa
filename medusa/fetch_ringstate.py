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

import google.cloud.storage
import sys
from medusa.storage import Storage


def main(args):
    client = google.cloud.storage.Client.from_service_account_json(args.key_file)
    storage = Storage(args.bucket_name, client)
    backup = storage.get_backup_item(fqdn=args.fqdn, name=args.backup_name,
                                     prefix=args.prefix)
    if not backup.exists():
        print('No such backup')
        sys.exit(1)

    print(backup.ringstate.download_as_string().decode('utf-8'))