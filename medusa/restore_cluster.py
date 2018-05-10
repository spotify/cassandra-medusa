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
import time

import logging
import sys
import json
import random
import collections
import uuid

from medusa.cassandra import CqlSessionProvider
from medusa.storage import Storage
import paramiko

Remote = collections.namedtuple('Remote', ['target', 'connect_args', 'client', 'channel'])


def orchestrate(args, config):
    storage = Storage(config=config.storage)
    backups = storage.list_backup_items()
    usable_seed_backups = list(filter(lambda b: b.name == args.backup_name and b.finished, backups))
    if len(usable_seed_backups) < 1:
        logging.error('No backup for {}.'.format(args.backup_name))
        sys.exit(1)

    cluster_backup = ClusterBackup.discover(usable_seed_backups[0], config)
    if not cluster_backup.is_complete():
        logging.error('Backup {} is incomplete!'.format(args.backup_name))
        sys.exit(1)
    restore = cluster_backup.restore(args.targets)
    restore.execute()


class ClusterBackup(object):
    def __init__(self, cluster_backup, tokenmap, config):
        self.cluster_backup = cluster_backup
        self.tokenmap = tokenmap
        self.config = config

    def restore(self, targets):
        # TODO: Add username=.., password=.. to CqlSessionProvider from config.cassandra
        with CqlSessionProvider(targets[0],
                                username=self.config.cassandra.cql_username,
                                password=self.config.cassandra.cql_password).new_session() as session:
            target_tokenmap = session.tokenmap()
            for host, ringitem in target_tokenmap.items():
                if not ringitem.get('is_up'):
                    raise Exception(f'Target {host} is not up!')
            if len(target_tokenmap) != len(self.tokenmap):
                raise Exception('Cannot restore to a tokenmap of differing size: '
                                f'(#{len(target_tokenmap)}:#{len(self.tokenmap)}.')

            target_tokens = {ringitem['token']: host for host, ringitem in target_tokenmap.items()}
            backup_tokens = {ringitem['token']: host for host, ringitem in self.tokenmap.items()}
            if target_tokens.keys() != backup_tokens.keys():
                raise Exception('Tokenmap is differently distributed: '
                                f'{target_tokens.keys() ^ backup_tokens.keys()}')

            ringmap = collections.defaultdict(list)
            for ring in backup_tokens, target_tokens:
                for token, host in ring.items():
                    ringmap[token].append(host)

            return Restore(ringmap=ringmap, cluster_backup=self.cluster_backup, ssh_config=self.config.ssh)

    def is_complete(self):
        for b in self.cluster_backup:
            if b.finished is None:
                return False
        return True

    @staticmethod
    def discover(backup, config):
        tokenmap = json.loads(backup.tokenmap)
        dc = tokenmap[backup.fqdn]['dc']
        all_backups_in_set = [
            backup.storage.get_backup_item(fqdn=node, name=backup.name)
            for node, config in tokenmap.items()
            if config.get('dc') == dc
        ]

        return ClusterBackup(all_backups_in_set, tokenmap, config)


class Restore(object):
    def __init__(self, *, ringmap, cluster_backup, ssh_config):
        self.id = uuid.uuid4()
        self.ringmap = ringmap
        self.cluster_backup = cluster_backup
        self.ssh_config = ssh_config
        self.remotes = []

    def execute(self):
        # create workdir on each target host
        # Later: distribute a credential
        # construct command for each target host
        # invoke `nohup medusa-wrapper #{command}` on each target host
        # wait for exit on each

        targets = [target for source, target in self.ringmap.values()]
        for target in targets:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            connect_args = {
                'hostname': target,
                'username': self.ssh_config.username,
                # TODO: consider restricting the authentication to just the key provided
                'key_filename': self.ssh_config.key_file,
            }
            client.connect(**connect_args)
            sftp = client.open_sftp()
            sftp.mkdir(f'medusa-#{self.id}')
            sftp.close()
            command = 'sleep 10'  # TODO: Make command
            stdin, stdout, stderr = client.exec_command(command)
            stdin.close()
            stdout.close()
            stderr.close()
            self.remotes.append(Remote(target, connect_args, client, stdout.channel))

        finished, broken = [], []
        while True:
            time.sleep(5)  # TODO: configure sleep
            for remote in finished:
                logging.info(f"Finished: {remote.target}")
            for remote in broken:
                logging.info(f"Broken: {remote.target}")
            logging.info(f"Total: #{self.remotes}")

            if len(self.remotes) == len(finished) + len(broken):
                # TODO: make a nicer exit condition
                break
            pass

            for i, remote in enumerate(self.remotes):

                if remote in broken or remote in finished:
                    continue

                # If the remote does not set an exit status and the channel closes
                # the exit_status is negative.
                if remote.channel.exit_status_ready and remote.channel.exit_status >= 0:
                    if remote.channel.exit_status == 0:
                        finished.append(remote)
                    else:
                        broken.append(remote)
                    # We got an exit code that does not indicate an error, but not necessarily
                    # success. Cleanup channel and move to next remote. remote.client could still
                    # be used.
                    remote.channel.close()
                    continue

                if remote.client.get_transport().is_alive() and not remote.channel.closed:
                    # Send an ignored packet for keep alive and later noticing a broken connection
                    logging.debug(f"Keeping #{remote.target} alive.")
                    remote.client.get_transport().send_ignore()
                else:
                    client = paramiko.SSHClient()
                    client.load_system_host_keys()
                    client.connect(**remote.connect_args)
                    command = 'sleep 90'  # TODO: Make command
                    stdin, stdout, stderr = client.exec_command(command)
                    stdin.close()
                    stdout.close()
                    stderr.close()
                    self.remotes[i] = Remote(target, connect_args, client, stdout.channel)

        logging.info(f'finished: #{finished}')
        logging.info(f'broken: #{broken}')
