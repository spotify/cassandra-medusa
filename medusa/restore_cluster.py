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

    cluster_backup = discover(usable_seed_backups[0])
    session_provider = CqlSessionProvider(args.seed_target,
                                          username=config.cassandra.cql_username,
                                          password=config.cassandra.cql_password)

    restore = RestoreJob(cluster_backup, session_provider, config.ssh, args.temp_dir)
    restore.execute()


def discover(backup):
    tokenmap = json.loads(backup.tokenmap)
    dc = tokenmap[backup.fqdn]['dc']
    all_backups_in_set = [
        backup.storage.get_backup_item(fqdn=node, name=backup.name)
        for node, config in tokenmap.items()
        if config.get('dc') == dc
    ]

    return all_backups_in_set


class RestoreJob(object):
    def __init__(self, cluster_backup, session_provider, ssh_config, temp_dir):
        self.id = uuid.uuid4()
        self.ringmap = None
        self.cluster_backup = cluster_backup
        self.session_provider = session_provider
        self.ssh_config = ssh_config
        self.temp_dir = temp_dir

    def execute(self):
        self._backup_complete()
        with self.session_provider.new_session() as session:
            self._populate_ringmap(self.cluster_backup[0], session)
            self._restore_schema(self.cluster_backup[0], session)
        self._restore_data()

    def _backup_complete(self):
        complete = all(backup.finished for backup in self.cluster_backup) and len(self.cluster_backup) > 0
        if not complete:
            raise Exception("Backup is not complete")

    def _populate_ringmap(self, backup, session):
        target_tokenmap = session.tokenmap()
        backup_tokenmap = json.loads(backup.tokenmap)

        for host, ringitem in target_tokenmap.items():
            if not ringitem.get('is_up'):
                raise Exception('Target {host} is not up!'.format(host=host))
            if len(target_tokenmap) != len(backup_tokenmap):
                raise Exception('Cannot restore to a tokenmap of differing size: ({target_tokenmap}:{tokenmap}).'
                                .format(target_tokenmap=len(target_tokenmap), tokenmap=len(backup_tokenmap)))

        target_tokens = {ringitem['token']: host for host, ringitem in target_tokenmap.items()}
        backup_tokens = {ringitem['token']: host for host, ringitem in backup_tokenmap.items()}
        if target_tokens.keys() != backup_tokens.keys():
            raise Exception('Tokenmap is differently distributed: {distribution}'.format(
                distribution=target_tokens.keys() ^ backup_tokens.keys()))

        ringmap = collections.defaultdict(list)
        for ring in backup_tokens, target_tokens:
            for token, host in ring.items():
                ringmap[token].append(host)
        self.ringmap = ringmap

    def _restore_schema(self, backup, session):
        schema = session.dump_schema()
        if not (schema == backup.schema or schema == ''):
            raise Exception('Schema not compatible')
        if self.cluster_backup[0].schema == session.dump_schema():
            logging.info('Not restoring schema, equivalent.')
            return
        parts = filter(bool, self.cluster_backup[0].schema.split(';'))
        for i, part in enumerate(parts):
            logging.info(
                'Restoring schema part {i}: "{start}"..'.format(i=i, start=part[0:35]))
            session.session.execute(part)  # TODO: `session.session` one of them is not a session...
        logging.info('Finished restoring schema')
        return

    def _restore_data(self):
        # create workdir on each target host
        # Later: distribute a credential
        # construct command for each target host
        # invoke `nohup medusa-wrapper #{command}` on each target host
        # wait for exit on each

        work = self.temp_dir / 'medusa-job-{id}'.format(id=self.id)
        logging.info('Medusa is working in: {}'.format(work))
        remotes = []
        for source, target in self.ringmap.values():
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            connect_args = {
                'hostname': target,
                'username': self.ssh_config.username,
                # TODO: consider restricting the authentication to just the key provided
                # TODO: consider forwarding agent
                'key_filename': self.ssh_config.key_file,
            }
            client.connect(**connect_args)
            sftp = client.open_sftp()
            sftp.mkdir(str(work))
            sftp.close()
            # TODO: If this command fails, the node is currently still marked as finished and not as broken.
            command = 'cd {work}; ~parmus/medusa/env/bin/medusa-wrapper ~parmus/medusa/env/bin/medusa restore_node -vvv --fqdn={fqdn} {backup}'.format(
                work=work,
                fqdn=source,
                backup=self.cluster_backup[0].name
            )
            stdin, stdout, stderr = client.exec_command(command)
            stdin.close()
            stdout.close()
            stderr.close()
            remotes.append(Remote(target, connect_args, client, stdout.channel))

        finished, broken = [], []
        while True:
            time.sleep(5)  # TODO: configure sleep
            for remote in finished:
                logging.info("Finished: {}".format(remote.target))
            for remote in broken:
                logging.info("Broken: {}".format(remote.target))
            logging.info("Total: {}".format(len(remotes)))

            if len(remotes) == len(finished) + len(broken):
                # TODO: make a nicer exit condition
                break
            pass

            for i, remote in enumerate(remotes):

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
                    logging.debug("Keeping {} alive.".format(remote.target))
                    remote.client.get_transport().send_ignore()
                else:
                    client = paramiko.SSHClient()
                    client.load_system_host_keys()
                    client.connect(**remote.connect_args)
                    # TODO: check pid to exist before assuming medusa-wrapper to pick it up
                    command = 'cd {work}; ~parmus/medusa/env/bin/medusa-wrapper'.format(work=work)
                    stdin, stdout, stderr = client.exec_command(command)
                    stdin.close()
                    stdout.close()
                    stderr.close()
                    remotes[i] = Remote(remote.target, remote.connect_args, client,
                                        stdout.channel)

        logging.info('finished: {}'.format(finished))
        logging.info('broken: {}'.format(broken))
