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
import collections
import logging
import paramiko
import binascii
import sys
import time
import uuid

from medusa.cassandra_utils import CqlSessionProvider
from medusa.storage import Storage

Remote = collections.namedtuple('Remote', ['target', 'connect_args', 'client', 'channel'])


def orchestrate(config, backup_name, seed_target, temp_dir):
    storage = Storage(config=config.storage)
    try:
        cluster_backup = storage.get_cluster_backup(backup_name)
    except KeyError:
        logging.error('No such backup')
        sys.exit(1)

    session_provider = CqlSessionProvider(seed_target,
                                          username=config.cassandra.cql_username,
                                          password=config.cassandra.cql_password)

    restore = RestoreJob(cluster_backup, session_provider,
                         config.ssh, temp_dir)
    restore.execute()


class RestoreJob(object):
    def __init__(self, cluster_backup, session_provider, ssh_config, temp_dir):
        self.id = uuid.uuid4()
        self.ringmap = None
        self.cluster_backup = cluster_backup
        self.session_provider = session_provider
        self.ssh_config = ssh_config
        if not temp_dir.is_dir():
            logging.error('{} is not a directory'.format(temp_dir))
            sys.exit(1)
        self.temp_dir = temp_dir

    def execute(self):
        if not self.cluster_backup.is_complete():
            raise Exception("Backup is not complete")
        with self.session_provider.new_session() as session:
            self._populate_ringmap(session)
        self._restore_data()

    def _populate_ringmap(self, session):
        target_tokenmap = session.tokenmap()
        backup_tokenmap = self.cluster_backup.tokenmap

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

    def _restore_schema(self, session):
        current_schema = session.dump_schema()

        if current_schema == self.cluster_backup.schema:
            logging.info('Not restoring schema, equivalent.')
        elif current_schema == '':
            parts = filter(bool, self.cluster_backup.schema.split(';'))
            for i, part in enumerate(parts):
                logging.info(
                    'Restoring schema part {i}: "{start}"..'.format(i=i, start=part[0:35]))
                session.session.execute(part)  # TODO: `session.session` one of them is not a session...
            logging.info('Finished restoring schema')
            return
        else:
            raise Exception('Schema not compatible')

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
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            connect_args = {
                'hostname': target,
                'username': self.ssh_config.username,
                'key_filename': self.ssh_config.key_file,
                'allow_agent': True
            }

            try:
                client.connect(**connect_args)
            except (paramiko.ssh_exception.AuthenticationException, FileNotFoundError) as e:
                if isinstance(e, paramiko.ssh_exception.AuthenticationException):
                    logging.info('SSH connection failed with the provided key {}, \
                                 trying from ssh agent keys'.format(connect_args['key_filename']))
                else:
                    logging.info('No ssh key was given. Trying from \
                                 ssh agent keys'.format(connect_args['key_filename']))

                # Trying to connect through ssh agent keys available
                transport = client.get_transport()
                agent = paramiko.Agent()
                agent_keys = agent.get_keys()

                if not agent_keys:
                    logging.info("No key found on the ssh agent")

                for key in agent_keys:
                    fp = binascii.hexlify(key.get_fingerprint())
                    logging.debug("Trying ssh-agent key {}".format(fp))
                    try:
                        transport.auth_publickey(connect_args['username'], key)
                        logging.info("Successfully connected to \
                                     {}@{}!".format(connect_args['username'], connect_args['hostname']))
                        break
                    except paramiko.SSHException:
                        logging.debug('SSH connection using key {} in the agent did not work'.format(fp))

                    exit(1)

            sftp = client.open_sftp()
            sftp.mkdir(str(work))
            sftp.close()

            # Forwarding argent for the following exec_command
            transport = transport or client.get_transport()
            session = transport.open_session()
            paramiko.agent.AgentRequestHandler(session)

            # TODO: If this command fails, the node is currently still marked as finished and not as broken.
            command = 'nohup sh -c "cd {work} && medusa-wrapper medusa --fqdn={fqdn} ' \
                      '-vvv restore-node --backup-name {backup}"'.format(work=work, fqdn=source,
                                                                         backup=self.cluster_backup.name)
            stdin, stdout, stderr = client.exec_command(command)
            logging.debug('Running {} remotely on {}'.format(command, connect_args['hostname']))
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
                logging.debug("remote.channel.exit_status: {}".format(remote.channel.exit_status))
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
                    client = paramiko.client.SSHClient()
                    client.load_system_host_keys()
                    client.connect(**remote.connect_args)

                    # TODO: check pid to exist before assuming medusa-wrapper to pick it up
                    command = 'cd {work}; medusa-wrapper'.format(work=work)
                    stdin, stdout, stderr = client.exec_command(command)
                    stdin.close()
                    stdout.close()
                    stderr.close()
                    remotes[i] = Remote(remote.target, remote.connect_args, client,
                                        stdout.channel)

        logging.info('finished: {}'.format(finished))
        logging.info('broken: {}'.format(broken))
