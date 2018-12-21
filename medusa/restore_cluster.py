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
import sys
import time
import uuid

from medusa.cassandra_utils import CqlSessionProvider
from medusa.storage import Storage

Remote = collections.namedtuple('Remote', ['target', 'connect_args', 'client', 'channel'])


def orchestrate(config, backup_name, seed_target, temp_dir, host_list, bypass_checks):
    if seed_target is None and host_list is None:
        logging.error("You must either provide a seed target or a list of host.")
        sys.exit(1)

    if seed_target is not None and host_list is not None:
        logging.error("You must either provide a seed target or a list of host, not both.")
        sys.exit(1)

    storage = Storage(config=config.storage)
    try:
        cluster_backup = storage.get_cluster_backup(backup_name)
    except KeyError:
        logging.error('No such backup')
        sys.exit(1)

    restore = RestoreJob(cluster_backup, config, temp_dir, host_list, seed_target, bypass_checks)
    restore.execute()


class RestoreJob(object):
    def __init__(self, cluster_backup, config, temp_dir, host_list, seed_target, bypass_checks=False):
        self.id = uuid.uuid4()
        self.ringmap = None
        self.cluster_backup = cluster_backup
        self.session_provider = None
        self.config = config
        self.host_list = host_list
        self.seed_target = seed_target
        self.in_place = None
        if not temp_dir.is_dir():
            logging.error('{} is not a directory'.format(temp_dir))
            sys.exit(1)
        self.temp_dir = temp_dir
        self.host_map = {}  # Map of backup host/target host for the restore process
        self.bypass_checks = bypass_checks

    def execute(self):
        if not self.cluster_backup.is_complete():
            raise Exception("Backup is not complete")

        # CASE 1 : We're restoring in place and a seed target has been provided
        if self.seed_target is not None:
            self.in_place = True
            self.session_provider = CqlSessionProvider(self.seed_target,
                                                       username=self.config.cassandra.cql_username,
                                                       password=self.config.cassandra.cql_password)

            with self.session_provider.new_session() as session:
                self._populate_ringmap(self.cluster_backup.tokenmap, session.tokenmap())

        # CASE 2 : We're restoring out of place, i.e. doing a restore test
        if self.host_list is not None:
            self.in_place = False
            self._populate_hostmap()

        self._restore_data()

    def _populate_ringmap(self, tokenmap, target_tokenmap):
        for host, ringitem in target_tokenmap.items():
            if not ringitem.get('is_up'):
                raise Exception('Target {host} is not up!'.format(host=host))
            if len(target_tokenmap) != len(tokenmap):
                raise Exception('Cannot restore to a tokenmap of differing size: ({target_tokenmap}:{tokenmap}).'
                                .format(target_tokenmap=len(target_tokenmap), tokenmap=len(tokenmap)))

        def _tokens_from_ringitem(ringitem):
            return ','.join(map(str, ringitem['tokens']))

        target_tokens = {_tokens_from_ringitem(ringitem): host for host, ringitem in target_tokenmap.items()}
        backup_tokens = {_tokens_from_ringitem(ringitem): host for host, ringitem in tokenmap.items()}
        if target_tokens.keys() != backup_tokens.keys():
            raise Exception('Tokenmap is differently distributed: {distribution}'.format(
                distribution=target_tokens.keys() ^ backup_tokens.keys()))

        ringmap = collections.defaultdict(list)
        for ring in backup_tokens, target_tokens:
            for token, host in ring.items():
                ringmap[token].append(host)

        self.ringmap = ringmap
        for token, hosts in ringmap.items():
            self.host_map[hosts[0]] = {'target': hosts[1], 'seed': False}

    def _populate_hostmap(self):
        with open(self.host_list, 'r') as f:
            for line in f.readlines():
                token, seed, target, source = line.replace('\n', '').split(self.config.storage.host_file_separator)
                # in python, bool('False') evaluates to True. Need to test the membership as below
                self.host_map[source.strip()] = {'target': target.strip(), 'seed': seed in ['True']}

    def _restore_data(self):
        # create workdir on each target host
        # Later: distribute a credential
        # construct command for each target host
        # invoke `nohup medusa-wrapper #{command}` on each target host
        # wait for exit on each
        logging.info("Starting cluster restore...")
        work = self.temp_dir / 'medusa-job-{id}'.format(id=self.id)
        logging.debug('Medusa is working in: {}'.format(work))
        for source, target in self.host_map.items():
            logging.info("About to restore on {} using {} as backup source".format(target, source))

        logging.info("This will delete all data on the target nodes and replace it with backup {}."
                     .format(self.cluster_backup.name))
        proceed = None
        while (proceed != 'Y' and proceed != 'n') and not self.bypass_checks:
            proceed = input("Are you sure you want to proceed? (Y/n)")

        if proceed == 'n':
            sys.exit(1)

        # stop all target nodes
        stop_remotes = []
        logging.info("Stopping Cassandra on all nodes")
        for source, target in [(s, t['target']) for s, t in self.host_map.items()]:
            client, connect_args = self._connect(target, work)
            command = 'nohup sh -c "{}"'.format(self.config.cassandra.stop_cmd)
            stop_remotes.append(self._run(target, client, connect_args, command))

        # wait for all nodes to stop
        logging.info("Starting to wait for all nodes to stop")
        finished, broken = self._wait_for(work, stop_remotes)
        if len(broken) > 0:
            logging.error("Some Cassandras failed to stop. Exiting")
            sys.exit(1)

        # restore seeds first
        seed_remotes = []
        for source, target in [(s, t['target']) for s, t in self.host_map.items() if t['seed']]:
            logging.info('Restoring data on seed host {}...'.format(target))
            remote = self._trigger_restore(target, source, work)
            seed_remotes.append(remote)

        # wait for seed restores
        if len(seed_remotes) > 0:
            logging.info("Starting to wait for the seeds to restore")
            finished, broken = self._wait_for(work, seed_remotes)
            if len(broken) > 0:
                logging.error("Some seeds failed to restore. Exiting")
                sys.exit(1)

        # restore everything else
        normal_remotes = []
        for source, target in [(s, t['target']) for s, t in self.host_map.items() if not t['seed']]:
            logging.info('Restoring data on non-seed host {}...'.format(target))
            remote = self._trigger_restore(target, source, work)
            normal_remotes.append(remote)

        # wait for no-seed restores
        logging.info("Starting to wait for the normal nodes to restore")
        finished, broken = self._wait_for(work, normal_remotes)
        if len(broken) > 0:
            logging.error("Some normal nodes failed to restore. Exiting")
            sys.exit(1)

        logging.info('Restore process is complete. The cluster should be up shortly.')

    def _trigger_restore(self, target, source, work):

        client, connect_args = self._connect(target, work)

        # TODO: If this command fails, the node is currently still marked as finished and not as broken.
        in_place_option = "--in-place" if self.in_place else ""
        command = 'nohup sh -c "cd {work} && medusa-wrapper sudo medusa --fqdn={fqdn} ' \
                  '-vvv restore-node {in_place} --backup-name {backup}"'.format(work=work,
                                                                                fqdn=source,
                                                                                in_place=in_place_option,
                                                                                backup=self.cluster_backup.name)
        return self._run(target, client, connect_args, command)

    def _wait_for(self, work, remotes):
        finished, broken = [], []

        while True:
            time.sleep(5)  # TODO: configure sleep

            if len(remotes) == len(finished) + len(broken):
                # TODO: make a nicer exit condition
                logging.info('Exiting because all jobs are done.')
                break

            for i, remote in enumerate(remotes):

                if remote in broken or remote in finished:
                    continue

                # If the remote does not set an exit status and the channel closes
                # the exit_status is negative.
                logging.debug("remote.channel.exit_status: {}".format(remote.channel.exit_status))
                if remote.channel.exit_status_ready and remote.channel.exit_status >= 0:
                    if remote.channel.exit_status == 0:
                        finished.append(remote)
                        logging.info("Command succeeded on {}".format(remote.target))
                    else:
                        broken.append(remote)
                        logging.error("Command failed on {} : ".format(remote.target))
                        try:
                            stderr = self.read_file(remote, work / "stderr")
                        except IOError:
                            stderr = 'There was no stderr file'
                        logging.error(stderr)
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

        if len(broken) > 0:
            logging.info("Command failed on the following nodes :")
            for remote in broken:
                logging.info(remote.target)
        else:
            logging.info('Commands succeeded on all nodes')

        return finished, broken

    def _connect(self, target, work):
        logging.debug("Connecting to {}".format(target))

        pkey = None
        if self.config.ssh.key_file is not None and self.config.ssh.key_file != "":
            pkey = paramiko.RSAKey.from_private_key_file(self.config.ssh.key_file, None)

        client = paramiko.SSHClient()

        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        connect_args = {
            'hostname': target,
            'username': self.config.ssh.username,
            'pkey': pkey,
            'compress': True,
            'password': None
        }
        client.connect(**connect_args)

        logging.debug("Successfully connected to {}".format(target))
        sftp = client.open_sftp()
        try:
            sftp.mkdir(str(work))
        except OSError:
            logging.debug('Creating working directory {} on {} failed. It probably exists.'.format(str(work), target))
        except Exception as ex:
            logging.debug('Creating working directory on {} failed: {}'.format(target, str(ex)))
            sys.exit(1)
        finally:
            sftp.close()

        # Forwarding argent for the following exec_command
        transport = client.get_transport()
        session = transport.open_session()
        paramiko.agent.AgentRequestHandler(session)

        return client, connect_args

    def _run(self, target, client, connect_args, command):
        stdin, stdout, stderr = client.exec_command(command)
        logging.debug('Running {} remotely on {}'.format(command, connect_args['hostname']))
        stdin.close()
        stdout.close()
        stderr.close()
        return Remote(target, connect_args, client, stdout.channel)

    def read_file(self, remote, remotepath):
        with remote.client.open_sftp() as ftp_client:
            with ftp_client.file(remotepath.as_posix(), 'r') as f:
                return str(f.read(), 'utf-8')
