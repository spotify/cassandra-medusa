<!--
# Copyright 2019 Spotify AB. All rights reserved.
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
-->

Medusa
======

Medusa is an Apache Cassandra backup system.

Features
--------
Medusa is a command line tool that offers the following features:

* Single node backup
* Single node restore
* Cluster wide in place restore (restoring on the same cluster that was used for the backup)
* Cluster wide remote restore (restoring on a different cluster than the one used for the backup)
* Backup purge
* Support for local storage, Google Cloud Storage (GCS) and AWS S3 through [Apache Libcloud](https://libcloud.apache.org/). Can be extended to support other storage providers supported by Apache Libcloud.
* Support for clusters using single tokens or vnodes
* Full or incremental backups


Setup
-----
Choose and initialize the storage system:

* Local storage can be used in conjunction with NFS mounts to store backups off nodes.
* [Google Cloud Storage setup](docs/gcs_setup.md)
* [AWS S3 setup](docs/aws_s3_setup.md)

Install Medusa on each Cassandra node:
*Installation procedure depending on what packaging we'll be providing*

Modify `/etc/medusa/medusa.ini` to match your requirements:

```
[cassandra]
#stop_cmd = /etc/init.d/cassandra stop
#start_cmd = /etc/init.d/cassandra start
#config_file = <path to cassandra.yaml. Defaults to /etc/cassandra/cassandra.yaml>
#cql_username = <username>
#cql_password = <password>
#check_running = <Command ran to verify if Cassandra is running on a node. Defaults to "nodetool version">

[storage]
storage_provider = <Storage system used for backups. Currently either of "local", "google_storage" or the s3_* values from the following link: https://github.com/apache/libcloud/blob/trunk/libcloud/storage/types.py#L87-L105>
bucket_name = <Name of the bucket used for storing backups>
key_file = <JSON key file for service account with access to GCS bucket or AWS credentials file (~/.aws/credentials)>
#base_path = <Path of the local storage bucket (not used with providers other than "local">
#prefix = <Any prefix used for multitenancy in the same bucket>
#fqdn = <enforce the name of the local node. Computed automatically if not provided.>
#max_backup_age = <number of days before backups are purged. 0 means backups don't get purged (default)>
#max_backup_count = <number of backups to retain. Older backups will get purged beyond that number. 0 means backups don't get purged (default)>


[monitoring]
#monitoring_provider = <Provider used for sending metrics. Currently either of "ffwd" or "local">

[ssh]
#username = <SSH username to use for restoring clusters>
#key_file = <SSH key for use for restoring clusters. Expected in PEM unencrypted format.>
```


Usage
=====

```
$ medusa
Usage: medusa [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbosity          Verbosity
  --without-log-timestamp  Do not show timestamp in logs
  --config-file TEXT       Specify config file
  --bucket-name TEXT       Bucket name
  --key-file TEXT          GCP credentials key file
  --prefix TEXT            Prefix for shared storage
  --fqdn TEXT              Act as another host
  --ssh-username TEXT
  --ssh-key-file TEXT
  --help                   Show this message and exit.

Commands:
  backup                          Backup Cassandra
  build-index                     Builds indices for all present backups
                                  and...
  download                        Download backup
  fetch-tokenmap                  Backup Cassandra
  get-last-complete-cluster-backup
                                  Pints the name of the latest complete
                                  cluster...
  list-backups                    List backups
  purge                           Delete obsolete backups
  report-last-backup              Find time since last backup and print it
                                  to...
  restore-cluster                 Restore Cassandra cluster
  restore-node                    Restore single Cassandra node
  status                          Show status of backups
  verify                          Verify the integrity of a backup
```


Performing a backup
-------------------

```
$ medusa backup --help
Usage: medusa backup [OPTIONS]

  Backup Cassandra

Options:
  --backup-name TEXT           Custom name for the backup
  --stagger INTEGER            Check for staggering initial backups for
                               duration seconds
  --restore-verify-query TEXT
  --mode [full|incremental]
  --help                       Show this message and exit.
```

Once Medusa is setup, you can create a **full** backup with the following command:

```
$ medusa backup --backup-name=<name of the backup>
```

In order to perform an **incremental** backup, add the `--mode=incremental` argument to your command:

```
$ medusa backup --backup-name=<name of the backup> --mode=incremental
```

To perform cluster wide backups, the command must run on all nodes in the cluster, using the same backup name.


Listing existing backups
------------------------

```
$ medusa list-backups --help
Usage: medusa list-backups [OPTIONS]

  List backups

Options:
  --show-all / --no-show-all  List all backups in the bucket
  --help                      Show this message and exit.
```

List all backups for the current node/cluster:

```
$ medusa list-backups
2019080507 (started: 2019-08-05 07:07:03, finished: 2019-08-05 08:01:04)
2019080607 (started: 2019-08-06 07:07:04, finished: 2019-08-06 07:59:08)
2019080707 (started: 2019-08-07 07:07:04, finished: 2019-08-07 07:59:55)
2019080807 (started: 2019-08-08 07:07:03, finished: 2019-08-08 07:59:22)
2019080907 (started: 2019-08-09 07:07:04, finished: 2019-08-09 08:00:14)
2019081007 (started: 2019-08-10 07:07:04, finished: 2019-08-10 08:02:41)
2019081107 (started: 2019-08-11 07:07:04, finished: 2019-08-11 08:03:48)
2019081207 (started: 2019-08-12 07:07:04, finished: 2019-08-12 07:59:59)
2019081307 (started: 2019-08-13 07:07:03, finished: Incomplete [179 of 180 nodes])
2019081407 (started: 2019-08-14 07:07:04, finished: 2019-08-14 07:56:44)
2019081507 (started: 2019-08-15 07:07:03, finished: 2019-08-15 07:50:24)
```


Restoring a single node
-----------------------

```
$ medusa restore-node --help
Usage: medusa restore-node [OPTIONS]

  Restore single Cassandra node

Options:
  --temp-dir TEXT         Directory for temporary storage
  --backup-name TEXT      Backup name  [required]
  --in-place              Indicates if the restore happens on the node the
                          backup was done on.
  --keep-auth             Keep system_auth keyspace as found on the node
  --seeds TEXT            Nodes to wait for after downloading backup but
                          before starting C*
  --verify / --no-verify  Verify that the cluster is operational after the
                          restore completes,
  --help                  Show this message and exit.
```

In order to restore a backup on a single node, run the following command:

```
$ sudo medusa restore-node --backup-name=<name of the backup>
```

Medusa will need to run with `sudo` as it will:

* stop Cassandra
* wipe the existing files
* Download the files from backup storage locally and move them to Cassandra's storage directory
* Change the ownership of the files back to the one owning the Cassandra data directory
* start Cassandra


Restoring a cluster
-------------------

```
$ medusa restore-cluster --help
Usage: medusa restore-cluster [OPTIONS]

  Restore Cassandra cluster

Options:
  --backup-name TEXT              Backup name  [required]
  --seed-target TEXT              seed of the target hosts
  --temp-dir TEXT                 Directory for temporary storage
  --host-list TEXT                List of nodes to restore with the associated
                                  target host
  --keep-auth / --overwrite-auth  Keep/overwrite system_auth as found on the
                                  nodes
  -y, --bypass-checks             Bypasses the security check for restoring a
                                  cluster
  --verify / --no-verify          Verify that the cluster is operational after
                                  the restore completes,
  --help                          Show this message and exit.
```

### In place (same hardware)

In order to restore a backup for a full cluster, in the case where the restored cluster is the exact same as the backed up one:  

```
$ medusa restore-cluster --backup-name=<name of the backup> --seed-target node1.domain.net
```

Medusa will need to run without `sudo` as it will connect through ssh to all nodes in the cluster in order to perform remote operations. It will, by default, use the current user to connect and rely on agent forwarding for authentication (you must ssh into the server using `-A` to enable agent forwarding).
The `--seed-target` node is used to connect to Cassandra and retrieve the current topology of the cluster. This allows Medusa to map each backup to the correct node in the current topology.

The following operations will take place:

* stop Cassandra on all nodes
* check that the current topology matches the backed up one
* run `restore-node` on each node in the cluster
* start Cassandra on all nodes


### Remotely (different hardware)

In order to restore a backup for a full cluster but on different servers.  
This can be used to restore a production cluster data on a staging cluster (with the same number of nodes), or recovering from an outage where previously used hardware cannot be re-used.

```
$ medusa restore-cluster --backup-name=<name of the backup> --host-list /etc/medusa/restore_mapping.txt
```

The `restore-mapping.txt` file will provide the mapping between the backed up cluster nodes and the restore cluster ones. It is expected in the following CSV format: `<Is it a seed node?>,<source node>,<target node>`

Sample file:  

```
True,old_node1.foo.net,new_node1.foo.net
True,old_node2.foo.net,new_node2.foo.net
False,old_node3.foo.net,new_node3.foo.net
```

Medusa will need to run without `sudo` as it will connect through ssh to all nodes in the cluster in order to perform remote operations. It will, by default, use the current user to connect and rely on agent forwarding for authentication (you must ssh into the server using `-A` to enable agent forwarding).

* stop Cassandra on all nodes
* check that the current topology matches the backed up one
* run `restore-node` on each node in the cluster
* start Cassandra on all nodes

By default, Medusa will overwrite the `system_auth` keyspace with the backed up one. If you want to retain the existing system_auth keyspace.

Verify an existing backup
-------------------------

```
$ medusa verify --help
Usage: medusa verify [OPTIONS]

  Verify the integrity of a backup

Options:
  --backup-name TEXT  Backup name  [required]
  --help              Show this message and exit.
```

Run a health check on a backup, which will verify that:

* all nodes have completed the backup
* all files in the manifest are present in storage
* all backed up files are present in the manifest
* all files have the right hash as stored in the manifest

```
$ medusa verify --backup-name=2019090503
Validating 2019090503 ...
- Completion: OK!
- Manifest validated: OK!!
```

In case some nodes in the cluster didn't complete the backups, you'll get the following output:

```
$ medusa verify --backup-name=2019081703
Validating 2019081703 ...
- Completion: Not complete!
  - [127.0.0.2] Backup missing
- Manifest validated: OK!!
```

Purge old backups
-----------------

```
$ medusa purge --help
Usage: medusa purge [OPTIONS]

  Delete obsolete backups

Options:
  --help  Show this message and exit.
```

In order to remove obsolete backups from storage, according to the configured `max_backup_age` and/or `max_backup_count`, run:

```
$ medusa purge
[2019-09-04 13:44:16] INFO: Starting purge
[2019-09-04 13:44:17] INFO: 25 backups are candidate to be purged
[2019-09-04 13:44:17] INFO: Purging backup 2019082513...
[2019-09-04 13:44:17] INFO: Purging backup 2019082514...
[2019-09-04 13:44:18] INFO: Purging backup 2019082515...
[2019-09-04 13:44:18] INFO: Purging backup 2019082516...
[2019-09-04 13:44:19] INFO: Purging backup 2019082517...
[2019-09-04 13:44:19] INFO: Purging backup 2019082518...
[2019-09-04 13:44:19] INFO: Purging backup 2019082519...
[2019-09-04 13:44:20] INFO: Purging backup 2019082520...
[2019-09-04 13:44:20] INFO: Purging backup 2019082521...
[2019-09-04 13:44:20] INFO: Purging backup 2019082522...
[2019-09-04 13:44:21] INFO: Purging backup 2019082523...
[2019-09-04 13:44:21] INFO: Purging backup 2019082600...
[2019-09-04 13:44:21] INFO: Purging backup 2019082601...
[2019-09-04 13:44:22] INFO: Purging backup 2019082602...
[2019-09-04 13:44:22] INFO: Purging backup 2019082603...
[2019-09-04 13:44:23] INFO: Purging backup 2019082604...
[2019-09-04 13:44:23] INFO: Purging backup 2019082605...
[2019-09-04 13:44:23] INFO: Purging backup 2019082606...
[2019-09-04 13:44:24] INFO: Purging backup 2019082607...
[2019-09-04 13:44:24] INFO: Purging backup 2019082608...
[2019-09-04 13:44:24] INFO: Purging backup 2019082609...
[2019-09-04 13:44:25] INFO: Purging backup 2019082610...
[2019-09-04 13:44:25] INFO: Purging backup 2019082611...
[2019-09-04 13:44:25] INFO: Purging backup 2019082612...
[2019-09-04 13:44:26] INFO: Purging backup 2019082613...
[2019-09-04 13:44:26] INFO: Cleaning up orphaned files...
[2019-09-04 13:45:59] INFO: Purged 652 objects with a total size of 3.74 MB

```

Since SSTables and meta files are stored in different places for incremental backups, the purge is a two step process:  

- Delete all backup directories
- Scan remaining backup files from manifests and compare with the list of SSTables in the `data` directory. All orphaned SSTables will get deleted in that step.


Check the status of a backup
----------------------------

```
$ medusa status --help
Usage: medusa status [OPTIONS]

  Show status of backups

Options:
  --backup-name TEXT  Backup name  [required]
  --help              Show this message and exit.
```

Outputs a summary of a specific backup status:

```
$ medusa status --backup-name=2019090503
2019090503
- Started: 2019-09-05 03:53:04, Finished: 2019-09-05 04:49:52
- 32 nodes completed, 0 nodes incomplete, 0 nodes missing
- 163256 files, 12.20 TB
```


Display informations on the latest backup
-----------------------------------------
```
$ medusa report-last-backup --help
Usage: medusa report-last-backup [OPTIONS]

  Find time since last backup and print it to stdout :return:

Options:
  --push-metrics  Also push the information via metrics
  --help          Show this message and exit.

```

This command will display several informations on the latest backup:

```
$ medusa report-last-backup
[2019-09-04 12:56:15] INFO: Latest node backup finished 18746 seconds ago
[2019-09-04 12:56:18] INFO: Latest complete backup:
[2019-09-04 12:56:18] INFO: - Name: 2019090407
[2019-09-04 12:56:18] INFO: - Finished: 18173 seconds ago
[2019-09-04 12:56:19] INFO: Latest backup:
[2019-09-04 12:56:19] INFO: - Name: 2019090407
[2019-09-04 12:56:19] INFO: - Finished: True
[2019-09-04 12:56:19] INFO: - Details - Node counts
[2019-09-04 12:56:19] INFO: - Complete backup: 180 nodes have completed the backup
[2019-09-04 12:56:19] INFO: - Incomplete backup: 0 nodes have not completed the backup yet
[2019-09-04 12:56:19] INFO: - Missing backup: 0 nodes are not running backups
[2019-09-04 12:58:47] INFO: - Total size: 94.69 TiB
[2019-09-04 12:58:55] INFO: - Total files: 5168096
```

When used with `--push-metrics`, Medusa will push completion metrics to the configured monitoring system.
