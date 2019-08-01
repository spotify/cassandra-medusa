Medusa
======

Medusa is an Apache Cassandra backup system.


Design and Datastructures
-------------------------
Medusa is inspired by [GoogleCloudPlatform/cassandra-cloud-backup](https://github.com/GoogleCloudPlatform/cassandra-cloud-backup)
and various Spotify in-house Cassandra tools.
Medusa is designed to have node-specific operations, but cluster-aware data structures. Backups and
restores happen locally on each node without any centralized coordination, but the backed up data
structures are designed to aid making decisions about the backup state of the entire cluster.


### Backup
Backups are simply performed by making a Cassandra snapshot and copying the snapshot along with
the schema and topology to a Google Cloud Storage bucket. The main performance limitation is
network bandwidth.

#### Data structures
The backed up data is stored in a Google Cloud Storage using the following structure:
```
gs://<bucket name>/<optional prefix>/<fqdn>/<backup name>/data/<keyspace>/<column family>/<SSTAble files ...>
gs://<bucket name>/<optional prefix>/<fqdn>/<backup name>/meta/schema.cql
gs://<bucket name>/<optional prefix>/<fqdn>/<backup name>/meta/tokenmap.json
gs://<bucket name>/<optional prefix>/<fqdn>/<backup name>/meta/manifest.json
```

- `<optional prefix>` allows several clusters to share the same bucket, but is not encouraged as
buckets are cheap anyway. The support for this prefix might be dropped in later development.
- `<fqdn>` is the FQDN of the backed up node.
- `<backup name>` is the name of the backup, which defaults to a timestamp rounded to hours. Data
  from different nodes with the same `<backup name>` is considered part of the same backup, and
  expected to have been created at close to the same time.
- `schema.cql` contains the CQL commands to recreate the schema. This is the very first file to be
  uploaded to the bucket, and thus the existence of this file indicates that a backup has begun.
- `tokenmap.json` contains the topology (token) configuration of the cluster as seen by the node
  at the time of backup.
- `manifest.json` will contain a list of all expected data files along with expected sizes and
  MD5 checksums. This can be used to easily validate the content of a backup in a bucket.
  The content of `manifest.json` is generated on the node as part of the upload process.
  This is the last file to be uploaded to the bucket, thus the existence of this file means that the
  backup is complete.

#### Optimizations
As Cassandra's SSTables are immutable, it is possible to optimize the backup operation by
recognizing duplicate files in precious backups and avoid copying them twice. For the same reason, it is possible
to copy each SSTable exactly once and then refer to it from multiple manifests.

### Restore
Restoring is a bit more complicated and opinionated than backing up as it depends on whatever
tools you're using to manage the cluster's configuration and processes. Medusa provides both the necessary operations
to build your own restore scripts for your environment, but it also offers one implementation of the entire restore
process.

#### Restoring a single Cassandra node
- Before attempting to restore a Cassandra node, the restore script must compare the node's token
  configuration to the backed up data's topology, and make sure the token configurations match.
- If the topology matches, download all the backed up data to a temporary location.
- Stop the Cassandra process
- Delete any existing data
- Move the backed up data to Cassandra's data directory
- Start the Cassandra process
- Apply the schema from the backed up data
- Discover backed up SSTables

#### Restore a whole Cassandra cluster
- Pick a random node from the backed up data and download the topology.
- Configure the cluster to match the backed up topology. This step highly depends on the purpose
  of the restoring and which tools are used to configure Cassandra. It may involve allocating new
  nodes and configure them appropriately; or it may simply validate the configuration of an existing
  cluster and fail if it doesn't match.
- Run the [the previous section](#Restoring-a-single-Cassandra-node) on each individual node.

### Initial setup and access controls
We will provide `medusa-gcs-setup` which helps you set up the required infrastructure to use Medusa in GCP.
Specifically, the script should do the following:
- [ ] Create bucket
- [ ] Create service account
- [ ] Provision a key for service account
- [ ] Grant service account permissions on the bucket
- [ ] Configure [object lifecycle policies][olc]
- [ ] Configure (automated) restore tests

[olc]:https://cloud.google.com/storage/docs/lifecycle
