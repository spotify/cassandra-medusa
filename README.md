Medusa
======

Cassandra backup to GCS


Prototype roadmap
-----------------

### General
- Figure out credentials for gsutil
- Cached MD5 sum


### Backup
- Take snapshot
  - `nodetool snapshot -t <name of snapshot> -- <keyspaces..>`
  - `nodetool listsnapshots`
- Dump topology
  - `spjmxproxy ringstate`
- List content of previous backup
- Upload to bucket / copy from previous


### Status
- List directories
- List state files


### Restore