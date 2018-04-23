Notes
=====


Restore
-------

- We need a temporary location for downloading data.
  This should be on the same filesystem as Cassandra's
  data directory.
- [Q][Bases] Is creating the schema safe if directories with SSTables
  already exists?
  - SELECT keyspace_name, columnfamily_name, cf_id FROM system.schema_columnfamilies
- [Q][Bases] Does schema changes automatically propergate to other nodes?
  - nodetool describecluster 
- [Q][Design] What should be do if the schema already exists?
- [Q][Bases] Can schema creation partially succeed?
  - nodetool describecluster 
- [Q][Bases] When/how does data get replicated to neighbouring nodes?
  - repair / rebuild
- [Q][Bases] Will wiping just the data directory be faster for reconnecting?
  - No. But wiping system tables affects joining / reconnection behavior.
- [Q][Bases] Is 'sudo hecuba2-agent --unsafe-parallel-join' OK?
  - Joining only happens if system tables are wiped

- sudo spcassandra-stop
- sudo chown -R cassandra:cassandra
- sudo spcassandra-enable-hecuba
- stop-download-start or download-stop-start
- [requirement] cluster must be new and with no schema