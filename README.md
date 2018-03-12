Medusa
======

Cassandra backup to GCS


Prototype roadmap
-----------------

### General
- Figure out credentials for gsutil


### Backup
- List content of previous backup
- Upload to bucket / copy from previous


### Status
- List directories
- List state files


### Restore



Notes
-----

CLOUDSDK_CONFIG=./.gcloud gcloud auth list
CLOUDSDK_CONFIG=./.gcloud gcloud auth activate-service-account --key-file=medusa-test.json
Popen(env={})