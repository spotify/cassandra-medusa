# GCP Permissions Needed For Medusa To Operate

This document is relevant for Spotify environment. Once Medusa goes FOSS, this should be rewritten.

There are two roles involved in the operations of Medusa.

The first role, [MedusaSetupAgent](https://console.cloud.google.com/iam-admin/roles/details/projects%3Cmedusa-backups%3Croles%3CMedusaSetupAgent?organizationId=642708779950&project=medusa-backups), gathers all the permissions needed to setup backups with Medusa.
- This role is assigned to the default service account of `database-infra` project: `984981252538-compute@developer.gserviceaccount.com`.
- This makes it possible to run the `setup_once.sh` script from basesusers host.
- The setup script requires a set of permissions:
  - `storage.buckets.create` - to create the bucket.
  - `iam.serviceAccounts.create` - to create the service account.
  - `iam.serviceAccountKeys.create` - to create a key for this account.
  - `storage.buckets.getIamPolicy` - to get the IAM policy of the newly created bucket.
  - `storage.buckets.setIamPolicy` - to set the IAM policy, so the MedusaStorageAgent can be used.

The second role, [MedusaStorageAgent](https://console.cloud.google.com/iam-admin/roles/details/projects%3Cmedusa-backups%3Croles%3CMedusaStorageAgent?organizationId=642708779950&project=medusa-backups), gathers permissions needed by Medusa itself to upload and download files to GCS.
- When setting up Medusa backups, the setup script assigns this role to the service account used by Medusa to access GCP.
- Because all the backup buckets will live in this project, there’s no need to share this role across projects.
- These are the permissions granted:
  - `storage.buckets.get`
  - `storage.buckets.getIamPolicy`
  - `storage.objects.create`
  - `storage.objects.delete`
  - `storage.objects.get`
  - `storage.objects.getIamPolicy`
  - `storage.objects.list`
