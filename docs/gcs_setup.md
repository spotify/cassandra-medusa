Google Cloud Storage setup
==========================

### Create a role for backups

In order to perform backups in GCS, Medusa will need to use a service account [with appropriate permissions](permissions-setup.md).

Using the [Google Cloud SDK](https://cloud.google.com/sdk/install), run the following command to create the `MedusaStorageRole` (set the `$GCP_PROJECT` env variable appropriately):  

```
gcloud iam roles create MedusaStorageRole \
        --project ${GCP_PROJECT} \
        --stage GA \
        --title MedusaStorageRole \
        --description "Custom role for Medusa for accessing GCS safely" \
        --permissions storage.buckets.get,storage.buckets.getIamPolicy,storage.objects.create,storage.objects.delete,storage.objects.get,storage.objects.getIamPolicy,storage.objects.list
```

### Create a GCS bucket

Create a bucket for each Cassandra cluster, using the following command line (set the env variables appropriately):

```
gsutil mb -p ${GCP_PROJECT} -c regional -l ${LOCATION} ${BUCKET_URL}
```

### Create a service account and download its keys

Medusa will require a `credentials.json` file with the informations and keys for a service account with the appropriate role in order to interact with the bucket.

Create the service account (if it doesn't exist yet):

```
gcloud --project ${GCP_PROJECT} iam service-accounts create ${SERVICE_ACCOUNT_NAME} --display-name ${SERVICE_ACCOUNT_NAME}
```

And download the json key file:  

```
gcloud --project ${GCP_PROJECT} iam service-accounts keys create ${SERVICE_ACCOUNT_NAME}.json --iam-account=${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com
```

The `${SERVICE_ACCOUNT_NAME}.json` file will have to be placed on each Cassandra node running Medusa, under `/etc/medusa`.

### Configure the service account with the role

Once the service account has been created, and considering [jq](https://stedolan.github.io/jq/) is installed, run the following command to add the `MedusaStorageRole` to it, for our backup bucket:

```
gsutil iam set <(gsutil iam get ${BUCKET_URL} | jq ".bindings += [{\"members\":[\"serviceAccount:${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com\"],\"role\":\"projects/${GCP_PROJECT}/roles/MedusaStorageRole\"}]") ${BUCKET_URL}
```


