#!/bin/bash
set -e

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 GCP_PROJECT ROLE POD LOCATION" >&2
  echo "Example: $0 xpn-scarifprototype-1 identity2 gew1 europe-west1" >&2
  exit 1
fi

set -x

export GCP_PROJECT=$1
export ROLE=$2
export POD=$3
export LOCATION=$4

export SERVICE_ACCOUNT_NAME="${ROLE}-medusa-backup"
export BUCKET_NAME="${ROLE}-medusa-backup"
export BUCKET_URL="gs://${BUCKET_NAME}"

#Create bucket
echo "Creating GCP bucket ${BUCKET_NAME}"
gsutil mb -p ${GCP_PROJECT} -c regional -l ${LOCATION} ${BUCKET_URL}
echo

#Service account
echo "Setting up service account"
gcloud --project ${GCP_PROJECT} iam service-accounts create ${SERVICE_ACCOUNT_NAME} --display-name ${SERVICE_ACCOUNT_NAME}
gcloud --project ${GCP_PROJECT} iam service-accounts keys create ${SERVICE_ACCOUNT_NAME}.json --iam-account=${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com
echo

#Add credentials to celo
echo "Pushing service account's secret to CELO. Will ask for password"
curl --fail -u ${USER} -X POST https://celo.spotify.net/role/${ROLE}/production -d key='medusa::credentials' --data-urlencode secret@${SERVICE_ACCOUNT_NAME}.json && rm ${SERVICE_ACCOUNT_NAME}.json
echo

#Grant permissions
echo "Granting bucket permissions to the service account"
gsutil iam set <(gsutil iam get ${BUCKET_URL} | jq ".bindings += [{\"members\":[\"serviceAccount:${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com\"],\"role\":\"projects/medusa-backups/roles/MedusaStorageAgent\"}]") ${BUCKET_URL}
echo

#Configure the source cluster
echo "Calling hecuba-cli to configure the C* cluster"
hecuba2-cli enable-medusa --role ${ROLE} --pod ${POD} --bucket ${BUCKET_NAME} --frequency daily
echo

echo "Setup done. Please review & merge the PR above"
echo

#example:
#https://ghe.spotify.net/puppet/spotify-puppet/pull/38673


#Get access to add credentials
#echo edit https://ghe.spotify.net/favorite/identity2/edit/master/service-info/identity2-enc-cassandra.yaml
