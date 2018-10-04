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
export BUCKET_NAME="gs://${ROLE}-medusa-backup"

if [ ! -d spotify-puppet ]
then
	echo "run this script relative to spotify-puppet."
	exit 1
fi

#Create bucket
gsutil mb -p ${GCP_PROJECT} -c regional -l ${LOCATION} ${BUCKET_NAME}

#Service account
gcloud --project ${GCP_PROJECT} iam service-accounts create ${SERVICE_ACCOUNT_NAME} --display-name ${SERVICE_ACCOUNT_NAME}
gcloud --project ${GCP_PROJECT} iam service-accounts keys create ${SERVICE_ACCOUNT_NAME}.json --iam-account=${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com

#Add credentials to celo
curl --fail -u ${USER} -X POST https://celo.spotify.net/role/${ROLE}/production -d key='medusa::credentials' --data-urlencode secret@${SERVICE_ACCOUNT_NAME}.json && rm ${SERVICE_ACCOUNT_NAME}.json

#Grant permissions
gsutil iam set <(gsutil iam get ${BUCKET_NAME} | jq ".bindings += [{\"members\":[\"serviceAccount:${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com\"],\"role\":\"projects/medusa-backups/roles/MedusaStorageAgent\"}]") ${BUCKET_NAME}

#Append 3 lines to hiera-data/roles/$role/gew1.yaml
mkdir -p spotify-puppet/hiera-data/role/${ROLE}
echo "classes:
  medusa
medusa::bucket: $ROLE-test
medusa::stagger: 75600 # 21 hours"  >> spotify-puppet/hiera-data/role/${ROLE}/${POD}.yaml
echo "Make sure that the yaml file actually looks proper"


#example:
#https://ghe.spotify.net/puppet/spotify-puppet/pull/38673


#Get access to add credentials
#echo edit https://ghe.spotify.net/favorite/identity2/edit/master/service-info/identity2-enc-cassandra.yaml
