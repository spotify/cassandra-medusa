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

if [ ! -d spotify-puppet ]
then
	echo "run this script relative to spotify-puppet."
	exit 1
fi

#Create bucket
gsutil mb -p $GCP_PROJECT -c regional -l $LOCATION gs://$ROLE-test/

#Service account
gcloud --project $GCP_PROJECT iam service-accounts create  $ROLE-test --display-name  $ROLE-test
gcloud --project $GCP_PROJECT iam service-accounts keys create $ROLE-test.json --iam-account=$ROLE-test@$GCP_PROJECT.iam.gserviceaccount.com

#Add credentials to celo
curl --fail -u $USER -X POST https://celo.spotify.net/role/$ROLE/production -d key='medusa::credentials' --data-urlencode secret@$ROLE-test.json


#Grant permissions
#TODO move Medusa role to its own project
gsutil iam set <(gsutil iam get gs://${ROLE}-test | jq ".bindings += [{\"members\":[\"serviceAccount:${ROLE}-test@${GCP_PROJECT}.iam.gserviceaccount.com\"],\"role\":\"projects/xpn-scarifprototype-1/roles/MedusaStorageAgent\"}]") gs://${ROLE}-test

#Append 3 lines to hiera-data/roles/$role/gew1.yaml
mkdir -p spotify-puppet/hiera-data/role/$ROLE
echo "classes:
  medusa
medusa::bucket: $ROLE-test" >> spotify-puppet/hiera-data/role/$ROLE/$POD.yaml
echo "Make sure that the yaml file actually looks proper"


#example:
#https://ghe.spotify.net/puppet/spotify-puppet/pull/38673


#Get access to add credentials
#echo edit https://ghe.spotify.net/favorite/identity2/edit/master/service-info/identity2-enc-cassandra.yaml
