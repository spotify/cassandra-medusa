AWS S3 setup
============

### Create an S3 bucket

Create a new S3 bucket that will be used to store the backups, and do not enable public access.

### Create an IAM strategy for that bucket

Create an IAM strategy called `MedusaStorageStrategy`, with the following definition (replace `<bucket-name>` with the actual bucket name):

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucketByTags",
                "s3:GetLifecycleConfiguration",
                "s3:GetBucketTagging",
                "s3:GetInventoryConfiguration",
                "s3:PutAccelerateConfiguration",
                "s3:DeleteObjectVersion",
                "s3:GetObjectVersionTagging",
                "s3:ListBucketVersions",
                "s3:RestoreObject",
                "s3:ListBucket",
                "s3:GetAccelerateConfiguration",
                "s3:GetBucketPolicy",
                "s3:ReplicateObject",
                "s3:GetObjectVersionTorrent",
                "s3:GetObjectAcl",
                "s3:GetEncryptionConfiguration",
                "s3:GetBucketObjectLockConfiguration",
                "s3:AbortMultipartUpload",
                "s3:GetObjectVersionAcl",
                "s3:GetObjectTagging",
                "s3:GetMetricsConfiguration",
                "s3:DeleteObject",
                "s3:GetBucketPublicAccessBlock",
                "s3:GetBucketPolicyStatus",
                "s3:ListBucketMultipartUploads",
                "s3:GetObjectRetention",
                "s3:GetBucketWebsite",
                "s3:PutReplicationConfiguration",
                "s3:PutObjectLegalHold",
                "s3:GetBucketVersioning",
                "s3:GetBucketAcl",
                "s3:GetObjectLegalHold",
                "s3:GetReplicationConfiguration",
                "s3:ListMultipartUploadParts",
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectTorrent",
                "s3:PutObjectRetention",
                "s3:GetAnalyticsConfiguration",
                "s3:PutBucketObjectLockConfiguration",
                "s3:GetObjectVersionForReplication",
                "s3:GetBucketLocation",
                "s3:ReplicateDelete",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket-name>",
                "arn:aws:s3:::<bucket-name>/*"
            ]
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "s3:GetAccountPublicAccessBlock",
                "s3:HeadBucket"
            ],
            "Resource": "*"
        }
    ]
}
```

### Create an AWS user for backups

Create a new AWS user with "programmatic access type", which will only be able to use the API and CLI through access keys.
Assign the previously created `MedusaStorageStrategy` to this newly created API user.  


### Configure Medusa

Generate access keys for that user and save them in a file called `medusa-s3-credentials` in the following format:

```
[default]
aws_access_key_id = <access key id>
aws_secret_access_key = <access key secret>
```

Place this file on all Cassandra nodes running medusa under `/etc/medusa` and set the rights appropriately so that only users running Medusa can read/modify it.
Set the `key_file` value in the `[storage]` section of `/etc/medusa/medusa.ini` to the credentials file:  

```
bucket_name = my_s3_bucket
key_file = /etc/medusa/medusa-s3-credentials
```

Medusa should now be able to access the bucket and perform all required operations.
