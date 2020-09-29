# Minio S3 server for CloudStor Shards

## Synopsis 

This container provides a single user instance of an S3 gateway mapped to a directory on an EOS instance. 

## How to use
Like it's never been done befooooooooooooooore

## Configuration

This container is recommended to be run read-only, and ephemeral. It is required to be run as user apache:apache (AARNet standard is uid 48, gid 48).

### Port mapping
* `9000` - default minio port

### Volume mounts
* `/etc/k8screds/eos.keytab` or `/etc/eos.client.keytab` - EOS keytab file
* `/stage` - staging area for files being uploaded (usually SSD for performance)

### Required Environment Variables
* `MINIO_ACCESS_KEY` - 16 to 32 character key consisting of only the character a-z and A-Z
* `MINIO_SECRET_KEY` - 40 character key consisting of a-z, A-Z, and 0-9
* `VOLUME_PATH` - EOS path where the S3 buckets will be stored
* `EOS` - the hostname for the EOS instance

### Other Environment Variables
Not sure if this is the full list, but it's the ones that were obvious.

* `EOSREADMETHOD` - (default: webdav) the method used for reading from EOS (can be `webdav`, `xrootd` or `xrdcp`)
* `EOSUSER` - (default: minio) the user for interacting with EOS
* `EOSUID` - (default: 48) the numeric ID of the owner of the files in EOS
* `EOSGID` - (default: 48) the numeric ID of the group owner of the files in EOS
* `EOSSTAGE` - (default: /stage) staging directory for uploads
* `FOREGROUND_STAGING_TRANSFER` - (default: false) wait for the transfer from the staging area to EOS to complete before returning to user
* `SCRIPTS` - (default: /scripts) `<insert description because I don't know>`
* `EOSLOGLEVEL` - (default: 1) 1: errors 2: info/errors 3: "debug"
* `EOSREADONLY` - (no default) set to `true` to make it a read only container
* `EOSVALIDBUCKETS` - (default: true) validate bucket names, if set to false this will allow invalid bucket names to be used (may not work correctly)
* `EOS_HTTP_PROXY` - (default: <none>) URL to a HTTP proxy for communication between MinIO and the EOS httpd (use for debugging)
* `MAX_RETRY` - (default: 10) int, how many times to retry
* `OVERWRITEMAXKEYS` - (default: 0) int, overwrite how many keys to return when listing objects (0 to disable)
* `EOSSORTFILELISTING` - (default: 0) int, do you want to sort EOS output? (1: true, 0: false)

## Building the minio binary

The minio binary and docker image are automatically built by the Jenkins pipeline: https://jenkins.aarnet.net.au/job/CloudStor/job/minio/

The docker image will only be pushed into the registry if all tests pass. So we should work on that.

If you need to manually build the image you can run:

```
docker build -t aplregistry.aarnet.edu.au/cloudservices/minio/shard:manual -f Dockerfile.aarnet .
```
