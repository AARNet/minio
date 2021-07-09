#!/bin/sh
  echo; echo "Creating and setting permissions on minio directory /eos/shard/data/gateways/miniodev"
  echo;
  quiet_dcexec fst eos -r 0 0 -b mkdir -p /eos/shard/data/gateways/miniodev
  quiet_dcexec fst eos -r 0 0 -b chown 48:48 /eos/shard/data/gateways/miniodev
  quiet_dcexec mgm eos -r 0 0 -b attr -r set sys.forced.checksum=md5 /eos/shard/data/gateways/miniodev