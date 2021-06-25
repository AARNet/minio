#!/bin/sh
  echo; echo "Creating and setting permissions on minio directory /eos/minioshard/gateways/miniodev"
  echo;
  quiet_dcexec fst eos -r 0 0 -b mkdir -p /eos/minioshard/gateways/miniodev
  quiet_dcexec fst eos -r 0 0 -b chown 48:48 /eos/minioshard/gateways/miniodev
  quiet_dcexec mgm eos -r 0 0 -b attr -r set sys.forced.checksum=md5 /eos/minioshard/gateways/miniodev