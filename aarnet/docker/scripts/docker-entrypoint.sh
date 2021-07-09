#!/bin/bash

echo "Setting up EOS client"
keytab_location="false"
if [ -f /etc/k8screds/eos.keytab ]; then
  keytab_location="/etc/k8screds/eos.keytab"
fi

if [ -f /etc/eos.client.keytab ]; then
  keytab_location="/etc/eos.client.keytab"
fi

if [ "${keytab_location}" == "false" ]; then
  echo "No keytab found, exiting."
  exit 1
fi

echo "Copying EOS keytab to /etc/eos.keytab"
cp -f "${keytab_location}" /etc/eos.keytab

echo "Setting permissions on EOS keytab"
chmod 400 /etc/eos.keytab

EOS=${EOS:-false}
if [ "${EOS}" == "false" ]; then
  echo "EOS is required. This is the hostname for the EOS instance."
  exit 3
fi

VOLUME_PATH=${VOLUME_PATH:-false}
if [ "${VOLUME_PATH}" == "false" ]; then
  echo "VOLUME_PATH is required. This is the EOS path where the S3 buckets will be stored"
  exit 2
fi

MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-false}
if [ "${MINIO_ACCESS_KEY}" == "false" ]; then
  echo "MINIO_ACCESS_KEY is required."
  exit 2
fi

MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-false}
if [ "${MINIO_SECRET_KEY}" == "false" ]; then
  echo "MINIO_SECRET_KEY is required."
  exit 2
fi

echo "Starting Minio server"
chmod 755 /scripts/minio

# Required to be set
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY}"
export VOLUME_PATH="${VOLUME_PATH}"
export EOS=${EOS}

/scripts/minio ${MINIO_OPTS} --json gateway eos "${VOLUME_PATH}"

