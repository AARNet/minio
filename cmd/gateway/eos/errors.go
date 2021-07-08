package eos

import minio "github.com/minio/minio/cmd"

// MakeBucketFailed - bucket creation failed
type MakeBucketFailed minio.GenericError

func (e MakeBucketFailed) Error() string {
	return "Unable to create bucket: " + e.Bucket
}
