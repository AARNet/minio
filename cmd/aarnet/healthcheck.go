/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 *
 */

package main

import (
	"bytes"
	"context"
	"os"

	"github.com/minio/minio/cmd/gateway/eos"
)

const (
	healthCheckFile = ".minio-healthcheck"
)

func configure() *eos.FileSystem {
	if os.Getenv("DEBUG") == "true" {
		eos.MaxLogLevel = eos.LogLevelDebug
	} else {
		eos.MaxLogLevel = eos.LogLevelOff
	}

	e := &eos.EOS{}
	filesystem := e.SetupFileSystem()
	return filesystem
}

func main() {
	fs := configure()

	var err error
	ctx := context.Background()
	healthCheckContent := []byte("hello world")

	_, err = fs.IsDir(ctx, "/")
	if err != nil {
		eos.EOSLogger.Debug(ctx, "%s", err)
		os.Exit(1)
	}

	buf := bytes.NewReader(healthCheckContent)
	_, err = fs.PutBuffer(ctx, "/tmp", healthCheckFile, buf)
	if err != nil {
		eos.EOSLogger.Debug(ctx, "%s", err)
		os.Exit(2)
	}

	err = fs.Rm(ctx, healthCheckFile)
	if err != nil {
		eos.EOSLogger.Debug(ctx, "%s", err)
		os.Exit(3)
	}

	eos.EOSLogger.Debug(ctx, "All checks passed")
}
