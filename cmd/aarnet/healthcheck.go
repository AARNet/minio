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
	"net"
	"net/http"
	"os"
	"time"

	"github.com/minio/minio/cmd/gateway/eos"
)

const (
	healthCheckFile = ".minio-healthcheck"
)

func configure() {
	if os.Getenv("DEBUG") == "true" {
		eos.MaxLogLevel = eos.LogLevelDebug
	} else {
		eos.MaxLogLevel = eos.LogLevelOff
	}
}

// NewHTTPClient - create a http.Client with a connect timeout
func NewHTTPClient() http.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          10,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       10 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}
	client := http.Client{Transport: transport}
	return client
}

func newEOSFileSystem() *eos.FileSystem {
	e := &eos.EOS{}
	filesystem := e.SetupFileSystem()
	return filesystem
}

// checkMinio - check to see if minio is up
func checkMinio(ctx context.Context) {
	client := NewHTTPClient()

	endpoint := os.Getenv("HEALTHCHECK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:9000"
	}

	eos.EOSLogger.Debug(ctx, "Performing GET request to %s", endpoint)
	resp, err := client.Get(endpoint)
	if err != nil {
		eos.EOSLogger.Debug(ctx, "Get request failed: %s", err)
		os.Exit(11)
	}

	// We aren't authenticated so we expect a 403
	if resp.StatusCode != 403 {
		eos.EOSLogger.Debug(ctx, "Get request failed: HTTP response status is not Forbidden")
		os.Exit(12)
	}

	eos.EOSLogger.Debug(ctx, "GET request succeeded")
}

func checkEOS(ctx context.Context) {
	fs := newEOSFileSystem()

	var err error
	healthCheckContent := []byte("hello world")

	_, err = fs.IsDir(ctx, "/")
	if err != nil {
		eos.EOSLogger.Debug(ctx, "EOS: %s", err)
		os.Exit(21)
	}

	buf := bytes.NewReader(healthCheckContent)
	_, err = fs.PutBuffer(ctx, "/tmp", healthCheckFile, buf)
	if err != nil {
		eos.EOSLogger.Debug(ctx, "EOS: %s", err)
		os.Exit(22)
	}

	err = fs.Rm(ctx, healthCheckFile)
	if err != nil {
		eos.EOSLogger.Debug(ctx, "EOS: %s", err)
		os.Exit(23)
	}
}

func main() {
	configure()
	ctx := context.Background()
	//checkMinio(ctx) temporarily disabling as it fails due to lack of auth and fills the logs with spurious errors
	checkEOS(ctx)
	eos.EOSLogger.Debug(ctx, "All checks passed")
}
