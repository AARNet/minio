/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
	"os"
	"strconv"
	"strings"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
)

// EOS implements Gateway.
type EOS struct {
	path string
}

const (
	readMethodWebdav string = "webdav"
	readMethodXrootd string = "xrootd"
	readMethodXrdcp  string = "xrdcp"
)

// MaxLogLevel defines the log level globally because.. quick fix?
var MaxLogLevel int

// Name implements Gateway interface.
func (g *EOS) Name() string {
	return eosBackend
}

// NewGatewayLayer returns eos gatewaylayer.
func (g *EOS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	// TODO: Move loglevel to something other than eosObjects
	loglevel, ok := strconv.Atoi(os.Getenv("EOSLOGLEVEL"))
	if ok != nil {
		MaxLogLevel = LogLevelOff
	} else {
		MaxLogLevel = loglevel
	}

	stage := os.Getenv("EOSSTAGE")
	if stage != "" {
		os.MkdirAll(stage, 0700)
	}

	readonly := false
	if os.Getenv("EOSREADONLY") == "true" {
		readonly = true
	}

	readmethod := readMethodWebdav
	if strings.ToLower(os.Getenv("EOSREADMETHOD")) == readMethodXrootd {
		readmethod = readMethodXrootd
	} else if strings.ToLower(os.Getenv("EOSREADMETHOD")) == readMethodXrdcp {
		readmethod = readMethodXrdcp
	}

	validbuckets := true
	if os.Getenv("EOSVALIDBUCKETS") == "false" {
		validbuckets = false
	}

	// Require a staging area for puts
	if stage == "" {
		stage = "/tmp"
	}
	eosLogger.Startup("EOS staging: %s", stage)

	if readonly {
		eosLogger.Startup("EOS read only mode: ENABLED")
	}

	if !validbuckets {
		eosLogger.Startup("EOS allowing invalid bucket names (RISK)")
	}

	httphost := os.Getenv("EOSHTTPHOST")
	if httphost == "" {
		httphost = os.Getenv("EOS") + ":8000"
	}

	foregroundStaging := false
	if os.Getenv("FOREGROUND_STAGING_TRANSFER") == "true" {
		foregroundStaging = true
	}

	eosLogger.Startup("EOS URL: %s", os.Getenv("EOS"))
	eosLogger.Startup("EOS HTTP URL: %s", httphost)
	eosLogger.Startup("EOS HTTP Proxy: %s", os.Getenv("EOS_HTTP_PROXY"))
	eosLogger.Startup("EOS VOLUME PATH: %s", os.Getenv("VOLUME_PATH"))
	eosLogger.Startup("EOS USER (uid:gid): %s (%s:%s)", os.Getenv("EOSUSER"), os.Getenv("EOSUID"), os.Getenv("EOSGID"))
	eosLogger.Startup("EOS file hooks url: %s", os.Getenv("HOOKSURL"))
	eosLogger.Startup("EOS SCRIPTS PATH: %s", os.Getenv("SCRIPTS"))
	eosLogger.Startup("EOS READ METHOD: %s", readmethod)
	eosLogger.Startup("EOS FOREGROUND TRANSFER FROM STAGING: %t", foregroundStaging)
	eosLogger.Startup("EOS LOG LEVEL: %d", loglevel)

	// Init filesystem
	xrdcp := &Xrdcp{
		MGMHost: os.Getenv("EOS"),
		Path:    os.Getenv("VOLUME_PATH"),
		User:    os.Getenv("EOSUSER"),
		UID:     os.Getenv("EOSUID"),
		GID:     os.Getenv("EOSGID"),
	}

	filesystem := &eosFS{
		MGMHost:    os.Getenv("EOS"),
		HTTPHost:   httphost,
		Proxy:      os.Getenv("EOS_HTTP_PROXY"),
		Path:       os.Getenv("VOLUME_PATH"),
		User:       os.Getenv("EOSUSER"),
		UID:        os.Getenv("EOSUID"),
		GID:        os.Getenv("EOSGID"),
		ReadMethod: readmethod,
		Scripts:    os.Getenv("SCRIPTS"),
		StatCache:  NewRequestStatCache(os.Getenv("VOLUME_PATH")),
		Xrdcp:      xrdcp,
	}

	// and go
	return &eosObjects{
		path:              os.Getenv("VOLUME_PATH"),
		hookurl:           os.Getenv("HOOKSURL"),
		stage:             stage,
		validbuckets:      validbuckets,
		TransferList:      NewTransferList(),
		FileSystem:        filesystem,
		readonly:          readonly,
		foregroundStaging: foregroundStaging,
	}, nil
}

// Production - eos gateway is production ready.
func (g *EOS) Production() bool {
	return false
}
