package eos

import (
	"os"
	"strconv"
	"strings"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
)

// EOS implements Gateway.
type EOS struct {
	path string
}

// Name implements Gateway interface.
func (g *EOS) Name() string {
	return eosBackend
}

// NewGatewayLayer returns eos gatewaylayer.
func (g *EOS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	const CLR_W = "\x1b[37;1m"
	const CLR_B = "\x1b[34;1m"
	const CLR_Y = "\x1b[33;1m"
	const CLR_G = "\x1b[32;1m"
	const CLR_N = "\x1b[0m"

	loglevel, ok := strconv.Atoi(os.Getenv("EOSLOGLEVEL"))
	if ok != nil {
		loglevel = 0
	}

	stage := os.Getenv("EOSSTAGE")
	if stage != "" {
		os.MkdirAll(stage, 0700)
	}

	readonly := false
	if os.Getenv("EOSREADONLY") == "true" {
		readonly = true
	}

	readmethod := "webdav"
	if strings.ToLower(os.Getenv("EOSREADMETHOD")) == "xrootd" {
		readmethod = "xrootd"
	} else if strings.ToLower(os.Getenv("EOSREADMETHOD")) == "xrdcp" {
		readmethod = "xrdcp"
	}

	waitSleep := 100
	ret, err := strconv.Atoi(os.Getenv("EOSSLEEP"))
	if err == nil {
		waitSleep = ret
	}

	validbuckets := true
	if os.Getenv("EOSVALIDBUCKETS") == "false" {
		validbuckets = false
	}

	if stage != "" {
		logger.Info("EOS staging: %s", stage)
	} else {
		logger.Info("EOS staging: DISABLED")
	}

	if readonly {
		logger.Info("EOS read only mode: ENABLED")
	}

	if !validbuckets {
		logger.Info("EOS allowing invalid bucket names (RISK)")
	}

	logger.Info("EOS URL: %s", os.Getenv("EOS"))
	logger.Info("EOS VOLUME PATH: %s", os.Getenv("VOLUME_PATH"))
	logger.Info("EOS USER (uid:gid): %s (%s:%s)", os.Getenv("EOSUSER"), os.Getenv("EOSUID"), os.Getenv("EOSGID"))
	logger.Info("EOS file hooks url: %s", os.Getenv("HOOKSURL"))
	logger.Info("EOS SCRIPTS PATH: %s", os.Getenv("SCRIPTS"))
	logger.Info("EOS READ METHOD: %s", readmethod)
	logger.Info("EOS SLEEP: %d", waitSleep)
	logger.Info("EOS LOG LEVEL: %d", loglevel)

	return &eosObjects{
		loglevel:     loglevel,
		url:          os.Getenv("EOS"),
		path:         os.Getenv("VOLUME_PATH"),
		hookurl:      os.Getenv("HOOKSURL"),
		scripts:      os.Getenv("SCRIPTS"),
		user:         os.Getenv("EOSUSER"),
		uid:          os.Getenv("EOSUID"),
		gid:          os.Getenv("EOSGID"),
		stage:        stage,
		readonly:     readonly,
		readmethod:   readmethod,
		waitSleep:    waitSleep,
		validbuckets: validbuckets,
	}, nil
}

// Production - eos gateway is production ready.
func (g *EOS) Production() bool {
	return false
}
