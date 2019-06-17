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
	loglevel, ok := strconv.Atoi(os.Getenv("EOSLOGLEVEL"))
	if ok != nil {
		loglevel = 0
	}

	const CLR_W = "\x1b[37;1m"
	const CLR_B = "\x1b[34;1m"
	const CLR_Y = "\x1b[33;1m"
	const CLR_G = "\x1b[32;1m"
	const CLR_N = "\x1b[0m"

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

	procuserMax := 12
	ret, err := strconv.Atoi(os.Getenv("EOSMAXPROCUSER"))
	if err == nil {
		procuserMax = ret
	}

	webdavMax := 0
	ret, err = strconv.Atoi(os.Getenv("EOSMAXWEBDAV"))
	if err == nil {
		webdavMax = ret
	}

	xrdcpMax := 0
	ret, err = strconv.Atoi(os.Getenv("EOSMAXXRDCP"))
	if err == nil {
		xrdcpMax = ret
	}

	xrootdMax := 0
	ret, err = strconv.Atoi(os.Getenv("EOSMAXXROOTD"))
	if err == nil {
		xrootdMax = ret
	}

	waitSleep := 100
	ret, err = strconv.Atoi(os.Getenv("EOSSLEEP"))
	if err == nil {
		waitSleep = ret
	}

	validbuckets := true
	if os.Getenv("EOSVALIDBUCKETS") == "false" {
		validbuckets = false
	}

	logger.Info("EOS URL: %s", os.Getenv("EOS"))
	logger.Info("EOS VOLUME PATH: %s", os.Getenv("VOLUME_PATH"))
	logger.Info("EOS USER (uid:gid): %s (%s:%s)", os.Getenv("EOSUSER"), os.Getenv("EOSUID"), os.Getenv("EOSGID"))
	logger.Info("EOS file hooks url: %s", os.Getenv("HOOKSURL"))
	logger.Info("EOS SCRIPTS PATH: %s", os.Getenv("SCRIPTS"))

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

	logger.Info("EOS READ METHOD: %s", readmethod)
	logger.Info("EOS /proc/user MAX: %d", procuserMax)
	logger.Info("EOS WebDav MAX: %d", webdavMax)
	logger.Info("EOS xrdcp MAX: %d", xrdcpMax)
	logger.Info("EOS xrootd MAX: %d", xrootdMax)
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
	return false //hahahahaha
}
