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
	MaxLogLevel = g.GetLogLevel()
	stage := g.SetupStageArea()

	if g.IsReadOnly() {
		eosLogger.Startup("EOS read only mode: ENABLED")
	}

	if !g.GetValidBuckets() {
		eosLogger.Startup("EOS allowing invalid bucket names (RISK)")
	}

	eosLogger.Startup("EOS staging: %s", stage)
	eosLogger.Startup("EOS URL: %s", os.Getenv("EOS"))
	eosLogger.Startup("EOS HTTP URL: %s", g.GetHTTPHost())
	eosLogger.Startup("EOS HTTP Proxy: %s", os.Getenv("EOS_HTTP_PROXY"))
	eosLogger.Startup("EOS VOLUME PATH: %s", os.Getenv("VOLUME_PATH"))
	eosLogger.Startup("EOS USER (uid:gid): %s (%s:%s)", os.Getenv("EOSUSER"), os.Getenv("EOSUID"), os.Getenv("EOSGID"))
	eosLogger.Startup("EOS file hooks url: %s", os.Getenv("HOOKSURL"))
	eosLogger.Startup("EOS SCRIPTS PATH: %s", os.Getenv("SCRIPTS"))
	eosLogger.Startup("EOS READ METHOD: %s", g.GetReadMethod())
	eosLogger.Startup("EOS MAX RETRY: %d", g.GetMaxRetry())
	eosLogger.Startup("EOS OVERWRITE MAX KEYS: %d", g.GetMaxKeysOverride())
	eosLogger.Startup("EOS SORT FILE LISTING: %t", g.GetSort())
	eosLogger.Startup("EOS LOG LEVEL: %d", MaxLogLevel)

	// and go
	return &eosObjects{
		maxRetry:     g.GetMaxRetry(),
		maxKeys:      g.GetMaxKeysOverride(),
		path:         os.Getenv("VOLUME_PATH"),
		hookurl:      os.Getenv("HOOKSURL"),
		stage:        stage,
		validbuckets: g.GetValidBuckets(),
		TransferList: NewTransferList(),
		FileSystem:   g.SetupFileSystem(),
		readonly:     g.IsReadOnly(),
	}, nil
}

// Production - eos gateway is production ready.
func (g *EOS) Production() bool {
	return false
}

// GetLogLevel - returns the configured log level
func (g *EOS) GetLogLevel() int {
	loglevel, ok := strconv.Atoi(os.Getenv("EOSLOGLEVEL"))
	if ok != nil {
		loglevel = LogLevelOff
	}
	return loglevel
}

// GetReadMethod - returns the configured read method
func (g *EOS) GetReadMethod() string {
	readmethod := readMethodWebdav
	if strings.ToLower(os.Getenv("EOSREADMETHOD")) == readMethodXrootd {
		readmethod = readMethodXrootd
	} else if strings.ToLower(os.Getenv("EOSREADMETHOD")) == readMethodXrdcp {
		readmethod = readMethodXrdcp
	}
	return readmethod
}

// GetHTTPHost - return the host configured by the environment
func (g *EOS) GetHTTPHost() string {
	httphost := os.Getenv("EOSHTTPHOST")
	if httphost == "" {
		httphost = os.Getenv("EOS") + ":8000"
	}
	return httphost
}

// GetMaxRetry - return the max number of retries as configured by the environment
func (g *EOS) GetMaxRetry() int {
	maxRetry, ok := strconv.Atoi(os.Getenv("MAX_RETRY"))
	if ok != nil {
		maxRetry = 10
	}
	return maxRetry
}

// IsReadOnly - return whether the gateway has been configured as read only
func (g *EOS) IsReadOnly() bool {
	readonly := false
	if os.Getenv("EOSREADONLY") == "true" {
		readonly = true
	}
	return readonly
}

// GetSort - return whether or not to sort the file listing
func (g *EOS) GetSort() bool {
	sort, ok := strconv.Atoi(os.Getenv("EOSSORTFILELISTING"))
	if ok != nil {
		sort = 0
	}
	return (sort > 0)
}

// GetValidBuckets - return whether valid bucket names are required
func (g *EOS) GetValidBuckets() bool {
	validbuckets := true
	if os.Getenv("EOSVALIDBUCKETS") == "false" {
		validbuckets = false
	}
	return validbuckets
}

// GetMaxKeysOverride - return the override for the max number of keys returned
func (g *EOS) GetMaxKeysOverride() int {
	maxKeys, ok := strconv.Atoi(os.Getenv("OVERWRITEMAXKEYS"))
	if ok != nil {
		maxKeys = 0
	}
	return maxKeys
}

// SetupStageArea - setup stage area and return the path
func (g *EOS) SetupStageArea() string {
	stage := os.Getenv("EOSSTAGE")
	if stage != "" {
		os.MkdirAll(stage, 0700)
	} else {
		stage = "/tmp"
	}
	return stage
}

// SetupFileSystem - configures the filesystem
func (g *EOS) SetupFileSystem() *EOSFS {
	xrdcp := &Xrdcp{
		MaxRetry: g.GetMaxRetry(),
		MGMHost:  os.Getenv("EOS"),
		Path:     os.Getenv("VOLUME_PATH"),
		User:     os.Getenv("EOSUSER"),
		UID:      os.Getenv("EOSUID"),
		GID:      os.Getenv("EOSGID"),
	}

	filesystem := &EOSFS{
		MaxRetry:   g.GetMaxRetry(),
		Sort:       g.GetSort(),
		MGMHost:    os.Getenv("EOS"),
		HTTPHost:   g.GetHTTPHost(),
		Proxy:      os.Getenv("EOS_HTTP_PROXY"),
		Path:       os.Getenv("VOLUME_PATH"),
		User:       os.Getenv("EOSUSER"),
		UID:        os.Getenv("EOSUID"),
		GID:        os.Getenv("EOSGID"),
		ReadMethod: g.GetReadMethod(),
		Scripts:    os.Getenv("SCRIPTS"),
		StatCache:  NewRequestStatCache(os.Getenv("VOLUME_PATH")),
		Xrdcp:      xrdcp,
	}

	return filesystem
}
