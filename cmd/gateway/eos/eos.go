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
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
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

// Define the log level globally because.. quick fix?
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

	httphost := os.Getenv("EOSHTTPHOST")
	if httphost == "" {
		httphost = os.Getenv("EOS") + ":8000"
	}

	logger.Info("EOS URL: %s", os.Getenv("EOS"))
	logger.Info("EOS HTTP URL: %s", httphost)
	logger.Info("EOS HTTP Proxy: %s", os.Getenv("EOS_HTTP_PROXY"))
	logger.Info("EOS VOLUME PATH: %s", os.Getenv("VOLUME_PATH"))
	logger.Info("EOS USER (uid:gid): %s (%s:%s)", os.Getenv("EOSUSER"), os.Getenv("EOSUID"), os.Getenv("EOSGID"))
	logger.Info("EOS file hooks url: %s", os.Getenv("HOOKSURL"))
	logger.Info("EOS SCRIPTS PATH: %s", os.Getenv("SCRIPTS"))
	logger.Info("EOS READ METHOD: %s", readmethod)
	logger.Info("EOS LOG LEVEL: %d", loglevel)

	// Init the stat cache used by eosObjects and eosFS
	statCache := NewStatCache(os.Getenv("VOLUME_PATH"))

	// Init filesystem
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
	}

	// and go
	return &eosObjects{
		path:         os.Getenv("VOLUME_PATH"),
		hookurl:      os.Getenv("HOOKSURL"),
		stage:        stage,
		validbuckets: validbuckets,
		StatCache:    statCache,
		TransferList: NewTransferList(),
		DirCache:     NewDirCache(),
		BucketCache:  make(map[string]minio.BucketInfo),
		FileSystem:   filesystem,
	}, nil
}

// Production - eos gateway is production ready.
func (g *EOS) Production() bool {
	return false
}

// Interface conversion

func interfaceToInt64(in interface{}) int64 {
	if in == nil {
		return 0
	}
	f, _ := in.(float64)
	return int64(f)
}

func interfaceToUint32(in interface{}) uint32 {
	if in == nil {
		return 0
	}
	f, _ := in.(float64)
	return uint32(f)
}

func interfaceToString(in interface{}) string {
	if in == nil {
		return ""
	}
	s, _ := in.(string)
	return strings.TrimSpace(s)
}

const (
	SleepDefault int = 100
	SleepShort   int = 10
	SleepLong    int = 1000
)

// Sleep for t milliseconds
func SleepMs(t int) {
	time.Sleep(time.Duration(t) * time.Millisecond)
}

// Default sleep function
func Sleep() {
	SleepMs(SleepDefault)
}
