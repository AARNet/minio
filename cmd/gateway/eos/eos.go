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

	// Init filesystem
	xrdcp := &Xrdcp{
		MGMHost: os.Getenv("EOS"),
		Path:    os.Getenv("VOLUME_PATH"),
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
		path:         os.Getenv("VOLUME_PATH"),
		hookurl:      os.Getenv("HOOKSURL"),
		stage:        stage,
		validbuckets: validbuckets,
		TransferList: NewTransferList(),
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

func interfaceToString(in interface{}) string {
	if in == nil {
		return ""
	}
	s, _ := in.(string)
	return strings.TrimSpace(s)
}

// StringToInt converts a string to an int64
func StringToInt(str string) int64 {
	str = strings.TrimSpace(str)
	if str == "" {
		return 0
	}
	i, _ := strconv.ParseInt(str, 10, 64)
	return i
}

// StringToFloat converts a string to a float
func StringToFloat(str string) float64 {
	str = strings.TrimSpace(str)
	if str == "" {
		return 0
	}
	i, _ := strconv.ParseFloat(str, 64)
	return i
}

// StringToBool converts a string to a bool
func StringToBool(str string) bool {
	str = strings.TrimSpace(str)
	if str == "" {
		return false
	}
	b, _ := strconv.ParseBool(str)
	return b
}

const (
	// SleepDefault is the default sleep time in ms
	SleepDefault int = 100
	// SleepShort is a short sleep time in ms
	SleepShort int = 10
	// SleepLong is a longer sleep time in ms
	SleepLong int = 1000
)

// SleepMs pauses for t milliseconds
func SleepMs(t int) {
	time.Sleep(time.Duration(t) * time.Millisecond)
}

// Sleep pauses for SleepDefault
func Sleep() {
	SleepMs(SleepDefault)
}

// SplitKeyValuePair splits a key=value format string into 2 strings (key, value)
func SplitKeyValuePair(pair string) (string, string) {
	tmp := strings.Split(pair, "=")
	if len(tmp) > 1 {
		return tmp[0], tmp[1]
	}
	return "", ""
}