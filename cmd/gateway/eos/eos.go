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
	"fmt"
	"context"
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
	readMethodXrdcp string = "xrdcp"
)

// Define the log level globally because.. quick fix?
var MaxLogLevel int;

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

	// Init the stat cache used by eosObjects and eosFS
	statCache := NewStatCache(os.Getenv("VOLUME_PATH"))

	// Init filesystem
	filesystem := &eosFS{
		MGMHost: os.Getenv("EOS"),
		Path: os.Getenv("VOLUME_PATH"),
		User: os.Getenv("EOSUSER"),
		UID: os.Getenv("EOSUID"),
		GID: os.Getenv("EOSGID"),
		ReadMethod: readmethod,
		Scripts: os.Getenv("SCRIPTS"),
		StatCache: statCache,
	}

	// and go
	return &eosObjects{
		loglevel:     MaxLogLevel,
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

// Logging method
const (
	LogLevelStat int = 99
	LogLevelOff int = 0
	LogLevelError int = 1
	LogLevelInfo int = 2
	LogLevelDebug int = 3
)

func Log(level int, ctx context.Context, format string, a ...interface{}) {
	if ctx == nil {
		ctx = context.TODO()
	}
	// Always log the stat level commands
	if level <= MaxLogLevel || level == LogLevelStat {
	        switch level {
	        case LogLevelError:
	                err := fmt.Errorf(format, a...)
	                logger.LogIf(ctx, err)
	        case LogLevelDebug:
			logger.Info("DEBUG: "+format, a...)
	        default:
	                logger.Info(format, a...)
	        }
	}
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
        SleepShort int = 10
        SleepLong int = 1000
)

// Sleep for t milliseconds
func SleepMs(t int) {
        time.Sleep(time.Duration(t) * time.Millisecond)
}

// Default sleep function
func Sleep() {
        SleepMs(SleepDefault)
}


