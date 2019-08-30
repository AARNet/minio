package eos

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/minio/minio/cmd/logger"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Logging method
const (
	LogLevelStat  int = 99
	LogLevelOff   int = 0
	LogLevelError int = 1
	LogLevelInfo  int = 2
	LogLevelDebug int = 3
)

type eosLog struct{}

// LogEntry defines the structure of a log entry made by the EOS gateway
type LogEntry struct {
	Level      string   `json:"level"`
	RequestID  string   `json:"request_id,omitempty"`
	Time       string   `json:"time"`
	Message    string   `json:"message"`
	Method     string   `json:"method,omitempty"`
	RemoteHost string   `json:"remote_host,omitempty"`
	UserAgent  string   `json:"useragent,omitempty"`
	Error      string   `json:"error,omitempty"`
	Source     string   `json:"source,omitempty"`
	Trace      []string `json:"trace,omitempty"`
}

var eosLogger eosLog

func (e *eosLog) Log(ctx context.Context, level int, method string, message string, err error) {
	var levelString string
	switch level {
	case LogLevelError:
		levelString = "ERROR"
	case LogLevelDebug:
		levelString = "DEBUG"
	default:
		levelString = "INFO"
	}

	if level <= MaxLogLevel || level == LogLevelStat {
		req := logger.GetReqInfo(ctx)
		entry := &LogEntry{
			Level:      levelString,
			Time:       time.Now().UTC().Format(time.RFC3339Nano),
			Method:     method,
			Message:    message,
			RequestID:  req.RequestID,
			RemoteHost: req.RemoteHost,
			UserAgent:  req.UserAgent,
			Error:      fmt.Sprintf("%+v", err),
		}
		if err != nil {
			entry.Trace = getTrace(3)
		}

		jsonEntry, err := json.Marshal(entry)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonEntry))
	}
}

// Yoinked from https://github.com/minio/minio/blob/master/cmd/logger/logger.go
var matchingFuncNames = [...]string{
	"http.HandlerFunc.ServeHTTP",
	"cmd.serverMain",
	"cmd.StartGateway",
	"cmd.(*webAPIHandlers).ListBuckets",
	"cmd.(*webAPIHandlers).MakeBucket",
	"cmd.(*webAPIHandlers).DeleteBucket",
	"cmd.(*webAPIHandlers).ListObjects",
	"cmd.(*webAPIHandlers).RemoveObject",
	"cmd.(*webAPIHandlers).Login",
	"cmd.(*webAPIHandlers).GenerateAuth",
	"cmd.(*webAPIHandlers).SetAuth",
	"cmd.(*webAPIHandlers).GetAuth",
	"cmd.(*webAPIHandlers).CreateURLToken",
	"cmd.(*webAPIHandlers).Upload",
	"cmd.(*webAPIHandlers).Download",
	"cmd.(*webAPIHandlers).DownloadZip",
	"cmd.(*webAPIHandlers).GetBucketPolicy",
	"cmd.(*webAPIHandlers).ListAllBucketPolicies",
	"cmd.(*webAPIHandlers).SetBucketPolicy",
	"cmd.(*webAPIHandlers).PresignedGet",
	"cmd.(*webAPIHandlers).ServerInfo",
	"cmd.(*webAPIHandlers).StorageInfo",
	// add more here ..
}
var trimStrings []string

func getTrace(traceLevel int) []string {
	var trace []string
	pc, file, lineNumber, ok := runtime.Caller(traceLevel)

	for ok && file != "" {
		// Clean up the common prefixes
		file = trimTrace(file)
		// Get the function name
		_, funcName := filepath.Split(runtime.FuncForPC(pc).Name())
		// Skip duplicate traces that start with file name, "<autogenerated>"
		// and also skip traces with function name that starts with "runtime."
		if !strings.HasPrefix(file, "<autogenerated>") &&
			!strings.HasPrefix(funcName, "runtime.") {
			// Form and append a line of stack trace into a
			// collection, 'trace', to build full stack trace
			trace = append(trace, fmt.Sprintf("%v:%v:%v()", file, lineNumber, funcName))

			// Ignore trace logs beyond the following conditions
			for _, name := range matchingFuncNames {
				if funcName == name {
					return trace
				}
			}
		}
		traceLevel++
		// Read stack trace information from PC
		pc, file, lineNumber, ok = runtime.Caller(traceLevel)
	}
	return trace
}

func trimTrace(f string) string {
	for _, trimString := range trimStrings {
		f = strings.TrimPrefix(filepath.ToSlash(f), filepath.ToSlash(trimString))
	}
	return filepath.FromSlash(f)
}
