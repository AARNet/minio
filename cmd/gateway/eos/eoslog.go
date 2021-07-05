package eos

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
)

// Logging method
const (
	LogLevelStat  int = 99
	LogLevelOff   int = 0
	LogLevelError int = 1
	LogLevelInfo  int = 2
	LogLevelDebug int = 3
)

type EOSLog struct{}

// LogEntry defines the structure of a log entry made by the EOS gateway
type LogEntry struct {
	Time       string   `json:"time"`
	Level      string   `json:"level"`
	RequestID  string   `json:"request_id,omitempty"`
	Message    string   `json:"message"`
	Method     string   `json:"method,omitempty"`
	RemoteHost string   `json:"remote_host,omitempty"`
	UserAgent  string   `json:"useragent,omitempty"`
	Error      string   `json:"error,omitempty"`
	Source     string   `json:"source,omitempty"`
	Trace      []string `json:"trace,omitempty"`
}

var EOSLogger EOSLog

// Debug -
func (e *EOSLog) Debug(ctx context.Context, messagefmt string, args ...interface{}) {
	if LogLevelDebug <= MaxLogLevel {
		methodName := e.GetFunctionName()
		e.Log(ctx, LogLevelDebug, methodName, fmt.Sprintf(messagefmt, args...), nil)
	}
}

// Error -
func (e *EOSLog) Error(ctx context.Context, err error, messagefmt string, args ...interface{}) {
	if LogLevelError <= MaxLogLevel {
		methodName := e.GetFunctionName()
		e.Log(ctx, LogLevelError, methodName, fmt.Sprintf(messagefmt, args...), err)
	}
}

// Info -
func (e *EOSLog) Info(ctx context.Context, messagefmt string, args ...interface{}) {
	if LogLevelInfo <= MaxLogLevel {
		methodName := e.GetFunctionName()
		e.Log(ctx, LogLevelInfo, methodName, fmt.Sprintf(messagefmt, args...), nil)
	}
}

// Stat -
func (e *EOSLog) Stat(ctx context.Context, messagefmt string, args ...interface{}) {
	// Always log stats.
	methodName := e.GetFunctionName()
	e.Log(ctx, LogLevelStat, methodName, fmt.Sprintf(messagefmt, args...), nil)
}

// GetFunctionName gets the name of the function that called Debug/Error/Info/Stat
func (e *EOSLog) GetFunctionName() string {
	pc := make([]uintptr, 15)
	n := runtime.Callers(3, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	frame, _ = frames.Next() // Since this is called by a log function, it's 2 frames up from where we are
	return filepath.Base(frame.Function)
}

// Startup -
func (e *EOSLog) Startup(messagefmt string, args ...interface{}) {
	e.Log(context.TODO(), LogLevelInfo, "", fmt.Sprintf(messagefmt, args...), nil)
}

// Log actually logs the message
func (e *EOSLog) Log(ctx context.Context, level int, method string, message string, err error) {

	var levelString string
	switch level {
	case LogLevelError:
		levelString = "ERROR"
	case LogLevelDebug:
		levelString = "DEBUG"
	default:
		levelString = "INFO"
	}

	format := "2006-01-02 15:04:05.00"
	if level <= MaxLogLevel || level == LogLevelStat {
		req := logger.GetReqInfo(ctx)
		entry := &LogEntry{
			Level: levelString,
			Time:  time.Now().UTC().Format(format),
			//Time:       time.Now().UTC().Format(time.RFC3339Nano),
			Method:     method,
			Message:    message,
			RequestID:  req.RequestID,
			RemoteHost: req.RemoteHost,
			UserAgent:  req.UserAgent,
		}
		if err != nil {
			entry.Error = fmt.Sprintf("%+v", err)
			entry.Trace = getTrace(3)
		}

		jsonEntry, err := json.Marshal(entry)
		if err != nil {
			panic(err)
		}
		// Don't log the LivenessChecks unless we're in Debugging
		if method != "cmd.LivenessCheckHandler" {
			fmt.Println(string(jsonEntry))
		}
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
