/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 */

package eos

import (
	"path"
	"strconv"
	"strings"
	"net/url"
	"time"
)

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
		return strings.TrimSpace(tmp[0]), strings.TrimSpace(tmp[1])
	}
	return "", ""
}

// PathJoin uses path.Clean and path.Join while retaining the trailing slash
func PathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if strings.HasSuffix(elem[len(elem)-1], "/") {
			trailingSlash = "/"
		}
	}
	return path.Clean(path.Join(elem...)) + trailingSlash
}

// PathDir uses path.Dir() while retaining the trailing slash
func PathDir(elem string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if strings.HasSuffix(elem, "/") {
			trailingSlash = "/"
		}
	}
	return path.Dir(elem) + trailingSlash
}

// UnescapePath - unescape each segment of a path individually
func UnescapePath(str string) (string) {
	// Then replace percentages
	str = strings.Replace(str, "%", "%25", -1)
	unescaped := ""

	// Iterate each segment of the path and unescape it where possible
	splitPath := strings.Split(str, "/")
	for _, part := range splitPath {
		t, err := url.QueryUnescape(part)
		if err != nil {
			unescaped = path.Join(unescaped, part)
		} else {
			unescaped = path.Join(unescaped, t)
		}
	}

	// Add first and last slashes back if they existed previously
	if strings.HasPrefix(str, "/") && !strings.HasPrefix(unescaped, "/") {
		unescaped = "/" + unescaped
	}
	if strings.HasSuffix(str, "/") && !strings.HasSuffix(unescaped, "/") {
		unescaped = unescaped + "/"
	}

	return unescaped
}
