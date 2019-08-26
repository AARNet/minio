/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// Xrdcp ... struct
type Xrdcp struct {
	Path    string
	MGMHost string
	UID     string
	GID     string
}

// AbsoluteEOSPath Normalise an EOS path
func (x *Xrdcp) AbsoluteEOSPath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", errFilePathBad
	}
	path = strings.ReplaceAll(path, "//", "/")
	eosPath = strings.TrimSuffix(x.Path+"/"+path, ".")
	eosPath = filepath.Clean(eosPath)
	return eosPath, nil
}

// FileExists Check if a file or directory exists (returns true on error or file existence, otherwise false)
func (x *Xrdcp) FileExists(ctx context.Context, path string) (bool, error) {
	_, retc, err := x.Ls(ctx, "sdF", path)
	if retc == 0 {
		return true, nil
	}

	if retc == 1 && err != nil {
		return true, err
	}

	return false, nil
}

// IsDir Check if a path is a file or directory
func (x *Xrdcp) IsDir(ctx context.Context, path string) (bool, error) {
	result, _, err := x.Ls(ctx, "dF", path)
	if err == nil && strings.HasSuffix(result, "/") {
		return true, nil
	}
	return false, err
}

// Ls Perform an "ls" using xrdcp
func (x *Xrdcp) Ls(ctx context.Context, lsflags string, path string) (string, int64, error) {
	rooturl, err := url.QueryUnescape(fmt.Sprintf("root://%s//proc/user/?mgm.cmd=ls&mgm.option=%s&mgm.path=%s", x.MGMHost, lsflags, path))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Xrdcp.Ls", fmt.Sprintf("ERROR: can not url.QueryUnescape() [path: %s, uri: %s]", path, rooturl), err)
		return "", 1, err
	}
	eosLogger.Log(ctx, LogLevelStat, "Xrdcp.Ls", fmt.Sprintf("EOScmd: xrdcp.LS: [path: %s, rooturl: %s]", path, rooturl), nil)

	cmd := exec.CommandContext(ctx, "/usr/bin/xrdcp", "-s", rooturl, "-")
	output, err := cmd.CombinedOutput()

	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Xrdcp.Ls", fmt.Sprintf("ERROR: can not /usr/bin/xrdcp %s", rooturl), err)
		return "", 1, err
	}

	outputStr := string(output)
	outputStr = strings.TrimSpace(outputStr)

	stdout, stderr, retc := x.ParseOutput(ctx, outputStr)
	eosLogger.Log(ctx, LogLevelDebug, "Xrdcp.Ls", fmt.Sprintf("xrdcp result [stdout: %s, stderr: %s, retc: %d]", stdout, stderr, retc), err)
	if retc > 0 {
		if stderr != "" {
			return "", retc, errors.New(stderr)
		}
		return "", retc, errors.New("Unknown error when parsing ls result")
	}

	return stdout, retc, err
}

// ParseOutput Helper method to parse xrdcp result format
func (x *Xrdcp) ParseOutput(ctx context.Context, result string) (string, string, int64) {

	stdoutidx := strings.Index(result, "mgm.proc.stdout=")
	stderridx := strings.Index(result, "&mgm.proc.stderr=")
	retcidx := strings.Index(result, "&mgm.proc.retc=")

	retc := StringToInt(result[(retcidx + len("&mgm.proc.retc=")):])
	stderr := strings.TrimSpace(result[(stderridx + len("&mgm.proc.stderr=")):retcidx])
	stdout := strings.TrimSpace(result[(stdoutidx + len("mgm.proc.stdout=")):stderridx])

	return stdout, stderr, retc

}

// Find - use find -I to get file information
func (x *Xrdcp) Find(ctx context.Context, path string) ([]map[string]string, error) {
	rooturl, err := url.QueryUnescape(fmt.Sprintf("root://%s//proc/user/?mgm.cmd=find&mgm.option=I&mgm.find.maxdepth=1&mgm.path=%s", x.MGMHost, path))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "xrdcpFind", fmt.Sprintf("ERROR: can not url.QueryUnescape() [path: %s, uri: %s]", path, rooturl), err)
		return nil, err
	}
	eosLogger.Log(ctx, LogLevelStat, "Xrdcp.Find", fmt.Sprintf("EOScmd: xrdcp.FIND: [path: %s, rooturl: %s]", path, rooturl), nil)

	cmd := exec.CommandContext(ctx, "/usr/bin/xrdcp", "-s", rooturl, "-")
	pipe, _ := cmd.StdoutPipe()

	if err := cmd.Start(); err != nil {
		eosLogger.Log(ctx, LogLevelError, "xrdcpFind", fmt.Sprintf("ERROR: can not /usr/bin/xrdcp %s", rooturl), err)
		return nil, err
	}
	defer cmd.Wait()

	parsedobjects := make([]map[string]string, 0)
	var object string
	reader := bufio.NewReader(pipe)
	for err == nil {
		object, err = reader.ReadString('\n')
		eosLogger.Log(ctx, LogLevelDebug, "xrdcpFind", fmt.Sprintf("Object: %s", object), err)
		// First result is prefixed with &mgm.proc.stdout=, so strip it
		if strings.Index(object, "&mgm.proc.stdout=") == 0 {
			object = object[len("&mgm.proc.stdout="):]
		}

		// Once you get &mgm.proc.stderr=, you have hit the end
		if strings.Index(object, "&mgm.proc.stderr=") == 0 {
			errormsg := strings.TrimSpace(object[len("&mgm.proc.stderr="):])
			if errormsg != "" && errormsg != "&mgm.proc.retc=0" {
				return nil, errors.New(errormsg)
			}
			break
		}

		parsed := x.ParseFileInfo(ctx, object)
		if parsed != nil {
			parsedobjects = append(parsedobjects, parsed)
		}
	}

	// Make sure we close the pipe so the subprocess doesn't keep running
	if closePipeErr := pipe.Close(); closePipeErr != nil && err == nil {
		err = closePipeErr
	}

	return parsedobjects, nil
}

// Fileinfo use fileinfo -m to get file info
func (x *Xrdcp) Fileinfo(ctx context.Context, path string) ([]map[string]string, error) {
	rooturl, err := url.QueryUnescape(fmt.Sprintf("root://%s//proc/user/?mgm.cmd=fileinfo&mgm.file.info.option=-m&mgm.path=%s", x.MGMHost, path))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Xrdcp.Fileinfo", fmt.Sprintf("ERROR: can not url.QueryUnescape() [path: %s, uri: %s]", path, rooturl), err)
		return nil, err
	}
	eosLogger.Log(ctx, LogLevelStat, "Xrdcp.Fileinfo", fmt.Sprintf("EOScmd: xrdcp.FILEINFO: [path: %s, rooturl: %s]", path, rooturl), nil)

	cmd := exec.CommandContext(ctx, "/usr/bin/xrdcp", "-s", rooturl, "-")
	output, err := cmd.CombinedOutput()

	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Xrdcp.Fileinfo", fmt.Sprintf("ERROR: can not /usr/bin/xrdcp %s", rooturl), err)
		return nil, err
	}

	outputStr := strings.TrimSpace(string(output))
	stdout, stderr, retc := x.ParseOutput(ctx, outputStr)

	if retc != 0 {
		return nil, errors.New(stderr)
	}

	parsedobjects := make([]map[string]string, 0)
	parsed := x.ParseFileInfo(ctx, stdout)
	if parsed != nil {
		parsedobjects = append(parsedobjects, parsed)
	}

	return parsedobjects, nil
}

// ParseFileInfo parses the xrdcp formatted result into a named array
func (x *Xrdcp) ParseFileInfo(ctx context.Context, object string) map[string]string {
	object = strings.TrimSpace(object)
	if object == "" {
		return nil
	}

	// First pair should be keylength.file, which contains the filename length
	// so split the string on space once to get that value
	split := strings.SplitN(object, " ", 2)
	if len(split) < 2 {
		return nil
	}
	_, keytmp := SplitKeyValuePair(split[0])
	keylength, err := strconv.Atoi(keytmp)
	if err != nil {
		fmt.Println(err) // Log with warning here
		return nil
	}
	keylength = keylength + 5 // add 5 to it to include the "file=" prefix

	// Check the second part of the split string starts with file=
	if split[1][0:5] != "file=" {
		return nil
	}

	// Check that there'd be a character after file=
	if len(split[1]) < 6 {
		eosLogger.Log(ctx, LogLevelDebug, "xrdcpFindParseResult", fmt.Sprintf("Object not long enough [object: %s]", object), nil)
		return nil
	}

	// Check to make sure we hav enough characters to get the filename
	if len(split[1]) < keylength {
		eosLogger.Log(ctx, LogLevelDebug, "xrdcpFindParseResult", fmt.Sprintf("keylength.file longer than object [object: %s]", object), nil)
		return nil
	}

	// Get the filename using the filename length
	// to avoid splitting on spaces in the filename
	filename := split[1][5:keylength]

	// Remove the filename from the object
	object = split[1][keylength:len(split[1])]

	// Get the rest of the key value pairs
	m := make(map[string]string)
	m["file"] = filename
	kvpairs := strings.Split(object, " ")
	for idx, pair := range kvpairs {
		key, value := SplitKeyValuePair(pair)
		if key != "" {
			// If its an xattr, get the value from the next pair
			if key == "xattrn" {
				key = value
				_, value = SplitKeyValuePair(kvpairs[(idx + 1)])
			}
			// This is handled when xattrn is found, so skip it
			if key == "xattrv" || key == "" {
				continue
			}
			m[key] = value
		}
	}
	m["is_file"] = "true"
	// Let's just look for all the attributes that lead
	// to it being a directory.
	if _, ok := m["container"]; ok {
		m["is_file"] = "false"
	} else if _, ok := m["treesize"]; ok {
		m["is_file"] = "false"
	} else if _, ok := m["files"]; ok {
		m["is_file"] = "false"
	}
	return m
}

// Put ... puts a file
func (x *Xrdcp) Put(ctx context.Context, src, dst string, size int64) error {
	eospath, err := x.AbsoluteEOSPath(dst)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s&eos.bookingsize=%d", x.MGMHost, eospath, x.UID, x.GID, size))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Xrdcp.Put", fmt.Sprintf("ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl), err)
		return err
	}

	eosLogger.Log(ctx, LogLevelStat, "Xrdcp.Put", fmt.Sprintf("EOScmd: xrdcp.PUT: [eospath: %s, eosurl: %s]", eospath, eosurl), nil)

	cmd := exec.Command("/usr/bin/xrdcp", "-N", "-f", "-p", src, eosurl)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Xrdcp.Put", fmt.Sprintf("ERROR: can not /usr/bin/xrdcp -N -f -p %s %s [eospath: %s, eosurl: %s]", src, eosurl, eospath, eosurl), err)
	}
	output := strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))
	if output != "" {
		eosLogger.Log(ctx, LogLevelInfo, "Xrdcp.Put", output, nil)
	}

	return err
}

// ReadChunk ... reads a chunk
func (x *Xrdcp) ReadChunk(ctx context.Context, p string, offset, length int64, data io.Writer) (err error) {
	eospath, err := x.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s", x.MGMHost, eospath, x.UID, x.GID))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "ReadChunk", fmt.Sprintf("ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl), err)
		return err
	}

	eosLogger.Log(ctx, LogLevelStat, "ReadChunk", fmt.Sprintf("EOScmd: xrdcp.GET: [eosurl: %s]", eosurl), nil)

	cmd := exec.Command("/usr/bin/xrdcp", "-N", eosurl, "-")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err2 := cmd.Run()

	errStr := strings.TrimSpace(stderr.String())
	eosLogger.Log(ctx, LogLevelInfo, "ReadChunk", fmt.Sprintf("/usr/bin/xrdcp -N %s - %+v", eosurl, err2), nil)
	if errStr != "" {
		eosLogger.Log(ctx, LogLevelError, "ReadChunk", errStr, nil)
	}

	if offset >= 0 {
		stdout.Next(int(offset))
	}
	stdout.Truncate(int(length))
	stdout.WriteTo(data)
	return err
}
