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
	"strconv"
	"strings"
)

// Xrdcp ... struct
type Xrdcp struct {
	Path    string
	MGMHost string
	User    string
	UID     string
	GID     string
}

// GetXrootBase Returns the root:// base URL
func (x *Xrdcp) GetXrootBase() string {
	return fmt.Sprintf("root://%s@%s/", x.User, x.MGMHost)
}

// AbsoluteEOSPath Normalise an EOS path
func (x *Xrdcp) AbsoluteEOSPath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", errFilePathBad
	}
	eosPath = strings.TrimSuffix(PathJoin(x.Path, path), ".")
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
	rooturl, err := url.QueryUnescape(x.GetXrootBase() + "/proc/user/?mgm.cmd=ls&mgm.option=" + lsflags + "&mgm.path=" + path)
	if err != nil {
		eosLogger.Error(ctx, "Xrdcp.Ls", err, "ERROR: can not url.QueryUnescape() [path: %s, uri: %s]", path, rooturl)
		return "", 1, err
	}
	eosLogger.Stat(ctx, "Xrdcp.Ls", "EOScmd: xrdcp.LS: [path: %s, rooturl: %s]", path, rooturl)

	cmd := exec.CommandContext(ctx, "/usr/bin/xrdcp", "-s", rooturl, "-")
	output, err := cmd.CombinedOutput()

	if err != nil {
		eosLogger.Error(ctx, "Xrdcp.Ls", err, "ERROR: can not /usr/bin/xrdcp %s", rooturl)
		return "", 1, err
	}

	outputStr := string(output)
	outputStr = strings.TrimSpace(outputStr)

	stdout, stderr, retc := x.ParseOutput(ctx, outputStr)
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
func (x *Xrdcp) Find(ctx context.Context, path string) ([]*FileStat, error) {
	rooturl, err := url.QueryUnescape(x.GetXrootBase() + "/proc/user/?mgm.cmd=find&mgm.option=I&mgm.find.maxdepth=1&mgm.path=" + path)
	if err != nil {
		eosLogger.Error(ctx, "xrdcpFind", err, "ERROR: can not url.QueryUnescape() [path: %s, uri: %s]", path, rooturl)
		return nil, err
	}
	eosLogger.Stat(ctx, "Xrdcp.Find", "EOScmd: xrdcp.FIND: [path: %s, rooturl: %s]", path, rooturl)

	cmd := exec.CommandContext(ctx, "/usr/bin/xrdcp", "-s", rooturl, "-")
	pipe, _ := cmd.StdoutPipe()

	if err := cmd.Start(); err != nil {
		eosLogger.Error(ctx, "xrdcpFind", err, "ERROR: can not /usr/bin/xrdcp %s ", rooturl)
		return nil, err
	}
	defer cmd.Wait()

	parsedobjects := make([]*FileStat, 0)
	var object string
	reader := bufio.NewReader(pipe)
	for err == nil {
		object, err = reader.ReadString('\n')
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
func (x *Xrdcp) Fileinfo(ctx context.Context, path string) ([]*FileStat, error) {
	rooturl, err := url.QueryUnescape(x.GetXrootBase() + "/proc/user/?mgm.cmd=fileinfo&mgm.file.info.option=-m&mgm.path=" + path)
	if err != nil {
		eosLogger.Error(ctx, "Xrdcp.Fileinfo", err, "ERROR: can not url.QueryUnescape() [path: %s, uri: %s]", path, rooturl)
		return nil, err
	}
	eosLogger.Stat(ctx, "Xrdcp.Fileinfo", "EOScmd: xrdcp.FILEINFO: [path: %s, rooturl: %s]", path, rooturl)

	cmd := exec.CommandContext(ctx, "/usr/bin/xrdcp", "-s", rooturl, "-")
	output, err := cmd.CombinedOutput()

	if err != nil {
		eosLogger.Error(ctx, "Xrdcp.Fileinfo", err, "ERROR: can not /usr/bin/xrdcp %s", rooturl)
		return nil, err
	}

	outputStr := strings.TrimSpace(string(output))
	stdout, stderr, retc := x.ParseOutput(ctx, outputStr)

	if retc != 0 {
		return nil, errors.New(stderr)
	}

	parsedobjects := make([]*FileStat, 0)
	parsed := x.ParseFileInfo(ctx, stdout)
	if parsed != nil {
		parsedobjects = append(parsedobjects, parsed)
	}

	return parsedobjects, nil
}

// GetFilenameFromObject finds the filename, removes it from the object and returns it
func (x *Xrdcp) GetFilenameFromObject(ctx context.Context, object string) (string, string) {
	// First pair should be keylength.file, which contains the filename length
	// so split the string on space once to get that value
	split := strings.SplitN(object, " ", 2)
	if len(split) < 2 {
		return "", object
	}
	_, keytmp := SplitKeyValuePair(split[0])
	keylength, err := strconv.Atoi(keytmp)
	if err != nil {
		return "", object
	}
	keylength = keylength + 5 // add 5 to it to include the "file=" prefix

	// Check the second part of the split string starts with file=
	// (x added to make sure theres a filename)
	splitLen := len(split[1])
	if splitLen < len("file=x") || splitLen < keylength || split[1][0:5] != "file=" {
		return "", object
	}

	// Get the filename using the filename length
	// to avoid splitting on spaces in the filename
	filename := split[1][5:keylength]
	// Remove the filename from the object
	object = split[1][keylength:len(split[1])]

	return filename, object
}

// ParseFileInfo parses the xrdcp formatted result into a named array
func (x *Xrdcp) ParseFileInfo(ctx context.Context, object string) *FileStat {
	object = strings.TrimSpace(object)
	if object == "" {
		return nil
	}

	filename, object := x.GetFilenameFromObject(ctx, object)
	if filename == "" {
		eosLogger.Debug(ctx, "xrdcpFindParseResult", "Unable to get filename from object [object: %s]", object)
		return nil
	}

	// Get the rest of the key value pairs
	var (
		mtime       int64
		filesize    int64
		isfile      = true
		etag        string
		contenttype string
	)
	kvpairs := strings.Split(object, " ")
	for idx, pair := range kvpairs {
		key, value := SplitKeyValuePair(pair)
		if key != "" {
			switch key {
			case "mtime":
				mtime = int64(StringToFloat(value))
			case "size":
				filesize = StringToInt(value)
			case "container":
				// It's a directory
				isfile = false
			case "xattrn":
				// If its an xattr, get the value from the next pair
				key = value
				switch key {
				case "minio_etag":
					_, etag = SplitKeyValuePair(kvpairs[(idx + 1)])
				case "minio_contenttype":
					_, contenttype = SplitKeyValuePair(kvpairs[(idx + 1)])
				}
			}
		}
	}
	stat := NewFileStat(filename, filesize, isfile, mtime, etag, contenttype)
	return stat
}

// Put ... puts a file
func (x *Xrdcp) Put(ctx context.Context, src, dst string, size int64) error {
	eospath, err := x.AbsoluteEOSPath(dst)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(fmt.Sprintf("%s%s?eos.ruid=%s&eos.rgid=%s&eos.bookingsize=%d", x.GetXrootBase(), eospath, x.UID, x.GID, size))
	if err != nil {
		eosLogger.Error(ctx, "Xrdcp.Put", err, "ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl)
		return err
	}

	eosLogger.Stat(ctx, "Xrdcp.Put", "EOScmd: xrdcp.PUT: [eospath: %s, eosurl: %s]", eospath, eosurl)

	cmd := exec.Command("/usr/bin/xrdcp", "-N", "-f", "-p", src, eosurl)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		eosLogger.Error(ctx, "Xrdcp.Put", err, "ERROR: can not /usr/bin/xrdcp -N -f -p %s %s [eospath: %s]", src, eosurl, eospath)
	}
	output := strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))
	if output != "" {
		eosLogger.Info(ctx, "Xrdcp.Put", output, nil)
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
	eosurl, err := url.QueryUnescape(x.GetXrootBase() + eospath + "?eos.ruid=" + x.UID + "&eos.rgid=" + x.GID)
	if err != nil {
		eosLogger.Error(ctx, "ReadChunk", err, "ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl)
		return err
	}

	eosLogger.Stat(ctx, "ReadChunk", "EOScmd: xrdcp.GET: [eosurl: %s]", eosurl)

	cmd := exec.CommandContext(ctx, "/usr/bin/xrdcp", "-N", eosurl, "-")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err2 := cmd.Run()
	if err2 != nil {
		eosLogger.Info(ctx, "ReadChunk", "/usr/bin/xrdcp -N %s - %+v", eosurl, err2)
	}

	errStr := strings.TrimSpace(stderr.String())
	if errStr != "" {
		eosLogger.Error(ctx, "ReadChunk", nil, errStr)
	}

	if offset >= 0 {
		stdout.Next(int(offset))
	}
	stdout.Truncate(int(length))
	stdout.WriteTo(data)
	return err
}
