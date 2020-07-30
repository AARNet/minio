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
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// Xrdcp ... struct
type Xrdcp struct {
	maxRetry int
	Path     string
	MGMHost  string
	User     string
	UID      string
	GID      string
}

// XrdcpWithRetry - Run xrdcp in a retry loop
func (x *Xrdcp) XrdcpWithRetry(ctx context.Context, arg ...string) (outputStr string, err error) {
	for retry := 1; retry <= x.maxRetry; retry++ {
		cmd := exec.Command("/usr/bin/xrdcp", arg...)
		output, err := cmd.CombinedOutput()

		if err != nil {
			eosLogger.Error(ctx, err, "Failed to run /usr/bin/xrdcp %s (attempt:%d)", arg, retry)
			Sleep()
			continue
		} else {
			outputStr = strings.TrimSpace(string(output))
			break
		}
	}
	if err != nil {
		eosLogger.Error(ctx, err, "Failed to run /usr/bin/xrdcp %s : failed %d times.", arg, x.maxRetry)
		return "", err
	}
	return outputStr, nil
}

// GetXrootBase - Returns the root:// base URL
func (x *Xrdcp) GetXrootBase() string {
	return fmt.Sprintf("root://%s@%s/", x.User, x.MGMHost)
}

// UnescapedURI - Returns the unescaped URL for the xrd command, slap a {{filepath}} in the uripath where you want the filepath to be inserted
// This will remove any URI escaped characters (eg. %2F) from the path and URI separately to avoid errors (URI should be fine as it's static, but the filepath changes)
// eg x.UnescapedURI("{{filepath}}?eos.ruid=48&eos.rgid=48", "/eos/test-s3-1/%25/test/object.txt")
// returns root://localhost//eos/test-s3-1/%/test/object.txt?eos.ruid=48&eos.rgid=48
func (x *Xrdcp) UnescapedURI(uripath string, filepath string) (string, error) {
	baseuri := fmt.Sprintf("%s%s", x.GetXrootBase(), uripath)
	xrduri, err := url.QueryUnescape(baseuri)
	if err != nil {
		return "", err
	}
	unescFilepath := UnescapePath(filepath)
	xrduri = strings.Replace(xrduri, "{{filepath}}", unescFilepath, -1)

	return xrduri, nil
}

// AbsoluteEOSPath - Normalise an EOS path
func (x *Xrdcp) AbsoluteEOSPath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", errFilePathBad
	}
	eosPath = strings.TrimSuffix(PathJoin(x.Path, path), ".")
	return eosPath, nil
}

// FileExists - Check if a file or directory exists (returns true on error or file existence, otherwise false)
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

// IsDir - Check if a path is a file or directory
func (x *Xrdcp) IsDir(ctx context.Context, path string) (bool, error) {
	result, _, err := x.Ls(ctx, "dF", path)
	if err == nil && strings.HasSuffix(result, "/") {
		return true, nil
	}
	return false, err
}

// Ls - Perform an "ls" using xrdcp
func (x *Xrdcp) Ls(ctx context.Context, lsflags string, path string) (string, int64, error) {
	rooturl, err := x.UnescapedURI(fmt.Sprintf("/proc/user/?mgm.cmd=ls&mgm.option=%s&mgm.path={{filepath}}", lsflags), path)
	if err != nil {
		eosLogger.Error(ctx, err, "xrdcp.Ls: Failed to unescape URI [path: %s]", path)
		return "", 1, err
	}
	eosLogger.Debug(ctx, "xrdcp.LS: [rooturl: %s]", rooturl)

	outputStr, err := x.XrdcpWithRetry(ctx, "-s", rooturl, "-")
	if err != nil {
		return "", 1, err
	}
	stdout, stderr, retc := x.ParseOutput(ctx, outputStr)
	if retc > 0 {
		if stderr != "" {
			return "", retc, errors.New(stderr)
		}
		return "", retc, errors.New("Unknown error when parsing ls result")
	}

	return stdout, retc, err
}

// ParseOutput - Helper method to parse xrdcp result format
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
	rooturl, err := x.UnescapedURI("/proc/user/?mgm.cmd=find&mgm.option=I&mgm.find.maxdepth=1&mgm.path={{filepath}}", path)
	if err != nil {
		eosLogger.Error(ctx, err, "xrdcp.Find: Failed to unescape URI [path: %s]", path)
		return nil, err
	}
	eosLogger.Debug(ctx, "xrdcp.FIND: [rooturl: %s]", rooturl)

	cmd := exec.Command("/usr/bin/xrdcp", "-s", rooturl, "-")
	pipe, _ := cmd.StdoutPipe()

	if err := cmd.Start(); err != nil {
		eosLogger.Error(ctx, err, "Failed to run /usr/bin/xrdcp %s ", rooturl)
		return nil, err
	}
	defer cmd.Wait()

	parsedobjects := make([]*FileStat, 0)
	var object string
	reader := bufio.NewScanner(pipe)
	for reader.Scan() {
		object = reader.Text()
		object = strings.TrimPrefix(object, "&mgm.proc.stdout=")

		// Once you get &mgm.proc.stderr=, you have hit the end
		if strings.HasPrefix(object, "&mgm.proc.stderr=") {
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
	_ = pipe.Close()

	return parsedobjects, nil
}

// Fileinfo - use fileinfo -m to get file info
func (x *Xrdcp) Fileinfo(ctx context.Context, path string) ([]*FileStat, error) {
	rooturl, err := x.UnescapedURI("/proc/user/?mgm.cmd=fileinfo&mgm.file.info.option=-m&mgm.path={{filepath}}", path)
	if err != nil {
		eosLogger.Error(ctx, err, "xrdcp.Fileinfo: Failed to unescape URI [path: %s]", path)
		return nil, err
	}
	eosLogger.Debug(ctx, "xrdcp.FILEINFO: [rooturl: %s]", rooturl)

	outputStr, err := x.XrdcpWithRetry(ctx, "-s", rooturl, "-")
	if err != nil {
		return nil, err
	}

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

// GetFilenameFromObject - finds the filename, removes it from the object and returns it
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

// ParseFileInfo - parses the xrdcp formatted result into a named array
func (x *Xrdcp) ParseFileInfo(ctx context.Context, object string) *FileStat {
	object = strings.TrimSpace(object)
	if object == "" {
		return nil
	}

	filename, object := x.GetFilenameFromObject(ctx, object)
	if filename == "" {
		eosLogger.Debug(ctx, "Unable to get filename from object [object: %s]", object)
		return nil
	}

	// Get the rest of the key value pairs
	var (
		mtime       int64
		filesize    int64
		isfile      = true
		etag        string
		contenttype string
		eosETagType string
		eosETag     string
		minioETag   string
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
			case "xstype":
				eosETagType = value
			case "etag":
				eosETag = strings.Trim(value, "\"")
			case "xattrn":
				// If its an xattr, get the value from the next pair
				key = value
				switch key {
				case "minio_etag":
					_, minioETag = SplitKeyValuePair(kvpairs[(idx + 1)])
				case "minio_contenttype":
					_, contenttype = SplitKeyValuePair(kvpairs[(idx + 1)])
				}
			}
		}
	}

	// If EOS is set to use md5sum etags, use the one from EOS
	if eosETagType == "md5" && eosETag != "" {
		eosLogger.Debug(ctx, "xrdcp.ParseFileinfo: using EOS md5sum [filename: %s, etag: %s]", filename, eosETag)
		etag = eosETag
	} else {
		etag = minioETag
	}

	// If the filesize is 0 and the etag is not the md5sum for a zero byte file, make it a zerobyte etag.
	if filesize == 0 && etag != zerobyteETag {
		etag = zerobyteETag
	}
	stat := NewFileStat(filename, filesize, isfile, mtime, etag, contenttype)
	return stat
}

// PutFileResponse - Holds the information returned by --cksum md5:print
type PutFileResponse struct {
	ChecksumType string
	Checksum     string
	URI          string
	Size         string
}

// PutBuffer - non-multipart put a file
func (x *Xrdcp) PutBuffer(ctx context.Context, stream io.Reader, stagePath string, dst string) (*PutFileResponse, error) {
	// Write stream to temp file
	fd, err := ioutil.TempFile(stagePath, "putBuffer-")
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	defer os.RemoveAll(fd.Name())

	_, err = io.Copy(fd, stream)
	if err != nil {
		return nil, err
	}

	// Build the destination path
	dstPath, err := x.AbsoluteEOSPath(dst)
	if err != nil {
		return nil, err
	}

	// Unescape the URI so that it removes any URI escaped characters (eg. %2F)
	xrdURI, err := x.UnescapedURI("{{filepath}}", dstPath)
	if err != nil {
		eosLogger.Error(ctx, err, "Failed to unescape URI for %s", dst)
		return nil, err
	}

	var responseGlob *PutFileResponse
	for retry := 1; retry <= x.maxRetry; retry++ {

		// Execute command and collect buffers
		cmd := exec.Command("/usr/bin/xrdcp", "--silent", "--force", "--path", "--cksum", "md5:print", fd.Name(), xrdURI, fmt.Sprintf("-ODeos.ruid=%s&eos.rgid=%s", x.UID, x.GID))
		errBuf := &bytes.Buffer{}
		cmd.Stderr = errBuf

		err = cmd.Run()

		// Check the return code of the cmd.Run()
		// NOTE: this is straight from the reva eosclient:
		// https://github.com/cs3org/reva/blob/2b018b13c24dab305d92164cf0ad4da43807060c/pkg/eosclient/eosclient.go#L594-L622
		var exitStatus int
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				exitStatus = status.ExitStatus()
				switch exitStatus {
				case 0:
					err = nil
				case 2:
					err = fmt.Errorf("Not found: %s", errBuf.String())
				case 51:
					//Log something
					eosLogger.Debug(ctx, "PUT attempt #%d failed: %+v", retry, errBuf.String())
					//Clear buffers
					errBuf.Reset()
					//Last attempt failed, lets return
					if retry == x.maxRetry {
						return nil, fmt.Errorf("Write failed after %d attempts [tmp: %s, xrdURI: %s, error: %s]", x.maxRetry, fd.Name(), xrdURI, err)
					}
					SleepMs(1000)
					continue
				case 53:
					err = fmt.Errorf("Invalid checksum type set on EOS (data directory needs attribute: sys.forced.checksum=\"md5\")")
				}
			}
		}
		// If theres an error and it's not "file not found", return it
		if err != nil && exitStatus != 2 {
			return nil, fmt.Errorf("Write failed [tmp: %s, xrdURI: %s, error: %s, stderr: %v]", fd.Name(), xrdURI, err, errBuf.String())
		}

		// Pull the checksum and file information from stderr (xrdcp outputs it to stderr)
		errStr := errBuf.String()
		eosLogger.Debug(ctx, "xrdcp.PutBuffer: response: %s", errStr)
		response := &PutFileResponse{}
		responseGlob = response
		if strings.HasPrefix(errStr, "md5: ") {
			splitStr := strings.Split(errStr, " ")
			response.ChecksumType = strings.TrimRight(splitStr[0], ":")
			response.Checksum = splitStr[1]
			response.URI = splitStr[2]
			response.Size = splitStr[3]
			responseGlob = response
		} else {
			return nil, fmt.Errorf("Write failed: no --cksum information returned by xrdcp [response: %s]", errStr)
		}
		return response, nil
	}
	return responseGlob, nil
}

// Put - puts a file
func (x *Xrdcp) Put(ctx context.Context, src, dst string, size int64) error {
	eospath, err := x.AbsoluteEOSPath(dst)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(fmt.Sprintf("%s%s?eos.ruid=%s&eos.rgid=%s&eos.bookingsize=%d", x.GetXrootBase(), eospath, x.UID, x.GID, size))
	if err != nil {
		eosLogger.Error(ctx, err, "Failed to unescape URI [uri: %s]", eosurl)
		return err
	}
	eosLogger.Debug(ctx, "xrdcp.PUT: [eospath: %s, eosurl: %s]", eospath, eosurl)

	outputStr, err := x.XrdcpWithRetry(ctx, "-N", "-f", "-p", src, eosurl)
	if err != nil {
		eosLogger.Error(ctx, err, "Failed to run /usr/bin/xrdcp -N -f -p %s %s [eospath: %s]", src, eosurl, eospath)
	}
	if outputStr != "" {
		eosLogger.Info(ctx, outputStr, nil)
	}

	return err
}

// ReadChunk - reads a chunk
func (x *Xrdcp) ReadChunk(ctx context.Context, p string, offset, length int64, data io.Writer) (err error) {
	eospath, err := x.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(x.GetXrootBase() + eospath + "?eos.ruid=" + x.UID + "&eos.rgid=" + x.GID)
	if err != nil {
		eosLogger.Error(ctx, err, "Failed to unescape URI [uri: %s]", eosurl)
		return err
	}

	eosLogger.Debug(ctx, "xrdcp.GET: [eosurl: %s]", eosurl)

	cmd := exec.Command("/usr/bin/xrdcp", "-N", eosurl, "-")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err2 := cmd.Run()
	if err2 != nil {
		eosLogger.Error(ctx, err2, "Failed to run /usr/bin/xrdcp -N %s - %+v", eosurl, err2)
	}

	errStr := strings.TrimSpace(stderr.String())
	if errStr != "" {
		eosLogger.Error(ctx, fmt.Errorf(errStr), errStr) // TODO: second argument might need to change to a generic message
	}

	if offset >= 0 {
		stdout.Next(int(offset))
	}
	stdout.Truncate(int(length))
	stdout.WriteTo(data)
	return err
}
