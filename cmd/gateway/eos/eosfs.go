/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

type eosFS struct {
	maxRetry   int
	sort       bool
	MGMHost    string
	HTTPHost   string
	Proxy      string
	Path       string
	User       string
	UID        string
	GID        string
	ReadMethod string
	Scripts    string
	StatCache  *RequestStatCache
	Xrdcp      *Xrdcp
}

var (
	errFileNotFound           = errors.New("EOS: File Not Found")
	errDiskAccessDenied       = errors.New("EOS: Disk Access Denied")
	errFilePathBad            = errors.New("EOS: Bad File Path")
	errResponseIsNil          = errors.New("EOS: Response body is nil")
	errIncorrectPutStatusCode = errors.New("EOS: Statuscode for PUT response was not 201")
)

// HTTPClient sets up and returns a http.Client
func (e *eosFS) HTTPClient() *http.Client {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// This makes the logs pretty noisy, we'll leave it as a "dev" enable thing
			//eosLogger.Debug(context.Background(), "HTTPClient: http client wants to redirect [eosurl: %s]", req.URL.String())
			return nil
		},
		Timeout: 0, // HTTP Client default timeout
	}
	if e.Proxy != "" {
		proxyurl, err := url.Parse(e.Proxy)
		if err == nil {
			client.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyurl),
			}
		}
	}
	return client
}

// NewRequest sets up a client and a GET request for the MGM
func (e *eosFS) NewRequest(method string, url string, body io.Reader) (*http.Client, *http.Request, error) {
	client := e.HTTPClient()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Remote-User", e.User)
	return client, req, nil
}

// URLExtras returns common parameters for requests to MGM
func (e *eosFS) URLExtras() string {
	return "&eos.ruid=" + e.UID + "&eos.rgid=" + e.GID + "&mgm.format=json"
}

// AbsoluteEOSPath normalises and returns the absolute path in EOS
func (e *eosFS) AbsoluteEOSPath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", errFilePathBad
	}
	path = strings.ReplaceAll(path, "//", "/")
	eosPath = strings.TrimSuffix(PathJoin(e.Path, path), ".")
	return eosPath, nil
}

// MGMCurl makes GET requests to the MGM
func (e *eosFS) MGMcurl(ctx context.Context, cmd string) (body []byte, m map[string]interface{}, err error) {
	eosurl := "http://" + e.HTTPHost + "/proc/user/?" + cmd
	eosLogger.Debug(ctx, "EOSMGMcurl: [eosurl: %s]", eosurl)

	var (
		res    *http.Response
		client *http.Client
		req    *http.Request
	)
	client, req, err = e.NewRequest("GET", eosurl, nil)
	if err != nil {
		return nil, nil, err
	}

	res, err = client.Do(req)

	if res != nil {
		defer res.Body.Close()
	}
	if res == nil && err == nil {
		err = errResponseIsNil
	}
	if err != nil {
		return nil, nil, err
	}

	body, _ = ioutil.ReadAll(res.Body)
	m = make(map[string]interface{})
	err = json.Unmarshal([]byte(body), &m)

	return body, m, err
}

// MGMCurl makes GET requests to the MGM
func (e *eosFS) MGMcurlWithRetry(ctx context.Context, cmd string) (body []byte, m map[string]interface{}, err error) {
	eosurl := "http://" + e.HTTPHost + "/proc/user/?" + cmd
	maxRetries := 10
	eosLogger.Debug(ctx, "EOSMGMcurlWithRetry: [eosurl: %s, maxRetries: %d]", eosurl, maxRetries)

	for try := 1; try <= maxRetries; try++ {
		body, m, err = e.MGMcurl(ctx, cmd)
		if err == nil {
			break
		}
		Sleep()
	}
	return body, m, err
}

// isEOSSysFile - checks to see if it matches an EOS system file (prefixed with .sys.[a-z]#)
func (e *eosFS) isEOSSysFile(name string) bool {
	size := len(name)
	if size > 7 && strings.HasPrefix(name, ".sys.") && string(name[6]) == "#" {
		return true
	}
	return false
}

// BuildCache creates a cache of file stats for the duration of the request
func (e *eosFS) BuildCache(ctx context.Context, dirPath string, cacheReset bool) (entries []*FileStat, err error) {
	reqStatCache := e.StatCache.Get(ctx)
	if cacheReset {
		reqStatCache.Reset()
	}

	eospath, err := e.AbsoluteEOSPath(dirPath)
	if err != nil {
		eosLogger.Debug(ctx, "Unable to determine absolute path [dirPath: %s]", dirPath)
		return nil, err
	}

	objects, err := e.GetObjectStat(ctx, eospath)
	if err != nil {
		eosLogger.Debug(ctx, "Unable to read object [eospath: %s, error: %+v]", eospath, err)
		return nil, errFileNotFound
	}

	for _, object := range objects {
		if e.isEOSSysFile(object.Name) {
			continue
		}
		// Skip minio parts
		if strings.HasSuffix(object.Name, ".minio.sys") {
			continue
		}
		reqStatCache.Write(object.FullPath, object)
		entries = append(entries, object)
		eosLogger.Debug(ctx, "CACHE: ADD object.FullPath: %s : %+v", object.FullPath, object)
	}

	if e.sort {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].FullPath < entries[j].FullPath
		})
	}
	return entries, err
}

// DeleteCache deletes a cache produced by BuildCache
func (e *eosFS) DeleteCache(ctx context.Context) {
	e.StatCache.Delete(ctx)
}

// IsDir returns whether the path is a directory or not.
func (e *eosFS) IsDir(ctx context.Context, path string) (bool, error) {
	eospath, _ := e.AbsoluteEOSPath(path)
	return e.Xrdcp.IsDir(ctx, eospath)
}

// GetObjectStat returns stats for object(s) using Find or Fileinfo depending on it's type
func (e *eosFS) GetObjectStat(ctx context.Context, eospath string) (stats []*FileStat, err error) {
	// Check if it's a directory, will error if doesn't exist.
	isdir, err := e.Xrdcp.IsDir(ctx, eospath)
	if err != nil {
		return nil, err
	}

	// Find is faster for directories
	if isdir {
		return e.Xrdcp.Find(ctx, eospath)
	}

	return e.Xrdcp.Fileinfo(ctx, eospath)
}

// GetFolderStat is used for returning only stat information for a folder without recursing
func (e *eosFS) DirStat(ctx context.Context, p string) (fi *FileStat, err error) {
	reqStatCache := e.StatCache.Get(ctx)
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(eospath, "/") {
		eospath = eospath + "/"
	}

	// Check cache
	if fi, ok := reqStatCache.Read(eospath); ok {
		return fi, nil
	}

	isdir, err := e.Xrdcp.IsDir(ctx, eospath)
	if err != nil || !isdir {
		return nil, err
	}

	fileinfo, _ := e.Xrdcp.Fileinfo(ctx, eospath)
	if len(fileinfo) < 1 {
		return nil, errFileNotFound
	}
	// Grab the first result
	object := fileinfo[0]
	reqStatCache.Write(eospath, object)
	return object, err
}

func (e *eosFS) FileExists(ctx context.Context, p string) (bool, error) {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return true, err
	}
	return e.Xrdcp.FileExists(ctx, eospath)
}

// Stat looks up and returns information about the path (p) provided. path should be in the format "<bucket>/<prefix>"
func (e *eosFS) Stat(ctx context.Context, p string) (object *FileStat, err error) {
	reqStatCache := e.StatCache.Get(ctx)
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return nil, err
	}

	if fi, ok := reqStatCache.Read(eospath); ok {
		eosLogger.Debug(ctx, "eosfs.Stat: reading from cache: [eospath: %s]", eospath)
		return fi, nil
	}

	objects, err := e.GetObjectStat(ctx, eospath)
	if err != nil {
		eosLogger.Debug(ctx, "eosfs.Stat: Unable to read object [eospath: %s, error: %+v]", eospath, err)
		return nil, errFileNotFound
	}

	if len(objects) < 1 {
		return nil, errFileNotFound
	}

	// Grab the first entry, cache it and return it
	object = objects[0]
	reqStatCache.Write(eospath, object)
	return object, nil
}

func (e *eosFS) mkdirWithOption(ctx context.Context, p, option string) error {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Debug(ctx, "EOScmd: procuser.mkdir [eospath: %s]", eospath)
	_, m, err := e.MGMcurlWithRetry(ctx, fmt.Sprintf("mgm.cmd=mkdir%s&mgm.path=%s%s", option, url.QueryEscape(eospath), e.URLExtras()))
	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.mkdirWithOption: Failed to create directory [eospath: %s, error: %#v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, nil, "eosfs.mkdirWithOption: Failed to create directory [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
		return errDiskAccessDenied
	}

	return nil
}

// mkdirp is essentially `mkdir -p`, checks for existence before creating.
func (e *eosFS) mkdirp(ctx context.Context, dir string) (err error) {
	if exists, _ := e.FileExists(ctx, dir); !exists {
		err = e.mkdirWithOption(ctx, dir, "&mgm.option=p")
	}
	return err
}

// rmdir is the same as rm.
func (e *eosFS) rmdir(ctx context.Context, p string) (err error) {
	err = e.rm(ctx, p)
	return err
}

func (e *eosFS) rm(ctx context.Context, p string) error {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Debug(ctx, "EOScmd: procuser.rm [eospath: "+eospath+"]", nil)
	url := "mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=" + url.QueryEscape(eospath) + e.URLExtras()
	_, m, err := e.MGMcurlWithRetry(ctx, url)
	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.rm: request to MGM failed [eospath: %s]", eospath)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, fmt.Errorf(interfaceToString(m["errormsg"])), "eosfs.rm: remove failed [eospath: %s]", eospath)
		return errDiskAccessDenied
	}
	reqStatCache := e.StatCache.Get(ctx)
	reqStatCache.DeletePath(p)
	return nil
}

func (e *eosFS) Copy(ctx context.Context, src, dst string, size int64) error {
	eossrcpath, err := e.AbsoluteEOSPath(src)
	if err != nil {
		return err
	}
	eosdstpath, err := e.AbsoluteEOSPath(dst)
	if err != nil {
		return err
	}

	//need to wait for file, it is possible it is uploaded via a background job
	fileinfourl := "mgm.cmd=fileinfo&mgm.path=" + url.QueryEscape(eossrcpath) + e.URLExtras()
	for {
		eosLogger.Debug(ctx, "EOScmd: procuser.fileinfo [eospath: %s]", eossrcpath)
		_, m, err := e.MGMcurlWithRetry(ctx, fileinfourl)
		if err == nil && interfaceToInt64(m["size"]) >= size {
			break
		}
		eosLogger.Debug(ctx, "eosfs.Copy: waiting for source file to arrive: [eospath: %s, size: %d]", eossrcpath, size)
		SleepMs(SleepLong)
	}

	eosLogger.Debug(ctx, "EOScmd: procuser.file.copy [src: "+eossrcpath+", dst: "+eosdstpath+"]", nil)
	_, m, err := e.MGMcurlWithRetry(ctx, fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.URLExtras()))
	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.Copy: request to MGM failed [src: %s, dst: %s, error: %+v]", eossrcpath, eosdstpath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, nil, "eosfs.Copy: copy failed [src: %s, dst: %s, error: %s]", eossrcpath, eosdstpath, interfaceToString(m["errormsg"]))
		return errDiskAccessDenied
	}

	reqStatCache := e.StatCache.Get(ctx)
	reqStatCache.DeletePath(dst)

	return nil
}

func (e *eosFS) Touch(ctx context.Context, p string, size int64) error {
	//bookingsize is ignored by touch...
	//... then why do we specify it?
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Debug(ctx, "EOScmd: procuser.file.touch [eospath: %s]", eospath)
	_, m, err := e.MGMcurlWithRetry(ctx, fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.URLExtras(), size))
	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.Touch: request to MGM failed [eospath: %s]", eospath)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, fmt.Errorf(interfaceToString(m["errormsg"])), "eosfs.Touch: touch failed [eospath: %s]", eospath)
		return errDiskAccessDenied
	}

	return nil
}

func (e *eosFS) Rename(ctx context.Context, from, to string) error {
	eosfrompath, err := e.AbsoluteEOSPath(from)
	if err != nil {
		return err
	}
	eostopath, err := e.AbsoluteEOSPath(to)
	if err != nil {
		return err
	}

	eosLogger.Debug(ctx, "EOScmd: procuser.file.rename [src: "+eosfrompath+", dst: "+eostopath+"]", nil)
	renameurl := "mgm.cmd=file&mgm.subcmd=rename&mgm.path=" + url.QueryEscape(eosfrompath) + "&mgm.file.target=" + url.QueryEscape(eostopath) + e.URLExtras()
	_, m, err := e.MGMcurlWithRetry(ctx, renameurl)
	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.Rename: request to MGM failed [src: %s, dst: %s]", eosfrompath, eostopath)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, fmt.Errorf(interfaceToString(m["errormsg"])), "eosfs.Rename: rename failed [src: %s, dst: %s]", eosfrompath, eostopath)
		return errDiskAccessDenied
	}

	return nil
}

func (e *eosFS) SetMeta(ctx context.Context, p, key, value string) error {
	if key == "" || value == "" {
		//dont bother setting if we don't get what we need
		return nil
	}
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}
	eosLogger.Debug(ctx, "EOScmd: procuser.attr.set [path: "+eospath+", key: "+key+", value: "+value+"]", nil)
	cmd := "mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=minio_" + url.QueryEscape(key) + "&mgm.attr.value=" + url.QueryEscape(value) + "&mgm.path=" + url.QueryEscape(eospath) + e.URLExtras()
	_, m, err := e.MGMcurlWithRetry(ctx, cmd)
	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.SetMeta: request to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, nil, "eosfs.SetMeta: attribute setting failed [eospath: %s, command: %s, error: %s]", eospath, cmd, interfaceToString(m["errormsg"]))
		return errors.New(interfaceToString(m["errormsg"]))
	}

	return nil
}

func (e *eosFS) SetContentType(ctx context.Context, p, ct string) error {
	return e.SetMeta(ctx, p, "contenttype", ct)
}

func (e *eosFS) SetETag(ctx context.Context, p, etag string) error {
	return e.SetMeta(ctx, p, "etag", etag)
}

// SetSourceChecksum - set an atttribute on the file containing the checksum of the source data
func (e *eosFS) SetSourceChecksum(ctx context.Context, p, etag string) error {
	return e.SetMeta(ctx, p, "source_checksum", etag)
}

func (e *eosFS) PutBuffer(ctx context.Context, stage string, p string, data io.Reader) (response *PutFileResponse, err error) {
	eosLogger.Debug(ctx, "EOScmd: xrdcp.PutBuffer [stage: %s, p: %s]", stage, p)
	response, err = e.Xrdcp.PutBuffer(ctx, data, stage, p)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *eosFS) Put(ctx context.Context, p string, data []byte) (err error) {
	//curl -L -X PUT -T somefile -H 'Remote-User: minio' -sw '%{http_code}' http://eos:8000/eos-path/somefile

	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl := "http://" + e.HTTPHost + eospath
	eosLogger.Debug(ctx, "EOScmd: webdav.PUT [eosurl: "+eosurl+"]", nil)

	for retry := 1; retry <= e.maxRetry; retry++ {
		// If it contains %, use curl (apparently the go http client doesn't do this well)
		if strings.IndexByte(p, '%') >= 0 {
			var doErr error
			eosLogger.Debug(ctx, "EOScmd: webdav.PUT : SPECIAL CASE using curl [eosurl: "+eosurl+"]", nil)
			cmd := exec.Command("curl", "-L", "-X", "PUT", "--data-binary", "@-", "-H", "Remote-User: minio", "-sw", "'%{http_code}'", eosurl)
			cmd.Stdin = bytes.NewReader(data)
			stdoutStderr, doErr := cmd.CombinedOutput()

			if doErr != nil {
				err = doErr
				eosLogger.Error(ctx, err, "eosfs.Put: (special) request failed [eosurl: %s]", eosurl)
				eosLogger.Debug(ctx, "eosfs.Put: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				Sleep()
				continue
			}
			if strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)) != "'201'" {
				doErr = errIncorrectPutStatusCode
				err = doErr
				eosLogger.Error(ctx, err, "eos.Put: (special) incorrect response (expected 201) [eosurl: %s]", eosurl)
				eosLogger.Debug(ctx, "eos.Put: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				Sleep()
				continue
			} else {
				// This should mean success, so exit the loop
				err = nil
				break
			}
		} else { // Otherwise, use the go HTTP client
			var doErr error
			client, req, doErr := e.NewRequest("PUT", eosurl, bytes.NewReader(data))
			if doErr != nil {
				err = doErr
				break
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			req.ContentLength = int64(len(data))
			req.Close = true
			res, doErr := client.Do(req)

			if doErr != nil {
				err = doErr
				if res != nil {
					eosLogger.Debug(ctx, "eosfs.Put: http error response: [eosurl: %s, response: %+v]", eosurl, res)
				}

				Sleep()
				continue
			}

			if res != nil {
				defer res.Body.Close()
			} else {
				eosLogger.Debug(ctx, "eosfs.Put: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
				if doErr == nil {
					doErr = errResponseIsNil
				}
				err = doErr
				continue
			}

			if res.StatusCode != 201 {
				eosLogger.Debug(ctx, "eosfs.Put: http StatusCode != 201: [eosurl: %s, result: %+v]", eosurl, res)
				doErr = errIncorrectPutStatusCode
				err = doErr
				SleepMs(SleepShort)
				continue
			} else {
				// Exit loop if we get a 201
				err = nil
				break
			}
		}
	}

	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.Put: EOSput failed %d times. [eosurl %s]", e.maxRetry, eosurl)
		// remove the file on failure so we don't end up with left over 0 byte files
		_ = e.rm(ctx, p)
	}

	return err
}

func (e *eosFS) xrootdWriteChunk(ctx context.Context, p string, offset, size int64, checksum string, data []byte) error {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("root://%s@%s/%s", e.User, e.MGMHost, eospath)
	eosLogger.Debug(ctx, "EOScmd: xrootd.PUT: [eosurl: %s, offset: %d, size: %d, checksum: %s]", eosurl, offset, size, checksum)

	cmd := exec.CommandContext(ctx, e.Scripts+"/writeChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(size, 10), checksum, e.UID, e.GID)
	cmd.Stdin = bytes.NewReader(data)
	err = cmd.Run()
	if err != nil {
		eosLogger.Error(ctx, err, "eosfs.xrootdWriteChunk: writing chunk failed [eosurl: %s, offset: %d, size: %d, checksum: %s]", eosurl, offset, size, checksum)
	}

	return err
}

func (e *eosFS) ReadChunk(ctx context.Context, p string, offset, length int64, data io.Writer) (err error) {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	switch e.ReadMethod {
	case "xrootd":
		eosurl := fmt.Sprintf("root://%s@%s/%s", e.User, e.MGMHost, eospath)
		eosLogger.Debug(ctx, "EOScmd: xrootd.GET: [eospath: %s, eosurl: %s]", eospath, eosurl)

		cmd := exec.Command(e.Scripts+"/readChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(length, 10), e.UID, e.GID)
		var stderr bytes.Buffer
		cmd.Stdout = data
		cmd.Stderr = &stderr
		err = cmd.Run()
		if err != nil {
			return err
		}
		errStr := strings.TrimSpace(stderr.String())
		if errStr != "" {
			eosLogger.Error(ctx, fmt.Errorf(errStr), "eosfs.ReadChunk: read failed using xrootd. [eosurl: %s]", eosurl)
		}
	case "xrdcp":
		err = e.Xrdcp.ReadChunk(ctx, p, offset, length, data)
		if err != nil {
			eosLogger.Error(ctx, err, "eosfs.ReadChunk: read failed using xrdcp. [path: %s, offset: %d, length: %d]", p, offset, length)
		}
	default: //webdav
		//curl -L -X GET -H 'Remote-User: minio' -H 'Range: bytes=5-7' http://eos:8000/eos-path-to-file

		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl := fmt.Sprintf("http://%s%s", e.HTTPHost, eospath)
		eosLogger.Debug(ctx, "EOScmd: webdav.GET: [eosurl: %s]", eosurl)

		for retry := 1; retry <= e.maxRetry; retry++ {
			client, req, err := e.NewRequest("GET", eosurl, nil)
			if err != nil {
				Sleep()
				continue
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
			req.Close = true
			res, err := client.Do(req)

			if err != nil {
				eosLogger.Error(ctx, err, "eosfs.ReadChunk: webdav.GET [eosurl: %s]", eosurl)
				Sleep()
				continue
			}
			if res != nil {
				defer res.Body.Close()
			} else {
				eosLogger.Error(ctx, err, "eosfs.ReadChunk: webdav.GET: response body is nil [eosurl: %s]", eosurl)
				Sleep()
				continue
			}

			//did we get the right length?
			buf := &bytes.Buffer{}
			bRead, err := io.Copy(buf, res.Body)
			if err != nil {
				eosLogger.Error(ctx, err, "eosfs.ReadChunk: webdav.GET: Failed to copy curl data to buffer [eosurl: %s, bRead: %d, length: %d]", eosurl, bRead, length)
				Sleep()
				continue
			}
			if bRead != length {
				eosLogger.Error(ctx, err, "eosfs.ReadChunk: webdav.GET: Failed to copy curl data to buffer with correct length [eosurl: %s, bRead: %d, length: %d]", eosurl, bRead, length)
				Sleep()
				continue
			}

			//write buffer to data writer
			written, err := io.Copy(data, buf)
			if err != nil {
				eosLogger.Error(ctx, err, "eosfs.ReadChunk: webdav.GET: Failed to copy buffer data to data writer [eosurl: %s, written: %d]", eosurl, written)
				Sleep()
				continue
			}
			break
		}
		if err != nil {
			eosLogger.Error(ctx, err, "eosfs.ReadChunk: webdav.GET: Failed %d times. [eosurl %s]", e.maxRetry, eosurl)
		}
	}
	return err
}
