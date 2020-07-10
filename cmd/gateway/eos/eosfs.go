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
			eosLogger.Debug(context.Background(), "HTTPClient: http client wants to redirect [eosurl: %s]", req.URL.String())
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
	eosLogger.Debug(ctx, "EOSMGMcurl: [eosurl: %s]", eosurl)

	maxRetries := 10
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
func (e *eosFS) BuildCache(ctx context.Context, dirPath string, cacheReset bool) (entries []string, err error) {
	reqStatCache := e.StatCache.Get(ctx)
	if cacheReset {
		reqStatCache.Reset()
	}

	eospath, err := e.AbsoluteEOSPath(dirPath)
	if err != nil {
		eosLogger.Debug(ctx, "ERROR: Unable to determine absolute path [dirPath: %s]", dirPath)
		return nil, err
	}

	objects, err := e.GetObjectStat(ctx, eospath)
	if err != nil {
		eosLogger.Debug(ctx, "ERROR: Unable to read object [eospath: %s, error: %+v]", eospath, err)
		return nil, errFileNotFound
	}

	if err != nil {
		// Debug level since it happens quite often
		// when a file that doesn't exist is checked for
		eosLogger.Debug(ctx, "ERROR: Unable to read directory [eospath: %s, error: %+v]", eospath, err)
		return nil, errFileNotFound
	}

	for _, object := range objects {
		if e.isEOSSysFile(object.Name) {
			eosLogger.Debug(ctx, "Skipping EOS System file: %s", object.Name)
			continue
		}
		// Skip minio parts
		if strings.HasSuffix(object.Name, ".minio.sys") {
			eosLogger.Debug(ctx, "Skipping MinIO part file: %s", object.Name)
			continue
		}
		reqStatCache.Write(object.FullPath, object)

		if object.File {
			// If we find an entry matching the eospath and it's a file, return it.
			if object.FullPath == strings.TrimSuffix(eospath, "/") {
				eosLogger.Debug(ctx, "Object matches requested path, returning it [object: %s, path: %s]", object.FullPath, eospath)
				return []string{object.Name}, nil
			}
			entries = append(entries, object.Name)
		} else {
			// If we find an entry matching the eospath and is a directory, skip it.
			if object.FullPath == strings.TrimSuffix(eospath, "/")+"/" {
				continue
			}
			// Add a slash if it's a directory
			entries = append(entries, object.Name+"/")
		}
	}

	sort.Strings(entries)
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
		eosLogger.Debug(ctx, "eosfs.GetObjectStat(%s) -> Find", eospath)
		return e.Xrdcp.Find(ctx, eospath)
	}

	eosLogger.Debug(ctx, "eosfs.GetObjectStat(%s) -> FileInfo", eospath)
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
		eosLogger.Debug(ctx, "EOSfsStat: cache hit: [p: %s, eospath: %s]", p, eospath)
		return fi, nil
	}

	objects, err := e.GetObjectStat(ctx, eospath)
	if err != nil {
		eosLogger.Debug(ctx, "ERROR: Unable to read object [eospath: %s, error: %+v]", eospath, err)
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
		eosLogger.Error(ctx, err, "ERROR: EOSmkdirWithOption curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, nil, "ERROR: EOS procuser.mkdir [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
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
		eosLogger.Error(ctx, err, "ERROR: EOSrm curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, nil, "ERROR EOS procuser.rm [eospath: %s, error: %+v]", eospath, interfaceToString(m["errormsg"]))
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
		eosLogger.Info(ctx, "EOScopy waiting for src file to arrive: [eospath: %s, size: %d]", eossrcpath, size)
		SleepMs(SleepLong)
	}

	eosLogger.Debug(ctx, "EOScmd: procuser.file.copy [src: "+eossrcpath+", dst: "+eosdstpath+"]", nil)
	_, m, err := e.MGMcurlWithRetry(ctx, fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.URLExtras()))
	if err != nil {
		eosLogger.Error(ctx, err, "ERROR: EOScopy curl to MGM failed [src: %s, dst: %s, error: %+v]", eossrcpath, eosdstpath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, nil, "ERROR: EOS procuser.file.copy [src: %s, dst: %s, error: %s]", eossrcpath, eosdstpath, interfaceToString(m["errormsg"]))
		return errDiskAccessDenied
	}

	reqStatCache := e.StatCache.Get(ctx)
	reqStatCache.DeletePath(dst)

	return nil
}

func (e *eosFS) Touch(ctx context.Context, p string, size int64) error {
	//bookingsize is ignored by touch...
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Debug(ctx, "EOScmd: procuser.file.touch [eospath: %s]", eospath)
	_, m, err := e.MGMcurlWithRetry(ctx, fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.URLExtras(), size))
	if err != nil {
		eosLogger.Error(ctx, err, "ERROR: EOStouch curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, err, "ERROR: EOS procuser.file.touch [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
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
		eosLogger.Error(ctx, err, "ERROR: EOSrename curl to MGM failed [src: %s, dst: %s, error: %+v]", eosfrompath, eostopath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, err, "ERROR: EOS procuser.file.rename [src: %s, dst: %s, error: %s]", eosfrompath, eostopath, interfaceToString(m["errormsg"]))
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
		eosLogger.Error(ctx, err, "ERROR: EOSsetMeta curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Error(ctx, nil, "ERROR EOS procuser.attr.set [eospath: %s, command: %s, error: %s]", eospath, cmd, interfaceToString(m["errormsg"]))
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

		// SPECIAL CASE = contains a %
		if strings.IndexByte(p, '%') >= 0 {
			eosLogger.Debug(ctx, "EOScmd: webdav.PUT : SPECIAL CASE using curl [eosurl: "+eosurl+"]", nil)
			cmd := exec.Command("curl", "-L", "-X", "PUT", "--data-binary", "@-", "-H", "Remote-User: minio", "-sw", "'%{http_code}'", eosurl)
			cmd.Stdin = bytes.NewReader(data)
			stdoutStderr, err := cmd.CombinedOutput()

			if err != nil {
				eosLogger.Error(ctx, err, "ERROR: curl failed [eosurl: %s]", eosurl)
				eosLogger.Debug(ctx, "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				Sleep()
				continue
			}
			if strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)) != "'201'" {
				err = errIncorrectPutStatusCode
				eosLogger.Error(ctx, nil, "ERROR: incorrect response from curl [eosurl: %s]", eosurl)
				eosLogger.Debug(ctx, "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				Sleep()
				continue
			}
			break
		}

		client, req, err := e.NewRequest("PUT", eosurl, bytes.NewReader(data))
		if err != nil {
			break
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = int64(len(data))
		req.Close = true
		res, err := client.Do(req)

		if err != nil {
			if res != nil {
				eosLogger.Debug(ctx, "EOSput: http ERROR response: [eosurl: %s, response: %+v]", eosurl, res)
			}

			Sleep()
			continue
		}

		if res != nil {
			defer res.Body.Close()
		} else {
			eosLogger.Error(ctx, nil, "ERROR: EOSput: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
			if err == nil {
				err = errResponseIsNil
			}
		}

		if res.StatusCode != 201 {
			eosLogger.Debug(ctx, "EOSput: http StatusCode != 201: [eosurl: %s, result: %+v]", eosurl, res)
			err = errIncorrectPutStatusCode
			SleepMs(SleepShort)
			continue
		}
	}
	eosLogger.Error(ctx, err, "ERROR: EOSput failed %d times. [eosurl %s, error: %+v]", e.maxRetry, eosurl, err)
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
		eosLogger.Error(ctx, err, "ERROR: can not write chunk [eosurl: %s, offset: %d, size: %d, checksum: %s]", eosurl, offset, size, checksum)
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
			eosLogger.Error(ctx, nil, "ERROR: EOSreadChunk [eosurl: %s, error: %s]", eosurl, errStr)
		}
	case "xrdcp":
		err = e.Xrdcp.ReadChunk(ctx, p, offset, length, data)
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
				eosLogger.Error(ctx, err, "ERROR: webdav.GET [eosurl: %s, error: %+v]", eosurl, err)
				Sleep()
				continue
			}
			// TODO: Might need to return here if res is nil
			if res != nil {
				defer res.Body.Close()
			} else {
				eosLogger.Error(ctx, nil, "ERROR: webdav.GET: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
			}

			//did we get the right length?
			buf := &bytes.Buffer{}
			bRead, err := io.Copy(buf, res.Body)
			if err != nil {
				eosLogger.Error(ctx, nil, "ERROR: webdav.GET: Failed to copy curl data to buffer [eosurl: %s, error: %+v, bRead: %d, length: %d]", eosurl, err, bRead, length)
				Sleep()
				continue
			}
			if bRead != length {
				eosLogger.Error(ctx, nil, "ERROR: webdav.GET: Failed to copy curl data to buffer with correct length [eosurl: %s, error: %+v, bRead: %d, length: %d]", eosurl, err, bRead, length)
				Sleep()
				continue
			}

			//write buffer to data writer
			written, err := io.Copy(data, buf)
			if err != nil {
				eosLogger.Error(ctx, nil, "ERROR: webdav.GET: Failed to copy buffer data to data writer [eosurl: %s, error: %+v, written: %d]", eosurl, err, written)
				Sleep()
				continue
			}
			break
		}
	}
	return err
}
