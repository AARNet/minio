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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"errors"
	"context"
)

type eosFS struct {
	MGMHost string
	Path string
	User string
	UID string
	GID string
	ReadMethod string
	Scripts string 
	StatCache *StatCache
}

var (
	eoserrFileNotFound = errors.New("EOS: File Not Found")
	eoserrDiskAccessDenied = errors.New("EOS: Disk Access Denied")
	eoserrCantPut = errors.New("EOS: Unable to PUT")
	eoserrFilePathBad = errors.New("EOS: Bad File Path")
	eoserrResponseIsNil = errors.New("EOS: Response body is nil")
)

// Returns a HTTPClient
func (e *eosFS) HTTPClient() *http.Client {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			Log(LogLevelDebug, context.Background(), "HTTPClient: http client wants to redirect [eosurl: %s]", req.URL)
			return nil
		},
		Timeout: 0,
	}
	return client
}

// Sets up a client and a GET request for the MGM
func (e *eosFS) NewRequest(method string, url string, body io.Reader) (*http.Client, *http.Request, error) {
	client := e.HTTPClient()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Remote-User", e.User)
	return client, req, nil
}

// Returns common parameters for requests to MGM
func (e *eosFS) UrlExtras() string {
	return fmt.Sprintf("&eos.ruid=%s&eos.rgid=%s&mgm.format=json", e.UID, e.GID)
}

// NMormalise an EOS path
func (e *eosFS) NormalisePath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", eoserrFilePathBad
	}

	path = strings.Replace(path, "//", "/", -1)
	eosPath = strings.TrimSuffix(e.Path+"/"+path, ".")
	return eosPath, nil
}

// Makes GET requests to the MGM
func (e *eosFS) MGMcurl(cmd string) (body []byte, m map[string]interface{}, err error) {
	eosurl := fmt.Sprintf("http://%s:8000/proc/user/?%s", e.MGMHost, cmd)
	Log(LogLevelDebug, context.Background(), "EOSMGMcurl: [eosurl: %s]", eosurl)

	client, req, err := e.NewRequest("GET", eosurl, nil)
	if err != nil {
		return nil, nil, err
	}

	maxRetries := 5
	var res *http.Response
	for try := 1; try <= maxRetries; try++ {
		res, err = client.Do(req)

		if res != nil {
			defer res.Body.Close()
		}
		if res == nil && err == nil {
			err = eoserrResponseIsNil
		}
		Sleep()
	}
	if err != nil {
		return nil, nil, err
	}

	body, _ = ioutil.ReadAll(res.Body)
	m = make(map[string]interface{})
	err = json.Unmarshal([]byte(body), &m)

	return body, m, err
}


func (e *eosFS) ReadDir(dirPath string, cacheReset bool) (entries []string, err error) {
	if cacheReset {
		e.StatCache.Reset()
	}

	eospath, err := e.NormalisePath(dirPath)
	if err != nil {
		return nil, err
	}

	Log(LogLevelStat, context.Background(), "EOScmd: procuser.fileinfo [eospath: %s, method: ReadDir]", eospath)
	body, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOSreadDir curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return nil, err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR: EOS procuser.fileinfo [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	if c, ok := m["children"]; ok {
		err = json.Unmarshal([]byte(body), &c)
		if err != nil {
			Log(LogLevelError, context.Background(), "ERROR: EOSreadDir can not json.Unmarshal() children [eospath: %s]", eospath)
			return nil, err
		}

		eospath = strings.TrimSuffix(eospath, "/") + "/"
		children := m["children"].([]interface{})
		for _, childi := range children {
			child, _ := childi.(map[string]interface{})

			obj := interfaceToString(child["name"])
			if !strings.HasPrefix(obj, ".sys.v#.") {
				isFile := true
				if interfaceToInt64(child["mode"]) == 0 {
					obj += "/"
					isFile = false
				}
				entries = append(entries, obj)

				//some defaults
				meta := make(map[string]string)
				meta["contenttype"] = "application/octet-stream"
				meta["etag"] = defaultETag
				if _, ok := child["xattr"]; ok {
					xattr, _ := child["xattr"].(map[string]interface{})
					if contenttype, ok := xattr["minio_contenttype"]; ok {
						meta["contenttype"] = interfaceToString(contenttype)
					}
					if etag, ok := xattr["minio_etag"]; ok {
						meta["etag"] = interfaceToString(etag)
					}
				}

				e.StatCache.Write(eospath+obj, eosFileStat{
					id:          interfaceToInt64(child["id"]),
					name:        interfaceToString(child["name"]),
					size:        interfaceToInt64(child["size"]) + interfaceToInt64(child["treesize"]),
					file:        isFile,
					modTime:     time.Unix(interfaceToInt64(child["mtime"]), 0),
					etag:        meta["etag"],
					contenttype: meta["contenttype"],
					//checksum:    interfaceToString(child["checksumvalue"]),
				})
			}
		}
	}

	return entries, err
}

func (e *eosFS) Stat(p string) (*eosFileStat, error) {
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return nil, err
	}
	if fi, ok := e.StatCache.Read(eospath); ok {
		Log(LogLevelDebug, context.Background(), "EOSfsStat: cache hit: [eospath: %s]", eospath)
		return fi, nil
	}

	Log(LogLevelDebug, context.Background(), "EOSfsStat: cache miss: [eospath: %s]", eospath)
	Log(LogLevelStat, context.Background(), "EOScmd: procuser.fileinfo [eospath: %s, method: Stat]", eospath)

	// Sometimes too many stats at once causes no response, so we want to back off if it fails.
	var (
		body []byte
		m map[string]interface{}
	)
	retries := 5
	for try := 1; try <= retries; try++ {
		body, m, err = e.MGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
		if err == nil && body != nil {
			break
		}
		if err != nil {
			Log(LogLevelDebug, context.Background(), "ERROR: EOSfsStat curl to MGM failed [eospath: %s, try: %d, error: %+v]", eospath, try, err)
		}
		if body == nil {
			Log(LogLevelDebug, context.Background(), "ERROR: EOSfsStat curl to MGM failed [eospath: %s, try: %d, error: response body is nil]", eospath, try)
		}
		if try > 5 {
			SleepMs(5000)
		} else {
			SleepMs(try * 1000)
		}
	}
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOSfsStat curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return nil, err
	}
	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelDebug, context.Background(), "EOS procuser.fileinfo [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	Log(LogLevelDebug, context.Background(), "EOSfsStat: stat result [eospath: %s, name: %s, mtime: %d, mode:%d, size: %d]", eospath, interfaceToString(m["name"]), interfaceToInt64(m["mtime"]), interfaceToInt64(m["mode"]), interfaceToInt64(m["size"]))
	Log(LogLevelDebug, context.Background(), "EOSfsStat: request response body [eospath: %s, body: %s]", eospath, strings.Replace(string(body), "\n", " ", -1))

	//some defaults
	meta := make(map[string]string)
	meta["contenttype"] = "application/octet-stream"
	meta["etag"] = defaultETag
	if _, ok := m["xattr"]; ok {
		xattr, _ := m["xattr"].(map[string]interface{})
		if contenttype, ok := xattr["minio_contenttype"]; ok {
			ct := interfaceToString(contenttype)
			if ct != "" {
				meta["contenttype"] = ct
			}
		}
		if etag, ok := xattr["minio_etag"]; ok {
			et := interfaceToString(etag)
			if et != "" {
				meta["etag"] = et
			}
		}
	}

	fi := eosFileStat{
		id:          interfaceToInt64(m["id"]),
		name:        interfaceToString(m["name"]),
		size:        interfaceToInt64(m["size"]) + interfaceToInt64(m["treesize"]),
		file:        interfaceToUint32(m["mode"]) != 0,
		modTime:     time.Unix(interfaceToInt64(m["mtime"]), 0),
		etag:        meta["etag"],
		contenttype: meta["contenttype"],
	}

	e.StatCache.Write(eospath, fi)

	return &fi, nil
}

func (e *eosFS) mkdirWithOption(p, option string) error {
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}

	Log(LogLevelStat, context.Background(), "EOScmd: procuser.mkdir [eospath: %s]", eospath)
	_, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=mkdir%s&mgm.path=%s%s", option, url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOSmkdirWithOption curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR: EOS procuser.mkdir [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	//e.messagebusAddPutJob(p)

	return nil
}

func (e *eosFS) rmdir(p string) error {
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}

	Log(LogLevelStat, context.Background(), "EOScmd: procuser.rm [eospath: %s]", eospath)
	_, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOSrmdir curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR EOS procuser.rm [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	//e.messagebusAddDeleteJob(p)

	return nil
}

func (e *eosFS) rm(p string) error {
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}

	Log(LogLevelStat, context.Background(), "EOScmd: procuser.rm [eospath: %s]", eospath)
	_, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOSrm curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR EOS procuser.rm [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}
	e.StatCache.DeletePath(p)
	//e.messagebusAddDeleteJob(p)
	return nil
}

func (e *eosFS) Copy(src, dst string, size int64) error {
	eossrcpath, err := e.NormalisePath(src)
	if err != nil {
		return err
	}
	eosdstpath, err := e.NormalisePath(dst)
	if err != nil {
		return err
	}

	//need to wait for file, it is possible it is uploaded via a background job
	for {
		Log(LogLevelStat, context.Background(), "EOScmd: procuser.fileinfo [eospath: %s, method: Copy]", eossrcpath)
		_, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eossrcpath), e.UrlExtras()))
		if err == nil {
			if interfaceToInt64(m["size"]) >= size {
				break
			}
		}
		Log(LogLevelInfo, context.Background(), "EOScopy waiting for src file to arrive: [eospath: %s, size: %d]", eossrcpath, size)
		Log(LogLevelDebug, context.Background(), "EOScopy expecting size: %d found size: %d [eospath: %s]", size, interfaceToInt64(m["size"]), eossrcpath)
		SleepMs(SleepLong)
	}

	Log(LogLevelStat, context.Background(), "EOScmd: procuser.file.copy [src: %s, dst: %s]", eossrcpath, eosdstpath)
	_, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.UrlExtras()))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOScopy curl to MGM failed [src: %s, dst: %s, error: %+v]", eossrcpath, eosdstpath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR: EOS procuser.file.copy [src: %s, dst: %s, error: %s]", eossrcpath, eosdstpath, interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	e.StatCache.DeletePath(dst)
	//e.messagebusAddPutJob(dst)

	return nil
}

func (e *eosFS) Touch(p string, size int64) error {
	//bookingsize is ignored by touch...
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}

	Log(LogLevelStat, nil, "EOScmd: procuser.file.touch [eospath: %s]", eospath)
	_, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.UrlExtras(), size))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOStouch curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR: EOS procuser.file.touch [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosFS) Rename(from, to string) error {
	eosfrompath, err := e.NormalisePath(from)
	if err != nil {
		return err
	}
	eostopath, err := e.NormalisePath(to)
	if err != nil {
		return err
	}

	Log(LogLevelStat, context.Background(), "EOScmd: procuser.file.rename [src: %s, dst: %s]", eosfrompath, eostopath)
	_, m, err := e.MGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=rename&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eosfrompath), url.QueryEscape(eostopath), e.UrlExtras()))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOSrename curl to MGM failed [src: %s, dst: %s, error: %+v]", eosfrompath, eostopath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR: EOS procuser.file.rename [src: %s, dst: %s, error: %s]", eosfrompath, eostopath, interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosFS) SetMeta(p, key, value string) error {
	if key == "" || value == "" {
		Log(LogLevelDebug, context.Background(), "procuser.attr.set key or value is empty. [path: %s, key: %s, value: %s]", p, key, value)
		//dont bother setting if we don't get what we need
		return nil
	}
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}
	Log(LogLevelStat, context.Background(), "EOScmd: procuser.attr.set [path: %s, key: %s, value: %s]", eospath, key, value)
	cmd := fmt.Sprintf("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=minio_%s&mgm.attr.value=%s&mgm.path=%s%s", url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(eospath), e.UrlExtras())
	body, m, err := e.MGMcurl(cmd)
	Log(LogLevelDebug, context.Background(), "EOSsetMeta: meta tag return body [eospath: %s, body: %s]", eospath, strings.Replace(string(body), "\n", " ", -1))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: EOSsetMeta curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		Log(LogLevelError, context.Background(), "ERROR EOS procuser.attr.set [eospath: %s, command: %s, error: %s]", eospath, cmd, interfaceToString(m["errormsg"]))
		// maybe better? > return errors.New(interfaceToString(m["errormsg"]))
	}

	return nil
}

func (e *eosFS) SetContentType(p, ct string) error { return e.SetMeta(p, "contenttype", ct) }
func (e *eosFS) SetETag(p, etag string) error      { return e.SetMeta(p, "etag", etag) }

func (e *eosFS) Put(p string, data []byte) error {
	Log(LogLevelInfo, context.Background(), "EOSput: [path: %s]", p)
	//curl -L -X PUT -T somefile -H 'Remote-User: minio' -sw '%{http_code}' http://eos:8000/eos-path/somefile

	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl := fmt.Sprintf("http://%s:8000%s", e.MGMHost, eospath)
	Log(LogLevelDebug, context.Background(), "DEBUG: [eosurl: %s]", eosurl)

	Log(LogLevelStat, context.Background(), "EOScmd: webdav.PUT [eosurl: %s]", eosurl)

	maxRetry := 10
	retry := 0
	for retry < maxRetry {
		retry = retry + 1

		// SPECIAL CASE = contains a %
		if strings.IndexByte(p, '%') >= 0 {
			Log(LogLevelStat, nil, "EOScmd: webdav.PUT : SPECIAL CASE using curl [eosurl: %s]", eosurl)
			cmd := exec.Command("curl", "-L", "-X", "PUT", "--data-binary", "@-", "-H", "Remote-User: minio", "-sw", "'%{http_code}'", eosurl)
			cmd.Stdin = bytes.NewReader(data)
			stdoutStderr, err := cmd.CombinedOutput()

			if err != nil {
				Log(LogLevelError, context.Background(), "ERROR: curl failed [eosurl: %s]", eosurl)
				Log(LogLevelDebug, context.Background(), "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				Sleep()
				continue
			}
			if strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)) != "'201'" {
				Log(LogLevelError, context.Background(), "ERROR: incorrect response from curl [eosurl: %s]", eosurl)
				Log(LogLevelDebug, context.Background(), "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				Sleep()
				continue
			}

			return err
		}

		client, req, err := e.NewRequest("PUT", eosurl, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = int64(len(data))
		req.Close = true
		res, err := client.Do(req)

		if err != nil {
			Log(LogLevelDebug, context.Background(), "EOSput: http ERROR message: [eosurl: %s, error: %+v]", eosurl, err)
			if res != nil {
				Log(LogLevelDebug, context.Background(), "EOSput: http ERROR response: [eosurl: %s, response: %+v]", eosurl, res)
			}

			Sleep()
			continue
		}
		if res != nil {
			defer res.Body.Close()
		} else {
			Log(LogLevelError, context.Background(), "ERROR: EOSput: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
			if err == nil {
				err = eoserrResponseIsNil
			}
		}

		if res.StatusCode != 201 {
			Log(LogLevelDebug, context.Background(), "EOSput: http StatusCode != 201: [eosurl: %s, result: %+v]", eosurl, res)
			err = eoserrCantPut
			Sleep()
			continue
		}

		//e.messagebusAddPutJob(p)
		return err
	}
	Log(LogLevelError, context.Background(), "ERROR: EOSput failed %d times. [eosurl %s, error: %+v]", maxRetry, eosurl, err)
	return err
}

func (e *eosFS) xrootdWriteChunk(p string, offset, size int64, checksum string, data []byte) error {
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("root://%s@%s/%s", e.User, e.MGMHost, eospath)
	Log(LogLevelStat, nil, "EOScmd: xrootd.PUT: [script: %s, eosurl: %s, offset: %d, size: %d, checksum: %s, uid: %d, gid: %d]", e.Scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.UID, e.GID)

	cmd := exec.Command(e.Scripts+"/writeChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(size, 10), checksum, e.UID, e.GID)
	cmd.Stdin = bytes.NewReader(data)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: can not [script: %s, eosurl: %s, offset: %d, size: %d, checksum: %s, uid: %s, gid: %s]", e.Scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.UID, e.GID)
		Log(LogLevelDebug, context.Background(), "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
	}

	return err
}

func (e *eosFS) xrdcp(src, dst string, size int64) error {
	eospath, err := e.NormalisePath(dst)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s&eos.bookingsize=%d", e.MGMHost, eospath, e.UID, e.GID, size))
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl)
		return err
	}

	Log(LogLevelStat, nil, "EOScmd: xrdcp.PUT: [eospath: %s, eosurl: %s]", eospath, eosurl)

	cmd := exec.Command("/usr/bin/xrdcp", "-N", "-f", "-p", src, eosurl)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		Log(LogLevelError, context.Background(), "ERROR: can not /usr/bin/xrdcp -N -f -p %s %s [eospath: %s, eosurl: %s]", src, eosurl, eospath, eosurl)
	}
	output := strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))
	if output != "" {
		Log(LogLevelInfo, context.Background(), "%s", output)
	}

	return err
}

func (e *eosFS) ReadChunk(p string, offset, length int64, data io.Writer) (err error) {
	eospath, err := e.NormalisePath(p)
	if err != nil {
		return err
	}

	switch e.ReadMethod {
	case "xrootd":
		eosurl := fmt.Sprintf("root://%s@%s/%s", e.User, e.MGMHost, eospath)
		Log(LogLevelStat, nil, "EOScmd: xrootd.GET: [eospath: %s, eosurl: %s]", eospath, eosurl)

		cmd := exec.Command(e.Scripts+"/readChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(length, 10), e.UID, e.GID)
		var stderr bytes.Buffer
		cmd.Stdout = data
		cmd.Stderr = &stderr
		err2 := cmd.Run()
		errStr := strings.TrimSpace(string(stderr.Bytes()))
		Log(LogLevelInfo, context.Background(), "EOSreadChunk: [script: %s, eosurl: %s, offset: %d, length: %d, uid: %s, gid: %s, error: %+v]", e.Scripts+"/readChunk.py", eosurl, offset, length, e.UID, e.GID, err2)
		if errStr != "" {
			Log(LogLevelError, context.Background(), "ERROR: EOSreadChunk [eosurl: %s, error: %s]", eosurl, errStr)
		}
	case "xrdcp":
		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s", e.MGMHost, eospath, e.UID, e.GID))
		if err != nil {
			Log(LogLevelError, context.Background(), "ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl)
			return err
		}

		Log(LogLevelStat, nil, "EOScmd: xrdcp.GET: [eosurl: %s]", eosurl)

		cmd := exec.Command("/usr/bin/xrdcp", "-N", eosurl, "-")
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err2 := cmd.Run()

		errStr := strings.TrimSpace(string(stderr.Bytes()))
		Log(LogLevelInfo, context.Background(), "/usr/bin/xrdcp -N %s - %+v", eosurl, err2)
		if errStr != "" {
			Log(LogLevelInfo, context.Background(), "%s", errStr)
		}

		if offset >= 0 {
			stdout.Next(int(offset))
		}
		stdout.Truncate(int(length))
		stdout.WriteTo(data)
	default: //webdav
		//curl -L -X GET -H 'Remote-User: minio' -H 'Range: bytes=5-7' http://eos:8000/eos-path-to-file

		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl := fmt.Sprintf("http://%s:8000%s", e.MGMHost, eospath)
		Log(LogLevelStat, nil, "EOScmd: webdav.GET: [eosurl: %s]", eosurl)

		client, req, err := e.NewRequest("GET", eosurl, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		req.Close = true
		res, err := client.Do(req)

		if err != nil {
			Log(LogLevelError, context.Background(), "ERROR: webdav.GET [eosurl: %s, error: %+v]", eosurl, err)
			return err
		}
		// TODO: Might need to return here if res is nil
		if res != nil {
			defer res.Body.Close()
		} else {
			Log(LogLevelError, context.Background(), "ERROR: webdav.GET: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
			err = eoserrResponseIsNil
		}
		_, err = io.CopyN(data, res.Body, length)
	}
	return err
}
