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
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type eosFS struct {
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
}

var (
	eoserrFileNotFound     = errors.New("EOS: File Not Found")
	eoserrDiskAccessDenied = errors.New("EOS: Disk Access Denied")
	eoserrCantPut          = errors.New("EOS: Unable to PUT")
	eoserrFilePathBad      = errors.New("EOS: Bad File Path")
	eoserrResponseIsNil    = errors.New("EOS: Response body is nil")
)

// Returns a HTTPClient
func (e *eosFS) HTTPClient() *http.Client {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			eosLogger.Log(context.Background(), LogLevelDebug, "HTTPClient", fmt.Sprintf("HTTPClient: http client wants to redirect [eosurl: %s]", req.URL), nil)
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

// Normalise an EOS path
func (e *eosFS) AbsoluteEOSPath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", eoserrFilePathBad
	}

	path = strings.ReplaceAll(path, "//", "/")
	eosPath = strings.TrimSuffix(e.Path+"/"+path, ".")
	eosPath = filepath.Clean(eosPath)
	return eosPath, nil
}

// Makes GET requests to the MGM
func (e *eosFS) MGMcurl(ctx context.Context, cmd string) (body []byte, m map[string]interface{}, err error) {
	eosurl := fmt.Sprintf("http://%s/proc/user/?%s", e.HTTPHost, cmd)
	eosLogger.Log(ctx, LogLevelDebug, "MGMcurl", fmt.Sprintf("EOSMGMcurl: [eosurl: %s]", eosurl), nil)

	maxRetries := 10
	var (
		res    *http.Response
		client *http.Client
		req    *http.Request
	)
	for try := 1; try <= maxRetries; try++ {
		client, req, err = e.NewRequest("GET", eosurl, nil)
		if err != nil {
			return nil, nil, err
		}

		res, err = client.Do(req)

		if res != nil {
			defer res.Body.Close()
			if res.StatusCode > 0 {
				break
			}
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

func (e *eosFS) BuildCache(ctx context.Context, dirPath string, cacheReset bool) (entries []string, err error) {
	reqStatCache := e.StatCache.Get(ctx)
	if cacheReset {
		reqStatCache.Reset()
	}

	eospath, err := e.AbsoluteEOSPath(dirPath)
	if err != nil {
		return nil, err
	}

	eosLogger.Log(ctx, LogLevelStat, "BuildCache", fmt.Sprintf("EOScmd: procuser.fileinfo [eospath: %s]", eospath), nil)
	body, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "BuildCache", fmt.Sprintf("ERROR: EOSreadDir curl to MGM failed [eospath: %s, error: %+v]", eospath, err), err)
		return nil, err
	}

	if interfaceToString(m["errormsg"]) != "" {
		if strings.HasPrefix(interfaceToString(m["errormsg"]), "error: cannot stat") {
			eosLogger.Log(ctx, LogLevelDebug, "BuildCache", fmt.Sprintf("ERROR: EOS procuser.fileinfo [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"])), nil)
		} else {
			eosLogger.Log(ctx, LogLevelError, "BuildCache", fmt.Sprintf("ERROR: EOS procuser.fileinfo [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"])), nil)
		}
		return nil, eoserrFileNotFound
	}

	if c, ok := m["children"]; ok {
		err = json.Unmarshal([]byte(body), &c)
		if err != nil {
			eosLogger.Log(ctx, LogLevelError, "BuildCache", fmt.Sprintf("ERROR: EOSreadDir can not json.Unmarshal() children [eospath: %s]", eospath), err)
			return nil, err
		}

		eospath = strings.TrimSuffix(eospath, "/") + "/"
		children := m["children"].([]interface{})
		for _, childi := range children {
			child, _ := childi.(map[string]interface{})

			obj := interfaceToString(child["name"])
			if !strings.HasPrefix(obj, ".sys.v#.") {
				isFile := true
				// If the file doesn't have an fxid, it's a directory
				if _, ok := child["fxid"]; !ok {
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

				reqStatCache.Write(eospath+obj, eosFileStat{
					id:          interfaceToInt64(child["id"]),
					name:        interfaceToString(child["name"]),
					size:        interfaceToInt64(child["size"]) + interfaceToInt64(child["treesize"]),
					file:        isFile,
					modTime:     time.Unix(interfaceToInt64(child["mtime"]), 0),
					etag:        meta["etag"],
					contenttype: meta["contenttype"],
				})
			}
		}
		// In case it isn't a directory we're trying to read and is a single file
	} else if _, ok := m["fxid"]; ok {
		eospath = strings.TrimSuffix(eospath, "/") + "/"
		meta := make(map[string]string)
		meta["contenttype"] = "application/octet-stream"
		meta["etag"] = defaultETag
		obj := interfaceToString(m["name"])
		entries = append(entries, obj)
		if _, ok := m["xattr"]; ok {
			xattr, _ := m["xattr"].(map[string]interface{})
			if contenttype, ok := xattr["minio_contenttype"]; ok {
				meta["contenttype"] = interfaceToString(contenttype)
			}
			if etag, ok := xattr["minio_etag"]; ok {
				meta["etag"] = interfaceToString(etag)
			}
		}
		reqStatCache.Write(eospath+obj, eosFileStat{
			id:          interfaceToInt64(m["id"]),
			name:        interfaceToString(m["name"]),
			size:        interfaceToInt64(m["size"]) + interfaceToInt64(m["treesize"]),
			file:        true,
			modTime:     time.Unix(interfaceToInt64(m["mtime"]), 0),
			etag:        meta["etag"],
			contenttype: meta["contenttype"],
		})
	}
	return entries, err
}

func (e *eosFS) Stat(ctx context.Context, p string) (*eosFileStat, error) {
	reqStatCache := e.StatCache.Get(ctx)
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return nil, err
	}
	if fi, ok := reqStatCache.Read(eospath); ok {
		eosLogger.Log(ctx, LogLevelDebug, "Stat", fmt.Sprintf("EOSfsStat: cache hit: [eospath: %s]", eospath), nil)
		return fi, nil
	}

	eosLogger.Log(ctx, LogLevelDebug, "Stat", fmt.Sprintf("EOSfsStat: cache miss: [eospath: %s]", eospath), nil)
	eosLogger.Log(ctx, LogLevelStat, "Stat", fmt.Sprintf("EOScmd: procuser.fileinfo [eospath: %s]", eospath), nil)

	// Sometimes too many stats at once causes no response, so we want to back off if it fails.
	var (
		body []byte
		m    map[string]interface{}
	)

	body, m, err = e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Stat", fmt.Sprintf("ERROR: EOSfsStat curl to MGM failed [eospath: %s, error: %+v]", eospath, err), err)
		return nil, err
	}
	if body == nil {
		eosLogger.Log(ctx, LogLevelDebug, "Stat", fmt.Sprintf("ERROR: EOSfsStat curl to MGM failed [eospath: %s, error: response body is nil]", eospath), nil)
		return nil, eoserrResponseIsNil
	}
	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Log(ctx, LogLevelDebug, "Stat", fmt.Sprintf("EOS procuser.fileinfo [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"])), nil)
		return nil, eoserrFileNotFound
	}

	eosLogger.Log(ctx, LogLevelDebug, "Stat", fmt.Sprintf("EOSfsStat: stat result [eospath: %s, name: %s, mtime: %d, mode:%d, size: %d]", eospath, interfaceToString(m["name"]), interfaceToInt64(m["mtime"]), interfaceToInt64(m["mode"]), interfaceToInt64(m["size"])), nil)

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

	reqStatCache.Write(eospath, fi)

	return &fi, nil
}

func (e *eosFS) mkdirWithOption(ctx context.Context, p, option string) error {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Log(ctx, LogLevelStat, "mkdirWithOption", fmt.Sprintf("EOScmd: procuser.mkdir [eospath: %s]", eospath), nil)
	_, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=mkdir%s&mgm.path=%s%s", option, url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "mkdirWithOption", fmt.Sprintf("ERROR: EOSmkdirWithOption curl to MGM failed [eospath: %s, error: %+v]", eospath, err), err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Log(ctx, LogLevelError, "mkdirWithOption", fmt.Sprintf("ERROR: EOS procuser.mkdir [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"])), err)
		return eoserrDiskAccessDenied
	}

	//e.messagebusAddPutJob(p)

	return nil
}

func (e *eosFS) rmdir(ctx context.Context, p string) error {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Log(ctx, LogLevelStat, "rmdir", fmt.Sprintf("EOScmd: procuser.rmdir [eospath: %s]", eospath), nil)
	_, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "rmdir", fmt.Sprintf("ERROR: EOSrmdir curl to MGM failed [eospath: %s, error: %+v]", eospath, err), err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		if strings.HasPrefix(interfaceToString(m["errormsg"]), "error: no such file or directory") {
			return nil
		}
		eosLogger.Log(ctx, LogLevelError, "rmdir", fmt.Sprintf("ERROR EOS procuser.rm [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"])), err)
		return eoserrDiskAccessDenied
	}

	//e.messagebusAddDeleteJob(p)

	return nil
}

func (e *eosFS) rm(ctx context.Context, p string) error {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Log(ctx, LogLevelStat, "rm", fmt.Sprintf("EOScmd: procuser.rm [eospath: %s]", eospath), nil)
	_, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.UrlExtras()))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "rm", fmt.Sprintf("ERROR: EOSrm curl to MGM failed [eospath: %s, error: %+v]", eospath, err), err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Log(ctx, LogLevelError, "rm", fmt.Sprintf("ERROR EOS procuser.rm [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"])), nil)
		return eoserrDiskAccessDenied
	}
	reqStatCache := e.StatCache.Get(ctx)
	reqStatCache.DeletePath(p)
	//e.messagebusAddDeleteJob(p)
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
	for {
		eosLogger.Log(ctx, LogLevelStat, "Copy", fmt.Sprintf("EOScmd: procuser.fileinfo [eospath: %s]", eossrcpath), nil)
		_, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eossrcpath), e.UrlExtras()))
		if err == nil {
			if interfaceToInt64(m["size"]) >= size {
				break
			}
		}
		eosLogger.Log(ctx, LogLevelInfo, "Copy", fmt.Sprintf("EOScopy waiting for src file to arrive: [eospath: %s, size: %d]", eossrcpath, size), nil)
		eosLogger.Log(ctx, LogLevelDebug, "Copy", fmt.Sprintf("EOScopy expecting size: %d found size: %d [eospath: %s]", size, interfaceToInt64(m["size"]), eossrcpath), nil)
		SleepMs(SleepLong)
	}

	eosLogger.Log(ctx, LogLevelStat, "Copy", fmt.Sprintf("EOScmd: procuser.file.copy [src: %s, dst: %s]", eossrcpath, eosdstpath), nil)
	_, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.UrlExtras()))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Copy", fmt.Sprintf("ERROR: EOScopy curl to MGM failed [src: %s, dst: %s, error: %+v]", eossrcpath, eosdstpath, err), err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Log(ctx, LogLevelError, "Copy", fmt.Sprintf("ERROR: EOS procuser.file.copy [src: %s, dst: %s, error: %s]", eossrcpath, eosdstpath, interfaceToString(m["errormsg"])), nil)
		return eoserrDiskAccessDenied
	}

	reqStatCache := e.StatCache.Get(ctx)
	reqStatCache.DeletePath(dst)
	//e.messagebusAddPutJob(dst)

	return nil
}

func (e *eosFS) Touch(ctx context.Context, p string, size int64) error {
	//bookingsize is ignored by touch...
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}

	eosLogger.Log(ctx, LogLevelStat, "Touch", fmt.Sprintf("EOScmd: procuser.file.touch [eospath: %s]", eospath), nil)
	_, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.UrlExtras(), size))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Touch", fmt.Sprintf("ERROR: EOStouch curl to MGM failed [eospath: %s, error: %+v]", eospath, err), err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Log(ctx, LogLevelError, "Touch", fmt.Sprintf("ERROR: EOS procuser.file.touch [eospath: %s, error: %s]", eospath, interfaceToString(m["errormsg"])), nil)
		return eoserrDiskAccessDenied
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

	eosLogger.Log(ctx, LogLevelStat, "Rename", fmt.Sprintf("EOScmd: procuser.file.rename [src: %s, dst: %s]", eosfrompath, eostopath), nil)
	_, m, err := e.MGMcurl(ctx, fmt.Sprintf("mgm.cmd=file&mgm.subcmd=rename&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eosfrompath), url.QueryEscape(eostopath), e.UrlExtras()))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "Rename", fmt.Sprintf("ERROR: EOSrename curl to MGM failed [src: %s, dst: %s, error: %+v]", eosfrompath, eostopath, err), err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Log(ctx, LogLevelError, "Rename", fmt.Sprintf("ERROR: EOS procuser.file.rename [src: %s, dst: %s, error: %s]", eosfrompath, eostopath, interfaceToString(m["errormsg"])), nil)
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosFS) SetMeta(ctx context.Context, p, key, value string) error {
	if key == "" || value == "" {
		eosLogger.Log(ctx, LogLevelDebug, "SetMeta", fmt.Sprintf("procuser.attr.set key or value is empty. [path: %s, key: %s, value: %s]", p, key, value), nil)
		//dont bother setting if we don't get what we need
		return nil
	}
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}
	eosLogger.Log(ctx, LogLevelStat, "SetMeta", fmt.Sprintf("EOScmd: procuser.attr.set [path: %s, key: %s, value: %s]", eospath, key, value), nil)
	cmd := fmt.Sprintf("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=minio_%s&mgm.attr.value=%s&mgm.path=%s%s", url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(eospath), e.UrlExtras())
	body, m, err := e.MGMcurl(ctx, cmd)
	eosLogger.Log(ctx, LogLevelDebug, "SetMeta", fmt.Sprintf("EOSsetMeta: meta tag return body [eospath: %s, body: %s]", eospath, strings.Replace(string(body), "\n", " ", -1)), nil)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "SetMeta", fmt.Sprintf("ERROR: EOSsetMeta curl to MGM failed [eospath: %s, error: %+v]", eospath, err), err)
		return err
	}

	if interfaceToString(m["errormsg"]) != "" {
		eosLogger.Log(ctx, LogLevelError, "SetMeta", fmt.Sprintf("ERROR EOS procuser.attr.set [eospath: %s, command: %s, error: %s]", eospath, cmd, interfaceToString(m["errormsg"])), nil)
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

func (e *eosFS) Put(ctx context.Context, p string, data []byte) error {
	eosLogger.Log(ctx, LogLevelInfo, "Put", fmt.Sprintf("EOSput: [path: %s]", p), nil)
	//curl -L -X PUT -T somefile -H 'Remote-User: minio' -sw '%{http_code}' http://eos:8000/eos-path/somefile

	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl := fmt.Sprintf("http://%s%s", e.HTTPHost, eospath)
	eosLogger.Log(ctx, LogLevelStat, "Put", fmt.Sprintf("EOScmd: webdav.PUT [eosurl: %s]", eosurl), nil)

	maxRetry := 10
	for retry := 1; retry <= maxRetry; retry++ {

		// SPECIAL CASE = contains a %
		if strings.IndexByte(p, '%') >= 0 {
			eosLogger.Log(ctx, LogLevelStat, "Put", fmt.Sprintf("EOScmd: webdav.PUT : SPECIAL CASE using curl [eosurl: %s]", eosurl), nil)
			cmd := exec.Command("curl", "-L", "-X", "PUT", "--data-binary", "@-", "-H", "Remote-User: minio", "-sw", "'%{http_code}'", eosurl)
			cmd.Stdin = bytes.NewReader(data)
			stdoutStderr, err := cmd.CombinedOutput()

			if err != nil {
				eosLogger.Log(ctx, LogLevelError, "Put", fmt.Sprintf("ERROR: curl failed [eosurl: %s]", eosurl), err)
				eosLogger.Log(ctx, LogLevelDebug, "Put", fmt.Sprintf("DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))), nil)
				Sleep()
				continue
			}
			if strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)) != "'201'" {
				eosLogger.Log(ctx, LogLevelError, "Put", fmt.Sprintf("ERROR: incorrect response from curl [eosurl: %s]", eosurl), nil)
				eosLogger.Log(ctx, LogLevelDebug, "Put", fmt.Sprintf("DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))), nil)
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
			eosLogger.Log(ctx, LogLevelDebug, "Put", fmt.Sprintf("EOSput: http ERROR message: [eosurl: %s, error: %+v]", eosurl, err), err)
			if res != nil {
				eosLogger.Log(ctx, LogLevelDebug, "Put", fmt.Sprintf("EOSput: http ERROR response: [eosurl: %s, response: %+v]", eosurl, res), nil)
			}

			Sleep()
			continue
		}
		if res != nil {
			defer res.Body.Close()
		} else {
			eosLogger.Log(ctx, LogLevelError, "Put", fmt.Sprintf("ERROR: EOSput: response body is nil [eosurl: %s, error: %+v]", eosurl, err), err)
			if err == nil {
				err = eoserrResponseIsNil
			}
		}

		if res.StatusCode != 201 {
			eosLogger.Log(ctx, LogLevelDebug, "Put", fmt.Sprintf("EOSput: http StatusCode != 201: [eosurl: %s, result: %+v]", eosurl, res), nil)
			err = eoserrCantPut
			SleepMs(SleepShort)
			continue
		}

		//e.messagebusAddPutJob(p)
		return err
	}
	eosLogger.Log(ctx, LogLevelError, "Put", fmt.Sprintf("ERROR: EOSput failed %d times. [eosurl %s, error: %+v]", maxRetry, eosurl, err), err)
	return err
}

func (e *eosFS) xrootdWriteChunk(ctx context.Context, p string, offset, size int64, checksum string, data []byte) error {
	eospath, err := e.AbsoluteEOSPath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("root://%s@%s/%s", e.User, e.MGMHost, eospath)
	eosLogger.Log(ctx, LogLevelStat, "xrootdWriteChunk", fmt.Sprintf("EOScmd: xrootd.PUT: [script: %s, eosurl: %s, offset: %d, size: %d, checksum: %s, uid: %d, gid: %d]", e.Scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.UID, e.GID), nil)

	cmd := exec.Command(e.Scripts+"/writeChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(size, 10), checksum, e.UID, e.GID)
	cmd.Stdin = bytes.NewReader(data)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "xrootdWriteChunk", fmt.Sprintf("ERROR: can not [script: %s, eosurl: %s, offset: %d, size: %d, checksum: %s, uid: %s, gid: %s]", e.Scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.UID, e.GID), err)
		eosLogger.Log(ctx, LogLevelDebug, "xrootdWriteChunk", fmt.Sprintf("DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))), nil)
	}

	return err
}

func (e *eosFS) xrdcp(ctx context.Context, src, dst string, size int64) error {
	eospath, err := e.AbsoluteEOSPath(dst)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s&eos.bookingsize=%d", e.MGMHost, eospath, e.UID, e.GID, size))
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "xrdcp", fmt.Sprintf("ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl), err)
		return err
	}

	eosLogger.Log(ctx, LogLevelStat, "xrdcp", fmt.Sprintf("EOScmd: xrdcp.PUT: [eospath: %s, eosurl: %s]", eospath, eosurl), nil)

	cmd := exec.Command("/usr/bin/xrdcp", "-N", "-f", "-p", src, eosurl)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "xrdcp", fmt.Sprintf("ERROR: can not /usr/bin/xrdcp -N -f -p %s %s [eospath: %s, eosurl: %s]", src, eosurl, eospath, eosurl), err)
	}
	output := strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))
	if output != "" {
		eosLogger.Log(ctx, LogLevelInfo, "xrdcp", fmt.Sprintf("%s", output), nil)
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
		eosLogger.Log(ctx, LogLevelStat, "ReadChunk", fmt.Sprintf("EOScmd: xrootd.GET: [eospath: %s, eosurl: %s]", eospath, eosurl), nil)

		cmd := exec.Command(e.Scripts+"/readChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(length, 10), e.UID, e.GID)
		var stderr bytes.Buffer
		cmd.Stdout = data
		cmd.Stderr = &stderr
		err2 := cmd.Run()
		errStr := strings.TrimSpace(string(stderr.Bytes()))
		eosLogger.Log(ctx, LogLevelInfo, "ReadChunk", fmt.Sprintf("EOSreadChunk: [script: %s, eosurl: %s, offset: %d, length: %d, uid: %s, gid: %s, error: %+v]", e.Scripts+"/readChunk.py", eosurl, offset, length, e.UID, e.GID, err2), nil)
		if errStr != "" {
			eosLogger.Log(ctx, LogLevelError, "ReadChunk", fmt.Sprintf("ERROR: EOSreadChunk [eosurl: %s, error: %s]", eosurl, errStr), nil)
		}
	case "xrdcp":
		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s", e.MGMHost, eospath, e.UID, e.GID))
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

		errStr := strings.TrimSpace(string(stderr.Bytes()))
		eosLogger.Log(ctx, LogLevelInfo, "ReadChunk", fmt.Sprintf("/usr/bin/xrdcp -N %s - %+v", eosurl, err2), nil)
		if errStr != "" {
			eosLogger.Log(ctx, LogLevelError, "ReadChunk", fmt.Sprintf("%s", errStr), nil)
		}

		if offset >= 0 {
			stdout.Next(int(offset))
		}
		stdout.Truncate(int(length))
		stdout.WriteTo(data)
	default: //webdav
		//curl -L -X GET -H 'Remote-User: minio' -H 'Range: bytes=5-7' http://eos:8000/eos-path-to-file

		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl := fmt.Sprintf("http://%s%s", e.HTTPHost, eospath)
		eosLogger.Log(ctx, LogLevelStat, "ReadChunk", fmt.Sprintf("EOScmd: webdav.GET: [eosurl: %s]", eosurl), nil)

		client, req, err := e.NewRequest("GET", eosurl, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		req.Close = true
		res, err := client.Do(req)

		if err != nil {
			eosLogger.Log(ctx, LogLevelError, "ReadChunk", fmt.Sprintf("ERROR: webdav.GET [eosurl: %s, error: %+v]", eosurl, err), err)
			return err
		}
		// TODO: Might need to return here if res is nil
		if res != nil {
			defer res.Body.Close()
		} else {
			eosLogger.Log(ctx, LogLevelError, "ReadChunk", fmt.Sprintf("ERROR: webdav.GET: response body is nil [eosurl: %s, error: %+v]", eosurl, err), nil)
			err = eoserrResponseIsNil
		}
		_, err = io.CopyN(data, res.Body, length)
	}
	return err
}
