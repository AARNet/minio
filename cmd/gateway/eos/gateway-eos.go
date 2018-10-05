/*
 * AARNet 2018
 *
 * Michael D'Silva
 *
 * This is a gateway for AARNet's CERN's EOS storage backend (v4.2.29)
 *
 */

package eos

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	//"hash/adler32"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	miniohash "github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"
)

const (
	eosBackend = "eos"
)

func init() {
	const eosGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PATH
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PATH:
  Path to EOS mount point.

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of minimum 3 characters in length.
     MINIO_SECRET_KEY: Password or secret key of minimum 8 characters in length.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

  EOS:
     EOSLOGLEVEL: 0..n 0=off 100000000000000000000000=lots of logs
     EOS: url to eos
     EOSUSER: eos username
     EOSUID: eos user uid
     EOSGID: eos user gid
     EOSSTAGE: local fast disk to stage multipart uploads
     EOSREADONLY: true/false
     EOSREADMETHOD: webdav/xrootd/xrdcp (DEFAULT: webdav)
     EOSMAXPROCUSER: int (default 12)
     EOSSLEEPPROCUSER: int ms sleep 1000ms = 1s (default 100 ms)
     VOLUME_PATH: path on eos
     HOOKSURL: url to s3 hooks (not setting this will disable hooks)
     SCRIPTS: path to xroot script

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).

EXAMPLES:
  1. Start minio gateway server for EOS backend.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ export EOS=url to eos
     $ export EOSLOGLEVEL=1
     $ export EOSUSER=eos username
     $ export EOSUID=eos user uid
     $ export EOSGID=eos user gid
     $ export EOSSTAGE=local fast disk to stage multipart uploads
     $ export VOLUME_PATH=path on eos
     $ export HOOKSURL=url to s3 hooks
     $ export SCRIPTS=path to xroot script
     $ {{.HelpName}} ${VOLUME_PATH}

  2. Start minio gateway server for EOS with edge caching enabled.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ export EOS=url to eos
     $ export EOSLOGLEVEL=1
     $ export EOSUSER=eos username
     $ export EOSUID=eos user uid
     $ export EOSGID=eos user gid
     $ export EOSSTAGE=local fast disk to stage multipart uploads
     $ export VOLUME_PATH=path on eos
     $ export HOOKSURL=url to s3 hooks
     $ export SCRIPTS=path to xroot script
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
     $ export MINIO_CACHE_MAXUSE=80
     $ {{.HelpName}} ${VOLUME_PATH}
`
	minio.RegisterGatewayCommand(cli.Command{
		Name:               eosBackend,
		Usage:              "AARNet's CERN's EOS",
		Action:             eosGatewayMain,
		CustomHelpTemplate: eosGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway eos' command line.
func eosGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, eosBackend, 1)
	}

	minio.StartGateway(ctx, &EOS{ctx.Args().First()})
}

// EOS implements Gateway.
type EOS struct {
	path string
}

// Name implements Gateway interface.
func (g *EOS) Name() string {
	return eosBackend
}

// NewGatewayLayer returns eos gatewaylayer.
func (g *EOS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	loglevel, ok := strconv.Atoi(os.Getenv("EOSLOGLEVEL"))
	if ok != nil {
		loglevel = 0
	}

	const CLR_W = "\x1b[37;1m"
	const CLR_B = "\x1b[34;1m"
	const CLR_Y = "\x1b[33;1m"
	const CLR_G = "\x1b[32;1m"
	const CLR_N = "\x1b[0m"

	stage := os.Getenv("EOSSTAGE")
	if stage != "" {
		os.MkdirAll(stage, 0700)
	}

	readonly := false
	if os.Getenv("EOSREADONLY") == "true" {
		readonly = true
	}

	readmethod := "webdav"
	if strings.ToLower(os.Getenv("EOSREADMETHOD")) == "xrootd" {
		readmethod = "xrootd"
	} else if strings.ToLower(os.Getenv("EOSREADMETHOD")) == "xrdcp" {
		readmethod = "xrdcp"
	}

	procuserMax := 12
	ret, err := strconv.Atoi(os.Getenv("EOSMAXPROCUSER"))
	if err == nil {
		procuserMax = ret
	}
	procuserSleep := 100
	ret, err = strconv.Atoi(os.Getenv("EOSSLEEPPROCUSER"))
	if err == nil {
		procuserSleep = ret
	}

	fmt.Printf("------%sEOS CONFIG%s------\n", CLR_G, CLR_N)
	fmt.Printf("%sEOS URL              %s:%s %s%s\n", CLR_B, CLR_N, CLR_W, os.Getenv("EOS"), CLR_N)
	fmt.Printf("%sEOS VOLUME PATH      %s:%s %s%s\n", CLR_B, CLR_N, CLR_W, os.Getenv("VOLUME_PATH"), CLR_N)
	fmt.Printf("%sEOS USER (uid:gid)   %s:%s %s (%s:%s)%s\n", CLR_B, CLR_N, CLR_W, os.Getenv("EOSUSER"), os.Getenv("EOSUID"), os.Getenv("EOSGID"), CLR_N)
	fmt.Printf("%sEOS file hooks url   %s:%s %s%s\n", CLR_B, CLR_N, CLR_W, os.Getenv("HOOKSURL"), CLR_N)
	fmt.Printf("%sEOS SCRIPTS PATH     %s:%s %s%s\n", CLR_B, CLR_N, CLR_W, os.Getenv("SCRIPTS"), CLR_N)

	if stage != "" {
		fmt.Printf("%sEOS staging          %s:%s %s%s\n", CLR_B, CLR_N, CLR_W, stage, CLR_N)
	} else {
		fmt.Printf("%sEOS staging          %s: %sDISABLED%s\n", CLR_B, CLR_N, CLR_Y, CLR_N)
	}

	if readonly {
		fmt.Printf("%sEOS read only mode   %s: %sENABLED%s\n", CLR_B, CLR_N, CLR_W, CLR_N)
	}

	fmt.Printf("%sEOS READ METHOD      %s:%s %s%s\n", CLR_B, CLR_N, CLR_W, readmethod, CLR_N)
	fmt.Printf("%sEOS /proc/user MAX   %s:%s %d%s\n", CLR_B, CLR_N, CLR_W, procuserMax, CLR_N)
	fmt.Printf("%sEOS /proc/user SLEEP %s:%s %d%s\n", CLR_B, CLR_N, CLR_W, procuserSleep, CLR_N)

	fmt.Printf("%sEOS LOG LEVEL        %s:%s %d%s\n", CLR_B, CLR_N, CLR_W, loglevel, CLR_N)

	return &eosObjects{
		loglevel:      loglevel,
		url:           os.Getenv("EOS"),
		path:          os.Getenv("VOLUME_PATH"),
		hookurl:       os.Getenv("HOOKSURL"),
		scripts:       os.Getenv("SCRIPTS"),
		user:          os.Getenv("EOSUSER"),
		uid:           os.Getenv("EOSUID"),
		gid:           os.Getenv("EOSGID"),
		stage:         stage,
		readonly:      readonly,
		readmethod:    readmethod,
		procuserMax:   procuserMax,
		procuserSleep: procuserSleep,
	}, nil
}

// Production - eos gateway is production ready.
func (g *EOS) Production() bool {
	return false //hahahahaha
}

// eosObjects implements gateway for Minio and S3 compatible object storage servers.
type eosObjects struct {
	loglevel      int
	url           string
	path          string
	hookurl       string
	scripts       string
	user          string
	uid           string
	gid           string
	stage         string
	readonly      bool
	readmethod    string
	procuserMax   int
	procuserSleep int
}

// IsNotificationSupported returns whether notifications are applicable for this layer.
func (e *eosObjects) IsNotificationSupported() bool {
	return false
}

// IsEncryptionSupported returns whether server side encryption is applicable for this layer.
func (e *eosObjects) IsEncryptionSupported() bool {
	return true
}

// Shutdown - nothing to do really...
func (e *eosObjects) Shutdown(ctx context.Context) error {
	return nil
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (e *eosObjects) IsCompressionSupported() bool {
	return false
}

// StorageInfo
func (e *eosObjects) StorageInfo(ctx context.Context) (storageInfo minio.StorageInfo) {
	return storageInfo
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Bucket

// GetBucketInfo - Get bucket metadata
func (e *eosObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	e.Log(1, "DEBUG: GetBucketInfo %s\n", bucket)

	if bi, ok := eosBucketCache[bucket]; ok {
		e.Log(3, "bucket cache hit: %s\n", bucket)
		return bi, nil
	}
	e.Log(3, "bucket cache miss: %s\n", bucket)
	stat, err := e.EOSfsStat(bucket)

	if err == nil {
		bi = minio.BucketInfo{
			Name:    bucket,
			Created: stat.ModTime()}

		eosBucketCache[bucket] = bi
	} else {
		err = minio.BucketNotFound{Bucket: bucket}
	}
	return bi, err
}

// ListBuckets - Lists all root folders
func (e *eosObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	e.Log(1, "DEBUG: ListBuckets\n")

	eosBucketCache = make(map[string]minio.BucketInfo)

	dirs, err := e.EOSreadDir("", false)
	if err != nil {
		return buckets, err
	}

	for _, dir := range dirs {
		var stat *eosFileStat
		stat, err = e.EOSfsStat(dir)
		if stat != nil {
			if stat.IsDir() && minio.IsValidBucketName(strings.TrimRight(dir, "/")) {
				b := minio.BucketInfo{
					Name:    dir,
					Created: stat.ModTime()}
				buckets = append(buckets, b)
				eosBucketCache[strings.TrimSuffix(dir, "/")] = b
			} else {
				if !stat.IsDir() {
					e.Log(3, "DEBUG: Bucket: %s not a directory\n", dir)
				}
				if !minio.IsValidBucketName(strings.TrimRight(dir, "/")) {
					e.Log(3, "DEBUG: Bucket: %s not a valid name\n", dir)
				}
			}
		} else {
			e.Log(1, "ERROR: ListBuckets - can not stat %s\n", dir)
		}
	}

	eosDirCache.path = ""

	e.Log(4, "DEBUG: BucketCache: %+v\n", eosBucketCache)

	return buckets, err
}

// MakeBucketWithLocation - Create a new container.
func (e *eosObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	e.Log(1, "DEBUG: MakeBucketWithLocation: %s %s\n", bucket, location)

	if e.readonly {
		return minio.NotImplemented{}
	}

	// Verify if bucket is valid.
	if !minio.IsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	if _, err := e.EOSfsStat(bucket); err != nil {
		err := e.EOSmkdirWithOption(bucket, "")
		if err != nil {
			return minio.BucketNotFound{Bucket: bucket}
		}
	} else {
		return minio.BucketExists{Bucket: bucket}
	}
	return nil
}

// DeleteBucket - delete a container
func (e *eosObjects) DeleteBucket(ctx context.Context, bucket string) error {
	e.Log(1, "DEBUG: DeleteBucket: %s\n", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return e.EOSrmdir(bucket)
}

// GetBucketPolicy - Get the container ACL
func (e *eosObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	e.Log(4, "DEBUG: GetBucketPolicy: %s\n", bucket)

	return &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.GetBucketLocationAction,
					policy.ListBucketAction,
					policy.GetObjectAction,
				),
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}, nil
}

// SetBucketPolicy
func (e *eosObjects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	e.Log(4, "DEBUG: SetBucketPolicy: %s, %+v\n", bucket, bucketPolicy)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (e *eosObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	e.Log(4, "DEBUG: DeleteBucketPolicy: %s\n", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Object

// CopyObject - Copies a blob from source container to destination container.
func (e *eosObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	e.Log(1, "DEBUG: CopyObject: %s -> %s : %+v\n", srcBucket+"/"+srcObject, destBucket+"/"+destObject, srcInfo)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	dir := destBucket + "/" + filepath.Dir(destObject)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.EOScopy(srcBucket+"/"+srcObject, destBucket+"/"+destObject, srcInfo.Size)
	if err != nil {
		e.Log(2, "ERROR: COPY:%+v\n", err)
		return objInfo, err
	}
	err = e.EOSsetETag(destBucket+"/"+destObject, srcInfo.ETag)
	if err != nil {
		e.Log(2, "ERROR: COPY:%+v\n", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(destBucket+"/"+destObject, srcInfo.ContentType)
	if err != nil {
		e.Log(2, "ERROR: COPY:%+v\n", err)
		return objInfo, err
	}

	e.EOScacheDeleteObject(destBucket, destObject)
	return e.GetObjectInfo(ctx, destBucket, destObject, dstOpts)
}

// CopyObjectPart creates a part in a multipart upload by copying
func (e *eosObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	e.Log(1, "DEBUG: CopyObjectPart: %s/%s to %s/%s\n", srcBucket, srcObject, destBucket, destObject)

	if e.readonly {
		return p, minio.NotImplemented{}
	}

	return p, minio.NotImplemented{}
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *miniohash.Reader, metadata map[string]string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	e.Log(1, "DEBUG: PutObject: %s/%s\n", bucket, object)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for key, val := range metadata {
		e.Log(3, "DEBUG: PutObject %s = %s\n", key, val)
	}

	buf, _ := ioutil.ReadAll(data)
	etag := hex.EncodeToString(data.MD5Current())

	dir := bucket + "/" + filepath.Dir(object)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.EOSput(bucket+"/"+object, buf)
	if err != nil {
		e.Log(2, "ERROR: PUT:%+v\n", err)
		return objInfo, err
	}
	err = e.EOSsetETag(bucket+"/"+object, etag)
	if err != nil {
		e.Log(2, "ERROR: PUT:%+v\n", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(bucket+"/"+object, metadata["content-type"])
	if err != nil {
		e.Log(2, "ERROR: PUT:%+v\n", err)
		return objInfo, err
	}

	e.EOScacheDeleteObject(bucket, object)
	return e.GetObjectInfo(ctx, bucket, object, opts)
}

// DeleteObject - Deletes a blob on azure container
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	e.Log(1, "DEBUG: DeleteObject: %s/%s\n", bucket, object)

	if e.readonly {
		return minio.NotImplemented{}
	}

	e.EOSrm(bucket + "/" + object)

	return nil
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)

	e.Log(1, "DEBUG: GetObject: %s from %d for %d byte(s)\n", path, startOffset, length)

	if etag != "" {
		objInfo, err := e.GetObjectInfo(ctx, bucket, object, opts)
		if err != nil {
			return err
		}
		if objInfo.ETag != etag {
			return minio.InvalidETag{}
		}
	}

	err := e.EOSreadChunk(path, startOffset, length, writer)

	return err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo
func (e *eosObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)

	e.Log(1, "DEBUG: GetObjectInfo: %s\n", path)

	stat, err := e.EOSfsStat(path)

	if err != nil {
		e.Log(4, "%+v\n", err)
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object}
		return objInfo, err
	}

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     stat.ModTime(),
		Size:        stat.Size(),
		IsDir:       stat.IsDir(),
		ETag:        stat.ETag(),
		ContentType: stat.ContentType(),
	}

	e.Log(2, "  %s etag:%s content-type:%s\n", path, stat.ETag(), stat.ContentType())
	return objInfo, err
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (e *eosObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = e.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := e.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, pipeCloser), nil
}

// ListMultipartUploads - lists all multipart uploads.
func (e *eosObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	e.Log(1, "DEBUG: ListMultipartUploads: %s %s %s %s %s %d\n", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, minio.NotImplemented{}
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string, opts minio.ObjectOptions) (uploadID string, err error) {
	e.Log(1, "DEBUG: NewMultipartUpload: %s/%s +%v +%v\n", bucket, object, metadata, opts)

	if e.readonly {
		return "", minio.NotImplemented{}
	}

	uploadID = bucket + "/" + object

	if strings.HasSuffix(uploadID, "/") {
		return "", minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	dir := bucket + "/" + filepath.Dir(object)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.Log(2, "  MKDIR : %s\n", dir)
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	mp := eosMultiPartsType{
		parts:       make(map[int]minio.PartInfo),
		partsCount:  0,
		size:        0,
		chunkSize:   0,
		firstByte:   0,
		contenttype: metadata["content-type"],
		md5:         md5.New(),
		md5PartID:   1,
	}

	if e.stage != "" {
		hasher := md5.New()
		hasher.Write([]byte(uploadID))
		stagepath := hex.EncodeToString(hasher.Sum(nil))

		//make sure it is clear of older junk
		os.RemoveAll(e.stage + "/" + stagepath)

		err = os.MkdirAll(e.stage+"/"+stagepath, 0700)
		if err != nil {
			e.Log(2, "  MKDIR %s FAILED %+v\n", e.stage+"/"+stagepath, err)
		}
		mp.stagepath = stagepath
	}

	//eosMultiPartsMutex.Lock()
	eosMultiParts[uploadID] = &mp
	//eosMultiPartsMutex.Unlock()

	e.Log(2, "  uploadID : %s\n", uploadID)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *miniohash.Reader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	e.Log(1, "DEBUG: PutObjectPart: %s/%s %s [%d] %d\n", bucket, object, uploadID, partID, data.Size())

	if e.readonly {
		return info, minio.NotImplemented{}
	}

	for eosMultiParts[uploadID].parts == nil {
		e.Log(1, "DEBUG: PutObjectPart called before NewMultipartUpload finished...\n")
		e.EOSsleep()
	}

	size := data.Size()
	buf, _ := ioutil.ReadAll(data)
	etag := hex.EncodeToString(data.MD5Current())

	newPart := minio.PartInfo{
		PartNumber:   partID,
		LastModified: time.Now(),
		ETag:         etag,
		Size:         size,
	}

	eosMultiParts[uploadID].mutex.Lock()
	eosMultiParts[uploadID].parts[partID] = newPart
	eosMultiParts[uploadID].mutex.Unlock()

	if partID == 1 {
		eosMultiParts[uploadID].mutex.Lock()
		eosMultiParts[uploadID].firstByte = buf[0]
		eosMultiParts[uploadID].chunkSize = size
		eosMultiParts[uploadID].mutex.Unlock()
	} else {
		for eosMultiParts[uploadID].chunkSize == 0 {
			e.Log(1, "DEBUG: PutObjectPart ok, waiting for first chunk (processing part %d)...\n", partID)
			e.EOSsleep()
		}
	}

	eosMultiParts[uploadID].mutex.Lock()
	eosMultiParts[uploadID].partsCount++
	eosMultiParts[uploadID].AddToSize(size)
	eosMultiParts[uploadID].mutex.Unlock()

	offset := eosMultiParts[uploadID].chunkSize * int64(partID-1)
	e.Log(3, "DEBUG: PutObjectPart offset = %d = %d\n", (partID - 1), offset)

	if e.stage != "" { //staging
		e.Log(1, "DEBUG: PutObjectPart Staging\n")

		f, err := os.OpenFile(e.stage+"/"+eosMultiParts[uploadID].stagepath+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			e.Log(2, "ERROR: Write ContentType: %+v\n", err)
			return newPart, err
		}
		_, err = f.WriteAt(buf, offset)
		if err != nil {
			e.Log(2, "ERROR: Write ContentType: %+v\n", err)
			return newPart, err
		}
		f.Close()
	} else {
		go func() {
			err = e.EOSwriteChunk(uploadID, offset, offset+size, "0", buf)
		}()
	}

	go func() {
		for eosMultiParts[uploadID].md5PartID != partID {
			e.Log(3, "DEBUG: PutObjectPart waiting for md5PartID = %d, currently = %d\n", eosMultiParts[uploadID].md5PartID, partID)
			e.EOSsleep()
		}
		eosMultiParts[uploadID].mutex.Lock()
		eosMultiParts[uploadID].md5.Write(buf)
		eosMultiParts[uploadID].md5PartID++
		eosMultiParts[uploadID].mutex.Unlock()
	}()

	return newPart, nil
}

// CompleteMultipartUpload
func (e *eosObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart) (objInfo minio.ObjectInfo, err error) {
	e.Log(1, "DEBUG: CompleteMultipartUpload: %s size : %d firstByte : %d\n", uploadID, eosMultiParts[uploadID].size, eosMultiParts[uploadID].firstByte)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for eosMultiParts[uploadID].md5PartID != eosMultiParts[uploadID].partsCount+1 {
		e.Log(3, "DEBUG: PutObjectPart waiting for all md5Parts, %d remaining\n", eosMultiParts[uploadID].partsCount+1-eosMultiParts[uploadID].md5PartID)
		e.EOSsleep()
	}
	etag := hex.EncodeToString(eosMultiParts[uploadID].md5.Sum(nil))
	e.Log(2, "ETAG: %s\n", etag)

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     time.Now(),
		Size:        eosMultiParts[uploadID].size,
		IsDir:       false,
		ETag:        etag,
		ContentType: eosMultiParts[uploadID].contenttype,
	}

	if e.stage != "" { //staging
		err = e.EOStouch(uploadID, eosMultiParts[uploadID].size)
		if err != nil {
			e.Log(2, "ERROR: CompleteMultipartUpload: EOStouch :%+v\n", err)
			return objInfo, err
		}

		//upload in background
		go func() {
			e.Log(3, "DEBUG: CompleteMultipartUpload xrdcp: %s => %s size: %d\n", e.stage+"/"+eosMultiParts[uploadID].stagepath+"/file", uploadID+".minio.sys", eosMultiParts[uploadID].size)
			err := e.EOSxrdcp(e.stage+"/"+eosMultiParts[uploadID].stagepath+"/file", uploadID+".minio.sys", eosMultiParts[uploadID].size)
			if err != nil {
				e.Log(2, "ERROR: CompleteMultipartUpload: xrdcp: %+v\n", err)
				return
			}

			err = e.EOSrename(uploadID+".minio.sys", uploadID)
			if err != nil {
				e.Log(2, "ERROR: CompleteMultipartUpload: EOSrename: %+v\n", err)
				return
			}

			err = e.EOSsetETag(uploadID, etag)
			if err != nil {
				e.Log(2, "ERROR: CompleteMultipartUpload: EOSsetETag: %+v\n", err)
				return
			}

			err = e.EOSsetContentType(uploadID, eosMultiParts[uploadID].contenttype)
			if err != nil {
				e.Log(2, "ERROR: CompleteMultipartUpload: EOSsetContentType: %+v\n", err)
				return
			}

			err = os.RemoveAll(e.stage + "/" + eosMultiParts[uploadID].stagepath)
			if err != nil {
				return
			}

			//eosMultiPartsMutex.Lock()
			delete(eosMultiParts, uploadID)
			//eosMultiPartsMutex.Unlock()
			e.messagebusAddPutJob(uploadID)
		}()
	} else {
		err = e.EOSwriteChunk(uploadID, 0, eosMultiParts[uploadID].size, "1", []byte{eosMultiParts[uploadID].firstByte})
		if err != nil {
			e.Log(2, "ERROR: CompleteMultipartUpload: EOSwriteChunk: %+v\n", err)
			return objInfo, err
		}

		//eosMultiPartsMutex.Lock()
		delete(eosMultiParts, uploadID)
		//eosMultiPartsMutex.Unlock()
		e.messagebusAddPutJob(uploadID)
	}

	err = e.EOSsetETag(uploadID, etag)
	if err != nil {
		e.Log(2, "ERROR: CompleteMultipartUpload:%+v\n", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(uploadID, eosMultiParts[uploadID].contenttype)
	if err != nil {
		e.Log(2, "ERROR: CompleteMultipartUpload:%+v\n", err)
		return objInfo, err
	}

	//populate cache
	e.EOScacheDeletePath(uploadID)
	stat, err := e.EOSfsStat(uploadID)
	e.EOScacheDeletePath(uploadID)
	stat.size = objInfo.Size
	stat.file = true
	stat.etag = objInfo.ETag
	stat.contenttype = objInfo.ContentType
	stat.modTime = objInfo.ModTime
	eospath, err := e.EOSpath(uploadID)
	e.EOScacheWrite(eospath, *stat)

	return objInfo, nil
}

//AbortMultipartUpload
func (e *eosObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	e.Log(1, "DEBUG: AbortMultipartUpload: %s/%s %s\n", bucket, object, uploadID)

	if e.readonly {
		return minio.NotImplemented{}
	}

	if e.stage != "" { //staging
		os.RemoveAll(e.stage + "/" + eosMultiParts[uploadID].stagepath)
	}

	e.EOSrm(bucket + "/" + object)

	//eosMultiPartsMutex.Lock()
	delete(eosMultiParts, uploadID)
	//eosMultiPartsMutex.Unlock()

	return nil
}

// ListObjectParts
func (e *eosObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (result minio.ListPartsInfo, err error) {
	e.Log(1, "DEBUG: ListObjectParts: %s %d %d\n", uploadID, partNumberMarker, maxParts)

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker

	eosMultiParts[uploadID].mutex.Lock()
	i := 0
	size := eosMultiParts[uploadID].partsCount
	result.Parts = make([]minio.PartInfo, size)
	for _, part := range eosMultiParts[uploadID].parts {
		if i < size {
			result.Parts[i] = part
			i++
		}
	}
	eosMultiParts[uploadID].mutex.Unlock()

	return result, nil
}

// ListObjects - lists all blobs in a container filtered by prefix and marker
func (e *eosObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	e.Log(1, "DEBUG: ListObjects: %s, %s, %s, %s, %d\n", bucket, prefix, marker, delimiter, maxKeys)
	if delimiter == "/" && prefix == "/" {
		e.Log(2, "  delimiter and prefix is slash\n")
		return result, nil
	}

	path := strings.TrimSuffix(bucket+"/"+prefix, "/")
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}

	//if eosDirCache.path != path || len(eosDirCache.objects) == 0 {
	e.Log(2, "  NEW CACHE for %s\n", path)
	eosDirCache.path = path
	eosDirCache.objects, err = e.EOSreadDir(path, true)
	if err != nil {
		return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
	}
	//}

	for _, obj := range eosDirCache.objects {
		var stat *eosFileStat
		stat, err = e.EOSfsStat(path + "/" + obj)

		if stat != nil && !strings.HasSuffix(obj, ".minio.sys") {
			e.Log(4, "  %s, %s, %s = %s\n", bucket, prefix, obj, bucket+"/"+prefix+obj)
			e.Log(3, "  %s <=> %s etag:%s content-type:%s\n", path+"/"+obj, prefix+obj, stat.ETag(), stat.ContentType())
			o := minio.ObjectInfo{
				Bucket:      bucket,
				Name:        prefix + obj,
				ModTime:     stat.ModTime(),
				Size:        stat.Size(),
				IsDir:       stat.IsDir(),
				ETag:        stat.ETag(),
				ContentType: stat.ContentType(),
			}

			result.Objects = append(result.Objects, o)

			if delimiter == "" { //recursive
				if stat.IsDir() {
					e.Log(3, "  ASKING FOR -r on : %s\n", prefix+obj)

					subdir, err := e.ListObjects(ctx, bucket, prefix+obj, marker, delimiter, -1)
					if err != nil {
						return result, err
					}
					for _, subobj := range subdir.Objects {
						result.Objects = append(result.Objects, subobj)
					}
				}
			}
		} else {
			e.Log(1, "ERROR: ListObjects - can not stat %s\n", path+"/"+obj)
		}
	}

	result.IsTruncated = false
	//result.Prefixes = append(result.Prefixes, prefix)

	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	e.Log(1, "DEBUG: ListObjectsV2: %s, %s, %s, %s, %d\n", bucket, prefix, continuationToken, delimiter, maxKeys)

	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	var resultV1 minio.ListObjectsInfo
	resultV1, err = e.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	result.Objects = resultV1.Objects
	result.Prefixes = resultV1.Prefixes
	result.ContinuationToken = continuationToken
	result.NextContinuationToken = resultV1.NextMarker
	result.IsTruncated = (resultV1.NextMarker != "")
	return result, nil
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Don't think we need this...

// HealFormat - no-op for fs
func (e *eosObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	e.Log(1, "DEBUG: HealFormat:\n")
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

// ReloadFormat - no-op for fs
func (e *eosObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	e.Log(1, "DEBUG: ReloadFormat:\n")
	return minio.NotImplemented{}
}

// ListObjectsHeal - list all objects to be healed.
func (e *eosObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	e.Log(1, "DEBUG: ListObjectsHeal:\n")
	return loi, minio.NotImplemented{}
}

// HealObject - no-op for fs.
func (e *eosObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool) (res madmin.HealResultItem, err error) {
	e.Log(1, "DEBUG: HealObject:\n")
	return res, minio.NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed
func (e *eosObjects) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	e.Log(1, "DEBUG: ListBucketsHeal:\n")
	return []minio.BucketInfo{}, minio.NotImplemented{}
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (e *eosObjects) HealBucket(ctx context.Context, bucket string, dryRun bool) (results []madmin.HealResultItem, err error) {
	e.Log(1, "DEBUG: HealBucket:\n")
	return nil, minio.NotImplemented{}
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Helpers

type eosFileStat struct {
	id          int64
	name        string
	size        int64
	file        bool
	modTime     time.Time
	sys         syscall.Stat_t
	etag        string
	contenttype string
	//checksum    string
}

func (fs *eosFileStat) Id() int64          { return fs.id }
func (fs *eosFileStat) Name() string       { return fs.name }
func (fs *eosFileStat) Size() int64        { return fs.size }
func (fs *eosFileStat) ModTime() time.Time { return fs.modTime }
func (fs *eosFileStat) Sys() interface{}   { return &fs.sys }
func (fs *eosFileStat) IsDir() bool        { return !fs.file }

//func (fs *eosFileStat) Checksum() string   { return fs.checksum }
func (fs *eosFileStat) ETag() string {
	if fs.IsDir() || fs.etag == "" {
		return "00000000000000000000000000000000"
	}
	return fs.etag
}
func (fs *eosFileStat) ContentType() string {
	if fs.IsDir() {
		return "application/x-directory"
	}
	if fs.contenttype == "" {
		return "application/octet-stream"
	}
	return fs.contenttype
}

type eosDirCacheType struct {
	objects []string
	path    string
}

var eosFileStatCache = make(map[string]eosFileStat)
var eosDirCache = eosDirCacheType{}
var eosBucketCache = make(map[string]minio.BucketInfo)

var eoserrFileNotFound = errors.New("EOS: file not found")
var eoserrDiskAccessDenied = errors.New("EOS: disk access denied")
var eoserrCantPut = errors.New("EOS: can't put")
var eoserrFilePathBad = errors.New("EOS: bad file path")

//var eoserrSetMeta = errors.New("EOS: can't set metadata")

var eosfsStatMutex = sync.RWMutex{}

type eosMultiPartsType struct {
	parts       map[int]minio.PartInfo
	partsCount  int
	size        int64
	chunkSize   int64
	firstByte   byte
	contenttype string
	stagepath   string
	md5         hash.Hash
	md5PartID   int
	mutex       sync.Mutex
}

func (mp *eosMultiPartsType) AddToSize(size int64) { mp.size += size }

var eosMultiParts = make(map[string]*eosMultiPartsType)

type procuserJob struct {
	busy    bool
	lastrun int64
}

var procuserJobs = make(map[int]*procuserJob)
var procuserAmountWaiting = 0
var procuserAmountMutex = sync.Mutex{}

func (e *eosObjects) procuserAmountRunning() int {
	count := 0
	for i := 0; i < e.procuserMax; i++ {
		job, ok := procuserJobs[i]
		if ok {
			if job.busy {
				count++
			}
		}
	}
	return count
}

func (e *eosObjects) procuserAmountRead() (waiting, running int) {
	procuserAmountMutex.Lock()
	procuserAmountRunning := e.procuserAmountRunning()
	defer procuserAmountMutex.Unlock()
	return procuserAmountWaiting, procuserAmountRunning
}

func (e *eosObjects) procuserAmountWaitingInc() (waiting, busy int) {
	procuserAmountMutex.Lock()
	procuserAmountWaiting++
	procuserAmountRunning := e.procuserAmountRunning()
	defer procuserAmountMutex.Unlock()
	return procuserAmountWaiting, procuserAmountRunning
}

func (e *eosObjects) procuserGetFreeSlot() int {
	slot := -1
	now := time.Now().Unix()
	procuserAmountMutex.Lock()
	for i := 0; i < e.procuserMax; i++ {
		job, ok := procuserJobs[i]
		if ok {
			e.Log(4, "DEBUG: procuser slot %d job: %+v\n", i, job)
			if !job.busy && now-job.lastrun >= 1 {
				slot = i
				procuserAmountWaiting--
				job.busy = true
				job.lastrun = now
				break
			}
		} else {
			e.Log(4, "DEBUG: procuser slot %d does not exist, creating\n", i)
			slot = i
			procuserAmountWaiting--
			newJob := procuserJob{
				busy:    true,
				lastrun: now,
			}
			procuserJobs[i] = &newJob
			break
		}
	}
	defer procuserAmountMutex.Unlock()
	return slot
}

func (e *eosObjects) procuserWaitForSlot() int {
	slot := -1
	if e.procuserMax > 0 {
		amountWaiting, amountRunning := e.procuserAmountWaitingInc()
		e.Log(3, "DEBUG: procuserAmountRunning / procuserMax (%d/%d), procuserAmountWaiting: %d Before curl\n", amountRunning, e.procuserMax, amountWaiting)

		//wait for task to be not busy
		for amountRunning >= e.procuserMax {
			e.Log(4, "DEBUG: procuserAmountRunning >= procuserMax (%d>=%d), procuserAmountWaiting: %d, sleeping %dms\n", amountRunning, e.procuserMax, amountWaiting, e.procuserSleep)
			e.EOSsleepMs(e.procuserSleep)
			amountWaiting, amountRunning = e.procuserAmountRead()
		}

		//wait for non busy slot to age at least 1s
		for slot == -1 {
			slot = e.procuserGetFreeSlot()
			if slot == -1 {
				e.Log(4, "DEBUG: procuser slots are maxed out this second, sleeping %dms\n", e.procuserSleep)
				e.EOSsleepMs(e.procuserSleep)
			}
		}
	}
	return slot
}

func (e *eosObjects) procuserFreeSlot(slot int) {
	e.Log(5, "DEBUG: procuser slot %d try to free\n", slot)
	if e.procuserMax == 0 || slot == -1 {
		return
	}

	procuserAmountMutex.Lock()
	job, ok := procuserJobs[slot]
	if ok {
		e.Log(4, "DEBUG: procuser slot %d set to free SUCCESS\n", slot)
		job.busy = false
	} else {
		e.Log(4, "DEBUG: procuser slot %d set to free FAILED\n", slot)
	}
	defer procuserAmountMutex.Unlock()
}

func (e *eosObjects) interfaceToInt64(in interface{}) int64 {
	if in == nil {
		return 0
	}
	f, _ := in.(float64)
	return int64(f)
}

func (e *eosObjects) interfaceToUint32(in interface{}) uint32 {
	if in == nil {
		return 0
	}
	f, _ := in.(float64)
	return uint32(f)
}

func (e *eosObjects) interfaceToString(in interface{}) string {
	if in == nil {
		return ""
	}
	s, _ := in.(string)
	return strings.TrimSpace(s)
}

func (e *eosObjects) Log(level int, format string, a ...interface{}) {
	if e.loglevel >= level {
		fmt.Printf(format, a...)
	}
}

func (e *eosObjects) messagebusAddJob(jobType, path string) {
	if e.hookurl == "" {
		return
	}

	eospath, err := e.EOSpath(path)
	if err != nil {
		return
	}

	go func() {
		joburl := fmt.Sprintf("%s?type=%s&file=%s", e.hookurl, jobType, url.QueryEscape(eospath))
		res, _ := http.Get(joburl)
		defer res.Body.Close()
		body, _ := ioutil.ReadAll(res.Body)
		e.Log(2, "S3 Hook: %s\n", joburl)
		e.Log(3, "  %s\n", body)
		_ = body
	}()

	return
}
func (e *eosObjects) messagebusAddPutJob(path string)    { e.messagebusAddJob("s3.Add", path) }
func (e *eosObjects) messagebusAddDeleteJob(path string) { e.messagebusAddJob("s3.Delete", path) }

//Sometimes EOS needs to breath
func (e *eosObjects) EOSsleepMs(t int) { time.Sleep(time.Duration(t) * time.Millisecond) }
func (e *eosObjects) EOSsleep()        { e.EOSsleepMs(0100) }
func (e *eosObjects) EOSsleepSlow()    { e.EOSsleepMs(1000) }
func (e *eosObjects) EOSsleepFast()    { e.EOSsleepMs(0010) }

func (e *eosObjects) EOSurlExtras() string {
	return fmt.Sprintf("&eos.ruid=%s&eos.rgid=%s&mgm.format=json", e.uid, e.gid)
}

func (e *eosObjects) EOSpath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", eoserrFilePathBad
	}

	path = strings.Replace(path, "//", "/", -1)
	eosPath = strings.TrimSuffix(e.path+"/"+path, ".")
	return eosPath, nil
}

func (e *eosObjects) EOScacheReset() {
	eosfsStatMutex.Lock()
	eosFileStatCache = make(map[string]eosFileStat)
	eosfsStatMutex.Unlock()
}

func (e *eosObjects) EOScacheRead(eospath string) (*eosFileStat, bool) {
	eosfsStatMutex.RLock()
	fi, ok := eosFileStatCache[eospath]
	eosfsStatMutex.RUnlock()
	if ok {
		return &fi, true
	}
	return nil, false
}

func (e *eosObjects) EOScacheWrite(eospath string, obj eosFileStat) {
	eosfsStatMutex.Lock()
	eosFileStatCache[eospath] = obj
	eosfsStatMutex.Unlock()
}

func (e *eosObjects) EOScacheDeletePath(eospath string) {
	eosfsStatMutex.Lock()
	if _, ok := eosFileStatCache[eospath]; ok {
		delete(eosFileStatCache, eospath)
	}
	eosfsStatMutex.Unlock()
}

func (e *eosObjects) EOScacheDeleteObject(bucket, object string) {
	eospath, err := e.EOSpath(bucket + "/" + object)
	if err == nil {
		e.EOScacheDeletePath(eospath)
	}
}

func (e *eosObjects) EOSMGMcurl(cmd string) (body []byte, m map[string]interface{}, err error) {
	slot := e.procuserWaitForSlot()

	eosurl := fmt.Sprintf("http://%s:8000/proc/user/?%s", e.url, cmd)
	e.Log(4, "DEBUG: curl '%s'\n", eosurl)
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			e.Log(4, "    http client wants to redirect\n")
			return nil
		},
		Timeout: 0,
	}
	req, _ := http.NewRequest("GET", eosurl, nil)
	req.Header.Set("Remote-User", "minio")
	res, _ := client.Do(req)
	defer res.Body.Close()
	body, _ = ioutil.ReadAll(res.Body)

	m = make(map[string]interface{})
	err = json.Unmarshal([]byte(body), &m)

	e.procuserFreeSlot(slot)
	return body, m, err
}

func (e *eosObjects) EOSreadDir(dirPath string, cacheReset bool) (entries []string, err error) {
	if cacheReset {
		e.EOScacheReset()
	}

	eospath, err := e.EOSpath(dirPath)
	if err != nil {
		return nil, err
	}

	e.Log(1, "DEBUG: EOS procuser.fileinfo %s\n", eospath)
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: EOSreadDir 1 can not json.Unmarshal()\n")
		return nil, err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.fileinfo %s : %s\n", eospath, e.interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	if c, ok := m["children"]; ok {
		err = json.Unmarshal([]byte(body), &c)
		if err != nil {
			e.Log(2, "ERROR: EOSreadDir 2 can not json.Unmarshal()\n")
			return nil, err
		}

		//e.Log(4,"%+v\n", c)
		eospath = strings.TrimSuffix(eospath, "/") + "/"
		children := m["children"].([]interface{})
		for _, childi := range children {
			//e.Log(4,"object: %+v\n", childi)
			child, _ := childi.(map[string]interface{})
			//e.Log(4,"  name: %s   %d\n", e.interfaceToString(child["name"]), e.interfaceToint64(child["mode"]))

			obj := e.interfaceToString(child["name"])
			if !strings.HasPrefix(obj, ".sys.v#.") {
				isFile := true
				if e.interfaceToInt64(child["mode"]) == 0 {
					obj += "/"
					isFile = false
				}
				entries = append(entries, obj)

				//some defaults
				meta := make(map[string]string)
				meta["contenttype"] = "application/octet-stream"
				meta["etag"] = "00000000000000000000000000000000"
				if _, ok := child["xattr"]; ok {
					xattr, _ := child["xattr"].(map[string]interface{})
					//e.Log(4, "xattr: %+v\n", xattr)
					if contenttype, ok := xattr["minio_contenttype"]; ok {
						meta["contenttype"] = e.interfaceToString(contenttype)
					}
					if etag, ok := xattr["minio_etag"]; ok {
						meta["etag"] = e.interfaceToString(etag)
					}
				}

				e.EOScacheWrite(eospath+obj, eosFileStat{
					id:          e.interfaceToInt64(child["id"]),
					name:        e.interfaceToString(child["name"]),
					size:        e.interfaceToInt64(child["size"]) + e.interfaceToInt64(child["treesize"]),
					file:        isFile,
					modTime:     time.Unix(e.interfaceToInt64(child["mtime"]), 0),
					etag:        meta["etag"],
					contenttype: meta["contenttype"],
					//checksum:    e.interfaceToString(child["checksumvalue"]),
				})
			}
		}
	}

	//e.Log(5,"cache: %+v\n", eosFileStatCache)
	return entries, err
}

func (e *eosObjects) EOSfsStat(p string) (*eosFileStat, error) {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return nil, err
	}
	if fi, ok := e.EOScacheRead(eospath); ok {
		e.Log(3, "cache hit: %s\n", eospath)
		return fi, nil
	}
	e.Log(3, "cache miss: %s\n", eospath)

	e.Log(1, "DEBUG: EOS procuser.fileinfo %s\n", eospath)
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: EOSfsStat can not json.Unmarshal()\n")
		return nil, err
	}
	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.fileinfo %s : %s\n", eospath, e.interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	e.Log(3, "EOS STAT name:%s mtime:%d mode:%d size:%d\n", e.interfaceToString(m["name"]), e.interfaceToInt64(m["mtime"]), e.interfaceToInt64(m["mode"]), e.interfaceToInt64(m["size"]))
	e.Log(4, "EOSfsStat return body : %s\n", strings.Replace(string(body), "\n", " ", -1))

	//some defaults
	meta := make(map[string]string)
	meta["contenttype"] = "application/octet-stream"
	meta["etag"] = "00000000000000000000000000000000"
	if _, ok := m["xattr"]; ok {
		xattr, _ := m["xattr"].(map[string]interface{})
		//e.Log(4, "xattr: %+v\n", xattr)
		if contenttype, ok := xattr["minio_contenttype"]; ok {
			ct := e.interfaceToString(contenttype)
			if ct != "" {
				meta["contenttype"] = ct
			}
		}
		if etag, ok := xattr["minio_etag"]; ok {
			et := e.interfaceToString(etag)
			if et != "" {
				meta["etag"] = et
			}
		}
	}

	fi := eosFileStat{
		id:          e.interfaceToInt64(m["id"]),
		name:        e.interfaceToString(m["name"]),
		size:        e.interfaceToInt64(m["size"]) + e.interfaceToInt64(m["treesize"]),
		file:        e.interfaceToUint32(m["mode"]) != 0,
		modTime:     time.Unix(e.interfaceToInt64(m["mtime"]), 0),
		etag:        meta["etag"],
		contenttype: meta["contenttype"],
		//checksum:    e.interfaceToString(m["checksumvalue"]),
	}

	e.EOScacheWrite(eospath, fi)

	return &fi, nil
}

func (e *eosObjects) EOSmkdirWithOption(p, option string) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	e.Log(1, "DEBUG: EOS procuser.mkdir %s\n", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=mkdir%s&mgm.path=%s%s", option, url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.mkdir %s : %s\n", eospath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	e.messagebusAddPutJob(p)

	return nil
}

func (e *eosObjects) EOSrmdir(p string) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	e.Log(1, "DEBUG: EOS procuser.rmdir %s\n", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rmdir&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.rmdir %s : %s\n", eospath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	e.messagebusAddDeleteJob(p)

	return nil
}

func (e *eosObjects) EOSrm(p string) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	e.Log(1, "DEBUG: EOS procuser.rm %s\n", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=rf&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.rm %s : %s\n", eospath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	e.EOScacheDeletePath(p)
	e.messagebusAddDeleteJob(p)

	return nil
}

func (e *eosObjects) EOScopy(src, dst string, size int64) error {
	eossrcpath, err := e.EOSpath(src)
	if err != nil {
		return err
	}
	eosdstpath, err := e.EOSpath(dst)
	if err != nil {
		return err
	}

	//need to wait for file, it is possible it is uploaded via a background job
	for {
		e.Log(1, "DEBUG: EOS procuser.fileinfo %s\n", eossrcpath)
		_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eossrcpath), e.EOSurlExtras()))
		if err == nil {
			if e.interfaceToInt64(m["size"]) >= size {
				break
			}
		}
		e.Log(1, "DEBUG: EOScopy waiting for src file to arrive : %s size: %d\n", eossrcpath, size)
		e.Log(3, "DEBUG: EOScopy expecting size: %d found size: %d\n", size, e.interfaceToInt64(m["size"]))
		e.EOSsleepSlow()
	}

	e.Log(1, "DEBUG: EOS procuser.file.copy %s %s\n", eossrcpath, eosdstpath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.copy %s %s : %s\n", eossrcpath, eosdstpath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	e.EOScacheDeletePath(dst)
	e.messagebusAddPutJob(dst)

	return nil
}

func (e *eosObjects) EOStouch(p string, size int64) error {
	//bookingsize is ignored by touch...
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	e.Log(1, "DEBUG: EOS procuser.file.touch %s\n", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.EOSurlExtras(), size))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.touch %s : %s\n", eospath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosObjects) EOSrename(from, to string) error {
	eosfrompath, err := e.EOSpath(from)
	if err != nil {
		return err
	}
	eostopath, err := e.EOSpath(to)
	if err != nil {
		return err
	}

	e.Log(1, "DEBUG: EOS procuser.file.rename %s %s\n", eosfrompath, eostopath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=rename&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eosfrompath), url.QueryEscape(eostopath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.rename %s : %s\n", eosfrompath, eostopath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosObjects) EOSsetMeta(p, key, value string) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	e.Log(1, "DEBUG: EOS procuser.attr.set %s=%s %s\n", key, value, eospath)
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=minio_%s&mgm.attr.value=%s&mgm.path=%s%s", url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(eospath), e.EOSurlExtras()))
	e.Log(3, "Meta Tag return body : %s\n", strings.Replace(string(body), "\n", " ", -1))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.attr.set %s : %s\n", eospath, e.interfaceToString(m["errormsg"]))
		//return eoserrSetMeta
	}

	return nil
}

func (e *eosObjects) EOSsetContentType(p, ct string) error { return e.EOSsetMeta(p, "contenttype", ct) }
func (e *eosObjects) EOSsetETag(p, etag string) error      { return e.EOSsetMeta(p, "etag", etag) }

func (e *eosObjects) EOSput(p string, data []byte) error {
	e.Log(2, "EOSput: %s\n", p)
	//curl -L -X PUT -T somefile -H 'Remote-User: minio' -sw '%{http_code}' http://eos:8000/eos-path/somefile

	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
	e.Log(3, "  %s\n", eosurl)

	e.Log(1, "DEBUG: EOS webdav.PUT : %s\n", eosurl)

	maxRetry := 10
	retry := 0
	for retry < maxRetry {
		retry = retry + 1
		client := &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				e.Log(2, "    http client wants to redirect\n")
				e.Log(5, "    %+v\n", req)
				return nil
			},
			Timeout: 0,
		}
		req, _ := http.NewRequest("PUT", eosurl, bytes.NewBuffer(data))
		req.Header.Set("Remote-User", "minio")
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = int64(len(data))
		req.Close = true
		res, err := client.Do(req)
		if err != nil {
			e.Log(2, "%+v\n", err)
			e.EOSsleep()
			continue
		}
		defer res.Body.Close()
		if res.StatusCode != 201 {
			e.Log(2, "%+v\n", res)
			err = eoserrCantPut
			e.EOSsleep()
			continue
		}

		e.messagebusAddPutJob(p)
		return err
	}
	e.Log(2, "ERROR: EOSput failed %d times\n", maxRetry)
	return err
}

func (e *eosObjects) EOSwriteChunk(p string, offset, size int64, checksum string, data []byte) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("root://%s@%s/%s", e.user, e.url, eospath)
	e.Log(1, "DEBUG: EOS xrootd.PUT : %s %s %d %d %s %d %d\n", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)

	cmd := exec.Command(e.scripts+"/writeChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(size, 10), checksum, e.uid, e.gid)
	cmd.Stdin = bytes.NewReader(data)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		e.Log(2, "ERROR: can not %s %s %d %d %s %s %s\n", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)
		e.Log(2, "%s\n", strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
	}
	return err
}

func (e *eosObjects) EOSxrdcp(src, dst string, size int64) error {
	eospath, err := e.EOSpath(dst)
	if err != nil {
		return err
	}
	eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s&eos.bookingsize=%d", e.url, eospath, e.uid, e.gid, size))
	if err != nil {
		fmt.Printf("ERROR: can not url.QueryUnescape()\n")
		return err
	}

	e.Log(1, "DEBUG: EOS xrdcp.PUT : %s\n", eosurl)
	cmd := exec.Command("/usr/bin/xrdcp", "-N", "-f", "-p", src, eosurl)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		e.Log(2, "ERROR: can not /usr/bin/xrdcp -N -f -p %s %s\n", src, eosurl)
	}
	output := strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))
	if output != "" {
		e.Log(2, "%s\n", output)
	}
	return err
}

func (e *eosObjects) EOSreadChunk(p string, offset, length int64, data io.Writer) (err error) {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	if e.readmethod == "xrootd" {
		eosurl := fmt.Sprintf("root://%s@%s/%s", e.user, e.url, eospath)
		e.Log(1, "DEBUG: EOS xrootd.GET : %s\n", eosurl)
		cmd := exec.Command(e.scripts+"/readChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(length, 10), e.uid, e.gid)
		var stderr bytes.Buffer
		cmd.Stdout = data
		cmd.Stderr = &stderr
		err2 := cmd.Run()
		errStr := strings.TrimSpace(string(stderr.Bytes()))
		e.Log(2, "DEBUG: %s %s %d %d %s %s %+v\n", e.scripts+"/readChunk.py", eosurl, offset, length, e.uid, e.gid, err2)
		if errStr != "" {
			e.Log(2, "%s\n", errStr)
		}
	} else if e.readmethod == "xrdcp" {
		eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s", e.url, eospath, e.uid, e.gid))
		if err != nil {
			fmt.Printf("ERROR: can not url.QueryUnescape()\n")
			return err
		}

		e.Log(1, "DEBUG: EOS xrdcp.GET : %s\n", eosurl)
		cmd := exec.Command("/usr/bin/xrdcp", "-N", eosurl, "-")
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err2 := cmd.Run()
		errStr := strings.TrimSpace(string(stderr.Bytes()))
		e.Log(2, "DEBUG: /usr/bin/xrdcp -N %s - %+v\n", eosurl, err2)
		if errStr != "" {
			e.Log(2, "%s\n", errStr)
		}

		if offset >= 0 {
			stdout.Next(int(offset))
		}
		stdout.Truncate(int(length))
		stdout.WriteTo(data)
	} else { //webdav
		//curl -L -X GET -H 'Remote-User: minio' -H 'Range: bytes=5-7' http://eos:8000/eos-path-to-file

		eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
		//e.Log(3,"  %s\n", eosurl)
		//e.Log(3,"  Range: bytes=%d-%d\n", offset, offset+length-1)
		e.Log(1, "DEBUG: EOS webdav.GET : %s\n", eosurl)

		client := &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				e.Log(2, "    http client wants to redirect\n")
				return nil
			},
			Timeout: 0,
		}
		req, _ := http.NewRequest("GET", eosurl, nil)
		req.Header.Set("Remote-User", "minio")
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		req.Close = true
		res, err := client.Do(req)
		if err != nil {
			e.Log(2, "%+v\n", err)
			return err
		}

		defer res.Body.Close()
		_, err = io.CopyN(data, res.Body, length)
	}
	return err
}

func (e *eosObjects) EOScalcMD5(p string) (md5sum string, err error) {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return "00000000000000000000000000000000", err
	}
	eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
	e.Log(1, "DEBUG: EOS webdav.GET : %s\n", eosurl)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			e.Log(2, "    http client wants to redirect\n")
			return nil
		},
		Timeout: 0,
	}
	req, _ := http.NewRequest("GET", eosurl, nil)
	req.Header.Set("Remote-User", "minio")
	req.Close = true
	res, err := client.Do(req)
	if err != nil {
		e.Log(2, "%+v\n", err)
		return "00000000000000000000000000000000", err
	}

	defer res.Body.Close()
	hash := md5.New()
	reader := bufio.NewReader(res.Body)
	for {
		buf := make([]byte, 1024, 1024)
		n, err := reader.Read(buf[:])
		hash.Write(buf[0:n])
		if err != nil {
			break
		}
	}
	md5sum = hex.EncodeToString(hash.Sum(nil))
	return md5sum, err
}
