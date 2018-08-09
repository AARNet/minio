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

	fmt.Printf("%sEOS LOG LEVEL        %s:%s %d%s\n", CLR_B, CLR_N, CLR_W, loglevel, CLR_N)
	fmt.Printf("----------------------\n\n")

	return &eosObjects{
		loglevel: loglevel,
		url:      os.Getenv("EOS"),
		path:     os.Getenv("VOLUME_PATH"),
		hookurl:  os.Getenv("HOOKSURL"),
		scripts:  os.Getenv("SCRIPTS"),
		user:     os.Getenv("EOSUSER"),
		uid:      os.Getenv("EOSUID"),
		gid:      os.Getenv("EOSGID"),
		stage:    stage,
		readonly: readonly,
	}, nil
}

// Production - eos gateway is production ready.
func (g *EOS) Production() bool {
	return false //hahahahaha
}

// eosObjects implements gateway for Minio and S3 compatible object storage servers.
type eosObjects struct {
	loglevel int
	url      string
	path     string
	hookurl  string
	scripts  string
	user     string
	uid      string
	gid      string
	stage    string
	readonly bool
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
		b := minio.BucketInfo{
			Name:    dir,
			Created: stat.ModTime()}
		buckets = append(buckets, b)
		eosBucketCache[strings.TrimSuffix(dir, "/")] = b
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
	e.Log(1, "DEBUG: DeleteBucket       : %s\n", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return e.EOSrmdir(bucket)
}

// GetBucketPolicy - Get the container ACL
func (e *eosObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	e.Log(4, "DEBUG: GetBucketPolicy    : %s\n", bucket)

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
	e.Log(4, "DEBUG: SetBucketPolicy    : %s, %+v\n", bucket, bucketPolicy)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (e *eosObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	e.Log(4, "DEBUG: DeleteBucketPolicy : %s\n", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Object

// CopyObject - Copies a blob from source container to destination container.
func (e *eosObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo) (objInfo minio.ObjectInfo, err error) {
	e.Log(1, "DEBUG: CopyObject         : %s -> %s : %+v\n", srcBucket+"/"+srcObject, destBucket+"/"+destObject, srcInfo)

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
	return e.GetObjectInfo(ctx, destBucket, destObject)
}

// CopyObjectPart creates a part in a multipart upload by copying
func (e *eosObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo) (p minio.PartInfo, err error) {
	if e.readonly {
		return p, minio.NotImplemented{}
	}

	return p, minio.NotImplemented{}
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *miniohash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	e.Log(1, "DEBUG: PutObject          : %s/%s\n", bucket, object)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for key, val := range metadata {
		e.Log(3, "DEBUG: PutObject            %s = %s\n", key, val)
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
	return e.GetObjectInfo(ctx, bucket, object)
}

// DeleteObject - Deletes a blob on azure container
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	e.Log(1, "DEBUG: DeleteObject       : %s/%s\n", bucket, object)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return e.EOSrm(bucket + "/" + object)
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) error {
	if etag != "" {
		objInfo, err := e.GetObjectInfo(ctx, bucket, object)
		if err != nil {
			return err
		}
		if objInfo.ETag != etag {
			return minio.InvalidETag{}
		}
	}

	err := e.EOSreadChunk(bucket+"/"+object, startOffset, length, writer)

	return err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo
func (e *eosObjects) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo minio.ObjectInfo, err error) {
	e.Log(1, "DEBUG: GetObjectInfo      : %s/%s\n", bucket, object)

	stat, err := e.EOSfsStat(bucket + "/" + object)

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

	e.Log(2, "  %s etag:%s content-type:%s\n", bucket+"/"+object, stat.ETag(), stat.ContentType())
	return objInfo, err
}

// ListMultipartUploads - lists all multipart uploads.
func (e *eosObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	e.Log(1, "DEBUG: ListMultipartUploads : %s %s %s %s %s %d\n", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, minio.NotImplemented{}
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string) (uploadID string, err error) {
	e.Log(1, "DEBUG: NewMultipartUpload : %s/%s\n           +%v\n", bucket, object, metadata)

	if e.readonly {
		return "", minio.NotImplemented{}
	}

	uploadID = bucket + "/" + object

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

	eosMultiPartsMutex.Lock()
	eosMultiParts[uploadID] = &mp
	eosMultiPartsMutex.Unlock()

	e.Log(2, "  uploadID : %s\n", uploadID)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *miniohash.Reader) (info minio.PartInfo, err error) {
	e.Log(1, "DEBUG: PutObjectPart      : %s/%s %s [%d] %d\n", bucket, object, uploadID, partID, data.Size())

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

	eosMultiPartsMutex.Lock()
	eosMultiParts[uploadID].parts[partID] = newPart
	eosMultiPartsMutex.Unlock()

	if partID == 1 {
		eosMultiParts[uploadID].firstByte = buf[0]
		eosMultiParts[uploadID].chunkSize = size
	} else {
		for eosMultiParts[uploadID].chunkSize == 0 {
			e.Log(1, "DEBUG: PutObjectPart        ok, waiting for first chunk...\n")
			e.EOSsleep()
		}
	}
	eosMultiParts[uploadID].partsCount++
	eosMultiParts[uploadID].AddToSize(size)

	offset := eosMultiParts[uploadID].chunkSize * int64(partID-1)
	e.Log(3, "DEBUG: PutObjectPart        offset = %d = %d\n", (partID - 1), offset)

	if e.stage != "" {
		e.Log(1, "DEBUG: PutObjectPart      : Staging\n")

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
		eosMultiParts[uploadID].md5.Write(buf)
		eosMultiParts[uploadID].md5PartID++
	}()

	return newPart, nil
}

// CompleteMultipartUpload
func (e *eosObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart) (objInfo minio.ObjectInfo, err error) {
	e.Log(1, "DEBUG: CompleteMultipartUpload : %s size : %d firstByte : %d\n", uploadID, eosMultiParts[uploadID].size, eosMultiParts[uploadID].firstByte)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for eosMultiParts[uploadID].md5PartID != eosMultiParts[uploadID].partsCount+1 {
		e.Log(3, "DEBUG: PutObjectPart waiting for all md5Parts, %d remaining\n", eosMultiParts[uploadID].partsCount+1-eosMultiParts[uploadID].md5PartID)
		e.EOSsleep()
	}
	etag := hex.EncodeToString(eosMultiParts[uploadID].md5.Sum(nil))

	if e.stage != "" {
		err = e.EOStouch(uploadID, eosMultiParts[uploadID].size)
		if err != nil {
			e.Log(2, "ERROR: CompleteMultipartUpload: EOStouch :%+v\n", err)
			return objInfo, err
		}

		/*
			//this could be done better
			file, err := os.Open(e.stage + "/" + eosMultiParts[uploadID].stagepath + "/file")
			if err != nil {
				e.Log(2, "ERROR: CompleteMultipartUpload: os.Open :%+v\n", err)
				return objInfo, err
			}
			hash := md5.New()
			if _, err := io.Copy(hash, file); err != nil {
				e.Log(2, "ERROR: CompleteMultipartUpload:%+v\n", err)
			}
			file.Close()
			etag := hex.EncodeToString(hash.Sum(nil))
		*/

		e.Log(2, "ETAG: %s\n", etag)
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

		//upload in background
		go func() {
			err := e.EOSxrdcp(e.stage+"/"+eosMultiParts[uploadID].stagepath+"/file", uploadID, eosMultiParts[uploadID].size)
			if err != nil {
				e.Log(2, "ERROR: CompleteMultipartUpload: xrdcp: %+v\n", err)
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
		//etag, _ := e.EOScalcMD5(uploadID)

		e.Log(2, "ETAG: %s\n", etag)
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
		//eosMultiPartsMutex.Lock()
		delete(eosMultiParts, uploadID)
		//eosMultiPartsMutex.Unlock()
		e.messagebusAddPutJob(uploadID)
	}

	return e.GetObjectInfo(ctx, bucket, object)
}

//AbortMultipartUpload
func (e *eosObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	e.Log(1, "DEBUG: AbortMultipartUpload : %s/%s %s\n", bucket, object, uploadID)

	if e.readonly {
		return minio.NotImplemented{}
	}

	_ = e.EOSrm(bucket + "/" + object)

	return nil
}

// ListObjectParts
func (e *eosObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (result minio.ListPartsInfo, err error) {
	e.Log(1, "DEBUG: ListObjectParts    : %s %d %d\n", uploadID, partNumberMarker, maxParts)

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker

	i := 0
	eosMultiPartsMutex.RLock()
	result.Parts = make([]minio.PartInfo, eosMultiParts[uploadID].partsCount)
	for _, part := range eosMultiParts[uploadID].parts {
		result.Parts[i] = part
		i++
	}
	eosMultiPartsMutex.RUnlock()

	return result, nil
}

// ListObjects - lists all blobs in a container filtered by prefix and marker
func (e *eosObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	e.Log(1, "DEBUG ListObjects         : %s, %s, %s, %s, %d\n", bucket, prefix, marker, delimiter, maxKeys)
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

		e.Log(5, "  %s, %s, %s = %s\n", bucket, prefix, obj, bucket+"/"+prefix+obj)
		e.Log(4, "  %s <=> %s etag:%s content-type:%s\n", path+"/"+obj, prefix+obj, stat.ETag(), stat.ContentType())
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
	}

	result.IsTruncated = false
	//result.Prefixes = append(result.Prefixes, prefix)

	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	return result, minio.NotImplemented{}
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Don't think we need this...

// HealFormat - no-op for fs
func (e *eosObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

// ReloadFormat - no-op for fs
func (e *eosObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	return minio.NotImplemented{}
}

// ListObjectsHeal - list all objects to be healed.
func (e *eosObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	return loi, minio.NotImplemented{}
}

// HealObject - no-op for fs.
func (e *eosObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool) (res madmin.HealResultItem, err error) {
	return res, minio.NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed
func (e *eosObjects) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	return []minio.BucketInfo{}, minio.NotImplemented{}
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (e *eosObjects) HealBucket(ctx context.Context, bucket string, dryRun bool) (results []madmin.HealResultItem, err error) {
	return nil, minio.NotImplemented{}
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Helpers

type eosFileStat struct {
	id          int64
	name        string
	size        int64
	mode        os.FileMode
	modTime     time.Time
	sys         syscall.Stat_t
	etag        string
	contenttype string
	checksum    string
}

func (fs *eosFileStat) Id() int64          { return fs.id }
func (fs *eosFileStat) Name() string       { return fs.name }
func (fs *eosFileStat) Size() int64        { return fs.size }
func (fs *eosFileStat) Mode() os.FileMode  { return fs.mode }
func (fs *eosFileStat) ModTime() time.Time { return fs.modTime }
func (fs *eosFileStat) Sys() interface{}   { return &fs.sys }
func (fs *eosFileStat) IsDir() bool        { return fs.mode == 0 }
func (fs *eosFileStat) Checksum() string   { return fs.checksum }
func (fs *eosFileStat) ETag() string       { return fs.etag }
func (fs *eosFileStat) ContentType() string {
	if fs.IsDir() {
		return "application/x-directory"
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

var eosMultiPartsMutex = sync.RWMutex{}

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
func (e *eosObjects) EOSsleep()     { time.Sleep(100 * time.Millisecond) }
func (e *eosObjects) EOSsleepSlow() { time.Sleep(1000 * time.Millisecond) }

func (e *eosObjects) EOSurlExtras() string {
	return fmt.Sprintf("&eos.ruid=%s&eos.rgid=%s&mgm.format=json", e.uid, e.gid)
}

func (e *eosObjects) EOSpath(path string) (eosPath string, err error) {
	if strings.Contains(path, "..") {
		return "", eoserrFilePathBad
	}

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
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: EOSreadDir 1 can not json.Unmarshal()\n")
		return nil, err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
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
				if e.interfaceToInt64(child["mode"]) == 0 {
					obj += "/"
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
					mode:        os.FileMode(e.interfaceToUint32(child["mode"])),
					modTime:     time.Unix(e.interfaceToInt64(child["mtime"]), 0),
					etag:        meta["etag"],
					contenttype: meta["contenttype"],
					checksum:    e.interfaceToString(child["checksumvalue"]),
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

	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: EOSfsStat can not json.Unmarshal()\n")
		return nil, err
	}
	if e.interfaceToString(m["errormsg"]) != "" {
		return nil, eoserrFileNotFound
	}

	e.Log(4, "EOS STAT name:%s mtime:%d mode:%d size:%d\n", e.interfaceToString(m["name"]), e.interfaceToInt64(m["mtime"]), e.interfaceToInt64(m["mode"]), e.interfaceToInt64(m["size"]))

	//some defaults
	meta := make(map[string]string)
	meta["contenttype"] = "application/octet-stream"
	meta["etag"] = "00000000000000000000000000000000"
	if _, ok := m["xattr"]; ok {
		xattr, _ := m["xattr"].(map[string]interface{})
		//e.Log(4, "xattr: %+v\n", xattr)
		if contenttype, ok := xattr["minio_contenttype"]; ok {
			meta["contenttype"] = e.interfaceToString(contenttype)
		}
		if etag, ok := xattr["minio_etag"]; ok {
			meta["etag"] = e.interfaceToString(etag)
		}
	}

	fi := eosFileStat{
		id:          e.interfaceToInt64(m["id"]),
		name:        e.interfaceToString(m["name"]),
		size:        e.interfaceToInt64(m["size"]) + e.interfaceToInt64(m["treesize"]),
		mode:        os.FileMode(e.interfaceToUint32(m["mode"])),
		modTime:     time.Unix(e.interfaceToInt64(m["mtime"]), 0),
		etag:        meta["etag"],
		contenttype: meta["contenttype"],
		checksum:    e.interfaceToString(m["checksumvalue"]),
	}

	e.EOScacheWrite(eospath, fi)

	return &fi, nil
}

func (e *eosObjects) EOSmkdirWithOption(p, option string) error {
	e.Log(1, "DEBUG: EOSmkdirWithOption : %s %s\n", p, option)

	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=mkdir%s&mgm.path=%s%s", option, url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
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
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rmdir&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
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

	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=rf&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
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

	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.EOSurlExtras()))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
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

	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.EOSurlExtras(), size))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosObjects) EOSsetMeta(p, key, value string) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=minio_%s&mgm.attr.value=%s&mgm.path=%s%s", url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(eospath), e.EOSurlExtras()))
	//e.Log(5,"body : %s\n", string(body))
	if err != nil {
		e.Log(2, "ERROR: can not json.Unmarshal()\n")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(2, "ERROR: %s\n", e.interfaceToString(m["errormsg"]))
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
	e.Log(1, "DEBUG: EOSwriteChunk      : %s %s %d %d %s %d %d\n", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)

	cmd := exec.Command(e.scripts+"/writeChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(size, 10), checksum, e.uid, e.gid)
	cmd.Stdin = bytes.NewReader(data)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		e.Log(2, "ERROR: can not %s %s %d %d %s %d %d\n", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)
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
	//curl -L -X GET -H 'Remote-User: minio' -H 'Range: bytes=5-7' http://eos:8000/eos-path-to-file

	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
	//e.Log(3,"  %s\n", eosurl)
	//e.Log(3,"  Range: bytes=%d-%d\n", offset, offset+length-1)

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
	reader := bufio.NewReader(res.Body)
	lengthLeft := length
	for {
		buf := make([]byte, 1024, 1024)
		n, err := reader.Read(buf[:])
		if n > 0 {
			if int64(n) > lengthLeft {
				n = int(lengthLeft)
			}
			lengthLeft -= int64(n)
			data.Write(buf[0:n])
			//nn, err := data.Write(buf[0:n])
			//e.Log(2,"DEBUG-WRITE:      %d, %s\n", n, string(buf[0:n]))
			//e.Log(2,"DEBUG-WRITE:      %d, %+v\n", nn, err)
		}
		if err != nil {
			break
		}
	}
	return err
}

func (e *eosObjects) EOScalcMD5(p string) (md5sum string, err error) {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return "00000000000000000000000000000000", err
	}
	eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)

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
