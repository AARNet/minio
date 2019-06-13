/*
 * AARNet 2018
 *
 * Michael D'Silva
 *
 * This is a gateway for AARNet's CERN's EOS storage backend (v4.2.29)
 *
 * TODO: Move EOS MGM and filesystem interactions to it's own type
 */

package eos

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"errors"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"
)

// eosObjects implements gateway for Minio and S3 compatible object storage servers.
type eosObjects struct {
	loglevel     int
	url          string
	path         string
	hookurl      string
	scripts      string
	user         string
	uid          string
	gid          string
	stage        string
	readonly     bool
	readmethod   string
	waitSleep    int
	validbuckets bool
	StatCache    *StatCache
	DirCache     eosDirCacheType
	BucketCache  map[string]minio.BucketInfo 
	TransferList *TransferList
}

// Errors
var eoserrFileNotFound = errors.New("EOS: file not found")
var eoserrDiskAccessDenied = errors.New("EOS: disk access denied")
var eoserrCantPut = errors.New("EOS: can't put")
var eoserrFilePathBad = errors.New("EOS: bad file path")

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
	e.Log(2, "S3cmd: GetBucketInfo [bucket: %s]", bucket)

	if bi, ok := e.BucketCache[bucket]; ok {
		e.Log(3, "GetBucketInfo: cache miss: [bucket: %s]", bucket)
		return bi, nil
	}
	e.Log(3, "GetBucketInfo: cache miss: [bucket: %s]", bucket)
	stat, err := e.EOSfsStat(bucket)

	if err == nil {
		bi = minio.BucketInfo{
			Name:    bucket,
			Created: stat.ModTime()}

		e.BucketCache[bucket] = bi
	} else {
		err = minio.BucketNotFound{Bucket: bucket}
	}
	return bi, err
}

// ListBuckets - Lists all root folders
func (e *eosObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	e.Log(2, "S3cmd: ListBuckets")

	e.BucketCache = make(map[string]minio.BucketInfo)

	dirs, err := e.EOSreadDir("", false)
	if err != nil {
		return buckets, err
	}

	for _, dir := range dirs {
		var stat *eosFileStat
		stat, err = e.EOSfsStat(dir)
		if stat != nil {
			if stat.IsDir() && e.IsValidBucketName(strings.TrimRight(dir, "/")) {
				b := minio.BucketInfo{
					Name:    dir,
					Created: stat.ModTime()}
				buckets = append(buckets, b)
				e.BucketCache[strings.TrimSuffix(dir, "/")] = b
			} else {
				if !stat.IsDir() {
					e.Log(3, "Bucket: %s not a directory", dir)
				}
				if !e.IsValidBucketName(strings.TrimRight(dir, "/")) {
					e.Log(3, "Bucket: %s not a valid name", dir)
				}
			}
		} else {
			e.Log(1, "ERROR: ListBuckets: unable to stat [dir: %s]", dir)
		}
	}

	e.DirCache.path = ""

	e.Log(3, "DEBUG: BucketCache: %+v", e.BucketCache)

	return buckets, err
}

// MakeBucketWithLocation - Create a new container.
func (e *eosObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	e.Log(2, "S3cmd: MakeBucketWithLocation: [bucket: %s, location: %s]", bucket, location)

	if e.readonly {
		return minio.NotImplemented{}
	}

	// Verify if bucket is valid.
	if !e.IsValidBucketName(bucket) {
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
	e.Log(2, "S3cmd: DeleteBucket: [bucket: %s]", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return e.EOSrmdir(bucket)
}

// GetBucketPolicy - Get the container ACL
func (e *eosObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	e.Log(2, "S3cmd: GetBucketPolicy: [bucket: %s]", bucket)

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
	e.Log(2, "S3cmd: SetBucketPolicy: [bucket: %s, bucketPolicy: %+v]", bucket, bucketPolicy)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (e *eosObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	e.Log(2, "S3cmd: DeleteBucketPolicy: [bucket: %s]", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this gateway.
func (e *eosObjects) IsListenBucketSupported() bool {
	return false
}

/*********************
 *  Object           *
 ********************/

// CopyObject - Copies a blob from source container to destination container.
func (e *eosObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	srcpath := srcBucket+"/"+srcObject
	destpath := destBucket+"/"+destObject
	e.Log(2, "S3cmd: CopyObject: [from: %s, to: %s, srcInfo: %+v]", srcpath, destpath, srcInfo)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	dir := destBucket + "/" + filepath.Dir(destObject)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.EOScopy(srcpath, destpath, srcInfo.Size)
	if err != nil {
		e.Log(1, "ERROR: COPY: %+v", err)
		return objInfo, err
	}
	err = e.EOSsetETag(destpath, srcInfo.ETag)
	if err != nil {
		e.Log(1, "ERROR: COPY: %+v", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(destpath, srcInfo.ContentType)
	if err != nil {
		e.Log(1, "ERROR: COPY: %+v", err)
		return objInfo, err
	}

	e.StatCache.DeleteObject(destBucket, destObject)
	return e.GetObjectInfo(ctx, destBucket, destObject, dstOpts)
}

// CopyObjectPart creates a part in a multipart upload by copying
func (e *eosObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	e.Log(2, "S3cmd: CopyObjectPart: [srcpath: %s/%s, destpath: %s/%s", srcBucket, srcObject, destBucket, destObject)

	if e.readonly {
		return p, minio.NotImplemented{}
	}

	return p, minio.NotImplemented{}
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	e.Log(2, "S3cmd: PutObject: [path: %s/%s]", bucket, object)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for key, val := range opts.UserDefined {
		e.Log(3, "PutObject [path: %s/%s, key: %s, value: %s]", bucket, object, key, val)
	}

	buf, _ := ioutil.ReadAll(data)
	hasher := md5.New()
	hasher.Write([]byte(buf))
	etag := hex.EncodeToString(hasher.Sum(nil))
	dir := bucket + "/" + filepath.Dir(object)
	objectpath := bucket+"/"+object

	if _, err := e.EOSfsStat(dir); err != nil {
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.EOSput(objectpath, buf)
	if err != nil {
		e.Log(1, "ERROR: PUT: %+v", err)
		return objInfo, err
	}
	err = e.EOSsetETag(objectpath, etag)
	if err != nil {
		e.Log(1, "ERROR: PUT: %+v", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(objectpath, opts.UserDefined["content-type"])
	if err != nil {
		e.Log(1, "ERROR: PUT: %+v", err)
		return objInfo, err
	}

	e.StatCache.DeleteObject(bucket, object)
	return e.GetObjectInfo(ctx, bucket, object, opts)
}

// DeleteObject - Deletes a blob on EOS
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	e.Log(2, "S3cmd: DeleteObject: %s/%s", bucket, object)

	if e.readonly {
		return minio.NotImplemented{}
	}

	e.EOSrm(bucket + "/" + object)
	return nil
}

// DeleteObjects - Deletes multiple blobs on EOS
func (e *eosObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	e.Log(2, "S3cmd: DeleteObjects: [bucket: %s]", bucket)

	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = e.DeleteObject(ctx, bucket, object)
	}

	return errs, nil
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)
	e.Log(2, "S3cmd: GetObject: [path: %s, startOffset: %d, length: %d]", path, startOffset, length)

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
	e.Log(2, "S3cmd: GetObjectInfo: [path: %s]", path)

	stat, err := e.EOSfsStat(path)
	if stat == nil {
		maxRetry := 20
		for retry := 0; retry < maxRetry; retry++ {
			stat, err = e.EOSfsStat(path)
			e.EOSsleep()
		}
	}

	if err != nil {
		e.Log(3, "DEBUG: GetObjectInfo: [error: %+v]", err)
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

	e.Log(3, "GetObjectInfo: [path: %s, etag: %s, content-type: %s]", path, stat.ETag(), stat.ContentType())
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
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts.CheckCopyPrecondFn, pipeCloser)
}

// ListMultipartUploads - lists all multipart uploads.
func (e *eosObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	e.Log(2, "S3cmd: ListMultipartUploads: [bucket: %s, prefix: %s, keyMarket: %s, uploadIDMarker: %s, delimiter: %s, maxUploads: %d]", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, minio.NotImplemented{}
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	e.Log(2, "S3cmd: NewMultipartUpload: [path: %s/%s, options:  +%v]", bucket, object, opts)

	if e.readonly {
		return "", minio.NotImplemented{}
	}

	uploadID = bucket + "/" + object

	if strings.HasSuffix(uploadID, "/") {
		return "", minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	dir := bucket + "/" + filepath.Dir(object)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.Log(2, "NewMultipartUpload: mkdir: [dir: %s]", dir)
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	mp := Transfer{
		parts:       make(map[int]minio.PartInfo),
		partsCount:  0,
		size:        0,
		chunkSize:   0,
		firstByte:   0,
		contenttype: opts.UserDefined["content-type"],
		md5:         md5.New(),
		md5PartID:   1,
	}

	if e.stage != "" {
		hasher := md5.New()
		hasher.Write([]byte(uploadID))
		stagepath := hex.EncodeToString(hasher.Sum(nil))

		//make sure it is clear of older junk
		absstagepath := e.stage + "/" + stagepath
		os.RemoveAll(absstagepath)

		err = os.MkdirAll(absstagepath, 0700)
		if err != nil {
			e.Log(2, "mkdir failed [path: %s, error: %+v]", absstagepath, err)
		}
		mp.stagepath = stagepath
	}

	e.TransferList.AddTransfer(uploadID, &mp)
	e.Log(2, "NewMultipartUpload: [uploadID: %s]", uploadID)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	e.Log(2, "S3cmd: PutObjectPart: [object: %s/%s, uploadID: %s, partId: %d, size: %d]", bucket, object, uploadID, partID, data.Size())

	if e.readonly {
		return info, minio.NotImplemented{}
	}

	transfer := e.TransferList.GetTransfer(uploadID)
	for {
		transfer.RLock()
		parts := transfer.parts
		transfer.RUnlock()
		//parts := transfer.GetParts()
		if parts != nil {
			break
		}
		e.Log(1, "PutObjectPart called before NewMultipartUpload finished. [object: %s/%s, uploadID: %d]", bucket, object, partID)
		e.EOSsleep()
	}

	size := data.Size()
	buf, _ := ioutil.ReadAll(data)
	etag := data.MD5CurrentHexString()

	newPart := minio.PartInfo{
		PartNumber:   partID,
		LastModified: time.Now(),
		ETag:         etag,
		Size:         size,
	}

	transfer.AddPart(partID, newPart)

	if partID == 1 {
		transfer.Lock()
		transfer.firstByte = buf[0]
		transfer.chunkSize = size
		transfer.Unlock()
	} else {
		for {
			transfer.RLock()
			chunksize := transfer.chunkSize
			transfer.RUnlock()

			if chunksize != 0 {
				break
			}
			e.Log(3, "PutObjectPart: waiting for first chunk [object: %s/%s, processing_part: %d]", bucket, object, partID)
			e.EOSsleep()
		}
	}

	transfer.Lock()
	transfer.partsCount++
	transfer.AddToSize(size)
	chunksize := transfer.chunkSize
	transfer.Unlock()

	offset := chunksize * int64(partID-1)
	e.Log(3, "PutObjectPart offset [object: %s/%s, partID: %d, offset: %d]", bucket, object, (partID - 1), offset)

	if e.stage != "" { //staging
		e.Log(3, "PutObjectPart: staging transfer [object: %s/%s]", bucket, object)

		stagepath := transfer.GetStagePath()
		absstagepath := e.stage+"/"+stagepath

		if _, err := os.Stat(absstagepath); os.IsNotExist(err) {
			err = os.MkdirAll(absstagepath, 0700)
		}
		f, err := os.OpenFile(absstagepath+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			e.Log(1, "ERROR: Write ContentType: %+v", err)
			return newPart, err
		}
		_, err = f.WriteAt(buf, offset)
		if err != nil {
			e.Log(1, "ERROR: Write ContentType: %+v", err)
			return newPart, err
		}
		f.Close()
	} else { // use xrootd
//		go func() {
			err = e.EOSxrootdWriteChunk(uploadID, offset, offset+size, "0", buf)
//		}()
	}

//	go func() {
//		transfer := e.TransferList.GetTransfer(uploadID)
		transfer = e.TransferList.GetTransfer(uploadID)
		for {
			if transfer != nil {
				transfer.RLock()
				md5PartID := transfer.md5PartID
				transfer.RUnlock()
				if md5PartID == partID {
					break
				}
				e.Log(3, "PutObjectPart: waiting for part [uploadID: %s, md5PartID: %d, currentPart: %d]", uploadID, md5PartID, partID)
			}
			e.EOSsleep()
		}
		transfer.Lock()
		transfer.md5.Write(buf)
		transfer.md5PartID++
		transfer.Unlock()
//	}()

	return newPart, nil
}

// CompleteMultipartUpload
func (e *eosObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	transfer := e.TransferList.GetTransfer(uploadID)
	transfer.RLock()
	size := transfer.size
	firstByte := transfer.firstByte
	transfer.RUnlock()
	e.Log(2, "S3cmd: CompleteMultipartUpload: [uploadID: %s, size: %d, firstByte: %d]", uploadID, size, firstByte)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for {
		transfer.RLock()
		if transfer.md5PartID == transfer.partsCount+1 {
			break
		}
		e.Log(3, "CompleteMultipartUpload waiting for all md5Parts [uploadID: %s, total_parts: %s, remaining: %d", uploadID, transfer.partsCount, transfer.partsCount+1-transfer.md5PartID)
		transfer.RUnlock()
		e.EOSsleep()
	}

	etag := transfer.GetETag()
	contenttype := transfer.GetContentType()
	stagepath := transfer.GetStagePath()

	e.Log(3, "CompleteMultipartUpload: [uploadID: %s, etag: %s]", uploadID, etag)

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     time.Now(),
		Size:        size,
		IsDir:       false,
		ETag:        etag,
		ContentType: contenttype,
	}

	if e.stage != "" { //staging
		err = e.EOStouch(uploadID, size)
		if err != nil {
			e.Log(1, "ERROR: CompleteMultipartUpload: EOStouch: [uploadID: %s, error: %+v]", uploadID, err)
			return objInfo, err
		}

		//upload in background
		go func() {
			fullstagepath := e.stage+"/"+stagepath+"/file"
			e.Log(3, "CompleteMultipartUpload: xrdcp: [stagepath: %s, uploadIDpath: %s, size: %d]", fullstagepath, uploadID+".minio.sys", size)
			err := e.EOSxrdcp(fullstagepath, uploadID+".minio.sys", size)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: xrdcp: [uploadID: %s, error: %+v]", uploadID, err)
				return
			}

			err = e.EOSrename(uploadID+".minio.sys", uploadID)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: EOSrename: [uploadID: %s, error: %+v]", uploadID, err)
				return
			}

			err = e.EOSsetETag(uploadID, etag)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: EOSsetETag: [uploadID: %s, error: %+v]", uploadID, err)
				return
			}

			err = e.EOSsetContentType(uploadID, contenttype)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: EOSsetContentType: [uploadID: %s, error: %+v]", uploadID, err)
				return
			}

			err = os.RemoveAll(e.stage + "/" + stagepath)
			if err != nil {
				return
			}

			e.TransferList.DeleteTransfer(uploadID)
			e.messagebusAddPutJob(uploadID)
		}()
	} else { //use xrootd
		transfer.RLock()
		firstbyte := []byte{transfer.firstByte}
		transfer.RUnlock()

		err = e.EOSxrootdWriteChunk(uploadID, 0, size, "1", firstbyte)
		if err != nil {
			e.Log(1, "ERROR: CompleteMultipartUpload: EOSwriteChunk: [uploadID: %s, error: %+v]", uploadID, err)
			return objInfo, err
		}

		e.TransferList.DeleteTransfer(uploadID)
		e.messagebusAddPutJob(uploadID)
	}

	err = e.EOSsetETag(uploadID, etag)
	if err != nil {
		e.Log(1, "ERROR: CompleteMultipartUpload: [uploadID: %s, error: %+v]", uploadID, err)
		return objInfo, err
	}
	err = e.EOSsetContentType(uploadID, contenttype)
	if err != nil {
		e.Log(1, "ERROR: CompleteMultipartUpload: [uploadID: %s, error: %+v]", err)
		return objInfo, err
	}

	//populate cache
	e.StatCache.DeletePath(uploadID)
	stat, err := e.EOSfsStat(uploadID)
	e.StatCache.DeletePath(uploadID) // not sure why this is here twice..
	stat.size = objInfo.Size
	stat.file = true
	stat.etag = objInfo.ETag
	stat.contenttype = objInfo.ContentType
	stat.modTime = objInfo.ModTime
	eospath, err := e.EOSpath(uploadID)
	if stat != nil {
		e.StatCache.Write(eospath, *stat)
	}

	return objInfo, nil
}

//AbortMultipartUpload
func (e *eosObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	e.Log(2, "S3cmd: AbortMultipartUpload: [object: %s/%s, uploadID: %d]", bucket, object, uploadID)

	if e.readonly {
		return minio.NotImplemented{}
	}

	if e.stage != "" && e.TransferList.TransferExists(uploadID) {
		transfer := e.TransferList.GetTransfer(uploadID)
		stagepath := transfer.GetStagePath()

		os.RemoveAll(e.stage + "/" + stagepath)
	}

	e.EOSrm(bucket + "/" + object)
	e.TransferList.DeleteTransfer(uploadID)

	return nil
}

// ListObjectParts
func (e *eosObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, options minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	e.Log(2, "S3cmd: ListObjectParts: [uploadID: %s, part: %d, maxParts: %d]", uploadID, partNumberMarker, maxParts)

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker

	transfer := e.TransferList.GetTransfer(uploadID)
	transfer.RLock()
	size := transfer.partsCount
	parts := transfer.parts
	result.Parts = make([]minio.PartInfo, size)
	i := 0
	for _, part := range parts {
		if i < size {
			result.Parts[i] = part
			i++
		}
	}
	transfer.RUnlock()

	return result, nil
}

// Return an initialised minio.ObjectInfo
func (e *eosObjects) NewObjectInfo(bucket string, name string, stat *eosFileStat) (obj minio.ObjectInfo) {
	return minio.ObjectInfo{
		Bucket:      bucket,
		Name:        name,
		ModTime:     stat.ModTime(),
		Size:        stat.Size(),
		IsDir:       stat.IsDir(),
		ETag:        stat.ETag(),
		ContentType: stat.ContentType(),
	}
}

// ListObjects - lists all blobs in a container filtered by prefix and marker
func (e *eosObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	e.Log(2, "S3cmd: ListObjects: [bucket: %s, prefix: %s, marker: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, marker, delimiter, maxKeys)
	if delimiter == "/" && prefix == "/" {
		e.Log(2, "ListObjects: delimiter and prefix is slash")
		return result, nil
	}

	path := strings.TrimSuffix(bucket+"/"+prefix, "/")

	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}

	e.Log(3, "ListObjects: Creating cache for %s", path)
	e.DirCache.path = path
	e.DirCache.objects, err = e.EOSreadDir(path, true)
	if err != nil {
		return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
	}

	// Doesn't look like it's a directory, try it as an object
	object_found := false
	if len(e.DirCache.objects) == 0 {
		e.Log(3, "ListObjects: No objects found for path %s, treating as an object", path)
		var stat *eosFileStat
		prefix = strings.TrimSuffix(prefix, "/")
		stat, err = e.EOSfsStat(path)

                if stat != nil && !strings.HasSuffix(path, ".minio.sys") {
			o := e.NewObjectInfo(bucket, prefix, stat)
			result.Objects = append(result.Objects, o)
			object_found = true
			e.DirCache.objects = nil
			e.DirCache.objects = []string{}
		}
	}

	// If no object is found, treat it as a directory anyway
	if !object_found {
		for _, obj := range e.DirCache.objects {
			var stat *eosFileStat
			objpath := path + "/" + obj
			stat, err = e.EOSfsStat(objpath)

			if stat != nil && !strings.HasSuffix(obj, ".minio.sys") {
				e.Log(3, "ListObjects: Stat: %s <=> %s [etag: %s, content-type: %s]", objpath, prefix+obj, stat.ETag(), stat.ContentType())
				o := e.NewObjectInfo(bucket, prefix+obj, stat)
				result.Objects = append(result.Objects, o)

				if delimiter == "" && stat.IsDir() {
					e.Log(3, "ListObjects: Recursing through %s", prefix+obj)
					subdir, err := e.ListObjects(ctx, bucket, prefix+obj, marker, delimiter, -1)
					if err != nil {
						return result, err
					}
					for _, subobj := range subdir.Objects {
						result.Objects = append(result.Objects, subobj)
					}
				}
			} else {
				e.Log(1, "ERROR: ListObjects: unable to stat [objpath: %s]", objpath)
			}
		}
	}

	result.IsTruncated = false

	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	e.Log(2, "S3cmd: ListObjectsV2: [bucket: %s, prefix: %s, continuationToken: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, continuationToken, delimiter, maxKeys)

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

/* 
 *  Methods that are not implemented
 */

// HealFormat - no-op for fs
func (e *eosObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	e.Log(2, "S3cmd: HealFormat:")
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

// ReloadFormat - no-op for fs
func (e *eosObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	e.Log(2, "S3cmd: ReloadFormat:")
	return minio.NotImplemented{}
}

// ListObjectsHeal - list all objects to be healed.
func (e *eosObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	e.Log(2, "S3cmd: ListObjectsHeal:")
	return loi, minio.NotImplemented{}
}

// HealObject - no-op for fs.
func (e *eosObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool, remove bool, scanMode madmin.HealScanMode) (results madmin.HealResultItem, err error) {
	e.Log(2, "S3cmd: HealObject:")
	return results, minio.NotImplemented{}
}

// HealObjects - no-op for fs.
func (e *eosObjects) HealObjects(ctx context.Context, bucket, prefix string, fn func(string, string) error) (err error) {
	e.Log(2, "S3cmd: HealObjects:")
	return minio.NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed
func (e *eosObjects) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	e.Log(2, "S3cmd: ListBucketsHeal:")
	return []minio.BucketInfo{}, minio.NotImplemented{}
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (e *eosObjects) HealBucket(ctx context.Context, bucket string, dryRun bool, remove bool) (results madmin.HealResultItem, err error) {
	e.Log(2, "S3cmd: HealBucket:")
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

/*
 * Helpers
 */

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
	if level == 1 {
		err := fmt.Errorf(format, a...)
		logger.LogIf(context.Background(), err)
	} else if e.loglevel >= level {
		logger.Info(format, a...)
	}
}

func (e *eosObjects) IsValidBucketName(name string) bool {
	if e.validbuckets {
		return minio.IsValidBucketName(name)
	}
	return true
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
		res, err := http.Get(joburl)
		// TODO: Might need to return here if we're not getting a response
		if res != nil {
			defer res.Body.Close()
		} else {
			e.Log(1, "ERROR: messagebusAddJob: response body is nil [joburl: %s, error: %+v]", joburl, err)
		}
		if err != nil {
			e.Log(1, "S3 Hook: Add job failed [joburl: %s, error: %+v]", joburl, err)
			return
		}
		body, _ := ioutil.ReadAll(res.Body)
		e.Log(2, "S3 Hook: %s", joburl)
		e.Log(3, "%s", body)
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

func (e *eosObjects) EOSMGMcurl(cmd string) (body []byte, m map[string]interface{}, err error) {
	eosurl := fmt.Sprintf("http://%s:8000/proc/user/?%s", e.url, cmd)
	e.Log(3, "EOSGMcurl: [eosurl: %s]", eosurl)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			e.Log(3, "EOSMGMcurl: http client wants to redirect")
			return nil
		},
		Timeout: 0,
//		Transport: MGMTransport,
	}
	req, _ := http.NewRequest("GET", eosurl, nil)
	req.Header.Set("Remote-User", e.user)
	res, err := client.Do(req)
	// TODO: Might need to return here if we're not getting a response
	if res != nil {
		defer res.Body.Close()
	} else {
		e.Log(1, "ERROR: EOSMGMcurl - response body is nil [eosurl: %s, error: %+v]", eosurl, err)
	}


	if err != nil {
		return nil, nil, err
	}
	body, _ = ioutil.ReadAll(res.Body)

	m = make(map[string]interface{})
	err = json.Unmarshal([]byte(body), &m)

	return body, m, err
}

func (e *eosObjects) EOSreadDir(dirPath string, cacheReset bool) (entries []string, err error) {
	if cacheReset {
		e.StatCache.Reset()
	}

	eospath, err := e.EOSpath(dirPath)
	if err != nil {
		return nil, err
	}

	e.Log(2, "EOScmd: procuser.fileinfo [eospath: %s]", eospath)
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSreadDir curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return nil, err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR: EOS procuser.fileinfo [eospath: %s, error: %s]", eospath, e.interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	if c, ok := m["children"]; ok {
		err = json.Unmarshal([]byte(body), &c)
		if err != nil {
			e.Log(1, "ERROR: EOSreadDir can not json.Unmarshal() children [eospath: %s]", eospath)
			return nil, err
		}

		eospath = strings.TrimSuffix(eospath, "/") + "/"
		children := m["children"].([]interface{})
		for _, childi := range children {
			child, _ := childi.(map[string]interface{})

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
					if contenttype, ok := xattr["minio_contenttype"]; ok {
						meta["contenttype"] = e.interfaceToString(contenttype)
					}
					if etag, ok := xattr["minio_etag"]; ok {
						meta["etag"] = e.interfaceToString(etag)
					}
				}

				e.StatCache.Write(eospath+obj, eosFileStat{
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

	return entries, err
}

func (e *eosObjects) EOSfsStat(p string) (*eosFileStat, error) {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return nil, err
	}
	if fi, ok := e.StatCache.Read(eospath); ok {
		e.Log(3, "EOSfsStat: cache hit: [eospath: %s]", eospath)
		return fi, nil
	}
	e.Log(3, "EOSfsStat: cache miss: [eospath: %s]", eospath)

	e.Log(2, "EOScmd: procuser.fileinfo [eospath: %s]", eospath)
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSfsStat curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return nil, err
	}
	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(3, "EOS procuser.fileinfo [eospath: %s, error: %s]", eospath, e.interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	e.Log(3, "EOSfsStat: stat result [eospath: %s, name: %s, mtime: %d, mode:%d, size: %d]", eospath, e.interfaceToString(m["name"]), e.interfaceToInt64(m["mtime"]), e.interfaceToInt64(m["mode"]), e.interfaceToInt64(m["size"]))
	e.Log(3, "EOSfsStat: request response body [eospath: %s, body: %s]", eospath, strings.Replace(string(body), "\n", " ", -1))

	//some defaults
	meta := make(map[string]string)
	meta["contenttype"] = "application/octet-stream"
	meta["etag"] = "00000000000000000000000000000000"
	if _, ok := m["xattr"]; ok {
		xattr, _ := m["xattr"].(map[string]interface{})
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

	e.StatCache.Write(eospath, fi)

	return &fi, nil
}

func (e *eosObjects) EOSmkdirWithOption(p, option string) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	e.Log(2, "EOScmd: procuser.mkdir %s", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=mkdir%s&mgm.path=%s%s", option, url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSmkdirWithOption curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR: EOS procuser.mkdir [eospath: %s, error: %s]", eospath, e.interfaceToString(m["errormsg"]))
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

	e.Log(2, "EOScmd: procuser.rm [eospath: %s]", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSrmdir curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.rm [eospath: %s, error: %s]", eospath, e.interfaceToString(m["errormsg"]))
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

	e.Log(2, "EOScmd: procuser.rm [eospath: %s]", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSrm curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.rm [eospath: %s, error: %s]", eospath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}
	e.StatCache.DeletePath(p)
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
		e.Log(2, "EOScmd: procuser.fileinfo [eospath: %s]", eossrcpath)
		_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eossrcpath), e.EOSurlExtras()))
		if err == nil {
			if e.interfaceToInt64(m["size"]) >= size {
				break
			}
		}
		e.Log(2, "EOScopy waiting for src file to arrive: [eospath: %s, size: %d]", eossrcpath, size)
		e.Log(3, "EOScopy expecting size: %d found size: %d [eospath: %s]", size, e.interfaceToInt64(m["size"]), eossrcpath)
		e.EOSsleepSlow()
	}

	e.Log(2, "EOScmd: procuser.file.copy [src: %s, dst: %s]", eossrcpath, eosdstpath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOScopy curl to MGM failed [src: %s, dst: %s, error: %+v]", eossrcpath, eosdstpath, err)
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.copy [src: %s, dst: %s, error: %s]", eossrcpath, eosdstpath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	e.StatCache.DeletePath(dst)
	e.messagebusAddPutJob(dst)

	return nil
}

func (e *eosObjects) EOStouch(p string, size int64) error {
	//bookingsize is ignored by touch...
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	e.Log(2, "EOScmd: procuser.file.touch [eospath: %s]", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.EOSurlExtras(), size))
	if err != nil {
		e.Log(1, "ERROR: EOStouch curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.touch [eospath: %s, error: %s]", eospath, e.interfaceToString(m["errormsg"]))
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

	e.Log(2, "EOScmd: procuser.file.rename [src: %s, dst: %s]", eosfrompath, eostopath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=rename&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eosfrompath), url.QueryEscape(eostopath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSrename curl to MGM failed [src: %s, dst: %s, error: %+v]", eosfrompath, eostopath, err)
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.rename [src: %s, dst: %s, error: %s]", eosfrompath, eostopath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosObjects) EOSsetMeta(p, key, value string) error {
	if key == "" || value == "" {
		e.Log(3, "procuser.attr.set key or value is empty. [path: %s, key: %s, value: %s]", p, key, value)
		//dont bother setting if we don't get what we need
		return nil
	}
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	e.Log(2, "EOScmd: procuser.attr.set [path: %s, key: %s, value: %s]", eospath, key, value)
	cmd := fmt.Sprintf("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=minio_%s&mgm.attr.value=%s&mgm.path=%s%s", url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(eospath), e.EOSurlExtras())
	body, m, err := e.EOSMGMcurl(cmd)
	e.Log(3, "EOSsetMeta: meta tag return body [eospath: %s, body: %s]", eospath, strings.Replace(string(body), "\n", " ", -1))
	if err != nil {
		e.Log(1, "ERROR: EOSsetMeta curl to MGM failed [eospath: %s, error: %+v]", eospath, err)
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.attr.set [eospath: %s, command: %s, error: %s]", eospath, cmd, e.interfaceToString(m["errormsg"]))
		//return eoserrSetMeta
	}

	return nil
}

func (e *eosObjects) EOSsetContentType(p, ct string) error { return e.EOSsetMeta(p, "contenttype", ct) }
func (e *eosObjects) EOSsetETag(p, etag string) error      { return e.EOSsetMeta(p, "etag", etag) }

func (e *eosObjects) EOSput(p string, data []byte) error {
	e.Log(2, "EOSput: [path: %s]", p)
	//curl -L -X PUT -T somefile -H 'Remote-User: minio' -sw '%{http_code}' http://eos:8000/eos-path/somefile

	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
	e.Log(3, "DEBUG: [eosurl: %s]", eosurl)

	e.Log(2, "EOScmd: webdav.PUT [eosurl: %s]", eosurl)

	maxRetry := 10
	retry := 0
	for retry < maxRetry {
		retry = retry + 1

		// SPECIAL CASE = contains a %
		if strings.IndexByte(p, '%') >= 0 {
			e.Log(2, "EOScmd: webdav.PUT : SPECIAL CASE using curl [eosurl: %s]", eosurl)
			cmd := exec.Command("curl", "-L", "-X", "PUT", "--data-binary", "@-", "-H", "Remote-User: minio", "-sw", "'%{http_code}'", eosurl)
			cmd.Stdin = bytes.NewReader(data)
			stdoutStderr, err := cmd.CombinedOutput()

			if err != nil {
				e.Log(1, "ERROR: curl failed [eosurl: %s]", eosurl)
				e.Log(3, "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				e.EOSsleep()
				continue
			}
			if strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)) != "'201'" {
				e.Log(1, "ERROR: incorrect response from curl [eosurl: %s]", eosurl)
				e.Log(3, "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				e.EOSsleep()
				continue
			}

			return err
		}

		client := &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				e.Log(3, "EOSput: http client wants to redirect [eosurl: %s]", eosurl)
				e.Log(3, "DEBUG: [eosurl: %s, req: %+v]", eosurl, req)
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
			e.Log(3, "EOSput: http ERROR message: [eosurl: %s, error: %+v]", eosurl, err)
			if res != nil {
				e.Log(3, "EOSput: http ERROR response: [eosurl: %s, response: %+v]", eosurl, res)
			}

			//req.URL.RawPath = strings.Replace(req.URL.RawPath[:strings.IndexByte(req.URL.RawPath, '?')], "%", "%25", -1) + "?" + req.URL.RawQuery

			e.EOSsleep()
			continue
		}
		// TODO: Might need to return here if we're not getting a response
		if res != nil {
			defer res.Body.Close()
		} else {
			e.Log(1, "ERROR: EOSput: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
		}

		if res.StatusCode != 201 {
			e.Log(3, "EOSput: http StatusCode != 201: [eosurl: %s, result: %+v]", eosurl, res)
			err = eoserrCantPut
			e.EOSsleep()
			continue
		}

		e.messagebusAddPutJob(p)
		return err
	}
	e.Log(1, "ERROR: EOSput failed %d times. [eosurl %s, error: %+v]", maxRetry, eosurl, err)
	return err
}

func (e *eosObjects) EOSxrootdWriteChunk(p string, offset, size int64, checksum string, data []byte) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("root://%s@%s/%s", e.user, e.url, eospath)
	e.Log(2, "EOScmd: xrootd.PUT: [script: %s, eosurl: %s, offset: %d, size: %d, checksum: %s, uid: %d, gid: %d]", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)

	cmd := exec.Command(e.scripts+"/writeChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(size, 10), checksum, e.uid, e.gid)
	cmd.Stdin = bytes.NewReader(data)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		e.Log(1, "ERROR: can not [script: %s, eosurl: %s, offset: %d, size: %d, checksum: %s, uid: %s, gid: %s]", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)
		e.Log(3, "DEBUG: [eosurl: %s, stderr: %s]", eosurl, strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
	}

	return err
}

func (e *eosObjects) EOSxrdcp(src, dst string, size int64) error {
	eospath, err := e.EOSpath(dst)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s&eos.bookingsize=%d", e.url, eospath, e.uid, e.gid, size))
	if err != nil {
		e.Log(1, "ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl)
		return err
	}

	e.Log(2, "EOScmd: xrdcp.PUT: [eospath: %s, eosurl: %s]", eospath, eosurl)

	cmd := exec.Command("/usr/bin/xrdcp", "-N", "-f", "-p", src, eosurl)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		e.Log(1, "ERROR: can not /usr/bin/xrdcp -N -f -p %s %s [eospath: %s, eosurl: %s]", src, eosurl, eospath, eosurl)
	}
	output := strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))
	if output != "" {
		e.Log(2, "%s", output)
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
		e.Log(2, "EOScmd: xrootd.GET: [eospath: %s, eosurl: %s]", eospath, eosurl)

		cmd := exec.Command(e.scripts+"/readChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(length, 10), e.uid, e.gid)
		var stderr bytes.Buffer
		cmd.Stdout = data
		cmd.Stderr = &stderr
		err2 := cmd.Run()
		errStr := strings.TrimSpace(string(stderr.Bytes()))
		e.Log(2, "EOSreadChunk: [script: %s, eosurl: %s, offset: %d, length: %d, uid: %s, gid: %s, error: %+v]", e.scripts+"/readChunk.py", eosurl, offset, length, e.uid, e.gid, err2)
		if errStr != "" {
			e.Log(1, "ERROR: EOSreadChunk [eosurl: %s, error: %s]", eosurl, errStr)
		}
	} else if e.readmethod == "xrdcp" {
		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s", e.url, eospath, e.uid, e.gid))
		if err != nil {
			e.Log(1, "ERROR: can not url.QueryUnescape() [eospath: %s, eosurl: %s]", eospath, eosurl)
			return err
		}

		e.Log(2, "EOScmd: xrdcp.GET: [eosurl: %s]", eosurl)

		cmd := exec.Command("/usr/bin/xrdcp", "-N", eosurl, "-")
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err2 := cmd.Run()

		errStr := strings.TrimSpace(string(stderr.Bytes()))
		e.Log(2, "/usr/bin/xrdcp -N %s - %+v", eosurl, err2)
		if errStr != "" {
			e.Log(2, "%s", errStr)
		}

		if offset >= 0 {
			stdout.Next(int(offset))
		}
		stdout.Truncate(int(length))
		stdout.WriteTo(data)
	} else { //webdav
		//curl -L -X GET -H 'Remote-User: minio' -H 'Range: bytes=5-7' http://eos:8000/eos-path-to-file

		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
		e.Log(2, "EOScmd: webdav.GET: [eosurl: %s]", eosurl)

		client := &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				e.Log(2, "webdav.GET: http client wants to redirect")
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
			e.Log(1, "ERROR: webdav.GET [eosurl: %s, error: %+v]", eosurl, err)
			return err
		}
		// TODO: Might need to return here if res is nil
		if res != nil {
			defer res.Body.Close()
		} else {
			e.Log(1, "ERROR: webdav.GET: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
		}
		_, err = io.CopyN(data, res.Body, length)
	}
	return err
}

func (e *eosObjects) EOScalcMD5(p string) (md5sum string, err error) {
	eospath, err := e.EOSpath(p)
	default_md5 := "00000000000000000000000000000000"
	if err != nil {
		return default_md5, err
	}
	eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
	e.Log(2, "EOScmd: webdav.GET: [eosurl: %s]", eosurl)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			e.Log(2, "EOScalcMD5: http client wants to redirect")
			return nil
		},
		Timeout: 0,
	}
	req, _ := http.NewRequest("GET", eosurl, nil)
	req.Header.Set("Remote-User", "minio")
	req.Close = true
	res, err := client.Do(req)

	if err != nil {
		e.Log(2, "%+v", err)
		return default_md5, err
	}

	// TODO: Might need to return here if we're not getting a response
	if res != nil {
		defer res.Body.Close()
	} else {
		e.Log(1, "ERROR: EOScalcMD5: response body is nil [eosurl: %s, error: %+v]", eosurl, err)
	}

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
