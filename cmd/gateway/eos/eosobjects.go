/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 */

package eos

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"sort"

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
	FileSystem   *eosFS
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
	Log(LogLevelInfo, "S3cmd: GetBucketInfo [bucket: %s]", bucket)

	if bi, ok := e.BucketCache[bucket]; ok {
		Log(LogLevelDebug, "GetBucketInfo: cache miss: [bucket: %s]", bucket)
		return bi, nil
	}
	Log(LogLevelDebug, "GetBucketInfo: cache miss: [bucket: %s]", bucket)
	stat, err := e.FileSystem.Stat(bucket)

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
	Log(LogLevelInfo, "S3cmd: ListBuckets")

	e.BucketCache = make(map[string]minio.BucketInfo)

	dirs, err := e.FileSystem.ReadDir("", false)
	if err != nil {
		return buckets, err
	}

	for _, dir := range dirs {
		var stat *eosFileStat
		stat, err = e.FileSystem.Stat(dir)
		if stat != nil {
			if stat.IsDir() && e.IsValidBucketName(strings.TrimRight(dir, "/")) {
				b := minio.BucketInfo{
					Name:    strings.TrimSuffix(dir, "/"),
					Created: stat.ModTime(),
				}
				buckets = append(buckets, b)
				e.BucketCache[strings.TrimSuffix(dir, "/")] = b
			} else {
				if !stat.IsDir() {
					Log(LogLevelDebug, "Bucket: %s not a directory", dir)
				}
				if !e.IsValidBucketName(strings.TrimRight(dir, "/")) {
					Log(LogLevelDebug, "Bucket: %s not a valid name", dir)
				}
			}
		} else {
			Log(LogLevelError, "ERROR: ListBuckets: unable to stat [dir: %s]", dir)
		}
	}

	e.DirCache.path = ""
	Log(LogLevelDebug, "DEBUG: BucketCache: %+v", e.BucketCache)

	return buckets, err
}

// MakeBucketWithLocation - Create a new container.
func (e *eosObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	Log(LogLevelInfo, "S3cmd: MakeBucketWithLocation: [bucket: %s, location: %s]", bucket, location)

	if e.readonly {
		return minio.NotImplemented{}
	}

	// Verify if bucket is valid.
	if !e.IsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	if _, err := e.FileSystem.Stat(bucket); err != nil {
		err := e.FileSystem.mkdirWithOption(bucket, "")
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
	Log(LogLevelInfo, "S3cmd: DeleteBucket: [bucket: %s]", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return e.FileSystem.rmdir(bucket)
}

// GetBucketPolicy - Get the container ACL
func (e *eosObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	Log(LogLevelInfo, "S3cmd: GetBucketPolicy: [bucket: %s]", bucket)

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
	Log(LogLevelInfo, "S3cmd: SetBucketPolicy: [bucket: %s, bucketPolicy: %+v]", bucket, bucketPolicy)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (e *eosObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	Log(LogLevelInfo, "S3cmd: DeleteBucketPolicy: [bucket: %s]", bucket)

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
	Log(LogLevelInfo, "S3cmd: CopyObject: [from: %s, to: %s, srcInfo: %+v]", srcpath, destpath, srcInfo)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	dir := destBucket + "/" + filepath.Dir(destObject)
	if _, err := e.FileSystem.Stat(dir); err != nil {
		e.FileSystem.mkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.FileSystem.Copy(srcpath, destpath, srcInfo.Size)
	if err != nil {
		Log(LogLevelError, "ERROR: COPY: %+v", err)
		return objInfo, err
	}

	err = e.FileSystem.SetETag(destpath, srcInfo.ETag)
	if err != nil {
		Log(LogLevelError, "ERROR: COPY: %+v", err)
		return objInfo, err
	}

	err = e.FileSystem.SetContentType(destpath, srcInfo.ContentType)
	if err != nil {
		Log(LogLevelError, "ERROR: COPY: %+v", err)
		return objInfo, err
	}

	e.StatCache.DeleteObject(destBucket, destObject)
	return e.GetObjectInfo(ctx, destBucket, destObject, dstOpts)
}

// CopyObjectPart creates a part in a multipart upload by copying
func (e *eosObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	Log(LogLevelInfo, "S3cmd: CopyObjectPart: [srcpath: %s/%s, destpath: %s/%s", srcBucket, srcObject, destBucket, destObject)

	if e.readonly {
		return p, minio.NotImplemented{}
	}

	return p, minio.NotImplemented{}
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	Log(LogLevelInfo, "S3cmd: PutObject: [path: %s/%s]", bucket, object)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for key, val := range opts.UserDefined {
		Log(LogLevelDebug, "PutObject [path: %s/%s, key: %s, value: %s]", bucket, object, key, val)
	}

	buf, _ := ioutil.ReadAll(data)
	hasher := md5.New()
	hasher.Write([]byte(buf))
	etag := hex.EncodeToString(hasher.Sum(nil))
	dir := bucket + "/" + filepath.Dir(object)
	objectpath := bucket+"/"+object

	if _, err := e.FileSystem.Stat(dir); err != nil {
		e.FileSystem.mkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.FileSystem.Put(objectpath, buf)
	if err != nil {
		Log(LogLevelError, "ERROR: PUT: %+v", err)
		return objInfo, err
	}
	err = e.FileSystem.SetETag(objectpath, etag)
	if err != nil {
		Log(LogLevelError, "ERROR: PUT: %+v", err)
		return objInfo, err
	}
	err = e.FileSystem.SetContentType(objectpath, opts.UserDefined["content-type"])
	if err != nil {
		Log(LogLevelError, "ERROR: PUT: %+v", err)
		return objInfo, err
	}

	e.StatCache.DeleteObject(bucket, object)
	return e.GetObjectInfo(ctx, bucket, object, opts)
}

// DeleteObject - Deletes a blob on EOS
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	Log(LogLevelInfo, "S3cmd: DeleteObject: %s/%s", bucket, object)

	if e.readonly {
		return minio.NotImplemented{}
	}

	e.FileSystem.rm(bucket + "/" + object)
	return nil
}

// DeleteObjects - Deletes multiple blobs on EOS
func (e *eosObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	Log(LogLevelInfo, "S3cmd: DeleteObjects: [bucket: %s]", bucket)

	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = e.DeleteObject(ctx, bucket, object)
	}

	return errs, nil
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)
	Log(LogLevelInfo, "S3cmd: GetObject: [path: %s, startOffset: %d, length: %d]", path, startOffset, length)

	if etag != "" {
		objInfo, err := e.GetObjectInfo(ctx, bucket, object, opts)
		if err != nil {
			return err
		}
		if objInfo.ETag != etag {
			return minio.InvalidETag{}
		}
	}

	err := e.FileSystem.ReadChunk(path, startOffset, length, writer)
	return err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo
func (e *eosObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)
	Log(LogLevelInfo, "S3cmd: GetObjectInfo: [path: %s]", path)

	stat, err := e.FileSystem.Stat(path)
	if stat == nil {
		maxRetry := 20
		for retry := 0; retry < maxRetry; retry++ {
			stat, err = e.FileSystem.Stat(path)
			e.EOSsleep()
		}
	}

	if err != nil {
		Log(LogLevelDebug, "DEBUG: GetObjectInfo: [error: %+v]", err)
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object}
		return objInfo, err
	}
	// Return Not found if object is a directory since S3 has no concept of directory
	if stat.IsDir() {
		Log(LogLevelDebug, "GetObjectInfo: Request is for directory, returning Not Found [path: %s]", path)
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

	Log(LogLevelDebug, "GetObjectInfo: [path: %s, etag: %s, content-type: %s]", path, stat.ETag(), stat.ContentType())
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
	Log(LogLevelInfo, "S3cmd: ListMultipartUploads: [bucket: %s, prefix: %s, keyMarket: %s, uploadIDMarker: %s, delimiter: %s, maxUploads: %d]", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, minio.NotImplemented{}
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	Log(LogLevelInfo, "S3cmd: NewMultipartUpload: [path: %s/%s, options:  +%v]", bucket, object, opts)

	if e.readonly {
		return "", minio.NotImplemented{}
	}

	uploadID = bucket + "/" + object

	if strings.HasSuffix(uploadID, "/") {
		return "", minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	dir := bucket + "/" + filepath.Dir(object)
	if _, err := e.FileSystem.Stat(dir); err != nil {
		Log(LogLevelInfo, "NewMultipartUpload: mkdir: [dir: %s]", dir)
		e.FileSystem.mkdirWithOption(dir, "&mgm.option=p")
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
			Log(LogLevelInfo, "mkdir failed [path: %s, error: %+v]", absstagepath, err)
		}
		mp.stagepath = stagepath
	}

	e.TransferList.AddTransfer(uploadID, &mp)
	Log(LogLevelInfo, "NewMultipartUpload: [uploadID: %s]", uploadID)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	Log(LogLevelInfo, "S3cmd: PutObjectPart: [object: %s/%s, uploadID: %s, partId: %d, size: %d]", bucket, object, uploadID, partID, data.Size())

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
		Log(LogLevelError, "PutObjectPart called before NewMultipartUpload finished. [object: %s/%s, uploadID: %d]", bucket, object, partID)
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
			Log(LogLevelDebug, "PutObjectPart: waiting for first chunk [object: %s/%s, processing_part: %d]", bucket, object, partID)
			e.EOSsleep()
		}
	}

	transfer.Lock()
	transfer.partsCount++
	transfer.AddToSize(size)
	chunksize := transfer.chunkSize
	transfer.Unlock()

	offset := chunksize * int64(partID-1)
	Log(LogLevelDebug, "PutObjectPart offset [object: %s/%s, partID: %d, offset: %d]", bucket, object, (partID - 1), offset)

	if e.stage != "" { //staging
		Log(LogLevelDebug, "PutObjectPart: staging transfer [object: %s/%s]", bucket, object)

		stagepath := transfer.GetStagePath()
		absstagepath := e.stage+"/"+stagepath

		if _, err := os.Stat(absstagepath); os.IsNotExist(err) {
			err = os.MkdirAll(absstagepath, 0700)
		}
		f, err := os.OpenFile(absstagepath+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			Log(LogLevelError, "ERROR: Write ContentType: %+v", err)
			return newPart, err
		}
		_, err = f.WriteAt(buf, offset)
		if err != nil {
			Log(LogLevelError, "ERROR: Write ContentType: %+v", err)
			return newPart, err
		}
		f.Close()
	} else { // use xrootd
		go func() {
			err = e.FileSystem.xrootdWriteChunk(uploadID, offset, offset+size, "0", buf)
		}()
	}

	transfer = e.TransferList.GetTransfer(uploadID)
	for {
		if transfer == nil {
			Log(LogLevelError, "PutObjectPart: Invalid transfer [uploadID: %s]", uploadID)
			break
		} else {
			transfer.RLock()
			md5PartID := transfer.md5PartID
			transfer.RUnlock()
			if md5PartID == partID {
				break
			}
			Log(LogLevelDebug, "PutObjectPart: waiting for part [uploadID: %s, md5PartID: %d, currentPart: %d]", uploadID, md5PartID, partID)
		}
		e.EOSsleep()
	}
	transfer.Lock()
	transfer.md5.Write(buf)
	transfer.md5PartID++
	transfer.Unlock()

	return newPart, nil
}

// CompleteMultipartUpload
func (e *eosObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	reqInfo := logger.GetReqInfo(ctx)
	transfer := e.TransferList.GetTransfer(uploadID)
	transfer.RLock()
	size := transfer.size
	firstByte := transfer.firstByte
	transfer.RUnlock()
	Log(LogLevelInfo, "S3cmd: CompleteMultipartUpload: [uploadID: %s, size: %d, firstByte: %d, useragent: %s]", uploadID, size, firstByte, reqInfo.UserAgent)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for {
		transfer.RLock()
		if transfer.md5PartID == transfer.partsCount+1 {
			break
		}
		Log(LogLevelDebug, "CompleteMultipartUpload: waiting for all md5Parts [uploadID: %s, total_parts: %s, remaining: %d]", uploadID, transfer.partsCount, transfer.partsCount+1-transfer.md5PartID)
		transfer.RUnlock()
		e.EOSsleep()
	}

	etag := transfer.GetETag()
	contenttype := transfer.GetContentType()
	stagepath := transfer.GetStagePath()

	Log(LogLevelDebug, "CompleteMultipartUpload: [uploadID: %s, etag: %s]", uploadID, etag)

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
		err = e.FileSystem.Touch(uploadID, size)
		if err != nil {
			Log(LogLevelError, "ERROR: CompleteMultipartUpload: EOStouch: [uploadID: %s, error: %+v]", uploadID, err)
			return objInfo, err
		}

		// Upload the transfer to EOS in the background
		if (strings.HasPrefix(reqInfo.UserAgent, "rclone")) {
			_ = e.TransferFromStaging(ctx, stagepath, uploadID, objInfo)
		} else {
			go func() {
				_ = e.TransferFromStaging(ctx, stagepath, uploadID, objInfo)
			}()
		}
	} else { //use xrootd
		transfer.RLock()
		firstbyte := []byte{transfer.firstByte}
		transfer.RUnlock()

		err = e.FileSystem.xrootdWriteChunk(uploadID, 0, size, "1", firstbyte)
		if err != nil {
			Log(LogLevelError, "ERROR: CompleteMultipartUpload: EOSwriteChunk: [uploadID: %s, error: %+v]", uploadID, err)
			return objInfo, err
		}

		e.TransferList.DeleteTransfer(uploadID)
		e.messagebusAddPutJob(uploadID)
	}

	err = e.FileSystem.SetETag(uploadID, etag)
	if err != nil {
		Log(LogLevelError, "ERROR: CompleteMultipartUpload: [uploadID: %s, error: %+v]", uploadID, err)
		return objInfo, err
	}
	err = e.FileSystem.SetContentType(uploadID, contenttype)
	if err != nil {
		Log(LogLevelError, "ERROR: CompleteMultipartUpload: [uploadID: %s, error: %+v]", err)
		return objInfo, err
	}

	//populate cache
	e.StatCache.DeletePath(uploadID)
	stat, err := e.FileSystem.Stat(uploadID)
	e.StatCache.DeletePath(uploadID) // not sure why this is here twice..
	stat.size = objInfo.Size
	stat.file = true
	stat.etag = objInfo.ETag
	stat.contenttype = objInfo.ContentType
	stat.modTime = objInfo.ModTime
	eospath, err := e.FileSystem.NormalisePath(uploadID)
	if stat != nil {
		e.StatCache.Write(eospath, *stat)
	}

	return objInfo, nil
}

// Transfer the upload from the staging area to it's final location
func (e *eosObjects) TransferFromStaging(ctx context.Context, stagepath string, uploadID string, objInfo minio.ObjectInfo) (error) {
	reqInfo := logger.GetReqInfo(ctx)
	fullstagepath := e.stage+"/"+stagepath+"/file"
	Log(LogLevelDebug, "CompleteMultipartUpload: xrdcp: [stagepath: %s, uploadIDpath: %s, size: %d, useragent: %s]", fullstagepath, uploadID+".minio.sys", objInfo.Size, reqInfo.UserAgent)
	err := e.FileSystem.xrdcp(fullstagepath, uploadID+".minio.sys", objInfo.Size)
	if err != nil {
		Log(LogLevelError, "ERROR: CompleteMultipartUpload: xrdcp: [uploadID: %s, UserAgent: %s, error: %+v]", uploadID, reqInfo.UserAgent, err)
		return err
	}
	err = e.FileSystem.Rename(uploadID+".minio.sys", uploadID)
	if err != nil {
		Log(LogLevelError, "ERROR: CompleteMultipartUpload: EOSrename: [uploadID: %s, error: %+v]", uploadID, err)
		return err
	}
	err = e.FileSystem.SetETag(uploadID, objInfo.ETag)
	if err != nil {
		Log(LogLevelError, "ERROR: CompleteMultipartUpload: EOSsetETag: [uploadID: %s, error: %+v]", uploadID, err)
		return err
	}
	err = e.FileSystem.SetContentType(uploadID, objInfo.ContentType)
	if err != nil {
		Log(LogLevelError, "ERROR: CompleteMultipartUpload: EOSsetContentType: [uploadID: %s, error: %+v]", uploadID, err)
		return err
	}
	err = os.RemoveAll(e.stage + "/" + stagepath)
		if err != nil {
		return err
	}
	e.TransferList.DeleteTransfer(uploadID)
	e.messagebusAddPutJob(uploadID)
	return nil
}


//AbortMultipartUpload
func (e *eosObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	Log(LogLevelInfo, "S3cmd: AbortMultipartUpload: [object: %s/%s, uploadID: %d]", bucket, object, uploadID)

	if e.readonly {
		return minio.NotImplemented{}
	}

	if e.stage != "" && e.TransferList.TransferExists(uploadID) {
		transfer := e.TransferList.GetTransfer(uploadID)
		stagepath := transfer.GetStagePath()
		os.RemoveAll(e.stage + "/" + stagepath)
	}

	e.FileSystem.rm(bucket + "/" + object)
	e.TransferList.DeleteTransfer(uploadID)

	return nil
}

// ListObjectParts
func (e *eosObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, options minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	Log(LogLevelInfo, "S3cmd: ListObjectParts: [uploadID: %s, part: %d, maxParts: %d]", uploadID, partNumberMarker, maxParts)

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
	Log(LogLevelInfo, "S3cmd: ListObjects: [bucket: %s, prefix: %s, marker: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, marker, delimiter, maxKeys)

	if delimiter == "/" && prefix == "/" {
		Log(LogLevelInfo, "ListObjects: delimiter and prefix is slash")
		return result, nil
	}

	path := strings.TrimSuffix(bucket+"/"+prefix, "/")

	// Seems like we always truncate it, so let's do it early.
	result.IsTruncated = false

	// First, let's see if it's an object
	prefix = strings.TrimSuffix(prefix, "/")
	stat, err := e.FileSystem.Stat(path)

	if stat != nil && !stat.IsDir() && !strings.HasSuffix(path, ".minio.sys") {
		o := e.NewObjectInfo(bucket, prefix, stat)
		result.Objects = append(result.Objects, o)
		return result, err
	}

	// If no object is found, treat it as a directory anyway
	Log(LogLevelDebug, "ListObjects: Creating cache for %s", path)
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}

	// Populate the directory cache
	e.DirCache.path = path
	e.DirCache.objects, err = e.FileSystem.ReadDir(path, true)
	if err != nil {
		return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
	}

	for _, obj := range e.DirCache.objects {
		var stat *eosFileStat
		objpath := path + "/" + obj
		stat, err = e.FileSystem.Stat(objpath)

		if stat != nil && !strings.HasSuffix(obj, ".minio.sys") {
			Log(LogLevelDebug, "ListObjects: Stat: %s <=> %s [etag: %s, content-type: %s]", objpath, prefix+obj, stat.ETag(), stat.ContentType())
			o := e.NewObjectInfo(bucket, prefix+obj, stat)
			// Directories get added to prefixes, files to objects.
			if stat.IsDir() {
				result.Prefixes = append(result.Prefixes, o.Name)
			} else {
				result.Objects = append(result.Objects, o)
			}
			if delimiter == "" && stat.IsDir() {
				Log(LogLevelDebug, "ListObjects: Recursing through %s", prefix+obj)
				subdir, err := e.ListObjects(ctx, bucket, prefix+obj, marker, delimiter, -1)
				if err != nil {
					return result, err
				}
				// Merge results for recursive
				for _, subobj := range subdir.Objects {
					result.Objects = append(result.Objects, subobj)
				}
				for _, subprefix := range subdir.Prefixes {
					result.Prefixes = append(result.Prefixes, subprefix)
				}
			}
		} else {
			Log(LogLevelError, "ERROR: ListObjects: unable to stat [objpath: %s]", objpath)
		}
	}

	// Sort the results to make them an easier list to read for most clients
	sort.SliceStable(result.Objects, func(i, j int) bool { return result.Objects[i].Name < result.Objects[j].Name })
	sort.SliceStable(result.Prefixes, func(i, j int) bool { return result.Prefixes[i] < result.Prefixes[j] })

	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	Log(LogLevelInfo, "S3cmd: ListObjectsV2: [bucket: %s, prefix: %s, continuationToken: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, continuationToken, delimiter, maxKeys)

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
	Log(LogLevelInfo, "S3cmd: HealFormat:")
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

// ReloadFormat - no-op for fs
func (e *eosObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	Log(LogLevelInfo, "S3cmd: ReloadFormat:")
	return minio.NotImplemented{}
}

// ListObjectsHeal - list all objects to be healed.
func (e *eosObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	Log(LogLevelInfo, "S3cmd: ListObjectsHeal:")
	return loi, minio.NotImplemented{}
}

// HealObject - no-op for fs.
func (e *eosObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool, remove bool, scanMode madmin.HealScanMode) (results madmin.HealResultItem, err error) {
	Log(LogLevelInfo, "S3cmd: HealObject:")
	return results, minio.NotImplemented{}
}

// HealObjects - no-op for fs.
func (e *eosObjects) HealObjects(ctx context.Context, bucket, prefix string, fn func(string, string) error) (err error) {
	Log(LogLevelInfo, "S3cmd: HealObjects:")
	return minio.NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed
func (e *eosObjects) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	Log(LogLevelInfo, "S3cmd: ListBucketsHeal:")
	return []minio.BucketInfo{}, minio.NotImplemented{}
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (e *eosObjects) HealBucket(ctx context.Context, bucket string, dryRun bool, remove bool) (results madmin.HealResultItem, err error) {
	Log(LogLevelInfo, "S3cmd: HealBucket:")
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

/*
 * Helpers
 */

func (e *eosObjects) IsValidBucketName(name string) bool {
	if e.validbuckets {
		return minio.IsValidBucketName(name)
	}
	return true
}

// TODO: These methods are not in use at the moment. They need a new home (like their own type, rather than being on eosObjects)
func (e *eosObjects) messagebusAddJob(jobType, path string) {
	if e.hookurl == "" {
		return
	}

	eospath, err := e.FileSystem.NormalisePath(path)
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
			Log(LogLevelError, "ERROR: messagebusAddJob: response body is nil [joburl: %s, error: %+v]", joburl, err)
		}
		if err != nil {
			Log(LogLevelError, "S3 Hook: Add job failed [joburl: %s, error: %+v]", joburl, err)
			return
		}
		body, _ := ioutil.ReadAll(res.Body)
		Log(LogLevelInfo, "S3 Hook: %s", joburl)
		Log(LogLevelDebug, "%s", body)
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
