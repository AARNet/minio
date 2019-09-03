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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"
)

// eosObjects implements gateway for Minio and S3 compatible object storage servers.
type eosObjects struct {
	path         string
	hookurl      string
	stage        string
	readonly     bool
	validbuckets bool
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
	eosLogger.Log(ctx, LogLevelStat, "GetBucketInfo", fmt.Sprintf("S3cmd: GetBucketInfo [bucket: %s]", bucket), nil)

	stat, err := e.FileSystem.DirStat(ctx, bucket)
	if err == nil {
		bi = minio.BucketInfo{
			Name:    bucket,
			Created: stat.ModTime(),
		}
	} else {
		err = minio.BucketNotFound{Bucket: bucket}
	}
	return bi, err
}

// ListBuckets - Lists all root folders
func (e *eosObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	eosLogger.Log(ctx, LogLevelStat, "ListBuckets", "S3cmd: ListBuckets", nil)

	dirs, err := e.FileSystem.BuildCache(ctx, "", false)
	defer e.FileSystem.DeleteCache(ctx)

	if err != nil {
		return buckets, err
	}

	for _, dir := range dirs {
		stat, err := e.FileSystem.DirStat(ctx, dir)

		if stat == nil {
			eosLogger.Log(ctx, LogLevelError, "ListBuckets", fmt.Sprintf("ERROR: ListBuckets: unable to stat [dir: %s]", dir), err)
			continue
		}

		if stat.IsDir() && e.IsValidBucketName(strings.TrimRight(dir, "/")) {
			b := minio.BucketInfo{
				Name:    strings.TrimSuffix(dir, "/"),
				Created: stat.ModTime(),
			}
			buckets = append(buckets, b)
		} else {
			if !stat.IsDir() {
				eosLogger.Log(ctx, LogLevelDebug, "ListBuckets", fmt.Sprintf("Bucket: %s not a directory", dir), nil)
			}
			if !e.IsValidBucketName(strings.TrimRight(dir, "/")) {
				eosLogger.Log(ctx, LogLevelDebug, "ListBuckets", fmt.Sprintf("Bucket: %s not a valid name", dir), nil)
			}
		}
	}

	eosLogger.Log(ctx, LogLevelDebug, "ListBuckets", fmt.Sprintf("Buckets found: %+v", buckets), nil)
	return buckets, err
}

// MakeBucketWithLocation - Create a new container.
func (e *eosObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	eosLogger.Log(ctx, LogLevelStat, "MakeBucketWithLocation", fmt.Sprintf("S3cmd: MakeBucketWithLocation [bucket: %s, location: %s]", bucket, location), nil)

	if e.readonly {
		return minio.NotImplemented{}
	}

	// Verify if bucket is valid.
	if !e.IsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	exists, _ := e.FileSystem.FileExists(ctx, bucket)
	if !exists {
		_ = e.FileSystem.mkdirWithOption(ctx, bucket, "")
	} else {
		return minio.BucketExists{Bucket: bucket}
	}
	return nil
}

// DeleteBucket - delete a container
func (e *eosObjects) DeleteBucket(ctx context.Context, bucket string) error {
	eosLogger.Log(ctx, LogLevelStat, "DeleteBucket", fmt.Sprintf("S3cmd: DeleteBucket [bucket: %s]", bucket), nil)

	if e.readonly {
		return minio.NotImplemented{}
	}

	err := e.FileSystem.rmdir(ctx, bucket)
	if err != nil {
		return minio.BucketNotFound{Bucket: bucket}
	}
	return err
}

// GetBucketPolicy - Get the container ACL
func (e *eosObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	eosLogger.Log(ctx, LogLevelStat, "GetBucketPolicy", fmt.Sprintf("S3cmd: GetBucketPolicy [bucket: %s]", bucket), nil)

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
	eosLogger.Log(ctx, LogLevelStat, "SetBucketPolicy", fmt.Sprintf("S3cmd: SetBucketPolicy [bucket: %s, bucketPolicy: %s]", bucket, bucketPolicy), nil)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (e *eosObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	eosLogger.Log(ctx, LogLevelStat, "DeleteBucketPolicy", fmt.Sprintf("S3cmd: DeleteBucketPolicy [bucket: %s]", bucket), nil)

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
	srcpath := srcBucket + "/" + srcObject
	destpath := destBucket + "/" + destObject
	eosLogger.Log(ctx, LogLevelStat, "CopyObject", fmt.Sprintf("S3cmd: CopyObject [from: %s, to: %s, srcInfo: %+v]", srcpath, destpath, srcInfo), nil)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	dir := destBucket + "/" + filepath.Dir(destObject)
	if exists, err := e.FileSystem.FileExists(ctx, dir); !exists && err != nil {
		e.FileSystem.mkdirWithOption(ctx, dir, "&mgm.option=p")
	}

	err = e.FileSystem.Copy(ctx, srcpath, destpath, srcInfo.Size)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "CopyObject", fmt.Sprintf("ERROR: COPY: %+v", err), err)
		return objInfo, err
	}

	err = e.FileSystem.SetETag(ctx, destpath, srcInfo.ETag)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "CopyObject", fmt.Sprintf("ERROR: COPY: %+v", err), err)
		return objInfo, err
	}

	err = e.FileSystem.SetContentType(ctx, destpath, srcInfo.ContentType)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "CopyObject", fmt.Sprintf("ERROR: COPY: %+v", err), err)
		return objInfo, err
	}

	return e.GetObjectInfoWithRetry(ctx, destBucket, destObject, dstOpts, 20)
}

// CopyObjectPart creates a part in a multipart upload by copying
func (e *eosObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	eosLogger.Log(ctx, LogLevelStat, "CopyObjectPart", fmt.Sprintf("S3cmd: CopyObjectPart: [srcpath: %s/%s, destpath: %s/%s]", srcBucket, srcObject, destBucket, destObject), nil)

	if e.readonly {
		return p, minio.NotImplemented{}
	}

	return p, minio.NotImplemented{}
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	eosLogger.Log(ctx, LogLevelStat, "PutObject", fmt.Sprintf("S3cmd: PutObject: [path: %s/%s]", bucket, object), nil)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for key, val := range opts.UserDefined {
		eosLogger.Log(ctx, LogLevelDebug, "PutObject", fmt.Sprintf("PutObject [path: %s/%s, key: %s, value: %s]", bucket, object, key, val), nil)
	}

	buf, _ := ioutil.ReadAll(data)
	hasher := md5.New()
	hasher.Write([]byte(buf))
	etag := hex.EncodeToString(hasher.Sum(nil))
	dir := bucket + "/" + filepath.Dir(object)
	objectpath := bucket + "/" + object

	if exists, _ := e.FileSystem.FileExists(ctx, dir); !exists {
		e.FileSystem.mkdirWithOption(ctx, dir, "&mgm.option=p")
	}

	err = e.FileSystem.Put(ctx, objectpath, buf)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "PutObject", fmt.Sprintf("ERROR: PUT: %+v", err), err)
		return objInfo, err
	}
	err = e.FileSystem.SetETag(ctx, objectpath, etag)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "PutObject", fmt.Sprintf("ERROR: PUT: %+v", err), err)
		return objInfo, err
	}
	err = e.FileSystem.SetContentType(ctx, objectpath, opts.UserDefined["content-type"])
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "PutObject", fmt.Sprintf("ERROR: PUT: %+v", err), err)
		return objInfo, err
	}

	return e.GetObjectInfoWithRetry(ctx, bucket, object, opts, 20)
}

// DeleteObject - Deletes a blob on EOS
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	eosLogger.Log(ctx, LogLevelStat, "DeleteObject", fmt.Sprintf("S3cmd: DeleteObject: [path: %s/%s]", bucket, object), nil)

	if e.readonly {
		return minio.NotImplemented{}
	}

	e.FileSystem.rm(ctx, filepath.Join(bucket, object))
	return nil
}

// DeleteObjects - Deletes multiple blobs on EOS
func (e *eosObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	eosLogger.Log(ctx, LogLevelStat, "DeleteObjects", fmt.Sprintf("S3cmd: DeleteObjects: [bucket: %s]", bucket), nil)

	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = e.DeleteObject(ctx, bucket, object)
	}

	return errs, nil
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)
	eosLogger.Log(ctx, LogLevelStat, "GetObject", fmt.Sprintf("S3cmd: GetObject: [path: %s, startOffset: %d, length: %d]", path, startOffset, length), nil)

	if etag != "" {
		objInfo, err := e.GetObjectInfo(ctx, bucket, object, opts)
		if err != nil {
			return err
		}
		if objInfo.ETag != etag {
			return minio.InvalidETag{}
		}
	}

	err := e.FileSystem.ReadChunk(ctx, path, startOffset, length, writer)
	return err
}

// GetObjectInfoWithRetry because sometimes we need to wait for EOS to properly register a file
func (e *eosObjects) GetObjectInfoWithRetry(ctx context.Context, bucket, object string, opts minio.ObjectOptions, maxRetry int) (objInfo minio.ObjectInfo, err error) {
	// We need to try and wait for the file to register with EOS if it's new
	if maxRetry < 1 {
		maxRetry = 1
	}
	sleepMax := 1000
	sleepTime := 100
	sleepInc := 100

	for retry := 0; retry < maxRetry; retry++ {
		objInfo, err = e.GetObjectInfo(ctx, bucket, object, opts)
		if err == nil {
			break
		}
		SleepMs(sleepTime)
		// We want to wait longer if we're not getting good results.
		if sleepTime < sleepMax {
			sleepTime = sleepTime + sleepInc
		}
	}
	return objInfo, err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo
func (e *eosObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)
	eosLogger.Log(ctx, LogLevelStat, "GetObjectInfo", fmt.Sprintf("S3cmd: GetObjectInfo: [path: %s]", path), nil)

	if isdir, _ := e.FileSystem.IsDir(ctx, path); isdir {
		eosLogger.Log(ctx, LogLevelDebug, "GetObjectInfo", fmt.Sprintf("GetObjectInfo: Request is for directory, returning Not Found [path: %s]", path), nil)
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object}
		return objInfo, err
	}

	stat, err := e.FileSystem.Stat(ctx, path)
	if err != nil {
		eosLogger.Log(ctx, LogLevelDebug, "GetObjectInfo", fmt.Sprintf("GetObjectInfo: [error: %+v]", err), nil)
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

	eosLogger.Log(ctx, LogLevelDebug, "GetObjectInfo", fmt.Sprintf("GetObjectInfo: [path: %s, etag: %s, content-type: %s]", path, stat.ETag(), stat.ContentType()), nil)
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
	eosLogger.Log(ctx, LogLevelStat, "ListMultipartUploads", fmt.Sprintf("S3cmd: ListMultipartUploads: [bucket: %s, prefix: %s, keyMarket: %s, uploadIDMarker: %s, delimiter: %s, maxUploads: %d]", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads), nil)
	return result, minio.NotImplemented{}
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	eosLogger.Log(ctx, LogLevelStat, "NewMultipartUpload", fmt.Sprintf("S3cmd: NewMultipartUpload: [path: %s/%s, options:  +%v]", bucket, object, opts), nil)

	if e.readonly {
		return "", minio.NotImplemented{}
	}

	uploadID = bucket + "/" + object

	if strings.HasSuffix(uploadID, "/") {
		return "", minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	dir := bucket + "/" + filepath.Dir(object)
	if exists, _ := e.FileSystem.FileExists(ctx, dir); !exists {
		eosLogger.Log(ctx, LogLevelInfo, "NewMultipartUpload", fmt.Sprintf("NewMultipartUpload: mkdir: [dir: %s]", dir), nil)
		e.FileSystem.mkdirWithOption(ctx, dir, "&mgm.option=p")
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
			eosLogger.Log(ctx, LogLevelInfo, "NewMultipartUpload", fmt.Sprintf("mkdir failed [path: %s, error: %+v]", absstagepath, err), nil)
		}
		mp.stagepath = stagepath
	}

	e.TransferList.AddTransfer(uploadID, &mp)
	eosLogger.Log(ctx, LogLevelInfo, "NewMultipartUpload", fmt.Sprintf("NewMultipartUpload: [uploadID: %s]", uploadID), nil)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	eosLogger.Log(ctx, LogLevelStat, "PutObjectPart", fmt.Sprintf("S3cmd: PutObjectPart: [object: %s/%s, uploadID: %s, partId: %d, size: %d]", bucket, object, uploadID, partID, data.Size()), nil)

	if e.readonly {
		return info, minio.NotImplemented{}
	}

	transfer := e.TransferList.GetTransfer(uploadID)
	for {
		transfer.RLock()
		parts := transfer.parts
		transfer.RUnlock()

		if parts != nil {
			break
		}
		eosLogger.Log(ctx, LogLevelError, "PutObjectPart", fmt.Sprintf("PutObjectPart called before NewMultipartUpload finished. [object: %s/%s, uploadID: %d]", bucket, object, partID), err)
		Sleep()
	}

	size := data.Size()
	etag := data.MD5CurrentHexString()
	buf, _ := ioutil.ReadAll(data)

	newPart := minio.PartInfo{
		PartNumber:   partID,
		LastModified: time.Now(),
		ETag:         etag,
		Size:         size,
	}

	transfer.AddPart(partID, newPart)

	if partID == 1 {
		transfer.Lock()
		if len(buf) < 1 {
			eosLogger.Log(ctx, LogLevelError, "PutObjectPart", fmt.Sprintf("PutObjectPart received 0 bytes in the buffer. [object: %s/%s, uploadID: %d]", bucket, object, partID), err)
			transfer.firstByte = 0
		} else {
			transfer.firstByte = buf[0]
		}
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
			eosLogger.Log(ctx, LogLevelDebug, "PutObjectPart", fmt.Sprintf("PutObjectPart: waiting for first chunk [object: %s/%s, processing_part: %d]", bucket, object, partID), nil)
			Sleep()
		}
	}

	transfer.Lock()
	transfer.partsCount++
	transfer.AddToSize(size)
	chunksize := transfer.chunkSize
	transfer.Unlock()

	offset := chunksize * int64(partID-1)
	eosLogger.Log(ctx, LogLevelDebug, "PutObjectPart", fmt.Sprintf("PutObjectPart offset [object: %s/%s, partID: %d, offset: %d]", bucket, object, (partID-1), offset), nil)

	if e.stage != "" { //staging
		eosLogger.Log(ctx, LogLevelDebug, "PutObjectPart", fmt.Sprintf("PutObjectPart: staging transfer [object: %s/%s]", bucket, object), nil)

		stagepath := transfer.GetStagePath()
		absstagepath := e.stage + "/" + stagepath

		if _, err := os.Stat(absstagepath); os.IsNotExist(err) {
			_ = os.MkdirAll(absstagepath, 0700)
		}
		f, err := os.OpenFile(absstagepath+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			eosLogger.Log(ctx, LogLevelError, "PutObjectPart", fmt.Sprintf("ERROR: Write ContentType: %+v", err), err)
			return newPart, err
		}
		_, err = f.WriteAt(buf, offset)
		if err != nil {
			eosLogger.Log(ctx, LogLevelError, "PutObjectPart", fmt.Sprintf("ERROR: Write ContentType: %+v", err), err)
			return newPart, err
		}
		f.Close()
	} else { // use xrootd
		go func() {
			err = e.FileSystem.xrootdWriteChunk(ctx, uploadID, offset, offset+size, "0", buf)
		}()
	}

	transfer = e.TransferList.GetTransfer(uploadID)
	for {
		if transfer == nil {
			eosLogger.Log(ctx, LogLevelError, "PutObjectPart", fmt.Sprintf("PutObjectPart: Invalid transfer [uploadID: %s]", uploadID), nil)
			break
		} else {
			transfer.RLock()
			md5PartID := transfer.md5PartID
			transfer.RUnlock()
			if md5PartID == partID {
				break
			}
			eosLogger.Log(ctx, LogLevelDebug, "PutObjectPart", fmt.Sprintf("PutObjectPart: waiting for part [uploadID: %s, md5PartID: %d, currentPart: %d]", uploadID, md5PartID, partID), nil)
		}
		Sleep()
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
	eosLogger.Log(ctx, LogLevelStat, "CompleteMultipartUpload", fmt.Sprintf("S3cmd: CompleteMultipartUpload: [uploadID: %s, size: %d, firstByte: %d, useragent: %s]", uploadID, size, firstByte, reqInfo.UserAgent), nil)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for {
		transfer.RLock()
		if transfer.md5PartID == transfer.partsCount+1 {
			break
		}
		eosLogger.Log(ctx, LogLevelDebug, "CompleteMultipartUpload", fmt.Sprintf("CompleteMultipartUpload: waiting for all md5Parts [uploadID: %s, total_parts: %d, remaining: %d]", uploadID, transfer.partsCount, transfer.partsCount+1-transfer.md5PartID), nil)
		transfer.RUnlock()
		Sleep()
	}

	etag := transfer.GetETag()
	contenttype := transfer.GetContentType()
	stagepath := transfer.GetStagePath()

	eosLogger.Log(ctx, LogLevelDebug, "CompleteMultipartUpload", fmt.Sprintf("CompleteMultipartUpload: [uploadID: %s, etag: %s]", uploadID, etag), nil)

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
		err = e.FileSystem.Touch(ctx, uploadID, size)
		if err != nil {
			eosLogger.Log(ctx, LogLevelError, "CompleteMultipartUpload", fmt.Sprintf("ERROR: CompleteMultipartUpload: EOStouch: [uploadID: %s, error: %+v]", uploadID, err), err)
			return objInfo, err
		}

		// Upload the transfer to EOS in the background
		if strings.HasPrefix(reqInfo.UserAgent, "rclone") {
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
		err = e.FileSystem.xrootdWriteChunk(ctx, uploadID, 0, size, "1", firstbyte)
		if err != nil {
			eosLogger.Log(ctx, LogLevelError, "CompleteMultipartUpload", fmt.Sprintf("ERROR: CompleteMultipartUpload: EOSwriteChunk: [uploadID: %s, error: %+v]", uploadID, err), err)
			return objInfo, err
		}

		e.TransferList.DeleteTransfer(uploadID)
	}

	err = e.FileSystem.SetETag(ctx, uploadID, etag)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "CompleteMultipartUpload", fmt.Sprintf("ERROR: CompleteMultipartUpload: [uploadID: %s, error: %+v]", uploadID, err), err)
		return objInfo, err
	}
	err = e.FileSystem.SetContentType(ctx, uploadID, contenttype)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "CompleteMultipartUpload", fmt.Sprintf("ERROR: CompleteMultipartUpload: [uploadID: %s, error: %+v]", uploadID, err), err)
		return objInfo, err
	}

	e.TransferList.DeleteTransfer(uploadID)
	return objInfo, nil
}

// Transfer the upload from the staging area to it's final location
func (e *eosObjects) TransferFromStaging(ctx context.Context, stagepath string, uploadID string, objInfo minio.ObjectInfo) error {
	reqInfo := logger.GetReqInfo(ctx)
	fullstagepath := e.stage + "/" + stagepath + "/file"
	eosLogger.Log(ctx, LogLevelDebug, "TransferFromStaging", fmt.Sprintf("CompleteMultipartUpload: xrdcp: [stagepath: %s, uploadIDpath: %s, size: %d, useragent: %s]", fullstagepath, uploadID+".minio.sys", objInfo.Size, reqInfo.UserAgent), nil)
	err := e.FileSystem.Xrdcp.Put(ctx, fullstagepath, uploadID+".minio.sys", objInfo.Size)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "TransferFromStaging", fmt.Sprintf("ERROR: CompleteMultipartUpload: xrdcp: [uploadID: %s, UserAgent: %s, error: %+v]", uploadID, reqInfo.UserAgent, err), err)
		return err
	}
	err = e.FileSystem.Rename(ctx, uploadID+".minio.sys", uploadID)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "TransferFromStaging", fmt.Sprintf("ERROR: CompleteMultipartUpload: EOSrename: [uploadID: %s, error: %+v]", uploadID, err), err)
		return err
	}
	err = e.FileSystem.SetETag(ctx, uploadID, objInfo.ETag)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "TransferFromStaging", fmt.Sprintf("ERROR: CompleteMultipartUpload: EOSsetETag: [uploadID: %s, error: %+v]", uploadID, err), nil)
		return err
	}
	err = e.FileSystem.SetContentType(ctx, uploadID, objInfo.ContentType)
	if err != nil {
		eosLogger.Log(ctx, LogLevelError, "TransferFromStaging", fmt.Sprintf("ERROR: CompleteMultipartUpload: EOSsetContentType: [uploadID: %s, error: %+v]", uploadID, err), err)
		return err
	}
	err = os.RemoveAll(e.stage + "/" + stagepath)
	if err != nil {
		return err
	}
	e.TransferList.DeleteTransfer(uploadID)
	return nil
}

//AbortMultipartUpload
func (e *eosObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	eosLogger.Log(ctx, LogLevelStat, "AbortMultipartUpload", fmt.Sprintf("S3cmd: AbortMultipartUpload: [object: %s/%s, uploadID: %s]", bucket, object, uploadID), nil)

	if e.readonly {
		return minio.NotImplemented{}
	}

	if e.stage != "" && e.TransferList.TransferExists(uploadID) {
		transfer := e.TransferList.GetTransfer(uploadID)
		stagepath := transfer.GetStagePath()
		os.RemoveAll(e.stage + "/" + stagepath)
	}

	e.FileSystem.rm(ctx, bucket+"/"+object)
	e.TransferList.DeleteTransfer(uploadID)

	return nil
}

// ListObjectParts
func (e *eosObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, options minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	eosLogger.Log(ctx, LogLevelStat, "ListObjectParts", fmt.Sprintf("S3cmd: ListObjectParts: [uploadID: %s, part: %d, maxParts: %d]", uploadID, partNumberMarker, maxParts), nil)

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

// Return an initialized minio.ObjectInfo
func (e *eosObjects) NewObjectInfo(bucket string, name string, stat *FileStat) (obj minio.ObjectInfo) {
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
	eosLogger.Log(ctx, LogLevelStat, "ListObjects", fmt.Sprintf("S3cmd: ListObjects: [bucket: %s, prefix: %s, marker: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, marker, delimiter, maxKeys), nil)

	result, err = e.ListObjectsRecurse(ctx, bucket, prefix, marker, delimiter, -1)

	// Sort the results to make them an easier list to read for most clients
	sort.SliceStable(result.Objects, func(i, j int) bool { return result.Objects[i].Name < result.Objects[j].Name })
	sort.SliceStable(result.Prefixes, func(i, j int) bool { return result.Prefixes[i] < result.Prefixes[j] })

	eosLogger.Log(ctx, LogLevelDebug, "ListObjects", fmt.Sprintf("ListObjects: Result: %+v", result), nil)
	return result, err
}

// ListObjectsRecurse - Recursive function for interating through a directory tree
func (e *eosObjects) ListObjectsRecurse(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	result.IsTruncated = false
	// Don't do anything if delimiter and prefix are both slashes
	if delimiter == "/" && prefix == "/" {
		return result, nil
	}

	// Make sure there is a trailing slash so it's added to prefixes correctly (otherwise you get ./<prefix> objects)

	// Get a list of objects in the directory
	// or the single object if it's not a directory
	path := filepath.Join(bucket, prefix)
	path = filepath.Clean(path)

	// We only want to list the directory and not it's contents if it doesn't end with /
	if prefix != "" && delimiter != "" && !strings.HasSuffix(prefix, "/") {
		if isdir, _ := e.FileSystem.IsDir(ctx, path); isdir {
			stat, err := e.FileSystem.DirStat(ctx, path)
			if err != nil {
				eosLogger.Log(ctx, LogLevelError, "ListObjects", "ListObjects: Unable to stat directory [path: "+path+"]", nil)
				return result, err
			}
			if stat != nil {
				result.Prefixes = append(result.Prefixes, prefix+"/")
				return result, err
			}
		}
	}

	// Otherwise we need to do some other stuff
	objects, err := e.FileSystem.BuildCache(ctx, path, true)
	defer e.FileSystem.DeleteCache(ctx)

	if err != nil {
		return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
	}

	for _, obj := range objects {
		var stat *FileStat
		objpath := filepath.Join(path, obj)
		objprefix := prefix
		objIsDir := strings.HasSuffix(obj, "/") // because filepath.Join() strips trailing slashes
		objCount := len(objects)

		if objCount == 1 && prefix != "" && filepath.Base(objprefix) == obj {
			// Jump back one directory to fix the prefixes
			// for individual files
			objpath = filepath.Join(bucket, objprefix)
			objprefix = filepath.Dir(objprefix)
		}

		objpath = filepath.Clean(objpath)

		// We need to call DirStat() so we don't recurse directories when we don't have to
		if objIsDir {
			objpath = objpath + "/"
			stat, err = e.FileSystem.DirStat(ctx, objpath)
		} else {
			stat, err = e.FileSystem.Stat(ctx, objpath)
		}

		if stat != nil {
			objName := filepath.Join(objprefix, obj)
			// Directories get added to prefixes, files to objects.
			if stat.IsDir() {
				objName = objName + "/"
				result.Prefixes = append(result.Prefixes, objName)
			} else {
				if objCount == 1 {
					// Don't add the object if there is one object and the prefix ends with / (ie. is a dir)
					if strings.HasSuffix(prefix, "/") && !strings.HasSuffix(objName, "/") {
						return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
					}

					// Don't add prefix since it'll be in the prefix list
					// Add the object's directory to prefixes
					objdir := filepath.Dir(objprefix)
					if objdir != "." {
						result.Prefixes = append(result.Prefixes, objdir+"/")
					}
				}
				if objName != "." {
					o := e.NewObjectInfo(bucket, objName, stat)
					result.Objects = append(result.Objects, o)
				}
			}

			// If there's no delimiter, we need to get information from subdirectories too
			if delimiter == "" && stat.IsDir() {
				eosLogger.Log(ctx, LogLevelDebug, "ListObjects", "ListObjects: Recursing through "+prefix+obj, nil)
				subdir, err := e.ListObjectsRecurse(ctx, bucket, prefix+obj, marker, delimiter, -1)
				if err != nil {
					return result, err
				}
				// Merge objects and prefixes from the recursive call
				result.Objects = append(result.Objects, subdir.Objects...)
				result.Prefixes = append(result.Prefixes, subdir.Prefixes...)
			}
		} else {
			eosLogger.Log(ctx, LogLevelError, "ListObjects", "ERROR: ListObjects: unable to stat [objpath: "+objpath+"]", nil)
		}
	}

	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	eosLogger.Log(ctx, LogLevelStat, "ListObjectsV2", fmt.Sprintf("S3cmd: ListObjectsV2: [bucket: %s, prefix: %s, continuationToken: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, continuationToken, delimiter, maxKeys), nil)

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
	eosLogger.Log(ctx, LogLevelStat, "HealFormat", "S3cmd: HealFormat:", nil)
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

// ReloadFormat - no-op for fs
func (e *eosObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	eosLogger.Log(ctx, LogLevelStat, "ReloadFormat", "S3cmd: ReloadFormat:", nil)
	return minio.NotImplemented{}
}

// ListObjectsHeal - list all objects to be healed.
func (e *eosObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	eosLogger.Log(ctx, LogLevelStat, "ListObjectsHeal", "S3cmd: ListObjectsHeal:", nil)
	return loi, minio.NotImplemented{}
}

// HealObject - no-op for fs.
func (e *eosObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool, remove bool, scanMode madmin.HealScanMode) (results madmin.HealResultItem, err error) {
	eosLogger.Log(ctx, LogLevelStat, "HealObject", "S3cmd: HealObject:", nil)
	return results, minio.NotImplemented{}
}

// HealObjects - no-op for fs.
func (e *eosObjects) HealObjects(ctx context.Context, bucket, prefix string, fn func(string, string) error) (err error) {
	eosLogger.Log(ctx, LogLevelStat, "HealObjects", "S3cmd: HealObjects:", nil)
	return minio.NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed
func (e *eosObjects) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	eosLogger.Log(ctx, LogLevelStat, "ListBucketsHeal", "S3cmd: ListBucketsHeal:", nil)
	return []minio.BucketInfo{}, minio.NotImplemented{}
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (e *eosObjects) HealBucket(ctx context.Context, bucket string, dryRun bool, remove bool) (results madmin.HealResultItem, err error) {
	eosLogger.Log(ctx, LogLevelStat, "HealBucket", "S3cmd: HealBucket:", nil)
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
