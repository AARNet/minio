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
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bytes"
	"compress/lzw"
	"encoding/base64"
	"encoding/gob"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/lifecycle"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
)

// eosObjects implements gateway for Minio and S3 compatible object storage servers.
type eosObjects struct {
	maxRetry          int
	maxKeys           int
	path              string
	hookurl           string
	stage             string
	foregroundStaging bool
	readonly          bool
	validbuckets      bool
	TransferList      *TransferList
	FileSystem        *eosFS
}

type ListObjectsMarker struct {
	Prefixes []string
	Skip     int
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
	eosLogger.Stat(ctx, "S3cmd: GetBucketInfo [bucket: %s]", bucket)

	stat, err := e.FileSystem.DirStat(ctx, bucket)
	if err == nil {
		bi = minio.BucketInfo{
			Name:    bucket,
			Created: stat.ModTime,
		}
	} else {
		err = minio.BucketNotFound{Bucket: bucket}
	}
	return bi, err
}

// ListBuckets - Lists all root folders
func (e *eosObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	eosLogger.Stat(ctx, "S3cmd: ListBuckets")

	dirs, err := e.FileSystem.BuildCache(ctx, "", false)
	defer e.FileSystem.DeleteCache(ctx)

	if err != nil {
		return buckets, err
	}

	for _, dir := range dirs {
		if dir.FullPath == e.path+"/" {
			continue
		}

		eosLogger.Debug(ctx, "Consider Bucket: %s (%s)", dir.Name, dir.FullPath)
		stat, err := e.FileSystem.DirStat(ctx, dir.Name)

		if stat == nil {
			eosLogger.Error(ctx, err, "ListBuckets: unable to stat [dir: %s]", dir)
			continue
		}

		if stat.IsDir() && e.IsValidBucketName(strings.TrimRight(dir.Name, "/")) {
			b := minio.BucketInfo{
				Name:    strings.TrimSuffix(dir.Name, "/"),
				Created: stat.ModTime,
			}
			buckets = append(buckets, b)
		} else {
			if !stat.IsDir() {
				eosLogger.Debug(ctx, "Bucket: %s not a directory", dir.Name)
			}
			if !e.IsValidBucketName(strings.TrimRight(dir.Name, "/")) {
				eosLogger.Debug(ctx, "Bucket: %s not a valid name", dir.Name)
			}
		}
	}

	eosLogger.Debug(ctx, "Buckets found: %+v", buckets)
	return buckets, err
}

// MakeBucketWithLocation - Create a new container.
func (e *eosObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	eosLogger.Stat(ctx, "S3cmd: MakeBucketWithLocation [bucket: %s, location: %s]", bucket, location)

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
	eosLogger.Stat(ctx, "S3cmd: DeleteBucket [bucket: %s]", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	err := e.FileSystem.rmdir(ctx, bucket)
	if err != nil {
		return minio.BucketNotFound{Bucket: bucket}
	}
	return err
}

// GetBucketLifecycle - not implemented
func (e *eosObjects) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	eosLogger.Stat(ctx, "S3cmd: GetBucketLifecycle [bucket: %s]", bucket)

	if e.readonly {
		return nil, minio.NotImplemented{}
	}

	return nil, minio.NotImplemented{}
}

// SetBucketLifecycle - not implemented
func (e *eosObjects) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	eosLogger.Stat(ctx, "S3cmd: SetBucketLifecycle [bucket: %s]", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketLifecycle - not implemented
func (e *eosObjects) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	eosLogger.Stat(ctx, "S3cmd: DeleteBucketLifecycle [bucket: %s]", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// GetBucketPolicy - Get the container ACL
func (e *eosObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	eosLogger.Stat(ctx, "S3cmd: GetBucketPolicy [bucket: %s]", bucket)
	return nil, minio.NotImplemented{}
}

// SetBucketPolicy
func (e *eosObjects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	eosLogger.Stat(ctx, "S3cmd: SetBucketPolicy [bucket: %s, bucketPolicy: %s]", bucket, bucketPolicy)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (e *eosObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	eosLogger.Stat(ctx, "S3cmd: DeleteBucketPolicy [bucket: %s]", bucket)

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
	eosLogger.Stat(ctx, "S3cmd: CopyObject [from: %s, to: %s, srcInfo: %+v]", srcpath, destpath, srcInfo)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	if srcpath != destpath {
		dir := destBucket + "/" + filepath.Dir(destObject)
		if exists, err := e.FileSystem.FileExists(ctx, dir); !exists && err != nil {
			e.FileSystem.mkdirWithOption(ctx, dir, "&mgm.option=p")
		}

		err = e.FileSystem.Copy(ctx, srcpath, destpath, srcInfo.Size)
		if err != nil {
			eosLogger.Error(ctx, err, "CopyObject: %+v", err)
			return objInfo, err
		}

		err = e.FileSystem.SetETag(ctx, destpath, srcInfo.ETag)
		if err != nil {
			eosLogger.Error(ctx, err, "CopyObject: %+v", err)
			return objInfo, err
		}

		err = e.FileSystem.SetContentType(ctx, destpath, srcInfo.ContentType)
		if err != nil {
			eosLogger.Error(ctx, err, "CopyObject: %+v", err)
			return objInfo, err
		}
	} else {
		eosLogger.Debug(ctx, "CopyObject srcpath==destpath")
	}

	return e.GetObjectInfoWithRetry(ctx, destBucket, destObject, dstOpts)
}

// CopyObjectPart creates a part in a multipart upload by copying
func (e *eosObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	eosLogger.Stat(ctx, "S3cmd: CopyObjectPart: [srcpath: %s/%s, destpath: %s/%s]", srcBucket, srcObject, destBucket, destObject)

	if e.readonly {
		return p, minio.NotImplemented{}
	}

	return p, minio.NotImplemented{}
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	eosLogger.Stat(ctx, "S3cmd: PutObject: [bucket: %s, object: %s]", bucket, object)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	// This just prints out user defined headers on the object, we're not doing anything with it
	//for key, val := range opts.UserDefined {
	//	eosLogger.Debug(ctx, "PutObject [path: %s%s, key: %s, value: %s]", bucket, object, key, val)
	//}

	buf := bufio.NewReader(data)
	dir := bucket + "/" + filepath.Dir(object)
	objectpath := bucket + "/" + object

	// Create the parent directory if it doesn't exist
	if exists, _ := e.FileSystem.FileExists(ctx, dir); !exists {
		e.FileSystem.mkdirWithOption(ctx, dir, "&mgm.option=p")
	}

	// Send the file
	response, err := e.FileSystem.PutBuffer(ctx, e.stage, objectpath, buf)
	if err != nil {
		eosLogger.Error(ctx, err, "PUT: %+v", err)
		objInfo.ETag = defaultETag
		return objInfo, minio.IncompleteBody{Bucket: bucket, Object: object}
	}
	eosLogger.Debug(ctx, "Put response: %#v", response)

	etag := response.Checksum
	err = e.FileSystem.SetETag(ctx, objectpath, etag)
	if err != nil {
		eosLogger.Error(ctx, err, "PUT.SetETag: %+v", err)
		objInfo.ETag = defaultETag
		return objInfo, minio.InvalidETag{}
	}

	err = e.FileSystem.SetContentType(ctx, objectpath, opts.UserDefined["content-type"])
	if err != nil {
		eosLogger.Error(ctx, err, "PUT.SetContentType: %+v", err)
		return objInfo, err
	}

	objInfo, err = e.GetObjectInfoWithRetry(ctx, bucket, object, opts)
	if err == nil && objInfo.Size != data.Size() {
		eosLogger.Error(ctx, err, "PUT: File on disk is not the correct size [disk: %d, expected: %d]", objInfo.Size, data.Size())
		// Remove the file
		_ = e.FileSystem.rm(ctx, objectpath)
		return objInfo, minio.IncompleteBody{Bucket: bucket, Object: object}
	}

	return objInfo, err
}

// DeleteObject - Deletes a blob on EOS
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	eosLogger.Stat(ctx, "S3cmd: DeleteObject: [bucket: %s object: %s]", bucket, object)

	if e.readonly {
		return minio.NotImplemented{}
	}

	_ = e.FileSystem.rm(ctx, PathJoin(bucket, object))
	return nil
}

// DeleteObjects - Deletes multiple blobs on EOS
func (e *eosObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	eosLogger.Stat(ctx, "S3cmd: DeleteObjects: [bucket: %s]", bucket)

	errs := make([]error, len(objects))
	deleted := make(map[string]bool)
	for idx, object := range objects {
		if _, ok := deleted[object]; !ok {
			errs[idx] = e.DeleteObject(ctx, bucket, object)
			deleted[object] = true
		}
	}

	return errs, nil
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)
	eosLogger.Stat(ctx, "S3cmd: GetObject: [bucket: %s, object: %s, path: %s, startOffset: %d, length: %d]", bucket, object, path, startOffset, length)

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
func (e *eosObjects) GetObjectInfoWithRetry(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	// We need to try and wait for the file to register with EOS if it's new
	sleepMax := 1000
	sleepTime := 100
	sleepInc := 100

	for retry := 0; retry < e.maxRetry; retry++ {
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
	if err != nil {
		eosLogger.Error(ctx, err, "Unable to retrieve object info. [bucket: %s, object: %s]", bucket, object)
		objInfo = minio.ObjectInfo{}
	}
	return objInfo, err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo
func (e *eosObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	path := strings.Replace(PathJoin(bucket, object), "//", "/", -1)
	eosLogger.Stat(ctx, "S3cmd: GetObjectInfo: [bucket: %s, object: %s, path: %s]", bucket, object, path)

	// If there is a transfer in progress, wait for it to finish.
	for {
		transfer := e.TransferList.GetTransfer(path)
		if transfer == nil {
			break
		}
		eosLogger.Debug(ctx, "Waiting for upload to complete [uploadID: %s]", path)
		Sleep()
	}

	var stat *FileStat
	// Stat() will use Find() on directories which also lists files in the directory
	// so, DirStat() forces it to use FileInfo() which only provides info on the directory
	if isdir, _ := e.FileSystem.IsDir(ctx, path); isdir {
		stat, err = e.FileSystem.DirStat(ctx, path)
	} else {
		stat, err = e.FileSystem.Stat(ctx, path)
	}
	if err != nil {
		eosLogger.Debug(ctx, "GetObjectInfo: [error: %+v]", err)
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object}
		return objInfo, err
	}

	// Some clients don't like it if you return a directory when
	// the prefix doesn't end with a / *cough* rclone *cough*
	if !strings.HasSuffix(object, "/") && stat.IsDir() {
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object}
		return objInfo, err
	}

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     stat.ModTime,
		Size:        stat.Size,
		IsDir:       stat.IsDir(),
		ETag:        stat.GetETag(),
		ContentType: stat.ContentType,
	}

	eosLogger.Debug(ctx, "GetObjectInfo: [path: %s, object: %s, isdir: %t, etag: %s, content-type: %s, err: %s]", path, object, stat.IsDir(), stat.GetETag(), stat.ContentType, err)
	return objInfo, err
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (e *eosObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	eosLogger.Stat(ctx, "S3cmd: GetObjectNInfo: [bucket: %s, object: %s]", bucket, object)
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
	eosLogger.Stat(ctx, "S3cmd: ListMultipartUploads: [bucket: %s, prefix: %s, keyMarket: %s, uploadIDMarker: %s, delimiter: %s, maxUploads: %d]", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, minio.NotImplemented{}
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	eosLogger.Stat(ctx, "S3cmd: NewMultipartUpload: [bucket: %s, object: %s, options:  +%v]", bucket, object, opts)

	if e.readonly {
		return "", minio.NotImplemented{}
	}

	uploadID = PathJoin(bucket, object)

	if strings.HasSuffix(uploadID, "/") {
		return "", minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	dir := PathJoin(bucket, filepath.Dir(object))
	e.FileSystem.mkdirp(ctx, dir)

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
		// Create staging area
		hasher := md5.New()
		hasher.Write([]byte(uploadID))
		stagepath := hex.EncodeToString(hasher.Sum(nil))

		//make sure it is clear of older junk
		absstagepath := PathJoin(e.stage, stagepath)
		os.RemoveAll(absstagepath)

		err = os.MkdirAll(absstagepath, 0700)
		if err != nil {
			eosLogger.Error(ctx, err, "mkdir failed [path: %s]", absstagepath)
		}
		mp.stagepath = stagepath
	}

	e.TransferList.AddTransfer(uploadID, &mp)
	eosLogger.Info(ctx, "NewMultipartUpload: [uploadID: %s]", uploadID)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	eosLogger.Stat(ctx, "S3cmd: PutObjectPart: [bucket: %s, object: %s, uploadID: %s, partId: %d, size: %d]", bucket, object, uploadID, partID, r.Size())

	if e.readonly {
		return info, minio.NotImplemented{}
	}

	if e.stage != "" {
		return e.PutObjectPartStaging(ctx, bucket, object, uploadID, partID, r, opts)
	}

	return e.PutObjectPartXrootd(ctx, bucket, object, uploadID, partID, r, opts)
}

// PutObjectPartStaging stages the part to e.stage before transferring to final location
func (e *eosObjects) PutObjectPartStaging(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	e.TransferList.WaitForTransfer(uploadID)
	var data = r.Reader

	buf, _ := ioutil.ReadAll(r) // This is a memory hog because it reads the entire chunk into memory
	hasher := md5.New()
	hasher.Write([]byte(buf))
	etag := hex.EncodeToString(hasher.Sum(nil))

	//force free hasher
	hasher = nil

	info.PartNumber = partID
	info.LastModified = minio.UTCNow()
	info.Size = data.Size()
	info.ETag = etag

	e.TransferList.AddPartToTransfer(uploadID, partID, info)

	if partID == 1 {
		e.TransferList.SetChunkSize(uploadID, info.Size)
	}

	var chunksize int64
	for chunksize == 0 {
		chunksize = e.TransferList.GetChunkSize(uploadID)
		if chunksize == 0 {
			eosLogger.Debug(ctx, "PutObjectPart: waiting for first chunk [bucket: %s, object: %s, processing_part: %d, etag: %s]", bucket, object, partID, info.ETag)
			Sleep()
		}
	}

	e.TransferList.IncrementPartsCount(uploadID)
	e.TransferList.AddToSize(uploadID, info.Size)

	offset := chunksize * int64(partID-1)
	eosLogger.Debug(ctx, "PutObjectPart: staging transfer [bucket: %s, object: %s, partID: %d, offset: %d, etag: %s]", bucket, object, (partID - 1), offset, info.ETag)
	stagepath := e.TransferList.GetStagePath(uploadID)
	absstagepath := e.stage + "/" + stagepath

	if _, err := os.Stat(absstagepath); os.IsNotExist(err) {
		_ = os.MkdirAll(absstagepath, 0700)
	}
	f, err := os.OpenFile(absstagepath+"/file", os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		eosLogger.Error(ctx, err, "PutObjectPart: Unable to open stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	_, err = f.Seek(offset, 0)
	if err != nil {
		eosLogger.Error(ctx, err, "PutObjectPart: Unable to seek to correct position in stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	bytesWritten, err := f.Write(buf)
	if err != nil {
		eosLogger.Error(ctx, err, "PutObjectPart: Unable to copy buffer into stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	f.Close()

	//force free buf
	buf = nil

	transfer := e.TransferList.GetTransfer(uploadID)
	for {
		if transfer == nil {
			eosLogger.Error(ctx, nil, "PutObjectPart: Invalid transfer [uploadID: %s]", uploadID)
			break
		} else {
			transfer.RLock()
			md5PartID := transfer.md5PartID
			transfer.RUnlock()
			if md5PartID == partID {
				break
			}
			eosLogger.Debug(ctx, "PutObjectPart: waiting for part [uploadID: %s, md5PartID: %d, currentPart: %d]", uploadID, md5PartID, partID)
		}
		Sleep()
	}

	// Open the file so we can read the same bytes into the md5 buffer
	f, err = os.Open(absstagepath + "/file")
	if err != nil {
		eosLogger.Error(ctx, err, "PutObjectPart: Unable to open stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	_, err = f.Seek(offset, 0)
	if err != nil {
		eosLogger.Error(ctx, err, "PutObjectPart: Unable to seek to correct position in stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	transfer.Lock()
	_, err = io.CopyN(transfer.md5, f, int64(bytesWritten))
	transfer.Unlock()
	if err != nil && err != io.EOF {
		eosLogger.Error(ctx, err, "PutObjectPart: Unable to copy buffer for hashing [stagepath: %s]", absstagepath)
		return info, err
	}
	f.Close()

	e.TransferList.IncrementMD5PartID(uploadID)

	return info, nil
}

// PutObjectPartXrootd ... uses xrootd
func (e *eosObjects) PutObjectPartXrootd(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	// Wait for transfer to be added to list
	e.TransferList.WaitForTransfer(uploadID)
	var data = r.Reader

	buf, _ := ioutil.ReadAll(r) // This is a memory hog because it reads the entire chunk into memory
	hasher := md5.New()
	hasher.Write([]byte(buf))
	etag := hex.EncodeToString(hasher.Sum(nil))

	//force free hasher
	hasher = nil

	info.PartNumber = partID
	info.LastModified = minio.UTCNow()
	info.Size = data.Size()
	info.ETag = etag

	e.TransferList.AddPartToTransfer(uploadID, partID, info)

	if partID == 1 {
		if info.Size < 1 {
			eosLogger.Error(ctx, err, "PutObjectPart received 0 bytes in the buffer. [bucket: %s, object: %s, uploadID: %d]", bucket, object, partID)
			e.TransferList.SetFirstByte(uploadID, 0)
		} else {
			e.TransferList.SetFirstByte(uploadID, buf[0])
		}
		e.TransferList.SetChunkSize(uploadID, info.Size)
	} else {
		for {
			chunksize := e.TransferList.GetChunkSize(uploadID)
			if chunksize != 0 {
				break
			}
			eosLogger.Debug(ctx, "PutObjectPart: waiting for first chunk [bucket: %s, object: %s, processing_part: %d]", bucket, object, partID)
			Sleep()
		}
	}

	e.TransferList.IncrementPartsCount(uploadID)
	e.TransferList.AddToSize(uploadID, info.Size)
	chunksize := e.TransferList.GetChunkSize(uploadID)

	offset := chunksize * int64(partID-1)
	eosLogger.Debug(ctx, "PutObjectPart offset [bucket: %s, object: %s, partID: %d, offset: %d, etag: %s]", bucket, object, (partID - 1), offset, info.ETag)

	go func() {
		err = e.FileSystem.xrootdWriteChunk(ctx, uploadID, offset, offset+info.Size, "0", buf)
	}()

	transfer := e.TransferList.GetTransfer(uploadID)
	for {
		if transfer == nil {
			eosLogger.Error(ctx, nil, "PutObjectPart: Invalid transfer [uploadID: %s]", uploadID)
			break
		} else {
			md5PartID := e.TransferList.GetMD5PartID(uploadID)
			if md5PartID == partID {
				break
			}
			eosLogger.Debug(ctx, "PutObjectPart: waiting for part [uploadID: %s, md5PartID: %d, currentPart: %d]", uploadID, md5PartID, partID)
		}
		Sleep()
	}

	transfer.Lock()
	transfer.md5.Write(buf)
	transfer.Unlock()
	e.TransferList.IncrementMD5PartID(uploadID)

	return info, nil
}

// CompleteMultipartUpload
func (e *eosObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	if e.stage != "" {
		return e.CompleteMultipartUploadStaging(ctx, bucket, object, uploadID, uploadedParts, opts)
	}
	return e.CompleteMultipartUploadXrootd(ctx, bucket, object, uploadID, uploadedParts, opts)
}

// CompleteMultipartUploadStaging
func (e *eosObjects) CompleteMultipartUploadStaging(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	size := e.TransferList.GetSize(uploadID)
	eosLogger.Stat(ctx, "S3cmd: CompleteMultipartUpload: [uploadID: %s, size: %d]", uploadID, size)

	for {
		transfer := e.TransferList.GetTransfer(uploadID)
		if transfer != nil {
			transfer.RLock()
			if transfer.md5PartID == transfer.partsCount+1 {
				break
			}
			eosLogger.Debug(ctx, "CompleteMultipartUpload: waiting for all md5Parts [uploadID: %s, total_parts: %d, remaining: %d]", uploadID, transfer.partsCount, transfer.partsCount+1-transfer.md5PartID)
			transfer.RUnlock()
		}
		Sleep()
	}

	etag := e.TransferList.GetEtag(uploadID)
	if size == 0 && etag != zerobyteETag {
		etag = zerobyteETag
	}

	contenttype := e.TransferList.GetContentType(uploadID)
	stagepath := e.TransferList.GetStagePath(uploadID)

	eosLogger.Debug(ctx, "CompleteMultipartUpload: [uploadID: %s, etag: %s]", uploadID, etag)

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     time.Now(),
		Size:        size,
		IsDir:       false,
		ETag:        etag,
		ContentType: contenttype,
	}

	// Create an empty file
	err = e.FileSystem.Touch(ctx, uploadID, size)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: EOStouch: [uploadID: %s, error: %+v]", uploadID, err)
		return objInfo, err
	}

	err = e.FileSystem.SetETag(ctx, uploadID, etag)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		return objInfo, err
	}
	err = e.FileSystem.SetContentType(ctx, uploadID, contenttype)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		return objInfo, err
	}

	// Upload the transfer to EOS in the background
	reqInfo := logger.GetReqInfo(ctx)
	if strings.HasPrefix(reqInfo.UserAgent, "rclone") || e.foregroundStaging {
		_ = e.TransferFromStaging(ctx, stagepath, uploadID, objInfo)
		e.TransferList.DeleteTransfer(uploadID)
	} else {
		go func() {
			_ = e.TransferFromStaging(ctx, stagepath, uploadID, objInfo)
			e.TransferList.DeleteTransfer(uploadID)
		}()
	}
	return objInfo, nil
}

// CompleteMultipartUploadXrootd
func (e *eosObjects) CompleteMultipartUploadXrootd(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	reqInfo := logger.GetReqInfo(ctx)
	transfer := e.TransferList.GetTransfer(uploadID)
	transfer.RLock()
	size := transfer.size
	firstByte := transfer.firstByte
	transfer.RUnlock()
	eosLogger.Stat(ctx, "S3cmd: CompleteMultipartUpload: [uploadID: %s, size: %d, firstByte: %d, useragent: %s]", uploadID, size, firstByte, reqInfo.UserAgent)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for {
		transfer.RLock()
		if transfer.md5PartID == transfer.partsCount+1 {
			break
		}
		eosLogger.Debug(ctx, "CompleteMultipartUpload: waiting for all md5Parts [uploadID: %s, total_parts: %d, remaining: %d]", uploadID, transfer.partsCount, transfer.partsCount+1-transfer.md5PartID)
		transfer.RUnlock()
		Sleep()
	}

	etag := transfer.GetETag()
	contenttype := transfer.GetContentType()

	eosLogger.Debug(ctx, "CompleteMultipartUpload: [uploadID: %s, etag: %s]", uploadID, etag)

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     time.Now(),
		Size:        size,
		IsDir:       false,
		ETag:        etag,
		ContentType: contenttype,
	}

	transfer.RLock()
	firstbyte := []byte{transfer.firstByte}
	transfer.RUnlock()

	// Write the first byte - I don't recall why we do this, I think it force updates the file metadata or something
	err = e.FileSystem.xrootdWriteChunk(ctx, uploadID, 0, size, "1", firstbyte)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: EOSwriteChunk: [uploadID: %s]", uploadID)
		objInfo.ETag = zerobyteETag
		return objInfo, minio.IncompleteBody{Bucket: bucket, Object: object}
	}

	e.TransferList.DeleteTransfer(uploadID)

	err = e.FileSystem.SetETag(ctx, uploadID, etag)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		objInfo.ETag = zerobyteETag
		return objInfo, minio.InvalidETag{}
	}
	err = e.FileSystem.SetContentType(ctx, uploadID, contenttype)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		return objInfo, err
	}

	return objInfo, nil
}

// Transfer the upload from the staging area to it's final location
func (e *eosObjects) TransferFromStaging(ctx context.Context, stagepath string, uploadID string, objInfo minio.ObjectInfo) error {
	fullstagepath := e.stage + "/" + stagepath + "/file"
	eosLogger.Debug(ctx, "CompleteMultipartUpload: xrdcp: [stagepath: %s, uploadIDpath: %s, size: %d]", fullstagepath, uploadID+".minio.sys", objInfo.Size)

	f, err := os.Open(fullstagepath)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: Failed to open chunk [uploadID: %s]", uploadID)
	}
	defer f.Close()

	err = e.FileSystem.Xrdcp.Put(ctx, fullstagepath, uploadID+".minio.sys", objInfo.Size)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: xrdcp: [uploadID: %s]", uploadID)
		return err
	}
	err = e.FileSystem.Rename(ctx, uploadID+".minio.sys", uploadID)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: EOSrename: [uploadID: %s]", uploadID)
		return err
	}
	err = e.FileSystem.SetETag(ctx, uploadID, objInfo.ETag)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: EOSsetETag: [uploadID: %s]", uploadID)
		return err
	}
	err = e.FileSystem.SetContentType(ctx, uploadID, objInfo.ContentType)
	if err != nil {
		eosLogger.Error(ctx, err, "CompleteMultipartUpload: EOSsetContentType: [uploadID: %s]", uploadID)
		return err
	}

	err = os.RemoveAll(e.stage + "/" + stagepath)
	if err != nil {
		return err
	}
	return nil
}

//AbortMultipartUpload
func (e *eosObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	eosLogger.Stat(ctx, "S3cmd: AbortMultipartUpload: [bucket: %s, object: %s, uploadID: %s]", bucket, object, uploadID)

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
	eosLogger.Stat(ctx, "S3cmd: ListObjectParts: [uploadID: %s, part: %d, maxParts: %d]", uploadID, partNumberMarker, maxParts)

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker

	transfer := e.TransferList.GetTransfer(uploadID)
	if transfer != nil {
		size := e.TransferList.GetPartsCount(uploadID)
		transfer.RLock()
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
	}

	return result, nil
}

// Return an initialized minio.ObjectInfo
func (e *eosObjects) NewObjectInfo(bucket string, name string, stat *FileStat) minio.ObjectInfo {
	return minio.ObjectInfo{
		Bucket:      bucket,
		Name:        name,
		ModTime:     stat.ModTime,
		Size:        stat.Size,
		IsDir:       stat.IsDir(),
		ETag:        stat.GetETag(),
		AccTime:     stat.ModTime,
		ContentType: stat.ContentType,
	}
}

// ListObjectsToMarker - binary encoder for when you need more than a string
func (e *eosObjects) ListObjectsToMarker(ctx context.Context, data ListObjectsMarker) (string, error) {
	// Make data a []byte
	b := bytes.Buffer{}
	en := gob.NewEncoder(&b)
	err := en.Encode(data)
	if err != nil {
		eosLogger.Error(ctx, err, "ListObjectsToMarker: failed gob Encode")
		return "", err
	}

	// Compress it
	var wb bytes.Buffer
	writer := lzw.NewWriter(&wb, lzw.LSB, 8)
	_, err = writer.Write(b.Bytes())
	if err != nil {
		eosLogger.Error(ctx, err, "ListObjectsToMarker: failed lzw write")
		return "", err
	}
	writer.Close()

	// base64 it
	return base64.RawStdEncoding.EncodeToString(wb.Bytes()), nil
}

// MarkerToListObjects - binary decoder for when you need more than a string
func (e *eosObjects) MarkerToListObjects(ctx context.Context, str string) (ListObjectsMarker, error) {
	data := ListObjectsMarker{}

	// Un-base64 it
	by, err := base64.RawStdEncoding.DecodeString(str)
	if err != nil {
		eosLogger.Error(ctx, err, "MarkerToListObjects: failed base64 Decode")
		return ListObjectsMarker{}, err
	}

	// Decompress it
	var rb bytes.Buffer
	r := lzw.NewReader(bytes.NewReader(by), lzw.LSB, 8)
	defer r.Close()
	rb.Reset()
	_, err = io.Copy(&rb, r)
	if err != nil {
		eosLogger.Error(ctx, err, "MarkerToListObjects: failed lzw read")
		return ListObjectsMarker{}, err
	}

	// Make []byte to data
	b := bytes.Buffer{}
	b.Write(rb.Bytes())
	d := gob.NewDecoder(&b)
	err = d.Decode(&data)
	if err != nil {
		eosLogger.Error(ctx, err, "MarkerToListObjects: failed gob Decode")
		return ListObjectsMarker{}, err
	}
	return data, nil
}

// ListObjects - lists all blobs in a container filtered by prefix and marker
func (e *eosObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	eosLogger.Stat(ctx, "S3cmd: ListObjects: [bucket: %s, prefix: %s, marker: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, marker, delimiter, maxKeys)

	result, err = e.ListObjectsPaging(ctx, bucket, prefix, marker, delimiter, maxKeys)

	eosLogger.Debug(ctx, "ListObjects: Result: %+v", result)
	return result, err
}

// ListObjectsPaging - Non recursive lists all blobs in a container filtered by prefix and marker
func (e *eosObjects) ListObjectsPaging(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	isRecursive := (len(delimiter) == 0)
	if maxKeys < 1 {
		maxKeys = 1
	}
	if maxKeys == 1000 { //1000 is the minio default
		maxKeys = e.maxKeys
	}

	// Don't do anything if delimiter and prefix are both slashes
	if delimiter == "/" && prefix == "/" {
		eosLogger.Debug(ctx, "ListObjectsPaging: Delimiter and prefix are both slash, returning blank result.")
		return result, nil
	}

	prefixes := []string{}
	skip := 0
	if marker != "" {
		decodedMarker, err := e.MarkerToListObjects(ctx, marker)
		if err != nil {
			return result, err
		}
		prefixes = decodedMarker.Prefixes
		skip = decodedMarker.Skip
	} else {
		prefixes = append(prefixes, prefix)
	}

	objCounter := 0
	for len(prefixes) > 0 {
		prefix, prefixes = prefixes[0], prefixes[1:]

		path := PathJoin(bucket, prefix)

		// If the prefix is empty, set it to slash so we can list a directory.
		if prefix == "" {
			path = path + "/"
		}

		// Are we dealing with a single file or directory?
		isdir, err := e.FileSystem.IsDir(ctx, path)
		if err != nil {
			return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
		}
		if !isdir {
			//dealing with just one file
			eosLogger.Debug(ctx, "ListObjectsPaging: MODE: one file [path: %s]", path)
			stat, err := e.FileSystem.Stat(ctx, path)
			if stat != nil {
				o := e.NewObjectInfo(bucket, prefix, stat)
				result.Objects = append(result.Objects, o)
			}
			return result, err
		}

		// At this point we are dealing with a directory

		// We only want to list the directory and not it's contents if it doesn't end with /
		if prefix != "" && !isRecursive && !strings.HasSuffix(prefix, "/") {
			eosLogger.Debug(ctx, "ListObjectsPaging: MODE: one directory only [path: %s]", path)
			stat, err := e.FileSystem.DirStat(ctx, path)
			if err != nil {
				eosLogger.Error(ctx, err, "ListObjectsPaging: Unable to stat directory [path: %s]", path)
				return result, err
			}
			if stat != nil {
				result.Prefixes = append(result.Prefixes, prefix+"/")
				return result, err
			}
		}

		// Otherwise we need to do some other stuff
		eosLogger.Debug(ctx, "ListObjectsPaging: MODE: full list [prefix: %s, isRecursive: %t]", prefix, isRecursive)

		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}

		eosLogger.Debug(ctx, "ListObjectsPaging: Consider [path: %s, prefix: %s, prefixes: %s]", path, prefix, prefixes)

		var objects []*FileStat

		objects, err = e.FileSystem.BuildCache(ctx, path, true)
		defer e.FileSystem.DeleteCache(ctx)
		if err != nil {
			// We want to return nil error if the file is not found. So we don't break restic and others that expect a certain response
			if err == errFileNotFound {
				eosLogger.Debug(ctx, "ListObjectsPaging: File not found [path: %s]", path)
				return result, nil
			}
			return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
		}

		objprefixCounter := 0
		for _, obj := range objects {
			objprefixCounter++
			if skip > 0 {
				skip--
				continue
			}

			//have we done enough?
			eosLogger.Debug(ctx, "ListObjectsPaging: obj.FullPath:%s, objCounter:%d, objprefixCounter:%d", obj.FullPath, objCounter, objprefixCounter)
			if objCounter > maxKeys {
				//add current prefix to the list of prefixes
				prefixes = append([]string{prefix}, prefixes...)
				data := ListObjectsMarker{Prefixes: prefixes, Skip: objprefixCounter - 1}
				result.IsTruncated = true
				result.NextMarker, err = e.ListObjectsToMarker(ctx, data)
				eosLogger.Debug(ctx, "ListObjectsPaging: NextMarker data:%+v", data)
				//eosLogger.Debug(ctx, "ListObjectsPaging: NextMarker encoded:%s", result.NextMarker)
				return result, err
			}

			objCounter++
			objpath := PathJoin(path, obj.Name)

			// We need to call DirStat() so we don't recurse directories when we don't have to
			var stat *FileStat
			if obj.File {
				eosLogger.Debug(ctx, "ListObjectsPaging: e.FileSystem.Stat(ctx, %s)", objpath)
				stat, err = e.FileSystem.Stat(ctx, objpath)
			} else {
				eosLogger.Debug(ctx, "ListObjectsPaging: e.FileSystem.DirStat(ctx, %s/)", objpath)
				stat, err = e.FileSystem.DirStat(ctx, objpath+"/")
			}

			if stat != nil {
				objName := strings.TrimPrefix(obj.FullPath, PathJoin(e.path, bucket)+"/")

				eosLogger.Debug(ctx, "ListObjectsPaging: found object: %s [bucket: %s, prefix: %s, objCounter: %d]", objName, bucket, prefix, objCounter)
				// Directories get added to prefixes, files to objects.
				if !obj.File {
					if objName != prefix {
						eosLogger.Debug(ctx, "ListObjectsPaging: NOTPREFIX : %s vs %s", prefix, objName)
						result.Prefixes = append(result.Prefixes, objName)
						if isRecursive {
							// We need to get information from subdirectories too
							nextPrefix := PathJoin(prefix, obj.Name)
							eosLogger.Debug(ctx, "ListObjectsPaging: Adding to prefixes: %s current prefix: %s prefixes: %s objName: %s", nextPrefix, prefix, prefixes, objName)
							prefixes = append(prefixes, nextPrefix)
						}
					} else {
						eosLogger.Debug(ctx, "ListObjectsPaging: Skipping prefix: %s", objName)
					}
				} else {
					if objName != "." {
						eosLogger.Debug(ctx, "ListObjectsPaging: Stat: NewObjectInfo(%s, %s)", bucket, objName)
						o := e.NewObjectInfo(bucket, objName, stat)
						result.Objects = append(result.Objects, o)
					}
				}
			} else {
				eosLogger.Error(ctx, err, "ListObjectsPaging: unable to stat [objpath: %s]", objpath)
			}
		}
		skip = 0
	}
	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	eosLogger.Stat(ctx, "S3cmd: ListObjectsV2: [bucket: %s, prefix: %s, continuationToken: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, continuationToken, delimiter, maxKeys)

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
	eosLogger.Stat(ctx, "S3cmd: HealFormat:")
	return madmin.HealResultItem{}, minio.NotImplemented{}
}

// ReloadFormat - no-op for fs
func (e *eosObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	eosLogger.Stat(ctx, "S3cmd: ReloadFormat:")
	return minio.NotImplemented{}
}

// ListObjectsHeal - list all objects to be healed.
func (e *eosObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	eosLogger.Stat(ctx, "S3cmd: ListObjectsHeal:")
	return loi, minio.NotImplemented{}
}

// HealObject - no-op for fs.
func (e *eosObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool, remove bool, scanMode madmin.HealScanMode) (results madmin.HealResultItem, err error) {
	eosLogger.Stat(ctx, "S3cmd: HealObject:")
	return results, minio.NotImplemented{}
}

// HealObjects - no-op for fs.
func (e *eosObjects) HealObjects(ctx context.Context, bucket, prefix string, fn func(string, string) error) (err error) {
	eosLogger.Stat(ctx, "S3cmd: HealObjects:")
	return minio.NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed
func (e *eosObjects) ListBucketsHeal(ctx context.Context) ([]minio.BucketInfo, error) {
	eosLogger.Stat(ctx, "S3cmd: ListBucketsHeal:")
	return []minio.BucketInfo{}, minio.NotImplemented{}
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (e *eosObjects) HealBucket(ctx context.Context, bucket string, dryRun bool, remove bool) (results madmin.HealResultItem, err error) {
	eosLogger.Stat(ctx, "S3cmd: HealBucket:")
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
