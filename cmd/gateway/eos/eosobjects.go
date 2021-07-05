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
	"bytes"
	"compress/lzw"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
)

// eosObjects implements gateway for Minio and S3 compatible object storage servers.
type eosObjects struct {
	minio.GatewayUnsupported
	maxRetry     int
	maxKeys      int
	path         string
	hookurl      string
	stage        string
	readonly     bool
	validbuckets bool
	TransferList *TransferList
	FileSystem   *EOSFS
}

// ListObjectsMarker is the marker used to continue listing objects when maxKeys is hit.
type ListObjectsMarker struct {
	Prefixes []string
	Skip     int
}

// IsEncryptionSupported returns whether server side encryption is applicable for this layer.
func (e *eosObjects) IsEncryptionSupported() bool {
	// note(mdu): don't think should be true unless we start using SSL between traefik + minio
	// 	      I'd remove this method but wanted to leave a note :)
	return false
}

// Shutdown - nothing to do really...
func (e *eosObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo
func (e *eosObjects) StorageInfo(ctx context.Context, _ bool) (si minio.StorageInfo, _ []error) {
	si.Backend.Type = minio.BackendGateway
	// See if we can determine if the gateway data directory is a directory to say things are up
	online, _ := e.FileSystem.IsDir(ctx, "/")
	si.Backend.GatewayOnline = online
	return si, nil
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Bucket

// GetBucketInfo - Get bucket metadata
func (e *eosObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	EOSLogger.Stat(ctx, "S3cmd: GetBucketInfo [bucket: %s]", bucket)

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
	EOSLogger.Stat(ctx, "S3cmd: ListBuckets")

	dirs, err := e.FileSystem.BuildCache(ctx, "", false)
	defer e.FileSystem.DeleteCache(ctx)

	if err != nil {
		return buckets, err
	}

	for _, dir := range dirs {
		if dir.FullPath == e.path+"/" {
			continue
		}

		EOSLogger.Debug(ctx, "Consider Bucket: %s (%s)", dir.Name, dir.FullPath)
		stat, err := e.FileSystem.DirStat(ctx, dir.Name)

		if stat == nil {
			EOSLogger.Error(ctx, err, "ListBuckets: unable to stat [dir: %s]", dir.Name)
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
				EOSLogger.Debug(ctx, "Bucket: %s not a directory", dir.Name)
			}
			if !e.IsValidBucketName(strings.TrimRight(dir.Name, "/")) {
				EOSLogger.Debug(ctx, "Bucket: %s not a valid name", dir.Name)
			}
		}
	}

	EOSLogger.Debug(ctx, "Buckets found: %+v", buckets)
	return buckets, err
}

// MakeBucketWithLocation - Create a new container.
func (e *eosObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	EOSLogger.Stat(ctx, "S3cmd: MakeBucketWithLocation [bucket: %s]", bucket)

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
func (e *eosObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	EOSLogger.Stat(ctx, "S3cmd: DeleteBucket [bucket: %s]", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	err := e.FileSystem.Rm(ctx, bucket)
	if err != nil {
		EOSLogger.Error(ctx, err, "DeleteBucket: %+v", err)
		return minio.BucketNotFound{Bucket: bucket}
	}
	return err
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
	EOSLogger.Stat(ctx, "S3cmd: CopyObject [from: %s, to: %s, srcInfo: %+v]", srcpath, destpath, srcInfo)

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
			EOSLogger.Error(ctx, err, "CopyObject: %+v", err)
			return objInfo, err
		}

		err = e.FileSystem.SetETag(ctx, destpath, srcInfo.ETag)
		if err != nil {
			EOSLogger.Error(ctx, err, "CopyObject: %+v", err)
			return objInfo, err
		}

		err = e.FileSystem.SetContentType(ctx, destpath, srcInfo.ContentType)
		if err != nil {
			EOSLogger.Error(ctx, err, "CopyObject: %+v", err)
			return objInfo, err
		}
	} else {
		EOSLogger.Debug(ctx, "CopyObject srcpath==destpath")
	}

	return e.GetObjectInfoWithRetry(ctx, destBucket, destObject, dstOpts)
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	EOSLogger.Stat(ctx, "S3cmd: PutObject: [bucket: %s, object: %s]", bucket, object)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	// This just prints out user defined headers on the object, we're not doing anything with it
	//for key, val := range opts.UserDefined {
	//	EOSLogger.Debug(ctx, "PutObject [path: %s%s, key: %s, value: %s]", bucket, object, key, val)
	//}

	dir := bucket + "/" + filepath.Dir(object)
	objectpath := bucket + "/" + object

	// Create the parent directory if it doesn't exist
	if exists, _ := e.FileSystem.FileExists(ctx, dir); !exists {
		e.FileSystem.mkdirWithOption(ctx, dir, "&mgm.option=p")
	}

	// If actual file being uploaded
	if !strings.HasSuffix(object, "/") {
		// Send the file
		buf := bufio.NewReader(data)
		response, err := e.FileSystem.PutBuffer(ctx, e.stage, objectpath, buf)
		if err != nil {
			EOSLogger.Error(ctx, err, "PUT: %+v", err)
			objInfo.ETag = defaultETag
			if strings.Contains(err.Error(), "attempts") {
				return objInfo, minio.SlowDown{}
			}
			return objInfo, minio.OperationTimedOut{}
		}
		EOSLogger.Debug(ctx, "Put response: %#v", response)

		err = e.FileSystem.SetETag(ctx, objectpath, response.Checksum)
		if err != nil {
			EOSLogger.Error(ctx, err, "PUT.SetETag: %+v", err)
			objInfo.ETag = defaultETag
			return objInfo, minio.InvalidETag{}
		}

		err = e.FileSystem.SetContentType(ctx, objectpath, opts.UserDefined["content-type"])
		if err != nil {
			EOSLogger.Error(ctx, err, "PUT.SetContentType: %+v", err)
			return objInfo, err
		}

		sourceChecksum := hex.EncodeToString(data.MD5Current())
		err = e.FileSystem.SetSourceChecksum(ctx, objectpath, sourceChecksum)
		if err != nil {
			EOSLogger.Error(ctx, err, "PUT.SetSourceChecksum: %+v", err)
			return objInfo, err
		}

		sourceSize := Int64ToString(data.Size())
		err = e.FileSystem.SetSourceSize(ctx, objectpath, sourceSize)
		if err != nil {
			EOSLogger.Error(ctx, err, "PUT.SetSourceSize: %+v", err)
			return objInfo, err
		}

		objInfo, err = e.GetObjectInfoWithRetry(ctx, bucket, object, opts)
		if err == nil && objInfo.Size != data.Size() {
			EOSLogger.Error(ctx, err, "PUT: File on disk is not the correct size [disk: %d, expected: %d]", objInfo.Size, data.Size())
			// Remove the file
			_ = e.FileSystem.Rm(ctx, objectpath)
			return objInfo, minio.IncompleteBody{Bucket: bucket, Object: object}
		}
	} else {
		objInfo, err = e.GetObjectInfoWithRetry(ctx, bucket, object, opts)
	}

	return objInfo, err
}

// DeleteObject - Deletes a blob on EOS
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	EOSLogger.Stat(ctx, "S3cmd: DeleteObject: [bucket: %s object: %s]", bucket, object)

	if e.readonly {
		return minio.ObjectInfo{}, minio.NotImplemented{}
	}

	_ = e.FileSystem.Rm(ctx, PathJoin(bucket, object))
	return minio.ObjectInfo{Bucket: bucket, Name: object}, nil
}

// DeleteObjects - Deletes multiple blobs on EOS
func (e *eosObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	EOSLogger.Stat(ctx, "S3cmd: DeleteObjects: [bucket: %s]", bucket)

	errs := make([]error, len(objects))
	deleted := make(map[string]bool)
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		if _, ok := deleted[object.ObjectName]; !ok {
			_, errs[idx] = e.DeleteObject(ctx, bucket, object.ObjectName, opts)
			deleted[object.ObjectName] = true
			dobjects[idx] = minio.DeletedObject{ObjectName: object.ObjectName}
		}
	}

	return dobjects, errs
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)
	EOSLogger.Stat(ctx, "S3cmd: GetObject: [bucket: %s, object: %s, path: %s, startOffset: %d, length: %d]", bucket, object, path, startOffset, length)

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
		EOSLogger.Error(ctx, err, "Unable to retrieve object info. [bucket: %s, object: %s]", bucket, object)
		objInfo = minio.ObjectInfo{}
	}
	return objInfo, err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo
func (e *eosObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	path := strings.Replace(PathJoin(bucket, object), "//", "/", -1)
	EOSLogger.Stat(ctx, "S3cmd: GetObjectInfo: [bucket: %s, object: %s, path: %s]", bucket, object, path)

	// If there is a transfer in progress, wait for it to finish.
	for {
		transfer := e.TransferList.GetTransfer(path)
		if transfer == nil {
			break
		}
		EOSLogger.Debug(ctx, "Waiting for upload to complete [uploadID: %s]", path)
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
		EOSLogger.Debug(ctx, "GetObjectInfo: [error: %+v]", err)
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

	EOSLogger.Debug(ctx, "GetObjectInfo: [path: %s, object: %s, isdir: %t, etag: %s, content-type: %s, err: %s]", path, object, stat.IsDir(), stat.GetETag(), stat.ContentType, err)
	return objInfo, err
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (e *eosObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	EOSLogger.Stat(ctx, "S3cmd: GetObjectNInfo: [bucket: %s, object: %s]", bucket, object)
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
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)
}

// ListMultipartUploads - lists all multipart uploads.
func (e *eosObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	EOSLogger.Stat(ctx, "S3cmd: ListMultipartUploads: [bucket: %s, prefix: %s, keyMarket: %s, uploadIDMarker: %s, delimiter: %s, maxUploads: %d]", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, nil
}

// GetMultipartInfo returns multipart info of the uploadId of the object
func (e *eosObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	EOSLogger.Stat(ctx, "S3cmd: GetMultipartInfo: [bucket: %s, object: %s, uploadID: %s]", bucket, object, uploadID)
	if e.TransferList.GetTransfer(uploadID) == nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	return result, nil
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	EOSLogger.Stat(ctx, "S3cmd: NewMultipartUpload: [bucket: %s, object: %s, options:  +%v]", bucket, object, opts)

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
			EOSLogger.Error(ctx, err, "mkdir failed [path: %s]", absstagepath)
		}
		mp.stagepath = stagepath
	}

	e.TransferList.AddTransfer(uploadID, &mp)
	EOSLogger.Info(ctx, "NewMultipartUpload: [uploadID: %s]", uploadID)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	EOSLogger.Stat(ctx, "S3cmd: PutObjectPart: [bucket: %s, object: %s, uploadID: %s, partId: %d, size: %d]", bucket, object, uploadID, partID, r.Size())

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
			EOSLogger.Debug(ctx, "PutObjectPart: waiting for first chunk [bucket: %s, object: %s, processing_part: %d, etag: %s]", bucket, object, partID, info.ETag)
			Sleep()
		}
	}

	e.TransferList.IncrementPartsCount(uploadID)
	e.TransferList.AddToSize(uploadID, info.Size)

	offset := chunksize * int64(partID-1)
	EOSLogger.Debug(ctx, "PutObjectPart: staging transfer [bucket: %s, object: %s, partID: %d, offset: %d, etag: %s]", bucket, object, (partID - 1), offset, info.ETag)
	stagepath := e.TransferList.GetStagePath(uploadID)
	absstagepath := e.stage + "/" + stagepath

	if _, err := os.Stat(absstagepath); os.IsNotExist(err) {
		_ = os.MkdirAll(absstagepath, 0700)
	}
	f, err := os.OpenFile(absstagepath+"/file", os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		EOSLogger.Error(ctx, err, "PutObjectPart: Unable to open stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	_, err = f.Seek(offset, 0)
	if err != nil {
		EOSLogger.Error(ctx, err, "PutObjectPart: Unable to seek to correct position in stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	bytesWritten, err := f.Write(buf)
	if err != nil {
		EOSLogger.Error(ctx, err, "PutObjectPart: Unable to copy buffer into stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	f.Close()

	//force free buf
	buf = nil //nolint:ineffassign

	transfer := e.TransferList.GetTransfer(uploadID)
	for {
		if transfer == nil {
			EOSLogger.Error(ctx, nil, "PutObjectPart: Invalid transfer [uploadID: %s]", uploadID)
			break
		} else {
			transfer.RLock()
			md5PartID := transfer.md5PartID
			transfer.RUnlock()
			if md5PartID == partID {
				break
			}
			EOSLogger.Debug(ctx, "PutObjectPart: waiting for part [uploadID: %s, md5PartID: %d, currentPart: %d]", uploadID, md5PartID, partID)
		}
		Sleep()
	}

	// Open the file so we can read the same bytes into the md5 buffer
	f, err = os.Open(absstagepath + "/file")
	if err != nil {
		EOSLogger.Error(ctx, err, "PutObjectPart: Unable to open stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	_, err = f.Seek(offset, 0)
	if err != nil {
		EOSLogger.Error(ctx, err, "PutObjectPart: Unable to seek to correct position in stage file [stagepath: %s]", absstagepath)
		return info, err
	}
	transfer.Lock()
	_, err = io.CopyN(transfer.md5, f, int64(bytesWritten))
	transfer.Unlock()
	if err != nil && err != io.EOF {
		EOSLogger.Error(ctx, err, "PutObjectPart: Unable to copy buffer for hashing [stagepath: %s]", absstagepath)
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
			EOSLogger.Error(ctx, err, "PutObjectPart received 0 bytes in the buffer. [bucket: %s, object: %s, uploadID: %d]", bucket, object, partID)
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
			EOSLogger.Debug(ctx, "PutObjectPart: waiting for first chunk [bucket: %s, object: %s, processing_part: %d]", bucket, object, partID)
			Sleep()
		}
	}

	e.TransferList.IncrementPartsCount(uploadID)
	e.TransferList.AddToSize(uploadID, info.Size)
	chunksize := e.TransferList.GetChunkSize(uploadID)

	offset := chunksize * int64(partID-1)
	EOSLogger.Debug(ctx, "PutObjectPart offset [bucket: %s, object: %s, partID: %d, offset: %d, etag: %s]", bucket, object, (partID - 1), offset, info.ETag)

	go func() {
		err = e.FileSystem.xrootdWriteChunk(ctx, uploadID, offset, offset+info.Size, "0", buf)
	}()

	transfer := e.TransferList.GetTransfer(uploadID)
	for {
		if transfer == nil {
			EOSLogger.Error(ctx, nil, "PutObjectPart: Invalid transfer [uploadID: %s]", uploadID)
			break
		} else {
			md5PartID := e.TransferList.GetMD5PartID(uploadID)
			if md5PartID == partID {
				break
			}
			EOSLogger.Debug(ctx, "PutObjectPart: waiting for part [uploadID: %s, md5PartID: %d, currentPart: %d]", uploadID, md5PartID, partID)
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
	EOSLogger.Stat(ctx, "S3cmd: CompleteMultipartUpload: [uploadID: %s, size: %d]", uploadID, size)

	for {
		transfer := e.TransferList.GetTransfer(uploadID)
		if transfer != nil {
			transfer.RLock()
			if transfer.md5PartID == transfer.partsCount+1 {
				break
			}
			EOSLogger.Debug(ctx, "CompleteMultipartUpload: waiting for all md5Parts [uploadID: %s, total_parts: %d, remaining: %d]", uploadID, transfer.partsCount, transfer.partsCount+1-transfer.md5PartID)
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

	EOSLogger.Debug(ctx, "CompleteMultipartUpload: [uploadID: %s, etag: %s]", uploadID, etag)

	objInfo = minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     time.Now(),
		Size:        size,
		IsDir:       false,
		ETag:        etag,
		ContentType: contenttype,
	}

	err = e.TransferFromStaging(ctx, stagepath, uploadID, objInfo)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		err = e.FileSystem.Rm(ctx, uploadID)
		if err != nil {
			EOSLogger.Error(ctx, err, "CompleteMultipartUpload: cleanup rm failed [uploadID: %s]", uploadID)
		}
	}

	// Successful upload
	e.TransferList.DeleteTransfer(uploadID)
	return e.GetObjectInfoWithRetry(ctx, bucket, object, opts)
}

// CompleteMultipartUploadXrootd
func (e *eosObjects) CompleteMultipartUploadXrootd(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	reqInfo := logger.GetReqInfo(ctx)
	transfer := e.TransferList.GetTransfer(uploadID)
	transfer.RLock()
	size := transfer.size
	firstByte := transfer.firstByte
	transfer.RUnlock()
	EOSLogger.Stat(ctx, "S3cmd: CompleteMultipartUpload: [uploadID: %s, size: %d, firstByte: %d, useragent: %s]", uploadID, size, firstByte, reqInfo.UserAgent)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for {
		transfer.RLock()
		if transfer.md5PartID == transfer.partsCount+1 {
			break
		}
		EOSLogger.Debug(ctx, "CompleteMultipartUpload: waiting for all md5Parts [uploadID: %s, total_parts: %d, remaining: %d]", uploadID, transfer.partsCount, transfer.partsCount+1-transfer.md5PartID)
		transfer.RUnlock()
		Sleep()
	}

	etag := transfer.GetETag()
	contenttype := transfer.GetContentType()

	EOSLogger.Debug(ctx, "CompleteMultipartUpload: [uploadID: %s, etag: %s]", uploadID, etag)

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
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: EOSwriteChunk: [uploadID: %s]", uploadID)
		objInfo.ETag = zerobyteETag
		return objInfo, minio.IncompleteBody{Bucket: bucket, Object: object}
	}

	e.TransferList.DeleteTransfer(uploadID)

	err = e.FileSystem.SetETag(ctx, uploadID, etag)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		objInfo.ETag = zerobyteETag
		return objInfo, minio.InvalidETag{}
	}
	err = e.FileSystem.SetContentType(ctx, uploadID, contenttype)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		return objInfo, err
	}

	return objInfo, nil
}

// Transfer the upload from the staging area to it's final location
func (e *eosObjects) TransferFromStaging(ctx context.Context, stagepath string, uploadID string, objInfo minio.ObjectInfo) error {
	fullstagepath := e.stage + "/" + stagepath + "/file"
	EOSLogger.Debug(ctx, "CompleteMultipartUpload: xrdcp: [stagepath: %s, uploadIDpath: %s, size: %d]", fullstagepath, uploadID+".minio.sys", objInfo.Size)

	// Check for file existence and permission to read
	stat, err := os.Stat(fullstagepath)
	if os.IsNotExist(err) {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: Chunk file does not exist [uploadID: %s]", uploadID)
	}
	if os.IsPermission(err) {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: Permission denied for chunk file [uploadID: %s]", uploadID)
	}
	if err != nil {
		return err
	}
	if stat.Size() != objInfo.Size {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: [uploadID: %s]", uploadID)
		return errors.New("File size of staged file does not match expected file size")
	}

	response, err := e.FileSystem.Xrdcp.Put(ctx, fullstagepath, uploadID+".minio.sys", objInfo.Size)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: xrdcp: [uploadID: %s]", uploadID)
		return err
	}

	err = e.FileSystem.Rename(ctx, uploadID+".minio.sys", uploadID)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: Rename: [uploadID: %s]", uploadID)
		return err
	}

	err = e.FileSystem.SetSourceChecksum(ctx, uploadID, objInfo.ETag)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: SetSourceChecksum: [uploadID: %s]", uploadID)
		return err
	}

	sourceSize := Int64ToString(objInfo.Size)
	err = e.FileSystem.SetSourceSize(ctx, uploadID, sourceSize)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: SetSourceSize: [uploadID: %s]", uploadID)
		return err
	}

	err = e.FileSystem.SetETag(ctx, uploadID, response.Checksum)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: SetETag: [uploadID: %s]", uploadID)
		return err
	}

	err = e.FileSystem.SetContentType(ctx, uploadID, objInfo.ContentType)
	if err != nil {
		EOSLogger.Error(ctx, err, "CompleteMultipartUpload: SetContentType: [uploadID: %s]", uploadID)
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
	EOSLogger.Stat(ctx, "S3cmd: AbortMultipartUpload: [bucket: %s, object: %s, uploadID: %s]", bucket, object, uploadID)

	if e.readonly {
		return minio.NotImplemented{}
	}

	if e.stage != "" && e.TransferList.TransferExists(uploadID) {
		transfer := e.TransferList.GetTransfer(uploadID)
		stagepath := transfer.GetStagePath()
		os.RemoveAll(e.stage + "/" + stagepath)
	}

	e.FileSystem.Rm(ctx, bucket+"/"+object)
	e.TransferList.DeleteTransfer(uploadID)

	return nil
}

// ListObjectParts
func (e *eosObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, options minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	EOSLogger.Stat(ctx, "S3cmd: ListObjectParts: [uploadID: %s, part: %d, maxParts: %d]", uploadID, partNumberMarker, maxParts)

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
		EOSLogger.Error(ctx, err, "ListObjectsToMarker: failed gob Encode")
		return "", err
	}

	// Compress it
	var wb bytes.Buffer
	writer := lzw.NewWriter(&wb, lzw.LSB, 8)
	_, err = writer.Write(b.Bytes())
	if err != nil {
		EOSLogger.Error(ctx, err, "ListObjectsToMarker: failed lzw write")
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
		EOSLogger.Error(ctx, err, "MarkerToListObjects: failed base64 Decode")
		return ListObjectsMarker{}, err
	}

	// Decompress it
	var rb bytes.Buffer
	r := lzw.NewReader(bytes.NewReader(by), lzw.LSB, 8)
	defer r.Close()
	_, err = io.Copy(&rb, r)
	if err != nil {
		EOSLogger.Error(ctx, err, "MarkerToListObjects: failed lzw read")
		return ListObjectsMarker{}, err
	}

	// Make []byte to data
	d := gob.NewDecoder(&rb)
	err = d.Decode(&data)
	if err != nil {
		EOSLogger.Error(ctx, err, "MarkerToListObjects: failed gob Decode")
		return ListObjectsMarker{}, err
	}
	return data, nil
}

// ListObjects - lists all blobs in a container filtered by prefix and marker
func (e *eosObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	EOSLogger.Stat(ctx, "S3cmd: ListObjects: [bucket: %s, prefix: %s, marker: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, marker, delimiter, maxKeys)

	result, err = e.ListObjectsPaging(ctx, bucket, prefix, marker, delimiter, maxKeys)

	EOSLogger.Debug(ctx, "ListObjects: Result: %+v", result)
	return result, err
}

// ListObjectsPaging - Non recursive lists all blobs in a container filtered by prefix and marker
func (e *eosObjects) ListObjectsPaging(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	isRecursive := (len(delimiter) == 0)
	if maxKeys < 1 {
		maxKeys = 1
	}
	if e.maxKeys > 0 { //overwrite minio's default as well as client requests, usually client/minio will ask for 1000
		maxKeys = e.maxKeys
	}

	// Don't do anything if delimiter and prefix are both slashes
	if delimiter == "/" && prefix == "/" {
		EOSLogger.Debug(ctx, "ListObjectsPaging: Delimiter and prefix are both slash, returning blank result.")
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
			EOSLogger.Debug(ctx, "ListObjectsPaging: MODE: one file [path: %s]", path)
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
			EOSLogger.Debug(ctx, "ListObjectsPaging: MODE: one directory only [path: %s]", path)
			stat, err := e.FileSystem.DirStat(ctx, path)
			if err != nil {
				EOSLogger.Error(ctx, err, "ListObjectsPaging: Unable to stat directory [path: %s]", path)
				return result, err
			}
			if stat != nil {
				result.Prefixes = append(result.Prefixes, prefix+"/")
				return result, err
			}
		}

		// Otherwise we need to do some other stuff
		EOSLogger.Debug(ctx, "ListObjectsPaging: MODE: full list [prefix: %s, isRecursive: %t]", prefix, isRecursive)

		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}

		EOSLogger.Debug(ctx, "ListObjectsPaging: Consider [path: %s, prefix: %s, prefixes: %s]", path, prefix, prefixes)

		var objects []*FileStat

		objects, err = e.FileSystem.BuildCache(ctx, path, true)
		defer e.FileSystem.DeleteCache(ctx)
		if err != nil {
			// We want to return nil error if the file is not found. So we don't break restic and others that expect a certain response
			if err == errFileNotFound {
				EOSLogger.Debug(ctx, "ListObjectsPaging: File not found [path: %s]", path)
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
			EOSLogger.Debug(ctx, "ListObjectsPaging: obj.FullPath:%s, objCounter:%d, objprefixCounter:%d", obj.FullPath, objCounter, objprefixCounter)
			if objCounter > maxKeys {
				//add current prefix to the list of prefixes
				prefixes = append([]string{prefix}, prefixes...)
				data := ListObjectsMarker{Prefixes: prefixes, Skip: objprefixCounter - 1}
				result.IsTruncated = true
				result.NextMarker, err = e.ListObjectsToMarker(ctx, data)
				EOSLogger.Debug(ctx, "ListObjectsPaging: NextMarker data:%+v", data)
				//EOSLogger.Debug(ctx, "ListObjectsPaging: NextMarker encoded:%s", result.NextMarker)
				return result, err
			}

			objCounter++

			// We need to call DirStat() so we don't recurse directories when we don't have to
			var stat *FileStat
			objpath := strings.TrimPrefix(obj.FullPath, e.path)
			if obj.File {
				EOSLogger.Debug(ctx, "ListObjectsPaging: e.FileSystem.Stat(ctx, %s)", objpath)
				stat, err = e.FileSystem.Stat(ctx, objpath)
			} else {
				EOSLogger.Debug(ctx, "ListObjectsPaging: e.FileSystem.DirStat(ctx, %s/)", objpath)
				stat, err = e.FileSystem.DirStat(ctx, objpath)
			}

			if stat != nil {
				objName := strings.TrimPrefix(obj.FullPath, PathJoin(e.path, bucket)+"/")

				EOSLogger.Debug(ctx, "ListObjectsPaging: found object: %s [bucket: %s, prefix: %s, objCounter: %d]", objName, bucket, prefix, objCounter)
				// Directories get added to prefixes, files to objects.
				if !obj.File {
					if objName != prefix {
						EOSLogger.Debug(ctx, "ListObjectsPaging: NOTPREFIX : %s vs %s", prefix, objName)
						result.Prefixes = append(result.Prefixes, objName)
						if isRecursive {
							// We need to get information from subdirectories too
							nextPrefix := PathJoin(prefix, obj.Name)
							EOSLogger.Debug(ctx, "ListObjectsPaging: Adding to prefixes: %s current prefix: %s prefixes: %s objName: %s", nextPrefix, prefix, prefixes, objName)
							prefixes = append(prefixes, nextPrefix)
						}
					} else {
						EOSLogger.Debug(ctx, "ListObjectsPaging: Skipping prefix: %s", objName)
					}
				} else {
					if objName != "." {
						EOSLogger.Debug(ctx, "ListObjectsPaging: Stat: NewObjectInfo(%s, %s)", bucket, objName)
						o := e.NewObjectInfo(bucket, objName, stat)
						result.Objects = append(result.Objects, o)
					}
				}
			} else {
				EOSLogger.Error(ctx, err, "ListObjectsPaging: unable to stat [obj: %+v e.path: %s]", obj, e.path)
			}
		}
		skip = 0
	}
	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	EOSLogger.Stat(ctx, "S3cmd: ListObjectsV2: [bucket: %s, prefix: %s, continuationToken: %s, delimiter: %s, maxKeys: %d]", bucket, prefix, continuationToken, delimiter, maxKeys)

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
 * Helpers
 */

func (e *eosObjects) IsValidBucketName(name string) bool {
	if e.validbuckets {
		return minio.IsValidBucketName(name)
	}
	return true
}
