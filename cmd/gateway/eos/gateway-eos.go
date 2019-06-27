/*
 * 2019 AARNet Pty Ltd
 *
 * Michael Usher <michael.usher@aarnet.edu.au>
 * Michael D'Silva
 *
 * Gateway for the EOS storage backend  
 *
 */

package eos

import (
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
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
     EOSLOGLEVEL: 0..n 0=off, 1=errors only, 2=errors/info, 3+ = "debug"
     EOS: url to eos
     EOSUSER: eos username
     EOSUID: eos user uid
     EOSGID: eos user gid
     EOSSTAGE: local fast disk to stage multipart uploads
     EOSREADONLY: true/false
     EOSREADMETHOD: webdav/xrootd/xrdcp (DEFAULT: webdav)
     EOSSLEEP: int ms sleep 1000ms = 1s (default 100 ms)
     VOLUME_PATH: path on eos
     HOOKSURL: url to s3 hooks (not setting this will disable hooks)
     SCRIPTS: path to xroot script
     EOSVALIDBUCKETS: true/false (DEFAULT: true)

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

	webdavMax := 0
	ret, err = strconv.Atoi(os.Getenv("EOSMAXWEBDAV"))
	if err == nil {
		webdavMax = ret
	}

	xrdcpMax := 0
	ret, err = strconv.Atoi(os.Getenv("EOSMAXXRDCP"))
	if err == nil {
		xrdcpMax = ret
	}

	xrootdMax := 0
	ret, err = strconv.Atoi(os.Getenv("EOSMAXXROOTD"))
	if err == nil {
		xrootdMax = ret
	}

	waitSleep := 100
	ret, err = strconv.Atoi(os.Getenv("EOSSLEEP"))
	if err == nil {
		waitSleep = ret
	}

	validbuckets := true
	if os.Getenv("EOSVALIDBUCKETS") == "false" {
		validbuckets = false
	}

	logger.Info("EOS URL: %s", os.Getenv("EOS"))
	logger.Info("EOS VOLUME PATH: %s", os.Getenv("VOLUME_PATH"))
	logger.Info("EOS USER (uid:gid): %s (%s:%s)", os.Getenv("EOSUSER"), os.Getenv("EOSUID"), os.Getenv("EOSGID"))
	logger.Info("EOS file hooks url: %s", os.Getenv("HOOKSURL"))
	logger.Info("EOS SCRIPTS PATH: %s", os.Getenv("SCRIPTS"))

	if stage != "" {
		logger.Info("EOS staging: %s", stage)
	} else {
		logger.Info("EOS staging: DISABLED")
	}

	if readonly {
		logger.Info("EOS read only mode: ENABLED")
	}

	if !validbuckets {
		logger.Info("EOS allowing invalid bucket names (RISK)")
	}

	logger.Info("EOS READ METHOD: %s", readmethod)
	logger.Info("EOS /proc/user MAX: %d", procuserMax)
	logger.Info("EOS WebDav MAX: %d", webdavMax)
	logger.Info("EOS xrdcp MAX: %d", xrdcpMax)
	logger.Info("EOS xrootd MAX: %d", xrootdMax)
	logger.Info("EOS SLEEP: %d", waitSleep)

	logger.Info("EOS LOG LEVEL: %d", loglevel)

	procuserJobs := eosJobs{
		kind:     "procuser",
		jobs:     make(map[int]*eosJob),
		max:      procuserMax,
		waiting:  0,
		sleep:    waitSleep,
		loglevel: loglevel,
	}

	webdavJobs := eosJobs{
		kind:     "webdav",
		jobs:     make(map[int]*eosJob),
		max:      webdavMax,
		waiting:  0,
		sleep:    waitSleep,
		loglevel: loglevel,
	}

	xrdcpJobs := eosJobs{
		kind:     "xrdcp",
		jobs:     make(map[int]*eosJob),
		max:      xrdcpMax,
		waiting:  0,
		sleep:    waitSleep,
		loglevel: loglevel,
	}

	xrootdJobs := eosJobs{
		kind:     "xrootd",
		jobs:     make(map[int]*eosJob),
		max:      xrootdMax,
		waiting:  0,
		sleep:    waitSleep,
		loglevel: loglevel,
	}

	return &eosObjects{
		loglevel:     loglevel,
		url:          os.Getenv("EOS"),
		path:         os.Getenv("VOLUME_PATH"),
		hookurl:      os.Getenv("HOOKSURL"),
		scripts:      os.Getenv("SCRIPTS"),
		user:         os.Getenv("EOSUSER"),
		uid:          os.Getenv("EOSUID"),
		gid:          os.Getenv("EOSGID"),
		stage:        stage,
		readonly:     readonly,
		readmethod:   readmethod,
		waitSleep:    waitSleep,
		procuserJobs: procuserJobs,
		webdavJobs:   webdavJobs,
		xrdcpJobs:    xrdcpJobs,
		xrootdJobs:   xrootdJobs,
		validbuckets: validbuckets,
	}, nil
}

// Production - eos gateway is production ready.
func (g *EOS) Production() bool {
	return false //hahahahaha
}

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
	procuserJobs eosJobs
	webdavJobs   eosJobs
	xrdcpJobs    eosJobs
	xrootdJobs   eosJobs
	validbuckets bool
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
	e.Log(2, "S3cmd: GetBucketInfo %s", bucket)

	if bi, ok := eosBucketCache[bucket]; ok {
		e.Log(3, "bucket cache hit: %s", bucket)
		return bi, nil
	}
	e.Log(3, "bucket cache miss: %s", bucket)
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
	e.Log(2, "S3cmd: ListBuckets")

	eosBucketCache = make(map[string]minio.BucketInfo)

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
				eosBucketCache[strings.TrimSuffix(dir, "/")] = b
			} else {
				if !stat.IsDir() {
					e.Log(3, "Bucket: %s not a directory", dir)
				}
				if !e.IsValidBucketName(strings.TrimRight(dir, "/")) {
					e.Log(3, "Bucket: %s not a valid name", dir)
				}
			}
		} else {
			e.Log(1, "ERROR: ListBuckets - can not stat %s", dir)
		}
	}

	eosDirCache.path = ""

	e.Log(3, "BucketCache: %+v", eosBucketCache)

	return buckets, err
}

// MakeBucketWithLocation - Create a new container.
func (e *eosObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	e.Log(2, "S3cmd: MakeBucketWithLocation: %s %s", bucket, location)

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
	e.Log(2, "S3cmd: DeleteBucket: %s", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return e.EOSrmdir(bucket)
}

// GetBucketPolicy - Get the container ACL
func (e *eosObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	e.Log(2, "S3cmd: GetBucketPolicy: %s", bucket)

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
	e.Log(2, "S3cmd: SetBucketPolicy: %s, %+v", bucket, bucketPolicy)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (e *eosObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	e.Log(2, "S3cmd: DeleteBucketPolicy: %s", bucket)

	if e.readonly {
		return minio.NotImplemented{}
	}

	return minio.NotImplemented{}
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this gateway.
func (e *eosObjects) IsListenBucketSupported() bool {
	return false
}

/////////////////////////////////////////////////////////////////////////////////////////////
//  Object

// CopyObject - Copies a blob from source container to destination container.
func (e *eosObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	e.Log(2, "S3cmd: CopyObject: %s -> %s : %+v", srcBucket+"/"+srcObject, destBucket+"/"+destObject, srcInfo)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	dir := destBucket + "/" + filepath.Dir(destObject)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.EOScopy(srcBucket+"/"+srcObject, destBucket+"/"+destObject, srcInfo.Size)
	if err != nil {
		e.Log(1, "ERROR: COPY:%+v", err)
		return objInfo, err
	}
	err = e.EOSsetETag(destBucket+"/"+destObject, srcInfo.ETag)
	if err != nil {
		e.Log(1, "ERROR: COPY:%+v", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(destBucket+"/"+destObject, srcInfo.ContentType)
	if err != nil {
		e.Log(1, "ERROR: COPY:%+v", err)
		return objInfo, err
	}

	e.EOScacheDeleteObject(destBucket, destObject)
	return e.GetObjectInfo(ctx, destBucket, destObject, dstOpts)
}

// CopyObjectPart creates a part in a multipart upload by copying
func (e *eosObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	e.Log(2, "S3cmd: CopyObjectPart: %s/%s to %s/%s", srcBucket, srcObject, destBucket, destObject)

	if e.readonly {
		return p, minio.NotImplemented{}
	}

	return p, minio.NotImplemented{}
}

// PutObject - Create a new blob with the incoming data
func (e *eosObjects) PutObject(ctx context.Context, bucket, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	e.Log(2, "S3cmd: PutObject: %s/%s", bucket, object)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for key, val := range opts.UserDefined {
		e.Log(3, "PutObject %s = %s", key, val)
	}

	buf, _ := ioutil.ReadAll(data)
	etag := hex.EncodeToString(data.MD5Current())

	dir := bucket + "/" + filepath.Dir(object)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	err = e.EOSput(bucket+"/"+object, buf)
	if err != nil {
		e.Log(1, "ERROR: PUT:%+v", err)
		return objInfo, err
	}
	err = e.EOSsetETag(bucket+"/"+object, etag)
	if err != nil {
		e.Log(1, "ERROR: PUT:%+v", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(bucket+"/"+object, opts.UserDefined["content-type"])
	if err != nil {
		e.Log(1, "ERROR: PUT:%+v", err)
		return objInfo, err
	}

	e.EOScacheDeleteObject(bucket, object)
	return e.GetObjectInfo(ctx, bucket, object, opts)
}

// DeleteObject - Deletes a blob on azure container
func (e *eosObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	e.Log(2, "S3cmd: DeleteObject: %s/%s", bucket, object)

	if e.readonly {
		return minio.NotImplemented{}
	}

	e.EOSrm(bucket + "/" + object)

	return nil
}

func (e *eosObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	e.Log(2, "S3cmd: DeleteObjects: %s", bucket)

	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = e.DeleteObject(ctx, bucket, object)
	}
	return errs, nil
}

// GetObject - reads an object from EOS
func (e *eosObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := strings.Replace(bucket+"/"+object, "//", "/", -1)

	e.Log(2, "S3cmd: GetObject: %s from %d for %d byte(s)", path, startOffset, length)

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

	e.Log(2, "S3cmd: GetObjectInfo: %s", path)

	stat, err := e.EOSfsStat(path)

	if err != nil {
		e.Log(3, "%+v", err)
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

	e.Log(2, "%s etag:%s content-type:%s", path, stat.ETag(), stat.ContentType())
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
	e.Log(2, "S3cmd: ListMultipartUploads: %s %s %s %s %s %d", bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	return result, minio.NotImplemented{}
}

// NewMultipartUpload
func (e *eosObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	e.Log(2, "S3cmd: NewMultipartUpload: %s/%s +%v", bucket, object, opts)

	if e.readonly {
		return "", minio.NotImplemented{}
	}

	uploadID = bucket + "/" + object

	if strings.HasSuffix(uploadID, "/") {
		return "", minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	dir := bucket + "/" + filepath.Dir(object)
	if _, err := e.EOSfsStat(dir); err != nil {
		e.Log(2, "MKDIR : %s", dir)
		e.EOSmkdirWithOption(dir, "&mgm.option=p")
	}

	mp := eosMultiPartsType{
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
		os.RemoveAll(e.stage + "/" + stagepath)

		err = os.MkdirAll(e.stage+"/"+stagepath, 0700)
		if err != nil {
			e.Log(2, "MKDIR %s FAILED %+v", e.stage+"/"+stagepath, err)
		}
		mp.stagepath = stagepath
	}

	eosMultiParts[uploadID] = &mp

	e.Log(2, "uploadID : %s", uploadID)
	return uploadID, nil
}

// PutObjectPart
func (e *eosObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	e.Log(2, "S3cmd: PutObjectPart: %s/%s %s [%d] %d", bucket, object, uploadID, partID, data.Size())

	if e.readonly {
		return info, minio.NotImplemented{}
	}

	for eosMultiParts[uploadID].parts == nil {
		e.Log(2, "PutObjectPart called before NewMultipartUpload finished...")
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
			e.Log(2, "PutObjectPart ok, waiting for first chunk (processing part %d)...", partID)
			e.EOSsleep()
		}
	}

	eosMultiParts[uploadID].mutex.Lock()
	eosMultiParts[uploadID].partsCount++
	eosMultiParts[uploadID].AddToSize(size)
	eosMultiParts[uploadID].mutex.Unlock()

	offset := eosMultiParts[uploadID].chunkSize * int64(partID-1)
	e.Log(3, "PutObjectPart offset = %d = %d", (partID - 1), offset)

	if e.stage != "" { //staging
		e.Log(2, "PutObjectPart Staging")

		f, err := os.OpenFile(e.stage+"/"+eosMultiParts[uploadID].stagepath+"/file", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
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
		go func() {
			err = e.EOSxrootdWriteChunk(uploadID, offset, offset+size, "0", buf)
		}()
	}

	go func() {
		for eosMultiParts[uploadID].md5PartID != partID {
			e.Log(3, "PutObjectPart waiting for md5PartID = %d, currently = %d", eosMultiParts[uploadID].md5PartID, partID)
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
func (e *eosObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	e.Log(2, "S3cmd: CompleteMultipartUpload: %s size : %d firstByte : %d", uploadID, eosMultiParts[uploadID].size, eosMultiParts[uploadID].firstByte)

	if e.readonly {
		return objInfo, minio.NotImplemented{}
	}

	for eosMultiParts[uploadID].md5PartID != eosMultiParts[uploadID].partsCount+1 {
		e.Log(3, "CompleteMultipartUpload waiting for all md5Parts, %d remaining", eosMultiParts[uploadID].partsCount+1-eosMultiParts[uploadID].md5PartID)
		e.EOSsleep()
	}
	etag := hex.EncodeToString(eosMultiParts[uploadID].md5.Sum(nil))
	e.Log(2, "ETAG: %s", etag)

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
			e.Log(1, "ERROR: CompleteMultipartUpload: EOStouch :%+v", err)
			return objInfo, err
		}

		//upload in background
		go func() {
			e.Log(3, "CompleteMultipartUpload xrdcp: %s => %s size: %d", e.stage+"/"+eosMultiParts[uploadID].stagepath+"/file", uploadID+".minio.sys", eosMultiParts[uploadID].size)
			err := e.EOSxrdcp(e.stage+"/"+eosMultiParts[uploadID].stagepath+"/file", uploadID+".minio.sys", eosMultiParts[uploadID].size)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: xrdcp: %+v", err)
				return
			}

			err = e.EOSrename(uploadID+".minio.sys", uploadID)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: EOSrename: %+v", err)
				return
			}

			err = e.EOSsetETag(uploadID, etag)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: EOSsetETag: %+v", err)
				return
			}

			err = e.EOSsetContentType(uploadID, eosMultiParts[uploadID].contenttype)
			if err != nil {
				e.Log(1, "ERROR: CompleteMultipartUpload: EOSsetContentType: %+v", err)
				return
			}

			err = os.RemoveAll(e.stage + "/" + eosMultiParts[uploadID].stagepath)
			if err != nil {
				return
			}

			delete(eosMultiParts, uploadID)
			e.messagebusAddPutJob(uploadID)
		}()
	} else { //use xrootd
		err = e.EOSxrootdWriteChunk(uploadID, 0, eosMultiParts[uploadID].size, "1", []byte{eosMultiParts[uploadID].firstByte})
		if err != nil {
			e.Log(1, "ERROR: CompleteMultipartUpload: EOSwriteChunk: %+v", err)
			return objInfo, err
		}

		delete(eosMultiParts, uploadID)
		e.messagebusAddPutJob(uploadID)
	}

	err = e.EOSsetETag(uploadID, etag)
	if err != nil {
		e.Log(1, "ERROR: CompleteMultipartUpload:%+v", err)
		return objInfo, err
	}
	err = e.EOSsetContentType(uploadID, eosMultiParts[uploadID].contenttype)
	if err != nil {
		e.Log(1, "ERROR: CompleteMultipartUpload:%+v", err)
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
	e.Log(2, "S3cmd: AbortMultipartUpload: %s/%s %s", bucket, object, uploadID)

	if e.readonly {
		return minio.NotImplemented{}
	}

	if e.stage != "" { //staging
		os.RemoveAll(e.stage + "/" + eosMultiParts[uploadID].stagepath)
	}

	e.EOSrm(bucket + "/" + object)

	delete(eosMultiParts, uploadID)

	return nil
}

// ListObjectParts
func (e *eosObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, options minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	e.Log(2, "S3cmd: ListObjectParts: %s %d %d", uploadID, partNumberMarker, maxParts)

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
	e.Log(2, "S3cmd: ListObjects: %s, %s, %s, %s, %d", bucket, prefix, marker, delimiter, maxKeys)
	if delimiter == "/" && prefix == "/" {
		e.Log(2, "delimiter and prefix is slash")
		return result, nil
	}

	path := strings.TrimSuffix(bucket+"/"+prefix, "/")

	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}
	//if eosDirCache.path != path || len(eosDirCache.objects) == 0 {
	e.Log(3, "NEW CACHE for %s", path)
	eosDirCache.path = path
	eosDirCache.objects, err = e.EOSreadDir(path, true)
	if err != nil {
		return result, minio.ObjectNotFound{Bucket: bucket, Object: prefix}
	}
	//}

	// No objects, in the given path - let's treat it like an object if it doesn't end in a delimiter
	if len(eosDirCache.objects) == 0 {
		e.Log(3, "No objects found for path %s, treating as an object", path)
		var stat *eosFileStat
		prefix = strings.TrimSuffix(prefix, "/")
		stat, err = e.EOSfsStat(path)

		if stat != nil && !strings.HasSuffix(path, ".minio.sys") {
			o := minio.ObjectInfo{
				Bucket:      bucket,
				Name:        prefix,
				ModTime:     stat.ModTime(),
				Size:        stat.Size(),
				IsDir:       stat.IsDir(),
				ETag:        stat.ETag(),
				ContentType: stat.ContentType(),
			}
			result.Objects = append(result.Objects, o)
		}
		// reset cache
		eosDirCache.objects = nil
		eosDirCache.objects = []string{}
	} else {
		for _, obj := range eosDirCache.objects {
			var stat *eosFileStat
			objpath := path + "/" + obj
			stat, err = e.EOSfsStat(objpath)

			if stat != nil && !strings.HasSuffix(obj, ".minio.sys") {
				e.Log(3, "%s, %s, %s = %s", bucket, prefix, obj, bucket+"/"+prefix+obj)
				e.Log(3, "%s <=> %s etag:%s content-type:%s", objpath, prefix+obj, stat.ETag(), stat.ContentType())
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
						e.Log(3, "ASKING FOR -r on : %s", prefix+obj)
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
				e.Log(1, "ERROR: ListObjects - can not stat %s", objpath)
			}
		}
	}

	result.IsTruncated = false
	//result.Prefixes = append(result.Prefixes, prefix)

	return result, err
}

// ListObjectsV2 - list all blobs in a container filtered by prefix
func (e *eosObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	e.Log(2, "S3cmd: ListObjectsV2: %s, %s, %s, %s, %d", bucket, prefix, continuationToken, delimiter, maxKeys)

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
		res, _ := http.Get(joburl)
		defer res.Body.Close()
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
	slot := e.procuserJobs.waitForSlot()

	eosurl := fmt.Sprintf("http://%s:8000/proc/user/?%s", e.url, cmd)
	e.Log(3, "curl '%s'", eosurl)
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			e.Log(3, "http client wants to redirect")
			return nil
		},
		Timeout: 0,
	}
	req, _ := http.NewRequest("GET", eosurl, nil)
	req.Header.Set("Remote-User", e.user)
	res, _ := client.Do(req)
	defer res.Body.Close()
	body, _ = ioutil.ReadAll(res.Body)

	m = make(map[string]interface{})
	err = json.Unmarshal([]byte(body), &m)

	e.procuserJobs.freeSlot(slot)
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

	e.Log(2, "EOScmd: procuser.fileinfo %s", eospath)
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSreadDir can not json.Unmarshal() MGM response")
		return nil, err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR: EOS procuser.fileinfo %s : %s", eospath, e.interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	if c, ok := m["children"]; ok {
		err = json.Unmarshal([]byte(body), &c)
		if err != nil {
			e.Log(1, "ERROR: EOSreadDir can not json.Unmarshal() children")
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

	//e.Log(3,"cache: %+v", eosFileStatCache)
	return entries, err
}

func (e *eosObjects) EOSfsStat(p string) (*eosFileStat, error) {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return nil, err
	}
	if fi, ok := e.EOScacheRead(eospath); ok {
		e.Log(3, "cache hit: %s", eospath)
		return fi, nil
	}
	e.Log(3, "cache miss: %s", eospath)

	e.Log(2, "EOScmd: procuser.fileinfo %s", eospath)
	body, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: EOSfsStat can not json.Unmarshal()")
		return nil, err
	}
	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(3, "EOS procuser.fileinfo %s : %s", eospath, e.interfaceToString(m["errormsg"]))
		return nil, eoserrFileNotFound
	}

	e.Log(3, "EOS STAT name:%s mtime:%d mode:%d size:%d", e.interfaceToString(m["name"]), e.interfaceToInt64(m["mtime"]), e.interfaceToInt64(m["mode"]), e.interfaceToInt64(m["size"]))
	e.Log(3, "EOSfsStat return body : %s", strings.Replace(string(body), "\n", " ", -1))

	//some defaults
	meta := make(map[string]string)
	meta["contenttype"] = "application/octet-stream"
	meta["etag"] = "00000000000000000000000000000000"
	if _, ok := m["xattr"]; ok {
		xattr, _ := m["xattr"].(map[string]interface{})
		//e.Log(3, "xattr: %+v", xattr)
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

	e.Log(2, "EOScmd: procuser.mkdir %s", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=mkdir%s&mgm.path=%s%s", option, url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: can not json.Unmarshal()")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.mkdir %s : %s", eospath, e.interfaceToString(m["errormsg"]))
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

	e.Log(2, "EOScmd: procuser.rm %s", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: can not json.Unmarshal()")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.rm %s : %s", eospath, e.interfaceToString(m["errormsg"]))
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

	e.Log(2, "EOScmd: procuser.rm %s", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=rm&mgm.option=r&mgm.deletion=deep&mgm.path=%s%s", url.QueryEscape(eospath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: can not json.Unmarshal()")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.rm %s : %s", eospath, e.interfaceToString(m["errormsg"]))
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
		e.Log(2, "EOScmd: procuser.fileinfo %s", eossrcpath)
		_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=fileinfo&mgm.path=%s%s", url.QueryEscape(eossrcpath), e.EOSurlExtras()))
		if err == nil {
			if e.interfaceToInt64(m["size"]) >= size {
				break
			}
		}
		e.Log(2, "EOScopy waiting for src file to arrive : %s size: %d", eossrcpath, size)
		e.Log(3, "EOScopy expecting size: %d found size: %d", size, e.interfaceToInt64(m["size"]))
		e.EOSsleepSlow()
	}

	e.Log(2, "EOScmd: procuser.file.copy %s %s", eossrcpath, eosdstpath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=copy&mgm.file.option=f&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eossrcpath), url.QueryEscape(eosdstpath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: can not json.Unmarshal()")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.copy %s %s : %s", eossrcpath, eosdstpath, e.interfaceToString(m["errormsg"]))
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

	e.Log(2, "EOScmd: procuser.file.touch %s", eospath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=touch&mgm.path=%s%s&eos.bookingsize=%d", url.QueryEscape(eospath), e.EOSurlExtras(), size))
	if err != nil {
		e.Log(1, "ERROR: can not json.Unmarshal()")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.touch %s : %s", eospath, e.interfaceToString(m["errormsg"]))
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

	e.Log(2, "EOScmd: procuser.file.rename %s %s", eosfrompath, eostopath)
	_, m, err := e.EOSMGMcurl(fmt.Sprintf("mgm.cmd=file&mgm.subcmd=rename&mgm.path=%s&mgm.file.target=%s%s", url.QueryEscape(eosfrompath), url.QueryEscape(eostopath), e.EOSurlExtras()))
	if err != nil {
		e.Log(1, "ERROR: can not json.Unmarshal()")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.file.rename %s : %s", eosfrompath, eostopath, e.interfaceToString(m["errormsg"]))
		return eoserrDiskAccessDenied
	}

	return nil
}

func (e *eosObjects) EOSsetMeta(p, key, value string) error {
	if key == "" || value == "" {
		//dont bother setting if we don't get what we need
		return nil
	}
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	e.Log(2, "EOScmd: procuser.attr.set %s=%s %s", key, value, eospath)
	cmd := fmt.Sprintf("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=minio_%s&mgm.attr.value=%s&mgm.path=%s%s", url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(eospath), e.EOSurlExtras())
	body, m, err := e.EOSMGMcurl(cmd)
	e.Log(3, "Meta Tag return body : %s", strings.Replace(string(body), "\n", " ", -1))
	if err != nil {
		e.Log(1, "ERROR: can not json.Unmarshal()")
		return err
	}

	if e.interfaceToString(m["errormsg"]) != "" {
		e.Log(1, "ERROR EOS procuser.attr.set %s : %s : %s", eospath, cmd, e.interfaceToString(m["errormsg"]))
		//return eoserrSetMeta
	}

	return nil
}

func (e *eosObjects) EOSsetContentType(p, ct string) error { return e.EOSsetMeta(p, "contenttype", ct) }
func (e *eosObjects) EOSsetETag(p, etag string) error      { return e.EOSsetMeta(p, "etag", etag) }

func (e *eosObjects) EOSput(p string, data []byte) error {
	e.Log(2, "EOSput: %s", p)
	//curl -L -X PUT -T somefile -H 'Remote-User: minio' -sw '%{http_code}' http://eos:8000/eos-path/somefile

	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	eospath = strings.Replace(eospath, "%", "%25", -1)
	eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
	e.Log(3, "%s", eosurl)

	e.Log(2, "EOScmd: webdav.PUT : %s", eosurl)

	maxRetry := 10
	retry := 0
	for retry < maxRetry {
		retry = retry + 1

		if strings.IndexByte(p, '%') >= 0 {
			slot := e.webdavJobs.waitForSlot()

			e.Log(2, "EOScmd: webdav.PUT : SPECIAL CASE using curl: %s", eosurl)
			cmd := exec.Command("curl", "-L", "-X", "PUT", "--data-binary", "@-", "-H", "Remote-User: minio", "-sw", "'%{http_code}'", eosurl)
			cmd.Stdin = bytes.NewReader(data)
			stdoutStderr, err := cmd.CombinedOutput()

			e.webdavJobs.freeSlot(slot)

			if err != nil {
				e.Log(1, "ERROR: can not curl %s", eosurl)
				e.Log(2, "%s", strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				e.EOSsleep()
				continue
			}
			if strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)) != "'201'" {
				e.Log(1, "ERROR: wrong response from curl")
				e.Log(2, "%s", strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
				e.EOSsleep()
				continue
			}

			return err
		}

		slot := e.webdavJobs.waitForSlot()
		client := &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				e.Log(2, "http client wants to redirect")
				e.Log(3, "%+v", req)
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
		e.webdavJobs.freeSlot(slot)

		if err != nil {
			e.Log(2, "http ERROR message: %+v", err)
			e.Log(2, "http ERROR response: %+v", res)

			//req.URL.RawPath = strings.Replace(req.URL.RawPath[:strings.IndexByte(req.URL.RawPath, '?')], "%", "%25", -1) + "?" + req.URL.RawQuery

			e.EOSsleep()
			continue
		}
		defer res.Body.Close()
		if res.StatusCode != 201 {
			e.Log(2, "http StatusCode != 201: %+v", res)
			err = eoserrCantPut
			e.EOSsleep()
			continue
		}

		e.messagebusAddPutJob(p)
		return err
	}
	e.Log(1, "ERROR: EOSput failed %d times", maxRetry)
	return err
}

func (e *eosObjects) EOSxrootdWriteChunk(p string, offset, size int64, checksum string, data []byte) error {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}
	eosurl := fmt.Sprintf("root://%s@%s/%s", e.user, e.url, eospath)
	e.Log(2, "EOScmd: xrootd.PUT : %s %s %d %d %s %d %d", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)
	slot := e.xrootdJobs.waitForSlot()

	cmd := exec.Command(e.scripts+"/writeChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(size, 10), checksum, e.uid, e.gid)
	cmd.Stdin = bytes.NewReader(data)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		e.Log(1, "ERROR: can not %s %s %d %d %s %s %s", e.scripts+"/writeChunk.py", eosurl, offset, size, checksum, e.uid, e.gid)
		e.Log(2, "%s", strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr)))
	}
	e.xrootdJobs.freeSlot(slot)

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
		e.Log(1, "ERROR: can not url.QueryUnescape()")
		return err
	}

	e.Log(2, "EOScmd: xrdcp.PUT : %s", eosurl)
	slot := e.xrdcpJobs.waitForSlot()

	cmd := exec.Command("/usr/bin/xrdcp", "-N", "-f", "-p", src, eosurl)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		e.Log(1, "ERROR: can not /usr/bin/xrdcp -N -f -p %s %s", src, eosurl)
	}
	output := strings.TrimSpace(fmt.Sprintf("%s", stdoutStderr))
	if output != "" {
		e.Log(2, "%s", output)
	}
	e.xrdcpJobs.freeSlot(slot)

	return err
}

func (e *eosObjects) EOSreadChunk(p string, offset, length int64, data io.Writer) (err error) {
	eospath, err := e.EOSpath(p)
	if err != nil {
		return err
	}

	if e.readmethod == "xrootd" {
		eosurl := fmt.Sprintf("root://%s@%s/%s", e.user, e.url, eospath)
		e.Log(2, "EOScmd: xrootd.GET : %s", eosurl)
		slot := e.xrootdJobs.waitForSlot()

		cmd := exec.Command(e.scripts+"/readChunk.py", eosurl, strconv.FormatInt(offset, 10), strconv.FormatInt(length, 10), e.uid, e.gid)
		var stderr bytes.Buffer
		cmd.Stdout = data
		cmd.Stderr = &stderr
		err2 := cmd.Run()
		errStr := strings.TrimSpace(string(stderr.Bytes()))
		e.Log(2, "%s %s %d %d %s %s %+v", e.scripts+"/readChunk.py", eosurl, offset, length, e.uid, e.gid, err2)
		if errStr != "" {
			e.Log(2, "%s", errStr)
		}
		e.xrootdJobs.freeSlot(slot)
	} else if e.readmethod == "xrdcp" {
		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl, err := url.QueryUnescape(fmt.Sprintf("root://%s/%s?eos.ruid=%s&eos.rgid=%s", e.url, eospath, e.uid, e.gid))
		if err != nil {
			e.Log(1, "ERROR: can not url.QueryUnescape()")
			return err
		}

		e.Log(2, "EOScmd: xrdcp.GET : %s", eosurl)
		slot := e.xrdcpJobs.waitForSlot()

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

		e.xrdcpJobs.freeSlot(slot)
	} else { //webdav
		//curl -L -X GET -H 'Remote-User: minio' -H 'Range: bytes=5-7' http://eos:8000/eos-path-to-file

		eospath = strings.Replace(eospath, "%", "%25", -1)
		eosurl := fmt.Sprintf("http://%s:8000%s", e.url, eospath)
		//e.Log(3,"%s", eosurl)
		//e.Log(3,"Range: bytes=%d-%d", offset, offset+length-1)
		e.Log(2, "EOScmd: webdav.GET : %s", eosurl)

		slot := e.webdavJobs.waitForSlot()
		client := &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				e.Log(2, "http client wants to redirect")
				return nil
			},
			Timeout: 0,
		}
		req, _ := http.NewRequest("GET", eosurl, nil)
		req.Header.Set("Remote-User", "minio")
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		req.Close = true
		res, err := client.Do(req)
		e.webdavJobs.freeSlot(slot)

		if err != nil {
			e.Log(2, "%+v", err)
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
	e.Log(2, "EOScmd: webdav.GET : %s", eosurl)

	slot := e.webdavJobs.waitForSlot()
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			e.Log(2, "http client wants to redirect")
			return nil
		},
		Timeout: 0,
	}
	req, _ := http.NewRequest("GET", eosurl, nil)
	req.Header.Set("Remote-User", "minio")
	req.Close = true
	res, err := client.Do(req)
	e.webdavJobs.freeSlot(slot)

	if err != nil {
		e.Log(2, "%+v", err)
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

/////////////////////////////////////////////////////////////////////////////////////////////
//  Rate Limit

type eosJob struct {
	busy    bool
	lastrun int64
}

type eosJobs struct {
	kind     string
	jobs     map[int]*eosJob
	max      int
	waiting  int
	sleep    int
	mutex    sync.Mutex
	loglevel int
}

func (j *eosJobs) Log(level int, format string, a ...interface{}) {
	if level == 1 {
		err := fmt.Errorf(format, a...)
		logger.LogIf(context.Background(), err)
	} else if j.loglevel >= level {
		logger.Info(format, a...)
	}
}

func (j *eosJobs) amountRunning() int {
	count := 0
	for i := 0; i < j.max; i++ {
		job, ok := j.jobs[i]
		if ok {
			if job.busy {
				count++
			}
		}
	}
	return count
}

func (j *eosJobs) amountRead() (waiting, running int) {
	j.mutex.Lock()
	running = j.amountRunning()
	defer j.mutex.Unlock()
	return j.waiting, running
}

func (j *eosJobs) amountWaitingInc() (waiting, busy int) {
	j.mutex.Lock()
	j.waiting++
	running := j.amountRunning()
	defer j.mutex.Unlock()
	return j.waiting, running
}

func (j *eosJobs) getFreeSlot() int {
	slot := -1
	if j.max == 0 {
		return -1
	}

	now := time.Now().Unix()
	j.mutex.Lock()
	for i := 0; i < j.max; i++ {
		job, ok := j.jobs[i]
		if ok {
			j.Log(3, "%s slot %d job: %+v", j.kind, i, job)
			if !job.busy && now-job.lastrun >= 1 {
				slot = i
				j.waiting--
				job.busy = true
				job.lastrun = now
				break
			}
		} else {
			j.Log(3, "%s slot %d does not exist, creating", j.kind, i)
			slot = i
			j.waiting--
			newJob := eosJob{
				busy:    true,
				lastrun: now,
			}
			j.jobs[i] = &newJob
			break
		}
	}
	defer j.mutex.Unlock()
	return slot
}

func (j *eosJobs) waitForSlot() int {
	slot := -1
	if j.max == 0 {
		return -1
	}

	amountWaiting, amountRunning := j.amountWaitingInc()
	j.Log(3, "%s AmountRunning / Max (%d/%d), AmountWaiting: %d Before curl", j.kind, amountRunning, j.max, amountWaiting)

	//wait for task to be not busy
	for amountRunning >= j.max {
		j.Log(3, "%s AmountRunning >= Max (%d>=%d), AmountWaiting: %d, sleeping %dms", j.kind, amountRunning, j.max, amountWaiting, j.sleep)
		time.Sleep(time.Duration(j.sleep) * time.Millisecond)
		amountWaiting, amountRunning = j.amountRead()
	}

	//wait for non busy slot to age at least 1s
	for slot == -1 {
		slot = j.getFreeSlot()
		if slot == -1 {
			j.Log(3, "%s slots are maxed out this second, sleeping %dms", j.kind, j.sleep)
			time.Sleep(time.Duration(j.sleep) * time.Millisecond)
		}
	}
	return slot
}

func (j *eosJobs) freeSlot(slot int) {
	if j.max == 0 || slot == -1 {
		return
	}
	j.Log(3, "%s slot %d try to free", j.kind, slot)

	j.mutex.Lock()
	job, ok := j.jobs[slot]
	if ok {
		j.Log(3, "%s slot %d set to free SUCCESS", j.kind, slot)
		job.busy = false
	} else {
		j.Log(3, "%s slot %d set to free FAILED", j.kind, slot)
	}
	defer j.mutex.Unlock()
}
