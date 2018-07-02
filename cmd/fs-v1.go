/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/policy"
)

// Default etag is used for pre-existing objects.
var defaultEtag = "00000000000000000000000000000000-1"

// FSObjects - Implements fs object layer.
type FSObjects struct {
	// Disk usage metrics
	totalUsed uint64 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// Disk usage running routine
	usageRunning int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG

	// Path to be exported over S3 API.
	fsPath string
	// meta json filename, varies by fs / cache backend.
	metaJSONFile string
	// Unique value to be used for all
	// temporary transactions.
	fsUUID string

	// This value shouldn't be touched, once initialized.
	fsFormatRlk *lock.RLockedFile // Is a read lock on `format.json`.

	// FS rw pool.
	rwPool *fsIOPool

	// ListObjects pool management.
	listPool *treeWalkPool

	appendFileMap   map[string]*fsAppendFile
	appendFileMapMu sync.Mutex

	// To manage the appendRoutine go-routines
	nsMutex *nsLockMap
}

// Represents the background append file.
type fsAppendFile struct {
	sync.Mutex
	parts    []PartInfo // List of parts appended.
	compressParts []CompressPartInfo
	filePath string     // Absolute path of the file in the temp location.
}

// Initializes meta volume on all the fs path.
func initMetaVolumeFS(fsPath, fsUUID string) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made less expensive
	// optimizing all other calls. Create minio meta volume,
	// if it doesn't exist yet.
	metaBucketPath := pathJoin(fsPath, minioMetaBucket)

	if err := os.MkdirAll(metaBucketPath, 0777); err != nil {
		return err
	}

	metaTmpPath := pathJoin(fsPath, minioMetaTmpBucket, fsUUID)
	if err := os.MkdirAll(metaTmpPath, 0777); err != nil {
		return err
	}

	metaMultipartPath := pathJoin(fsPath, minioMetaMultipartBucket)
	return os.MkdirAll(metaMultipartPath, 0777)

}

// NewFSObjectLayer - initialize new fs object layer.
func NewFSObjectLayer(fsPath string) (ObjectLayer, error) {
	ctx := context.Background()
	if fsPath == "" {
		return nil, errInvalidArgument
	}

	var err error
	if fsPath, err = getValidPath(fsPath); err != nil {
		return nil, uiErrUnableToWriteInBackend(err)
	}

	// Assign a new UUID for FS minio mode. Each server instance
	// gets its own UUID for temporary file transaction.
	fsUUID := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err = initMetaVolumeFS(fsPath, fsUUID); err != nil {
		return nil, err
	}

	// Initialize `format.json`, this function also returns.
	rlk, err := initFormatFS(ctx, fsPath)
	if err != nil {
		return nil, err
	}

	// Initialize fs objects.
	fs := &FSObjects{
		fsPath:       fsPath,
		metaJSONFile: fsMetaJSONFile,
		fsUUID:       fsUUID,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		nsMutex:       newNSLock(false),
		listPool:      newTreeWalkPool(globalLookupTimeout),
		appendFileMap: make(map[string]*fsAppendFile),
	}

	// Once the filesystem has initialized hold the read lock for
	// the life time of the server. This is done to ensure that under
	// shared backend mode for FS, remote servers do not migrate
	// or cause changes on backend format.
	fs.fsFormatRlk = rlk

	// Initialize notification system.
	if err = globalNotificationSys.Init(fs); err != nil {
		return nil, uiErrUnableToReadFromBackend(err).Msg("Unable to initialize notification system")
	}

	// Initialize policy system.
	if err = globalPolicySys.Init(fs); err != nil {
		return nil, uiErrUnableToReadFromBackend(err).Msg("Unable to initialize policy system")
	}

	go fs.diskUsage(globalServiceDoneCh)
	go fs.cleanupStaleMultipartUploads(ctx, globalMultipartCleanupInterval, globalMultipartExpiry, globalServiceDoneCh)

	// Return successfully initialized object layer.
	return fs, nil
}

// Shutdown - should be called when process shuts down.
func (fs *FSObjects) Shutdown(ctx context.Context) error {
	fs.fsFormatRlk.Close()

	// Cleanup and delete tmp uuid.
	return fsRemoveAll(ctx, pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID))
}

// diskUsage returns du information for the posix path, in a continuous routine.
func (fs *FSObjects) diskUsage(doneCh chan struct{}) {
	ticker := time.NewTicker(globalUsageCheckInterval)
	defer ticker.Stop()

	usageFn := func(ctx context.Context, entry string) error {
		var fi os.FileInfo
		var err error
		if hasSuffix(entry, slashSeparator) {
			fi, err = fsStatDir(ctx, entry)
		} else {
			fi, err = fsStatFile(ctx, entry)
		}
		if err != nil {
			return err
		}
		atomic.AddUint64(&fs.totalUsed, uint64(fi.Size()))
		return nil
	}

	// Check if disk usage routine is running, if yes then return.
	if atomic.LoadInt32(&fs.usageRunning) == 1 {
		return
	}
	atomic.StoreInt32(&fs.usageRunning, 1)
	defer atomic.StoreInt32(&fs.usageRunning, 0)

	if err := getDiskUsage(context.Background(), fs.fsPath, usageFn); err != nil {
		return
	}

	for {
		select {
		case <-doneCh:
			return
		case <-ticker.C:
			// Check if disk usage routine is running, if yes let it finish.
			if atomic.LoadInt32(&fs.usageRunning) == 1 {
				continue
			}
			atomic.StoreInt32(&fs.usageRunning, 1)

			var usage uint64
			usageFn = func(ctx context.Context, entry string) error {
				var fi os.FileInfo
				var err error
				if hasSuffix(entry, slashSeparator) {
					fi, err = fsStatDir(ctx, entry)
				} else {
					fi, err = fsStatFile(ctx, entry)
				}
				if err != nil {
					return err
				}
				usage = usage + uint64(fi.Size())
				return nil
			}

			if err := getDiskUsage(context.Background(), fs.fsPath, usageFn); err != nil {
				atomic.StoreInt32(&fs.usageRunning, 0)
				continue
			}

			atomic.StoreInt32(&fs.usageRunning, 0)
			atomic.StoreUint64(&fs.totalUsed, usage)
		}
	}
}

// StorageInfo - returns underlying storage statistics.
func (fs *FSObjects) StorageInfo(ctx context.Context) StorageInfo {
	storageInfo := StorageInfo{
		Used: atomic.LoadUint64(&fs.totalUsed),
	}
	storageInfo.Backend.Type = FS
	return storageInfo
}

// Locking operations

// ListLocks - List namespace locks held in object layer
func (fs *FSObjects) ListLocks(ctx context.Context, bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error) {
	return []VolumeLockInfo{}, NotImplemented{}
}

// ClearLocks - Clear namespace locks held in object layer
func (fs *FSObjects) ClearLocks(ctx context.Context, info []VolumeLockInfo) error {
	return NotImplemented{}
}

/// Bucket operations

// getBucketDir - will convert incoming bucket names to
// corresponding valid bucket names on the backend in a platform
// compatible way for all operating systems.
func (fs *FSObjects) getBucketDir(ctx context.Context, bucket string) (string, error) {
	if bucket == "" || bucket == "." || bucket == ".." {
		return "", errVolumeNotFound
	}
	bucketDir := pathJoin(fs.fsPath, bucket)
	return bucketDir, nil
}

func (fs *FSObjects) statBucketDir(ctx context.Context, bucket string) (os.FileInfo, error) {
	bucketDir, err := fs.getBucketDir(ctx, bucket)
	if err != nil {
		return nil, err
	}
	st, err := fsStatVolume(ctx, bucketDir)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// MakeBucketWithLocation - create a new bucket, returns if it
// already exists.
func (fs *FSObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	bucketLock := fs.nsMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer bucketLock.Unlock()
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		err := BucketNameInvalid{Bucket: bucket}
		logger.LogIf(ctx, err)
		return err
	}
	bucketDir, err := fs.getBucketDir(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	if err = fsMkdir(ctx, bucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	return nil
}

// GetBucketInfo - fetch bucket metadata info.
func (fs *FSObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	bucketLock := fs.nsMutex.NewNSLock(bucket, "")
	if e := bucketLock.GetRLock(globalObjectTimeout); e != nil {
		return bi, e
	}
	defer bucketLock.RUnlock()
	st, err := fs.statBucketDir(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}

	// As os.Stat() doesn't carry other than ModTime(), use ModTime() as CreatedTime.
	createdTime := st.ModTime()
	return BucketInfo{
		Name:    bucket,
		Created: createdTime,
	}, nil
}

// ListBuckets - list all s3 compatible buckets (directories) at fsPath.
func (fs *FSObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if err := checkPathLength(fs.fsPath); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	var bucketInfos []BucketInfo
	entries, err := readDir((fs.fsPath))
	if err != nil {
		logger.LogIf(ctx, errDiskNotFound)
		return nil, toObjectErr(errDiskNotFound)
	}

	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry) {
			continue
		}
		var fi os.FileInfo
		fi, err = fsStatVolume(ctx, pathJoin(fs.fsPath, entry))
		// There seems like no practical reason to check for errors
		// at this point, if there are indeed errors we can simply
		// just ignore such buckets and list only those which
		// return proper Stat information instead.
		if err != nil {
			// Ignore any errors returned here.
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Name: fi.Name(),
			// As os.Stat() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: fi.ModTime(),
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(bucketInfos))

	// Succes.
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket and all the metadata associated
// with the bucket including pending multipart, object metadata.
func (fs *FSObjects) DeleteBucket(ctx context.Context, bucket string) error {
	bucketLock := fs.nsMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalObjectTimeout); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer bucketLock.Unlock()
	bucketDir, err := fs.getBucketDir(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	// Attempt to delete regular bucket.
	if err = fsRemoveDir(ctx, bucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	// Cleanup all the bucket metadata.
	minioMetadataBucketDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket)
	if err = fsRemoveAll(ctx, minioMetadataBucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	// Delete all bucket metadata.
	deleteBucketMetadata(ctx, bucket, fs)

	return nil
}

/// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (fs *FSObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo) (oi ObjectInfo, e error) {
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	// Hold write lock on destination since in both cases
	// - if source and destination are same
	// - if source and destination are different
	// it is the sole mutating state.
	objectDWLock := fs.nsMutex.NewNSLock(dstBucket, dstObject)
	if err := objectDWLock.GetLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer objectDWLock.Unlock()
	// if source and destination are different, we have to hold
	// additional read lock as well to protect against writes on
	// source.
	if !cpSrcDstSame {
		// Hold read locks on source object only if we are
		// going to read data from source object.
		objectSRLock := fs.nsMutex.NewNSLock(srcBucket, srcObject)
		if err := objectSRLock.GetRLock(globalObjectTimeout); err != nil {
			return oi, err
		}
		defer objectSRLock.RUnlock()
	}
	if _, err := fs.statBucketDir(ctx, srcBucket); err != nil {
		return oi, toObjectErr(err, srcBucket)
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		// Close any writer which was initialized.
		defer srcInfo.Writer.Close()

		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, srcBucket, srcObject, fs.metaJSONFile)
		wlk, err := fs.rwPool.Write(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()

		// Save objects' metadata in `fs.json`.
		fsMeta := newFSMetaV1()
		if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		fsMeta.Meta = srcInfo.UserDefined
		fsMeta.Meta["etag"] = srcInfo.ETag
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Stat the file to get file size.
		fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, srcBucket, srcObject))
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Return the new object info.
		return fsMeta.ToObjectInfo(srcBucket, srcObject, fi), nil
	}

	go func() {
		if gerr := fs.getObject(ctx, srcBucket, srcObject, 0, srcInfo.Size, srcInfo.Writer, srcInfo.ETag, !cpSrcDstSame); gerr != nil {
			if gerr = srcInfo.Writer.Close(); gerr != nil {
				logger.LogIf(ctx, gerr)
			}
			return
		}
		// Close writer explicitly signalling we wrote all data.
		if gerr := srcInfo.Writer.Close(); gerr != nil {
			logger.LogIf(ctx, gerr)
			return
		}
	}()

	objInfo, err := fs.putObject(ctx, dstBucket, dstObject, srcInfo.Reader, srcInfo.UserDefined)
	if err != nil {
		return oi, toObjectErr(err, dstBucket, dstObject)
	}

	return objInfo, nil
}

// GetObject - reads an object from the disk.
// Supports additional parameters like offset and length
// which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (fs *FSObjects) GetObject(ctx context.Context, bucket, object string, offset int64, length int64, writer io.Writer, etag string, objInfo ObjectInfo) (err error) {
	
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return err
	}

	// Lock the object before reading.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer objectLock.RUnlock()

	if !isCompressed(objInfo.UserDefined) {
		return fs.getObject(ctx, bucket, object, offset, length, writer, etag, true)
	}

	// Preserving the read-write rule.
	// The read operation is blocked if changes in the decompressedSize is detected.
	// Allowing this will cause data corruption , as the data is changed during the run.
	modObjInfo, err := fs.getObjectInfo(ctx, bucket, object)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Check whether the decompressed size is changed.
	compressSizeModified := func(objInfo ObjectInfo, modObjInfo ObjectInfo) bool {
		if len(objInfo.Parts) == 0 && len(modObjInfo.Parts) == 0 {
			objDecompressedSize := getDecompressedSize(objInfo)
			modDecompressedSize := getDecompressedSize(modObjInfo)
			if objDecompressedSize > 0 || modDecompressedSize > 0 {
				if objDecompressedSize != modDecompressedSize { return true }
			}
		} else if len(objInfo.Parts) > 0 && len(modObjInfo.Parts) > 0 {
			for i,part := range objInfo.Parts {
				if part.Size != modObjInfo.Parts[i].Size {
					return true
				}
			}
		} else {
			// An object might be initially uploaded as parts and then could have uploaded normally.
			// And vice-versa applies.
			return true
		}
		return false
	}(objInfo, modObjInfo)

	// If changed, reply back with errReadBlock error.
	// ToDo - respond with a retry http status code thus making the http client retry.
        if compressSizeModified {
		logger.LogIf(ctx, errReadBlock)
		return toObjectErr(errReadBlock)
	} 

	return fs.getObject(ctx, bucket, object, offset, length, writer, etag, true)
}

// getObject - wrapper for GetObject
func (fs *FSObjects) getObject(ctx context.Context, bucket, object string, offset int64, length int64, writer io.Writer, etag string, lock bool) (err error) {
	if _, err = fs.statBucketDir(ctx, bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	// Offset cannot be negative.
	if offset < 0 {
		logger.LogIf(ctx, errUnexpected)
		return toObjectErr(errUnexpected, bucket, object)
	}

	// Writer cannot be nil.
	if writer == nil {
		logger.LogIf(ctx, errUnexpected)
		return toObjectErr(errUnexpected, bucket, object)
	}

	// If its a directory request, we return an empty body.
	if hasSuffix(object, slashSeparator) {
		_, err = writer.Write([]byte(""))
		logger.LogIf(ctx, err)
		return toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
		if lock {
			_, err = fs.rwPool.Open(fsMetaPath)
			if err != nil && err != errFileNotFound {
				logger.LogIf(ctx, err)
				return toObjectErr(err, bucket, object)
			}
			defer fs.rwPool.Close(fsMetaPath)
		}
	}

	if etag != "" && etag != defaultEtag {
		objEtag, perr := fs.getObjectETag(ctx, bucket, object, lock)
		if perr != nil {
			return toObjectErr(perr, bucket, object)
		}
		if objEtag != etag {
			logger.LogIf(ctx, InvalidETag{})
			return toObjectErr(InvalidETag{}, bucket, object)
		}
	}

	// Read the object, doesn't exist returns an s3 compatible error.
	fsObjPath := pathJoin(fs.fsPath, bucket, object)
	reader, size, err := fsOpenFile(ctx, fsObjPath, offset)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	defer reader.Close()

	bufSize := int64(readSizeV1)
	if length > 0 && bufSize > length {
		bufSize = length
	}

	// For negative length we read everything.
	if length < 0 {
		length = size - offset
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if offset > size || offset+length > size {
		err = InvalidRange{offset, length, size}
		logger.LogIf(ctx, err)
		return err
	}

	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))

	_, err = io.CopyBuffer(writer, io.LimitReader(reader, length), buf)
	if err == io.ErrClosedPipe {
		err = nil
	}
	logger.LogIf(ctx, err)
	return toObjectErr(err, bucket, object)
}

// Create a new fs.json file, if the existing one is corrupt. Should happen very rarely.
func (fs *FSObjects) createFsJSON(object, fsMetaPath string) error {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = make(map[string]string)
	fsMeta.Meta["etag"] = GenETag()
	if objectExt := path.Ext(object); objectExt != "" {
		if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
			fsMeta.Meta["content-type"] = content.ContentType
		} else {
			fsMeta.Meta["content-type"] = "application/octet-stream"
		}
	}
	wlk, werr := fs.rwPool.Create(fsMetaPath)
	if werr == nil {
		_, err := fsMeta.WriteTo(wlk)
		wlk.Close()
		return err
	}
	return werr
}

// Used to return default etag values when a pre-existing object's meta data is queried.
func (fs *FSObjects) defaultFsJSON(object string) fsMetaV1 {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = make(map[string]string)
	fsMeta.Meta["etag"] = defaultEtag
	if objectExt := path.Ext(object); objectExt != "" {
		if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
			fsMeta.Meta["content-type"] = content.ContentType
		} else {
			fsMeta.Meta["content-type"] = "application/octet-stream"
		}
	}
	return fsMeta
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (fs *FSObjects) getObjectInfo(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	fsMeta := fsMetaV1{}
	if hasSuffix(object, slashSeparator) {
		// Since we support PUT of a "directory" object, we allow HEAD.
		if !fsIsDir(ctx, pathJoin(fs.fsPath, bucket, object)) {
			return oi, errFileNotFound
		}
		fi, err := fsStatDir(ctx, pathJoin(fs.fsPath, bucket, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.

	rlk, err := fs.rwPool.Open(fsMetaPath)
	if err == nil {
		// Read from fs metadata only if it exists.
		_, rerr := fsMeta.ReadFrom(ctx, rlk.LockedFile)
		fs.rwPool.Close(fsMetaPath)
		if rerr != nil {
			return oi, rerr
		}
	}

	// Return a default etag and content-type based on the object's extension.
	if err == errFileNotFound {
		fsMeta = fs.defaultFsJSON(object)
	}

	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		logger.LogIf(ctx, err)
		return oi, err
	}

	// Stat the file to get file size.
	fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return oi, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// getObjectInfoWithLock - reads object metadata and replies back ObjectInfo.
func (fs *FSObjects) getObjectInfoWithLock(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	// Lock the object before reading.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer objectLock.RUnlock()

	if err := checkGetObjArgs(ctx, bucket, object); err != nil {
		return oi, err
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return oi, toObjectErr(err, bucket)
	}

	if strings.HasSuffix(object, slashSeparator) && !fs.isObjectDir(bucket, object) {
		return oi, errFileNotFound
	}

	return fs.getObjectInfo(ctx, bucket, object)
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (fs *FSObjects) GetObjectInfo(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	oi, err := fs.getObjectInfoWithLock(ctx, bucket, object)
	if err == errCorruptedFormat || err == io.EOF {
		objectLock := fs.nsMutex.NewNSLock(bucket, object)
		if err = objectLock.GetLock(globalObjectTimeout); err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
		err = fs.createFsJSON(object, fsMetaPath)
		objectLock.Unlock()
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		oi, err = fs.getObjectInfoWithLock(ctx, bucket, object)
	}
	return oi, toObjectErr(err, bucket, object)
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (fs *FSObjects) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." || p == "/" {
			return false
		}
		if fsIsFile(ctx, pathJoin(fs.fsPath, bucket, p)) {
			// If there is already a file at prefix "p", return true.
			return true
		}

		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, writes data directly to configured filesystem path.
// Additionally writes `fs.json` which carries the necessary metadata
// for future object operations.
func (fs *FSObjects) PutObject(ctx context.Context, bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, retErr error) {
	if err := checkPutObjectArgs(ctx, bucket, object, fs, data.Size()); err != nil {
		return ObjectInfo{}, err
	}
	// Lock the object.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetLock(globalObjectTimeout); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	defer objectLock.Unlock()
	return fs.putObject(ctx, bucket, object, data, metadata)
}

// putObject - wrapper for PutObject
func (fs *FSObjects) putObject(ctx context.Context, bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, retErr error) {
	// No metadata is set, allocate a new one.
	meta := make(map[string]string)
	for k, v := range metadata {
		meta[k] = v
	}
	var err error

	// Validate if bucket name is valid and exists.
	if _, err = fs.statBucketDir(ctx, bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	fsMeta := newFSMetaV1()
	fsMeta.Meta = meta
	// This is a special case with size as '0' and object ends
	// with a slash separator, we treat it like a valid operation
	// and return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
			logger.LogIf(ctx, errFileAccessDenied)
			return ObjectInfo{}, toObjectErr(errFileAccessDenied, bucket, object)
		}
		if err = mkdirAll(pathJoin(fs.fsPath, bucket, object), 0777); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		var fi os.FileInfo
		if fi, err = fsStatDir(ctx, pathJoin(fs.fsPath, bucket, object)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	if err = checkPutObjectArgs(ctx, bucket, object, fs, data.Size()); err != nil {
		return ObjectInfo{}, err
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		logger.LogIf(ctx, errFileAccessDenied)
		return ObjectInfo{}, toObjectErr(errFileAccessDenied, bucket, object)
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < 0 {
		logger.LogIf(ctx, errInvalidArgument)
		return ObjectInfo{}, errInvalidArgument
	}

	var wlk *lock.LockedFile
	if bucket != minioMetaBucket {
		bucketMetaDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix)

		fsMetaPath := pathJoin(bucketMetaDir, bucket, object, fs.metaJSONFile)
		wlk, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			// Remove meta file when PutObject encounters any error
			if retErr != nil {
				tmpDir := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID)
				fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
			}
		}()
	}

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := mustGetUUID()

	// Allocate a buffer to Read() from request body
	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}

	buf := make([]byte, int(bufSize))
	fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, tempObj)
	bytesWritten, err := fsCreateFile(ctx, fsTmpObjPath, data, buf, data.Size())
	if err != nil {
		fsRemoveFile(ctx, fsTmpObjPath)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	fsMeta.Meta["etag"] = hex.EncodeToString(data.MD5Current())

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	// Avoid the check if compression is enabled.
	if !isCompressed(fsMeta.Meta) {
		if bytesWritten < data.Size() {
			fsRemoveFile(ctx, fsTmpObjPath)
			return ObjectInfo{}, IncompleteBody{}
		}
	}
	// Delete the temporary object in the case of a
	// failure. If PutObject succeeds, then there would be
	// nothing to delete.
	defer fsRemoveFile(ctx, fsTmpObjPath)

	// Entire object was written to the temp location, now it's safe to rename it to the actual location.
	fsNSObjPath := pathJoin(fs.fsPath, bucket, object)
	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = fsStatFile(ctx, fsNSObjPath); err == nil {
			return ObjectInfo{}, ObjectAlreadyExists{Bucket: bucket, Object: object}
		}
	}
	if err = fsRenameFile(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Stat the file to fetch timestamp, size.
	fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Success.
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *FSObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	// Acquire a write lock before deleting the object.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objectLock.Unlock()

	if err := checkDelObjArgs(ctx, bucket, object); err != nil {
		return err
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	minioMetaBucketDir := pathJoin(fs.fsPath, minioMetaBucket)
	fsMetaPath := pathJoin(minioMetaBucketDir, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
	if bucket != minioMetaBucket {
		rwlk, lerr := fs.rwPool.Write(fsMetaPath)
		if lerr == nil {
			// This close will allow for fs locks to be synchronized on `fs.json`.
			defer rwlk.Close()
		}
		if lerr != nil && lerr != errFileNotFound {
			logger.LogIf(ctx, lerr)
			return toObjectErr(lerr, bucket, object)
		}
	}

	// Delete the object.
	if err := fsDeleteFile(ctx, pathJoin(fs.fsPath, bucket), pathJoin(fs.fsPath, bucket, object)); err != nil {
		return toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Delete the metadata object.
		err := fsDeleteFile(ctx, minioMetaBucketDir, fsMetaPath)
		if err != nil && err != errFileNotFound {
			return toObjectErr(err, bucket, object)
		}
	}
	return nil
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry
// is a leaf or non-leaf entry.
func (fs *FSObjects) listDirFactory(isLeaf isLeafFunc) listDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (entries []string, delayIsLeaf bool, err error) {
		entries, err = readDir(pathJoin(fs.fsPath, bucket, prefixDir))
		if err != nil {
			return nil, false, err
		}
		entries, delayIsLeaf = filterListEntries(bucket, prefixDir, entries, prefixEntry, isLeaf)
		return entries, delayIsLeaf, nil
	}

	// Return list factory instance.
	return listDir
}

// isObjectDir returns true if the specified bucket & prefix exists
// and the prefix represents an empty directory. An S3 empty directory
// is also an empty directory in the FS backend.
func (fs *FSObjects) isObjectDir(bucket, prefix string) bool {
	entries, err := readDirN(pathJoin(fs.fsPath, bucket, prefix), 1)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

// getObjectETag is a helper function, which returns only the md5sum
// of the file on the disk.
func (fs *FSObjects) getObjectETag(ctx context.Context, bucket, entry string, lock bool) (string, error) {
	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, entry, fs.metaJSONFile)

	var reader io.Reader
	var fi os.FileInfo
	var size int64
	if lock {
		// Read `fs.json` to perhaps contend with
		// parallel Put() operations.
		rlk, err := fs.rwPool.Open(fsMetaPath)
		// Ignore if `fs.json` is not available, this is true for pre-existing data.
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			return "", toObjectErr(err, bucket, entry)
		}

		// If file is not found, we don't need to proceed forward.
		if err == errFileNotFound {
			return "", nil
		}

		// Read from fs metadata only if it exists.
		defer fs.rwPool.Close(fsMetaPath)

		// Fetch the size of the underlying file.
		fi, err = rlk.LockedFile.Stat()
		if err != nil {
			logger.LogIf(ctx, err)
			return "", toObjectErr(err, bucket, entry)
		}

		size = fi.Size()
		reader = io.NewSectionReader(rlk.LockedFile, 0, fi.Size())
	} else {
		var err error
		reader, size, err = fsOpenFile(ctx, fsMetaPath, 0)
		if err != nil {
			return "", toObjectErr(err, bucket, entry)
		}
	}

	// `fs.json` can be empty due to previously failed
	// PutObject() transaction, if we arrive at such
	// a situation we just ignore and continue.
	if size == 0 {
		return "", nil
	}

	fsMetaBuf, err := ioutil.ReadAll(reader)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", toObjectErr(err, bucket, entry)
	}

	// Check if FS metadata is valid, if not return error.
	if !isFSMetaValid(parseFSVersion(fsMetaBuf)) {
		logger.LogIf(ctx, errCorruptedFormat)
		return "", toObjectErr(errCorruptedFormat, bucket, entry)
	}

	return extractETag(parseFSMetaMap(fsMetaBuf)), nil
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs *FSObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	if err := checkListObjsArgs(ctx, bucket, prefix, marker, delimiter, fs); err != nil {
		return loi, err
	}
	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented.Send an empty response
		if !hasPrefix(marker, prefix) {
			return ListObjectsInfo{}, e
		}
	}
	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return loi, err
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return loi, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	// Convert entry to ObjectInfo
	entryToObjectInfo := func(entry string) (objInfo ObjectInfo, err error) {
		// Protect the entry from concurrent deletes, or renames.
		objectLock := fs.nsMutex.NewNSLock(bucket, entry)
		if err = objectLock.GetRLock(globalListingTimeout); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, err
		}
		defer objectLock.RUnlock()
		return fs.getObjectInfo(ctx, bucket, entry)
	}

	heal := false // true only for xl.ListObjectsHeal()
	walkResultCh, endWalkCh := fs.listPool.Release(listParams{bucket, recursive, marker, prefix, heal})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, object string) bool {
			// bucket argument is unused as we don't need to StatFile
			// to figure if it's a file, just need to check that the
			// object string does not end with "/".
			return !hasSuffix(object, slashSeparator)
		}
		// Return true if the specified object is an empty directory
		isLeafDir := func(bucket, object string) bool {
			if !hasSuffix(object, slashSeparator) {
				return false
			}
			return fs.isObjectDir(bucket, object)
		}
		listDir := fs.listDirFactory(isLeaf)
		walkResultCh = startTreeWalk(ctx, bucket, prefix, marker, recursive, listDir, isLeaf, isLeafDir, endWalkCh)
	}

	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string

	// List until maxKeys requested.
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			// File not found is a valid case.
			if walkResult.err == errFileNotFound {
				return loi, nil
			}
			return loi, toObjectErr(walkResult.err, bucket, prefix)
		}
		objInfo, err := entryToObjectInfo(walkResult.entry)
		if err != nil {
			return loi, nil
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		if walkResult.end {
			eof = true
			break
		}
		i++
	}

	// Save list routine for the next marker if we haven't reached EOF.
	params := listParams{bucket, recursive, nextMarker, prefix, heal}
	if !eof {
		fs.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		if objInfo.IsDir && delimiter == slashSeparator {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	// Success.
	return result, nil
}

// ReloadFormat - no-op for fs, Valid only for XL.
func (fs *FSObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// HealFormat - no-op for fs, Valid only for XL.
func (fs *FSObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	logger.LogIf(ctx, NotImplemented{})
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealObject - no-op for fs. Valid only for XL.
func (fs *FSObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool) (
	res madmin.HealResultItem, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return res, NotImplemented{}
}

// HealBucket - no-op for fs, Valid only for XL.
func (fs *FSObjects) HealBucket(ctx context.Context, bucket string, dryRun bool) ([]madmin.HealResultItem,
	error) {
	logger.LogIf(ctx, NotImplemented{})
	return nil, NotImplemented{}
}

// ListObjectsHeal - list all objects to be healed. Valid only for XL
func (fs *FSObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	logger.LogIf(ctx, NotImplemented{})
	return loi, NotImplemented{}
}

// ListBucketsHeal - list all buckets to be healed. Valid only for XL
func (fs *FSObjects) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return []BucketInfo{}, NotImplemented{}
}

// SetBucketPolicy sets policy on bucket
func (fs *FSObjects) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	return savePolicyConfig(fs, bucket, policy)
}

// GetBucketPolicy will get policy on bucket
func (fs *FSObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return GetPolicyConfig(fs, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (fs *FSObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return removePolicyConfig(ctx, fs, bucket)
}

// ListObjectsV2 lists all blobs in bucket filtered by prefix
func (fs *FSObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	loi, err := fs.ListObjects(ctx, bucket, prefix, continuationToken, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	listObjectsV2Info := ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}
	return listObjectsV2Info, err
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (fs *FSObjects) IsNotificationSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is applicable for this layer.
func (fs *FSObjects) IsEncryptionSupported() bool {
	return true
}
