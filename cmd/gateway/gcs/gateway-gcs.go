/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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

package gcs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"math"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/cli"
	miniogopolicy "github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	minio "github.com/minio/minio/cmd"
)

var (
	// Project ID format is not valid.
	errGCSInvalidProjectID = fmt.Errorf("GCS project id is either empty or invalid")

	// Project ID not found
	errGCSProjectIDNotFound = fmt.Errorf("Unknown project id")

	// Invalid format.
	errGCSFormat = fmt.Errorf("Unknown format")
)

const (
	// Path where multipart objects are saved.
	// If we change the backend format we will use a different url path like /multipart/v2
	// but we will not migrate old data.
	gcsMinioMultipartPathV1 = minio.GatewayMinioSysTmp + "multipart/v1"

	// Multipart meta file.
	gcsMinioMultipartMeta = "gcs.json"

	// gcs.json version number
	gcsMinioMultipartMetaCurrentVersion = "1"

	// token prefixed with GCS returned marker to differentiate
	// from user supplied marker.
	gcsTokenPrefix = "{minio}"

	// Maximum component object count to create a composite object.
	// Refer https://cloud.google.com/storage/docs/composite-objects
	gcsMaxComponents = 32

	// Every 24 hours we scan minio.sys.tmp to delete expired multiparts in minio.sys.tmp
	gcsCleanupInterval = time.Hour * 24

	// The cleanup routine deletes files older than 2 weeks in minio.sys.tmp
	gcsMultipartExpiry = time.Hour * 24 * 14

	// Project ID key in credentials.json
	gcsProjectIDKey = "project_id"

	gcsBackend = "gcs"
)

func init() {
	const gcsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [PROJECTID]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PROJECTID:
  GCS project-id should be provided if GOOGLE_APPLICATION_CREDENTIALS environmental variable is not set.

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of GCS.
     MINIO_SECRET_KEY: Password or secret key of GCS.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.

  GCS credentials file:
     GOOGLE_APPLICATION_CREDENTIALS: Path to credentials.json

EXAMPLES:
  1. Start minio gateway server for GCS backend.
     $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
     (Instructions to generate credentials : https://developers.google.com/identity/protocols/application-default-credentials)
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ {{.HelpName}} mygcsprojectid

  2. Start minio gateway server for GCS backend with edge caching enabled.
     $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
     $ {{.HelpName}} mygcsprojectid
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               gcsBackend,
		Usage:              "Google Cloud Storage.",
		Action:             gcsGatewayMain,
		CustomHelpTemplate: gcsGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway gcs' command line.
func gcsGatewayMain(ctx *cli.Context) {
	projectID := ctx.Args().First()
	if projectID == "" && os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		logger.LogIf(context.Background(), errGCSProjectIDNotFound)
		cli.ShowCommandHelpAndExit(ctx, "gcs", 1)
	}
	if projectID != "" && !isValidGCSProjectIDFormat(projectID) {
		reqInfo := (&logger.ReqInfo{}).AppendTags("projectID", ctx.Args().First())
		contxt := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(contxt, errGCSInvalidProjectID)
		cli.ShowCommandHelpAndExit(ctx, "gcs", 1)
	}

	minio.StartGateway(ctx, &GCS{projectID})
}

// GCS implements Azure.
type GCS struct {
	projectID string
}

// Name returns the name of gcs ObjectLayer.
func (g *GCS) Name() string {
	return gcsBackend
}

// NewGatewayLayer returns gcs ObjectLayer.
func (g *GCS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	ctx := context.Background()

	var err error
	if g.projectID == "" {
		// If project ID is not provided on command line, we figure it out
		// from the credentials.json file.
		g.projectID, err = gcsParseProjectID(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"))
		if err != nil {
			return nil, err
		}
	}

	// Initialize a GCS client.
	// Send user-agent in this format for Google to obtain usage insights while participating in the
	// Google Cloud Technology Partners (https://cloud.google.com/partners/)
	client, err := storage.NewClient(ctx, option.WithUserAgent(fmt.Sprintf("Minio/%s (GPN:Minio;)", minio.Version)))
	if err != nil {
		return nil, err
	}

	gcs := &gcsGateway{
		client:    client,
		projectID: g.projectID,
		ctx:       ctx,
	}

	// Start background process to cleanup old files in minio.sys.tmp
	go gcs.CleanupGCSMinioSysTmp()
	return gcs, nil
}

// Production - GCS gateway is production ready.
func (g *GCS) Production() bool {
	return true
}

// Stored in gcs.json - Contents of this file is not used anywhere. It can be
// used for debugging purposes.
type gcsMultipartMetaV1 struct {
	Version string `json:"version"` // Version number
	Bucket  string `json:"bucket"`  // Bucket name
	Object  string `json:"object"`  // Object name
}

// Returns name of the multipart meta object.
func gcsMultipartMetaName(uploadID string) string {
	return fmt.Sprintf("%s/%s/%s", gcsMinioMultipartPathV1, uploadID, gcsMinioMultipartMeta)
}

// Returns name of the part object.
func gcsMultipartDataName(uploadID string, partNumber int, etag string) string {
	return fmt.Sprintf("%s/%s/%05d.%s", gcsMinioMultipartPathV1, uploadID, partNumber, etag)
}

// Convert Minio errors to minio object layer errors.
func gcsToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	bucket := ""
	object := ""
	uploadID := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}
	if len(params) == 3 {
		uploadID = params[2]
	}

	// in some cases just a plain error is being returned
	switch err.Error() {
	case "storage: bucket doesn't exist":
		err = minio.BucketNotFound{
			Bucket: bucket,
		}
		return err
	case "storage: object doesn't exist":
		if uploadID != "" {
			err = minio.InvalidUploadID{
				UploadID: uploadID,
			}
		} else {
			err = minio.ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
		}
		return err
	}

	googleAPIErr, ok := err.(*googleapi.Error)
	if !ok {
		// We don't interpret non Minio errors. As minio errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	if len(googleAPIErr.Errors) == 0 {
		return err
	}

	reason := googleAPIErr.Errors[0].Reason
	message := googleAPIErr.Errors[0].Message

	switch reason {
	case "required":
		// Anonymous users does not have storage.xyz access to project 123.
		fallthrough
	case "keyInvalid":
		fallthrough
	case "forbidden":
		err = minio.PrefixAccessDenied{
			Bucket: bucket,
			Object: object,
		}
	case "invalid":
		err = minio.BucketNameInvalid{
			Bucket: bucket,
		}
	case "notFound":
		if object != "" {
			err = minio.ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
			break
		}
		err = minio.BucketNotFound{Bucket: bucket}
	case "conflict":
		if message == "You already own this bucket. Please select another name." {
			err = minio.BucketAlreadyOwnedByYou{Bucket: bucket}
			break
		}
		if message == "Sorry, that name is not available. Please try a different one." {
			err = minio.BucketAlreadyExists{Bucket: bucket}
			break
		}
		err = minio.BucketNotEmpty{Bucket: bucket}
	default:
		err = fmt.Errorf("Unsupported error reason: %s", reason)
	}

	return err
}

// gcsProjectIDRegex defines a valid gcs project id format
var gcsProjectIDRegex = regexp.MustCompile("^[a-z][a-z0-9-]{5,29}$")

// isValidGCSProjectIDFormat - checks if a given project id format is valid or not.
// Project IDs must start with a lowercase letter and can have lowercase ASCII letters,
// digits or hyphens. Project IDs must be between 6 and 30 characters.
// Ref: https://cloud.google.com/resource-manager/reference/rest/v1/projects#Project (projectId section)
func isValidGCSProjectIDFormat(projectID string) bool {
	// Checking projectID format
	return gcsProjectIDRegex.MatchString(projectID)
}

// gcsGateway - Implements gateway for Minio and GCS compatible object storage servers.
type gcsGateway struct {
	minio.GatewayUnsupported
	client    *storage.Client
	projectID string
	ctx       context.Context
}

// Returns projectID from the GOOGLE_APPLICATION_CREDENTIALS file.
func gcsParseProjectID(credsFile string) (projectID string, err error) {
	contents, err := ioutil.ReadFile(credsFile)
	if err != nil {
		return projectID, err
	}
	googleCreds := make(map[string]string)
	if err = json.Unmarshal(contents, &googleCreds); err != nil {
		return projectID, err
	}
	return googleCreds[gcsProjectIDKey], err
}

// Cleanup old files in minio.sys.tmp of the given bucket.
func (l *gcsGateway) CleanupGCSMinioSysTmpBucket(bucket string) {
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Prefix: minio.GatewayMinioSysTmp, Versions: false})
	for {
		attrs, err := it.Next()
		if err != nil {
			if err != iterator.Done {
				reqInfo := &logger.ReqInfo{BucketName: bucket}
				ctx := logger.SetReqInfo(context.Background(), reqInfo)
				logger.LogIf(ctx, err)
			}
			return
		}
		if time.Since(attrs.Updated) > gcsMultipartExpiry {
			// Delete files older than 2 weeks.
			err := l.client.Bucket(bucket).Object(attrs.Name).Delete(l.ctx)
			if err != nil {
				reqInfo := &logger.ReqInfo{BucketName: bucket, ObjectName: attrs.Name}
				ctx := logger.SetReqInfo(context.Background(), reqInfo)
				logger.LogIf(ctx, err)
				return
			}
		}
	}
}

// Cleanup old files in minio.sys.tmp of all buckets.
func (l *gcsGateway) CleanupGCSMinioSysTmp() {
	for {
		it := l.client.Buckets(l.ctx, l.projectID)
		for {
			attrs, err := it.Next()
			if err != nil {
				if err != iterator.Done {
					ctx := logger.SetReqInfo(context.Background(), &logger.ReqInfo{})
					logger.LogIf(ctx, err)
				}
				break
			}
			l.CleanupGCSMinioSysTmpBucket(attrs.Name)
		}
		// Run the cleanup loop every 1 day.
		time.Sleep(gcsCleanupInterval)
	}
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *gcsGateway) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo - Not relevant to GCS backend.
func (l *gcsGateway) StorageInfo(ctx context.Context) minio.StorageInfo {
	return minio.StorageInfo{}
}

// MakeBucketWithLocation - Create a new container on GCS backend.
func (l *gcsGateway) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	bkt := l.client.Bucket(bucket)

	// we'll default to the us multi-region in case of us-east-1
	if location == "us-east-1" {
		location = "us"
	}

	err := bkt.Create(l.ctx, l.projectID, &storage.BucketAttrs{
		Location: location,
	})
	logger.LogIf(ctx, err)
	return gcsToObjectError(err, bucket)
}

// GetBucketInfo - Get bucket metadata..
func (l *gcsGateway) GetBucketInfo(ctx context.Context, bucket string) (minio.BucketInfo, error) {
	attrs, err := l.client.Bucket(bucket).Attrs(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.BucketInfo{}, gcsToObjectError(err, bucket)
	}

	return minio.BucketInfo{
		Name:    attrs.Name,
		Created: attrs.Created,
	}, nil
}

// ListBuckets lists all buckets under your project-id on GCS.
func (l *gcsGateway) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	it := l.client.Buckets(l.ctx, l.projectID)

	// Iterate and capture all the buckets.
	for {
		attrs, ierr := it.Next()
		if ierr == iterator.Done {
			break
		}

		if ierr != nil {
			logger.LogIf(ctx, ierr)
			return buckets, gcsToObjectError(ierr)
		}

		buckets = append(buckets, minio.BucketInfo{
			Name:    attrs.Name,
			Created: attrs.Created,
		})
	}

	return buckets, nil
}

// DeleteBucket delete a bucket on GCS.
func (l *gcsGateway) DeleteBucket(ctx context.Context, bucket string) error {
	itObject := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{
		Delimiter: "/",
		Versions:  false,
	})
	// We list the bucket and if we find any objects we return BucketNotEmpty error. If we
	// find only "minio.sys.tmp/" then we remove it before deleting the bucket.
	gcsMinioPathFound := false
	nonGCSMinioPathFound := false
	for {
		objAttrs, err := itObject.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logger.LogIf(ctx, err)
			return gcsToObjectError(err)
		}
		if objAttrs.Prefix == minio.GatewayMinioSysTmp {
			gcsMinioPathFound = true
			continue
		}
		nonGCSMinioPathFound = true
		break
	}
	if nonGCSMinioPathFound {
		logger.LogIf(ctx, minio.BucketNotEmpty{})
		return gcsToObjectError(minio.BucketNotEmpty{})
	}
	if gcsMinioPathFound {
		// Remove minio.sys.tmp before deleting the bucket.
		itObject = l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Versions: false, Prefix: minio.GatewayMinioSysTmp})
		for {
			objAttrs, err := itObject.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				logger.LogIf(ctx, err)
				return gcsToObjectError(err)
			}
			err = l.client.Bucket(bucket).Object(objAttrs.Name).Delete(l.ctx)
			if err != nil {
				logger.LogIf(ctx, err)
				return gcsToObjectError(err)
			}
		}
	}
	err := l.client.Bucket(bucket).Delete(l.ctx)
	logger.LogIf(ctx, err)
	return gcsToObjectError(err, bucket)
}

func toGCSPageToken(name string) string {
	length := uint16(len(name))

	b := []byte{
		0xa,
		byte(length & 0xFF),
	}

	length = length >> 7
	if length > 0 {
		b = append(b, byte(length&0xFF))
	}

	b = append(b, []byte(name)...)

	return base64.StdEncoding.EncodeToString(b)
}

// Returns true if marker was returned by GCS, i.e prefixed with
// ##minio by minio gcs minio.
func isGCSMarker(marker string) bool {
	return strings.HasPrefix(marker, gcsTokenPrefix)
}

// ListObjects - lists all blobs in GCS bucket filtered by prefix
func (l *gcsGateway) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{
		Delimiter: delimiter,
		Prefix:    prefix,
		Versions:  false,
	})

	isTruncated := false
	nextMarker := ""
	prefixes := []string{}

	// To accommodate S3-compatible applications using
	// ListObjectsV1 to use object keys as markers to control the
	// listing of objects, we use the following encoding scheme to
	// distinguish between GCS continuation tokens and application
	// supplied markers.
	//
	// - NextMarker in ListObjectsV1 response is constructed by
	//   prefixing "{minio}" to the GCS continuation token,
	//   e.g, "{minio}CgRvYmoz"
	//
	// - Application supplied markers are used as-is to list
	//   object keys that appear after it in the lexicographical order.

	// If application is using GCS continuation token we should
	// strip the gcsTokenPrefix we added.
	gcsMarker := isGCSMarker(marker)
	if gcsMarker {
		it.PageInfo().Token = strings.TrimPrefix(marker, gcsTokenPrefix)
	}

	it.PageInfo().MaxSize = maxKeys

	objects := []minio.ObjectInfo{}
	for {
		if len(objects) >= maxKeys {
			// check if there is one next object and
			// if that one next object is our hidden
			// metadata folder, then just break
			// otherwise we've truncated the output
			attrs, _ := it.Next()
			if attrs != nil && attrs.Prefix == minio.GatewayMinioSysTmp {
				break
			}

			isTruncated = true
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logger.LogIf(ctx, err)
			return minio.ListObjectsInfo{}, gcsToObjectError(err, bucket, prefix)
		}

		nextMarker = toGCSPageToken(attrs.Name)

		if attrs.Prefix == minio.GatewayMinioSysTmp {
			// We don't return our metadata prefix.
			continue
		}
		if !strings.HasPrefix(prefix, minio.GatewayMinioSysTmp) {
			// If client lists outside gcsMinioPath then we filter out gcsMinioPath/* entries.
			// But if the client lists inside gcsMinioPath then we return the entries in gcsMinioPath/
			// which will be helpful to observe the "directory structure" for debugging purposes.
			if strings.HasPrefix(attrs.Prefix, minio.GatewayMinioSysTmp) ||
				strings.HasPrefix(attrs.Name, minio.GatewayMinioSysTmp) {
				continue
			}
		}
		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
			continue
		}
		if !gcsMarker && attrs.Name <= marker {
			// if user supplied a marker don't append
			// objects until we reach marker (and skip it).
			continue
		}

		objects = append(objects, minio.ObjectInfo{
			Name:            attrs.Name,
			Bucket:          attrs.Bucket,
			ModTime:         attrs.Updated,
			Size:            attrs.Size,
			ETag:            minio.ToS3ETag(fmt.Sprintf("%d", attrs.CRC32C)),
			UserDefined:     attrs.Metadata,
			ContentType:     attrs.ContentType,
			ContentEncoding: attrs.ContentEncoding,
		})
	}

	return minio.ListObjectsInfo{
		IsTruncated: isTruncated,
		NextMarker:  gcsTokenPrefix + nextMarker,
		Prefixes:    prefixes,
		Objects:     objects,
	}, nil
}

// ListObjectsV2 - lists all blobs in GCS bucket filtered by prefix
func (l *gcsGateway) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error) {
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{
		Delimiter: delimiter,
		Prefix:    prefix,
		Versions:  false,
	})

	isTruncated := false
	it.PageInfo().MaxSize = maxKeys

	if continuationToken != "" {
		// If client sends continuationToken, set it
		it.PageInfo().Token = continuationToken
	} else {
		// else set the continuationToken to return
		continuationToken = it.PageInfo().Token
		if continuationToken != "" {
			// If GCS SDK sets continuationToken, it means there are more than maxKeys in the current page
			// and the response will be truncated
			isTruncated = true
		}
	}

	var prefixes []string
	var objects []minio.ObjectInfo

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			logger.LogIf(ctx, err)
			return minio.ListObjectsV2Info{}, gcsToObjectError(err, bucket, prefix)
		}

		if attrs.Prefix == minio.GatewayMinioSysTmp {
			// We don't return our metadata prefix.
			continue
		}
		if !strings.HasPrefix(prefix, minio.GatewayMinioSysTmp) {
			// If client lists outside gcsMinioPath then we filter out gcsMinioPath/* entries.
			// But if the client lists inside gcsMinioPath then we return the entries in gcsMinioPath/
			// which will be helpful to observe the "directory structure" for debugging purposes.
			if strings.HasPrefix(attrs.Prefix, minio.GatewayMinioSysTmp) ||
				strings.HasPrefix(attrs.Name, minio.GatewayMinioSysTmp) {
				continue
			}
		}

		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
			continue
		}

		objects = append(objects, fromGCSAttrsToObjectInfo(attrs))
	}

	return minio.ListObjectsV2Info{
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: continuationToken,
		Prefixes:              prefixes,
		Objects:               objects,
	}, nil
}

// GetObject - reads an object from GCS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *gcsGateway) GetObject(ctx context.Context, bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string, objInfo minio.ObjectInfo) error {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		logger.LogIf(ctx, err)
		return gcsToObjectError(err, bucket)
	}

	object := l.client.Bucket(bucket).Object(key)
	r, err := object.NewRangeReader(l.ctx, startOffset, length)
	if err != nil {
		logger.LogIf(ctx, err)
		return gcsToObjectError(err, bucket, key)
	}
	defer r.Close()

	if _, err := io.Copy(writer, r); err != nil {
		logger.LogIf(ctx, err)
		return gcsToObjectError(err, bucket, key)
	}

	return nil
}

// fromGCSAttrsToObjectInfo converts GCS BucketAttrs to gateway ObjectInfo
func fromGCSAttrsToObjectInfo(attrs *storage.ObjectAttrs) minio.ObjectInfo {
	// All google cloud storage objects have a CRC32c hash, whereas composite objects may not have a MD5 hash
	// Refer https://cloud.google.com/storage/docs/hashes-etags. Use CRC32C for ETag
	metadata := make(map[string]string)
	for k, v := range attrs.Metadata {
		metadata[k] = v
	}
	if attrs.ContentType != "" {
		metadata["content-type"] = attrs.ContentType
	}
	if attrs.ContentEncoding != "" {
		metadata["content-encoding"] = attrs.ContentEncoding
	}
	if attrs.CacheControl != "" {
		metadata["cache-control"] = attrs.CacheControl
	}
	if attrs.ContentDisposition != "" {
		metadata["content-disposition"] = attrs.ContentDisposition
	}
	if attrs.ContentLanguage != "" {
		metadata["content-language"] = attrs.ContentLanguage
	}
	return minio.ObjectInfo{
		Name:            attrs.Name,
		Bucket:          attrs.Bucket,
		ModTime:         attrs.Updated,
		Size:            attrs.Size,
		ETag:            minio.ToS3ETag(fmt.Sprintf("%d", attrs.CRC32C)),
		UserDefined:     metadata,
		ContentType:     attrs.ContentType,
		ContentEncoding: attrs.ContentEncoding,
	}
}

// applyMetadataToGCSAttrs applies metadata to a GCS ObjectAttrs instance
func applyMetadataToGCSAttrs(metadata map[string]string, attrs *storage.ObjectAttrs) {
	attrs.ContentType = metadata["content-type"]
	attrs.ContentEncoding = metadata["content-encoding"]
	attrs.CacheControl = metadata["cache-control"]
	attrs.ContentDisposition = metadata["content-disposition"]
	attrs.ContentLanguage = metadata["content-language"]

	attrs.Metadata = make(map[string]string)
	for k, v := range metadata {
		attrs.Metadata[k] = v
	}
	// Filter metadata which is stored as a unique attribute
	for _, key := range []string{
		"content-type", "content-encoding", "cache-control", "content-disposition", "content-language",
	} {
		delete(attrs.Metadata, key)
	}
}

// GetObjectInfo - reads object info and replies back ObjectInfo
func (l *gcsGateway) GetObjectInfo(ctx context.Context, bucket string, object string) (minio.ObjectInfo, error) {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket)
	}

	attrs, err := l.client.Bucket(bucket).Object(object).Attrs(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, object)
	}

	return fromGCSAttrsToObjectInfo(attrs), nil
}

// PutObject - Create a new object with the incoming data,
func (l *gcsGateway) PutObject(ctx context.Context, bucket string, key string, data *hash.Reader, metadata map[string]string) (minio.ObjectInfo, error) {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket)
	}

	object := l.client.Bucket(bucket).Object(key)

	w := object.NewWriter(l.ctx)
	// Disable "chunked" uploading in GCS client if the size of the data to be uploaded is below
	// the current chunk-size of the writer. This avoids an unnecessary memory allocation.
	if data.Size() < int64(w.ChunkSize) {
		w.ChunkSize = 0
	}
	applyMetadataToGCSAttrs(metadata, &w.ObjectAttrs)

	if _, err := io.Copy(w, data); err != nil {
		// Close the object writer upon error.
		w.CloseWithError(err)
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key)
	}

	// Close the object writer upon success.
	w.Close()

	attrs, err := object.Attrs(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key)
	}

	return fromGCSAttrsToObjectInfo(attrs), nil
}

// CopyObject - Copies a blob from source container to destination container.
func (l *gcsGateway) CopyObject(ctx context.Context, srcBucket string, srcObject string, destBucket string, destObject string,
	srcInfo minio.ObjectInfo) (minio.ObjectInfo, error) {

	src := l.client.Bucket(srcBucket).Object(srcObject)
	dst := l.client.Bucket(destBucket).Object(destObject)

	copier := dst.CopierFrom(src)
	applyMetadataToGCSAttrs(srcInfo.UserDefined, &copier.ObjectAttrs)

	attrs, err := copier.Run(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, destBucket, destObject)
	}

	return fromGCSAttrsToObjectInfo(attrs), nil
}

// DeleteObject - Deletes a blob in bucket
func (l *gcsGateway) DeleteObject(ctx context.Context, bucket string, object string) error {
	err := l.client.Bucket(bucket).Object(object).Delete(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return gcsToObjectError(err, bucket, object)
	}

	return nil
}

// NewMultipartUpload - upload object in multiple parts
func (l *gcsGateway) NewMultipartUpload(ctx context.Context, bucket string, key string, metadata map[string]string) (uploadID string, err error) {
	// generate new uploadid
	uploadID = minio.MustGetUUID()

	// generate name for part zero
	meta := gcsMultipartMetaName(uploadID)

	w := l.client.Bucket(bucket).Object(meta).NewWriter(l.ctx)
	defer w.Close()

	applyMetadataToGCSAttrs(metadata, &w.ObjectAttrs)

	if err = json.NewEncoder(w).Encode(gcsMultipartMetaV1{
		gcsMinioMultipartMetaCurrentVersion,
		bucket,
		key,
	}); err != nil {
		logger.LogIf(ctx, err)
		return "", gcsToObjectError(err, bucket, key)
	}
	return uploadID, nil
}

// ListMultipartUploads - lists all multipart uploads.
func (l *gcsGateway) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (minio.ListMultipartsInfo, error) {
	return minio.ListMultipartsInfo{
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		Prefix:         prefix,
		Delimiter:      delimiter,
	}, nil
}

// Checks if minio.sys.tmp/multipart/v1/<upload-id>/gcs.json exists, returns
// an object layer compatible error upon any error.
func (l *gcsGateway) checkUploadIDExists(ctx context.Context, bucket string, key string, uploadID string) error {
	_, err := l.client.Bucket(bucket).Object(gcsMultipartMetaName(uploadID)).Attrs(l.ctx)
	logger.LogIf(ctx, err)
	return gcsToObjectError(err, bucket, key, uploadID)
}

// PutObjectPart puts a part of object in bucket
func (l *gcsGateway) PutObjectPart(ctx context.Context, bucket string, key string, uploadID string, partNumber int, data *hash.Reader) (minio.PartInfo, error) {
	if err := l.checkUploadIDExists(ctx, bucket, key, uploadID); err != nil {
		return minio.PartInfo{}, err
	}
	etag := data.MD5HexString()
	if etag == "" {
		// Generate random ETag.
		etag = minio.GenETag()
	}
	object := l.client.Bucket(bucket).Object(gcsMultipartDataName(uploadID, partNumber, etag))
	w := object.NewWriter(l.ctx)
	// Disable "chunked" uploading in GCS client. If enabled, it can cause a corner case
	// where it tries to upload 0 bytes in the last chunk and get error from server.
	w.ChunkSize = 0
	if _, err := io.Copy(w, data); err != nil {
		// Make sure to close object writer upon error.
		w.Close()
		logger.LogIf(ctx, err)
		return minio.PartInfo{}, gcsToObjectError(err, bucket, key)
	}
	// Make sure to close the object writer upon success.
	w.Close()
	return minio.PartInfo{
		PartNumber:   partNumber,
		ETag:         etag,
		LastModified: minio.UTCNow(),
		Size:         data.Size(),
	}, nil

}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *gcsGateway) ListObjectParts(ctx context.Context, bucket string, key string, uploadID string, partNumberMarker int, maxParts int) (minio.ListPartsInfo, error) {
	return minio.ListPartsInfo{}, l.checkUploadIDExists(ctx, bucket, key, uploadID)
}

// Called by AbortMultipartUpload and CompleteMultipartUpload for cleaning up.
func (l *gcsGateway) cleanupMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	prefix := fmt.Sprintf("%s/%s/", gcsMinioMultipartPathV1, uploadID)

	// iterate through all parts and delete them
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Prefix: prefix, Versions: false})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logger.LogIf(ctx, err)
			return gcsToObjectError(err, bucket, key)
		}

		object := l.client.Bucket(bucket).Object(attrs.Name)
		// Ignore the error as parallel AbortMultipartUpload might have deleted it.
		object.Delete(l.ctx)
	}

	return nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *gcsGateway) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) error {
	if err := l.checkUploadIDExists(ctx, bucket, key, uploadID); err != nil {
		return err
	}
	return l.cleanupMultipartUpload(ctx, bucket, key, uploadID)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
// Note that there is a limit (currently 32) to the number of components that can
// be composed in a single operation. There is a limit (currently 1024) to the total
// number of components for a given composite object. This means you can append to
// each object at most 1023 times. There is a per-project rate limit (currently 200)
// to the number of components you can compose per second. This rate counts both the
// components being appended to a composite object as well as the components being
// copied when the composite object of which they are a part is copied.
func (l *gcsGateway) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadID string, uploadedParts []minio.CompletePart) (minio.ObjectInfo, error) {
	meta := gcsMultipartMetaName(uploadID)
	object := l.client.Bucket(bucket).Object(meta)

	partZeroAttrs, err := object.Attrs(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key, uploadID)
	}

	r, err := object.NewReader(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key)
	}
	defer r.Close()

	// Check version compatibility of the meta file before compose()
	multipartMeta := gcsMultipartMetaV1{}
	if err = json.NewDecoder(r).Decode(&multipartMeta); err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key)
	}

	if multipartMeta.Version != gcsMinioMultipartMetaCurrentVersion {
		logger.LogIf(ctx, errGCSFormat)
		return minio.ObjectInfo{}, gcsToObjectError(errGCSFormat, bucket, key)
	}

	// Validate if the gcs.json stores valid entries for the bucket and key.
	if multipartMeta.Bucket != bucket || multipartMeta.Object != key {
		return minio.ObjectInfo{}, gcsToObjectError(minio.InvalidUploadID{
			UploadID: uploadID,
		}, bucket, key)
	}

	var parts []*storage.ObjectHandle
	partSizes := make([]int64, len(uploadedParts))
	for i, uploadedPart := range uploadedParts {
		parts = append(parts, l.client.Bucket(bucket).Object(gcsMultipartDataName(uploadID,
			uploadedPart.PartNumber, uploadedPart.ETag)))
		partAttr, pErr := l.client.Bucket(bucket).Object(gcsMultipartDataName(uploadID, uploadedPart.PartNumber, uploadedPart.ETag)).Attrs(l.ctx)
		if pErr != nil {
			logger.LogIf(ctx, pErr)
			return minio.ObjectInfo{}, gcsToObjectError(pErr, bucket, key, uploadID)
		}
		partSizes[i] = partAttr.Size
	}

	// Error out if parts except last part sizing < 5MiB.
	for i, size := range partSizes[:len(partSizes)-1] {
		if size < 5*humanize.MiByte {
			logger.LogIf(ctx, minio.PartTooSmall{
				PartNumber: uploadedParts[i].PartNumber,
				PartSize:   size,
				PartETag:   uploadedParts[i].ETag,
			})
			return minio.ObjectInfo{}, minio.PartTooSmall{
				PartNumber: uploadedParts[i].PartNumber,
				PartSize:   size,
				PartETag:   uploadedParts[i].ETag,
			}
		}
	}

	// Returns name of the composed object.
	gcsMultipartComposeName := func(uploadID string, composeNumber int) string {
		return fmt.Sprintf("%s/tmp/%s/composed-object-%05d", minio.GatewayMinioSysTmp, uploadID, composeNumber)
	}

	composeCount := int(math.Ceil(float64(len(parts)) / float64(gcsMaxComponents)))
	if composeCount > 1 {
		// Create composes of every 32 parts.
		composeParts := make([]*storage.ObjectHandle, composeCount)
		for i := 0; i < composeCount; i++ {
			// Create 'composed-object-N' using next 32 parts.
			composeParts[i] = l.client.Bucket(bucket).Object(gcsMultipartComposeName(uploadID, i))
			start := i * gcsMaxComponents
			end := start + gcsMaxComponents
			if end > len(parts) {
				end = len(parts)
			}

			composer := composeParts[i].ComposerFrom(parts[start:end]...)
			composer.ContentType = partZeroAttrs.ContentType
			composer.Metadata = partZeroAttrs.Metadata

			if _, err = composer.Run(l.ctx); err != nil {
				logger.LogIf(ctx, err)
				return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key)
			}
		}

		// As composes are successfully created, final object needs to be created using composes.
		parts = composeParts
	}

	composer := l.client.Bucket(bucket).Object(key).ComposerFrom(parts...)
	composer.ContentType = partZeroAttrs.ContentType
	composer.ContentEncoding = partZeroAttrs.ContentEncoding
	composer.CacheControl = partZeroAttrs.CacheControl
	composer.ContentDisposition = partZeroAttrs.ContentDisposition
	composer.ContentLanguage = partZeroAttrs.ContentLanguage
	composer.Metadata = partZeroAttrs.Metadata
	attrs, err := composer.Run(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key)
	}
	if err = l.cleanupMultipartUpload(ctx, bucket, key, uploadID); err != nil {
		return minio.ObjectInfo{}, gcsToObjectError(err, bucket, key)
	}
	return fromGCSAttrsToObjectInfo(attrs), nil
}

// SetBucketPolicy - Set policy on bucket
func (l *gcsGateway) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	policyInfo, err := minio.PolicyToBucketAccessPolicy(bucketPolicy)
	if err != nil {
		// This should not happen.
		logger.LogIf(ctx, err)
		return gcsToObjectError(err, bucket)
	}

	var policies []minio.BucketAccessPolicy
	for prefix, policy := range miniogopolicy.GetPolicies(policyInfo.Statements, bucket, "") {
		policies = append(policies, minio.BucketAccessPolicy{
			Prefix: prefix,
			Policy: policy,
		})
	}

	prefix := bucket + "/*" // For all objects inside the bucket.

	if len(policies) != 1 {
		logger.LogIf(ctx, minio.NotImplemented{})
		return minio.NotImplemented{}
	}
	if policies[0].Prefix != prefix {
		logger.LogIf(ctx, minio.NotImplemented{})
		return minio.NotImplemented{}
	}

	acl := l.client.Bucket(bucket).ACL()
	if policies[0].Policy == miniogopolicy.BucketPolicyNone {
		if err := acl.Delete(l.ctx, storage.AllUsers); err != nil {
			logger.LogIf(ctx, err)
			return gcsToObjectError(err, bucket)
		}
		return nil
	}

	var role storage.ACLRole
	switch policies[0].Policy {
	case miniogopolicy.BucketPolicyReadOnly:
		role = storage.RoleReader
	case miniogopolicy.BucketPolicyWriteOnly:
		role = storage.RoleWriter
	default:
		logger.LogIf(ctx, minio.NotImplemented{})
		return minio.NotImplemented{}
	}

	if err := acl.Set(l.ctx, storage.AllUsers, role); err != nil {
		logger.LogIf(ctx, err)
		return gcsToObjectError(err, bucket)
	}

	return nil
}

// GetBucketPolicy - Get policy on bucket
func (l *gcsGateway) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	rules, err := l.client.Bucket(bucket).ACL().List(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, gcsToObjectError(err, bucket)
	}

	var readOnly, writeOnly bool
	for _, r := range rules {
		if r.Entity != storage.AllUsers || r.Role == storage.RoleOwner {
			continue
		}

		switch r.Role {
		case storage.RoleReader:
			readOnly = true
		case storage.RoleWriter:
			writeOnly = true
		}
	}

	actionSet := policy.NewActionSet()
	if readOnly {
		actionSet.Add(policy.GetBucketLocationAction)
		actionSet.Add(policy.ListBucketAction)
		actionSet.Add(policy.GetObjectAction)
	}
	if writeOnly {
		actionSet.Add(policy.GetBucketLocationAction)
		actionSet.Add(policy.ListBucketMultipartUploadsAction)
		actionSet.Add(policy.AbortMultipartUploadAction)
		actionSet.Add(policy.DeleteObjectAction)
		actionSet.Add(policy.ListMultipartUploadPartsAction)
		actionSet.Add(policy.PutObjectAction)
	}

	// Return NoSuchBucketPolicy error, when policy is not set
	if len(actionSet) == 0 {
		logger.LogIf(ctx, minio.BucketPolicyNotFound{})
		return nil, gcsToObjectError(minio.BucketPolicyNotFound{}, bucket)
	}

	return &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				actionSet,
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}, nil
}

// DeleteBucketPolicy - Delete all policies on bucket
func (l *gcsGateway) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	// This only removes the storage.AllUsers policies
	if err := l.client.Bucket(bucket).ACL().Delete(l.ctx, storage.AllUsers); err != nil {
		logger.LogIf(ctx, err)
		return gcsToObjectError(err, bucket)
	}

	return nil
}
