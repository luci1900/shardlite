package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// writeMeta is the user metadata key that holds an object's write id (S3 sends
// it as x-amz-meta-shardlite-write; keys come back lowercase).
const writeMeta = "shardlite-write"

// S3Options configure an S3 backend.
type S3Options struct {
	Bucket string
	// Endpoint is the URL of an S3-compatible store or emulator. AWS if empty.
	Endpoint string
	// HTTPClient, if set, replaces the default client (tests put a fault-injecting
	// transport behind it).
	HTTPClient *http.Client
}

// S3 is a Backend over the S3 API (AWS, and stores that speak it with conditional
// writes: R2, Tigris, MinIO and others). Region and credentials come from the SDK's
// standard chain (AWS_REGION, AWS_ACCESS_KEY_ID, profiles, IRSA and so on).
//
// The SDK's own retries are off: blob.Store owns them, since it reads write ids
// back after an unknown outcome. Addressing is always path style, which
// emulators and self-hosted stores need and AWS still accepts.
type S3 struct {
	c      *s3.Client
	bucket string
	region string
}

// NewS3 builds the client. It makes no request.
func NewS3(ctx context.Context, o S3Options) (*S3, error) {
	if o.Bucket == "" {
		return nil, errors.New("blob: an S3 bucket is required")
	}
	loadOpts := []func(*config.LoadOptions) error{
		config.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	}
	if o.HTTPClient != nil {
		loadOpts = append(loadOpts, config.WithHTTPClient(o.HTTPClient))
	} else {
		loadOpts = append(loadOpts, config.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			t.MaxIdleConnsPerHost = 256
			t.ResponseHeaderTimeout = 30 * time.Second
		})))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("blob: loading the AWS configuration: %w", err)
	}
	if cfg.Region == "" {
		if o.Endpoint == "" {
			return nil, errors.New("blob: no AWS region: set AWS_REGION")
		}
		cfg.Region = "us-east-1" // stores behind a custom endpoint mostly ignore it
	}
	c := s3.NewFromConfig(cfg, func(so *s3.Options) {
		so.UsePathStyle = true
		if o.Endpoint != "" {
			so.BaseEndpoint = aws.String(o.Endpoint)
		}
		// The SDK's default checksums break many compatible stores.
		so.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		so.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &S3{c: c, bucket: o.Bucket, region: cfg.Region}, nil
}

// EnsureBucket creates the bucket if it doesn't exist (and does nothing if it
// does, whoever owns it: a bucket owned by someone else fails on the first
// request that uses it). Only tests use it: the node never creates buckets.
func (s *S3) EnsureBucket(ctx context.Context) error {
	if _, err := s.c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)}); err == nil {
		return nil
	}
	in := &s3.CreateBucketInput{Bucket: aws.String(s.bucket)}
	if s.region != "us-east-1" {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraint(s.region)}
	}
	_, err := s.c.CreateBucket(ctx, in)
	var ae smithy.APIError
	if errors.As(err, &ae) && (ae.ErrorCode() == "BucketAlreadyOwnedByYou" || ae.ErrorCode() == "BucketAlreadyExists") {
		return nil // created meanwhile
	}
	if err != nil {
		return fmt.Errorf("blob: creating the bucket %q: %w", s.bucket, err)
	}
	return nil
}

// encodeWriteID is the metadata value: "epoch.n.node", the node escaped so the
// value stays plain ASCII.
func encodeWriteID(id WriteID) string {
	return strconv.FormatUint(id.Epoch, 10) + "." + strconv.FormatUint(id.N, 10) + "." + url.QueryEscape(id.Node)
}

func decodeWriteID(v string) WriteID {
	parts := strings.SplitN(v, ".", 3)
	if len(parts) != 3 {
		return WriteID{}
	}
	epoch, err1 := strconv.ParseUint(parts[0], 10, 64)
	n, err2 := strconv.ParseUint(parts[1], 10, 64)
	node, err3 := url.QueryUnescape(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return WriteID{}
	}
	return WriteID{Node: node, Epoch: epoch, N: n}
}

// s3ETag strips the quotes S3 puts around ETags, the same way everywhere.
// ETags are opaque to callers.
func s3ETag(e *string) ETag {
	if e == nil {
		return ""
	}
	return ETag(strings.Trim(strings.TrimPrefix(*e, "W/"), `"`))
}

func quoteETag(e ETag) *string { return aws.String(`"` + string(e) + `"`) }

// sent records whether a request was written out in full, so that a failure
// without a response can be told from one that may have reached the store.
type sent struct{ ok atomic.Bool }

func (s *sent) trace(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(i httptrace.WroteRequestInfo) {
			if i.Err == nil {
				s.ok.Store(true)
			}
		},
	})
}

// outcome says what a failed call means for a write or a read.
type kind int

const (
	kindCreate kind = iota
	kindUpdate
	kindRead
	kindDelete
)

// classify maps the SDK's error onto the outcomes of a Backend (docs/internals.md,
// Coordination):
//
//   - 412 is ErrExists for a create and ErrConflict for an update; 404 is ErrNotFound;
//   - 409 ConditionalRequestConflict (two conditional writes racing), 429 and
//     503 SlowDown are ErrNotApplied: the store did nothing, so the retry sees the real
//     answer. Any other 503 (ServiceUnavailable, or no code) is ErrUnknown: RustFS
//     answers ServiceUnavailable for a write that missed its quorum, which can
//     follow a partly applied commit whose rollback was incomplete;
//   - 500 and other server errors, timeouts, truncated responses and dropped
//     connections after the request was sent are ErrUnknown;
//   - a request that was never fully sent (connection refused, DNS, TLS, a context
//     that ended first) is ErrNotApplied, and one that was fully sent when the
//     context ended is ErrUnknown;
//   - other 4xx (access denied, bad request) are plain errors, which are not retried.
func classify(err error, k kind, wasSent bool) error {
	if err == nil {
		return nil
	}
	var se *smithyhttp.RequestSendError
	if errors.As(err, &se) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if k == kindRead {
			// Nothing to have changed: reads simply retry. A cancelled context is the caller's.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return fmt.Errorf("%w: %v", ErrNotApplied, err)
		}
		// A cancelled context or a passed deadline is unknown only if the request was
		// fully written; otherwise the store never had it. Either way the context's
		// own error stays findable.
		if wasSent {
			return fmt.Errorf("%w: %w", ErrUnknown, err)
		}
		return fmt.Errorf("%w: %w", ErrNotApplied, err)
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		status := re.HTTPStatusCode()
		code := ""
		var ae smithy.APIError
		if errors.As(err, &ae) {
			code = ae.ErrorCode()
		}
		switch {
		case code == "NoSuchBucket", code == "AccessDenied", code == "InvalidAccessKeyId", code == "SignatureDoesNotMatch":
			return fmt.Errorf("blob: s3: %w", err) // the setup is wrong: retrying won't help
		case status == http.StatusNotFound:
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		case status == http.StatusPreconditionFailed:
			if k == kindCreate {
				return fmt.Errorf("%w: %v", ErrExists, err)
			}
			return fmt.Errorf("%w: %v", ErrConflict, err)
		case status == http.StatusConflict && code == "ConditionalRequestConflict",
			status == http.StatusTooManyRequests,
			status == http.StatusServiceUnavailable && code == "SlowDown":
			return fmt.Errorf("%w: %v", ErrNotApplied, err)
		case status == http.StatusRequestTimeout, code == "RequestTimeout", status >= 500, status < 400:
			// (a status under 400 here is a good response the SDK could not read)
			return fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		return fmt.Errorf("blob: s3: %w", err)
	}
	var de *smithy.DeserializationError
	if errors.As(err, &de) {
		return fmt.Errorf("%w: %v", ErrUnknown, err) // the response was cut short
	}
	return fmt.Errorf("blob: s3: %w", err)
}

// bodySize is the length of body, which a request sends from its start without
// holding it in memory (the SDK rewinds it to hash and to send). The body is
// left at its start.
func bodySize(body io.ReadSeeker) (int64, error) {
	n, err := body.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	_, err = body.Seek(0, io.SeekStart)
	return n, err
}

func (s *S3) put(ctx context.Context, k kind, key string, body io.ReadSeeker, id WriteID, in *s3.PutObjectInput) (ETag, error) {
	n, err := bodySize(body)
	if err != nil {
		return "", fmt.Errorf("%w: sizing the body: %v", ErrNotApplied, err)
	}
	in.Bucket, in.Key, in.Body = aws.String(s.bucket), aws.String(key), body
	in.ContentLength = aws.Int64(n)
	if !id.IsZero() {
		in.Metadata = map[string]string{writeMeta: encodeWriteID(id)}
	}
	var tr sent
	out, err := s.c.PutObject(tr.trace(ctx), in)
	if err != nil {
		return "", classify(err, k, tr.ok.Load())
	}
	return s3ETag(out.ETag), nil
}

// Create is a PUT with If-None-Match: *.
func (s *S3) Create(ctx context.Context, key string, body io.ReadSeeker, id WriteID) (ETag, error) {
	return s.put(ctx, kindCreate, key, body, id, &s3.PutObjectInput{IfNoneMatch: aws.String("*")})
}

// Update is a PUT with If-Match.
func (s *S3) Update(ctx context.Context, key string, body io.ReadSeeker, expected ETag, id WriteID) (ETag, error) {
	return s.put(ctx, kindUpdate, key, body, id, &s3.PutObjectInput{IfMatch: quoteETag(expected)})
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, ETag, error) {
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, "", classify(err, kindRead, false)
	}
	return out.Body, s3ETag(out.ETag), nil
}

// GetRange reads length bytes from offset (to the end of the object if length is
// not positive or reaches past it).
func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, fmt.Errorf("blob: range offset %d out of bounds", offset)
	}
	rng := fmt.Sprintf("bytes=%d-", offset)
	if length > 0 {
		rng = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Range: aws.String(rng)})
	if err != nil {
		return nil, classify(err, kindRead, false)
	}
	return out.Body, nil
}

func (s *S3) Head(ctx context.Context, key string) (ObjectInfo, error) {
	out, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return ObjectInfo{}, classify(err, kindRead, false)
	}
	info := ObjectInfo{Key: key, Size: aws.ToInt64(out.ContentLength), ETag: s3ETag(out.ETag), LastModified: aws.ToTime(out.LastModified)}
	if v, ok := out.Metadata[writeMeta]; ok {
		info.WriteID = decodeWriteID(v)
	}
	return info, nil
}

// Delete is a DELETE, with If-Match when expected is set (412 is ErrConflict). S3
// answers a delete of a missing key with success, so without a condition this
// never returns ErrNotFound (callers accept both). A conditional delete of a
// missing key is ErrNotFound where the store says so.
func (s *S3) Delete(ctx context.Context, key string, expected ETag) error {
	in := &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	if expected != "" {
		in.IfMatch = quoteETag(expected)
	}
	var tr sent
	_, err := s.c.DeleteObject(tr.trace(ctx), in)
	if err != nil {
		if cerr := classify(err, kindDelete, tr.ok.Load()); expected != "" || !errors.Is(cerr, ErrNotFound) {
			return cerr
		}
	}
	return nil
}

// List is ListObjectsV2 with pagination. Listings carry no metadata, so the
// WriteID of the objects it returns is zero.
func (s *S3) List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		p := s3.NewListObjectsV2Paginator(s.c, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				yield(ObjectInfo{}, classify(err, kindRead, false))
				return
			}
			for _, o := range page.Contents {
				info := ObjectInfo{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: s3ETag(o.ETag), LastModified: aws.ToTime(o.LastModified)}
				if !yield(info, nil) {
					return
				}
			}
		}
	}
}

var _ Backend = (*S3)(nil)
