package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
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

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/model/pos"
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
	// Logger is where the backend warns that the node's clock is off the store's
	// (the Date header of every response). The default logger if nil.
	Logger *slog.Logger
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
		so.APIOptions = append(so.APIOptions, addClockSampler(newClockCheck(o.Logger)))
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
	if ae, ok := errors.AsType[smithy.APIError](err); ok && (ae.ErrorCode() == "BucketAlreadyOwnedByYou" || ae.ErrorCode() == "BucketAlreadyExists") {
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
	return strconv.FormatUint(uint64(id.Epoch), 10) + "." + strconv.FormatUint(id.N, 10) + "." + url.QueryEscape(string(id.Node))
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
	return WriteID{Node: cluster.Node(node), Epoch: pos.Epoch(epoch), N: n}
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
//     context or a timeout of the transport's ended it is ErrUnknown;
//   - a read that fails without a response, a timeout of the transport's included,
//     is ErrNotApplied (it changed nothing), except when the caller's context
//     ended: that error is returned as it is;
//   - other 4xx (access denied, bad request) are plain errors, which are not retried.
//
// The SDK's error stays in the chain (%w), so errors.As finds its smithy.APIError.
// It holds none of the outcomes above, so wrapping it can't change how an error
// classifies. A timeout of the transport's does match context.DeadlineExceeded.
func classify(ctx context.Context, err error, k kind, wasSent bool) error {
	if err == nil {
		return nil
	}
	if _, sent := errors.AsType[*smithyhttp.RequestSendError](err); sent || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if k == kindRead {
			// Nothing to have changed: reads simply retry. The caller's context ending
			// is theirs to see, but an error that matches a deadline while the
			// caller's context is still live is the transport's own timeout.
			if ctx.Err() != nil {
				return err
			}
			return fmt.Errorf("%w: %w", ErrNotApplied, err)
		}
		// Whether the context or the transport ended it, the failure is unknown only
		// if the request was fully written; otherwise the store never had it. Either
		// way the underlying error stays findable.
		if wasSent {
			return fmt.Errorf("%w: %w", ErrUnknown, err)
		}
		return fmt.Errorf("%w: %w", ErrNotApplied, err)
	}
	if re, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		status := re.HTTPStatusCode()
		code := ""
		if ae, ok := errors.AsType[smithy.APIError](err); ok {
			code = ae.ErrorCode()
		}
		switch {
		case code == "NoSuchBucket", code == "AccessDenied", code == "InvalidAccessKeyId", code == "SignatureDoesNotMatch":
			return fmt.Errorf("blob: s3: %w", err) // the setup is wrong: retrying won't help
		case status == http.StatusNotFound:
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		case status == http.StatusPreconditionFailed:
			if k == kindCreate {
				return fmt.Errorf("%w: %w", ErrExists, err)
			}
			return fmt.Errorf("%w: %w", ErrConflict, err)
		case status == http.StatusConflict && code == "ConditionalRequestConflict",
			status == http.StatusTooManyRequests,
			status == http.StatusServiceUnavailable && code == "SlowDown":
			return fmt.Errorf("%w: %w", ErrNotApplied, err)
		case status == http.StatusRequestTimeout, code == "RequestTimeout", status >= 500, status < 400:
			// (a status under 400 here is a good response the SDK could not read)
			return fmt.Errorf("%w: %w", ErrUnknown, err)
		}
		return fmt.Errorf("blob: s3: %w", err)
	}
	if _, ok := errors.AsType[*smithy.DeserializationError](err); ok {
		return fmt.Errorf("%w: %w", ErrUnknown, err) // the response was cut short
	}
	return fmt.Errorf("blob: s3: %w", err)
}

func (s *S3) put(ctx context.Context, k kind, key string, body io.ReadSeeker, id WriteID, in *s3.PutObjectInput) (ETag, error) {
	// The SDK seeks the body for its length, and rewinds it to hash and to send.
	in.Bucket, in.Key, in.Body = aws.String(s.bucket), aws.String(key), body
	if !id.IsZero() {
		in.Metadata = map[string]string{writeMeta: encodeWriteID(id)}
	}
	var tr sent
	out, err := s.c.PutObject(tr.trace(ctx), in)
	if err != nil {
		return "", classify(ctx, err, k, tr.ok.Load())
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
		return nil, "", classify(ctx, err, kindRead, false)
	}
	return out.Body, s3ETag(out.ETag), nil
}

// GetRange reads length bytes from offset (to the end of the object if length is
// not positive or reaches past it). A negative offset asks for the last
// -offset bytes, which the store serves as a suffix range.
func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		if length != 0 {
			return nil, fmt.Errorf("blob: a range from the end takes no length (got %d)", length)
		}
		return s.getSuffix(ctx, key, -offset)
	}
	if offset == 0 && length <= 0 {
		// The whole object, asked for with no range: a range of an empty object
		// is one the store cannot serve (416).
		rc, _, err := s.Get(ctx, key)
		return rc, err
	}
	rng := fmt.Sprintf("bytes=%d-", offset)
	if length > 0 {
		rng = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Range: aws.String(rng)})
	if err != nil {
		return nil, classify(ctx, err, kindRead, false)
	}
	return out.Body, nil
}

// getSuffix reads the last n bytes of an object. A store that answers a suffix
// range with a 416 may not serve them, so the object's size is asked for and
// the same bytes are read from the front instead. An empty object has no last
// bytes, which is the one thing a suffix range cannot have: it is an empty
// read.
func (s *S3) getSuffix(ctx context.Context, key string, n int64) (io.ReadCloser, error) {
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Range: aws.String(fmt.Sprintf("bytes=-%d", n))})
	if err == nil {
		return out.Body, nil
	}
	if !rangeNotSatisfiable(err) {
		return nil, classify(ctx, err, kindRead, false)
	}
	info, herr := s.Head(ctx, key)
	if herr != nil {
		return nil, herr
	}
	if info.Size == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	start := max(info.Size-n, 0)
	return s.GetRange(ctx, key, start, info.Size-start)
}

// rangeNotSatisfiable reports whether err is the store's answer to a range it
// cannot serve (416).
func rangeNotSatisfiable(err error) bool {
	re, ok := errors.AsType[*awshttp.ResponseError](err)
	return ok && re.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable
}

func (s *S3) Head(ctx context.Context, key string) (ObjectInfo, error) {
	out, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return ObjectInfo{}, classify(ctx, err, kindRead, false)
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
		if cerr := classify(ctx, err, kindDelete, tr.ok.Load()); expected != "" || !errors.Is(cerr, ErrNotFound) {
			return cerr
		}
	}
	return nil
}

// List is ListObjectsV2 with pagination, from StartAfter. Listings carry no
// metadata, so the WriteID of the objects it returns is zero.
func (s *S3) List(ctx context.Context, prefix, after string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		in := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)}
		if after != "" {
			in.StartAfter = aws.String(after)
		}
		p := s3.NewListObjectsV2Paginator(s.c, in)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				yield(ObjectInfo{}, classify(ctx, err, kindRead, false))
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
