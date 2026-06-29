package gateway

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// s3Error is one S3 error response: the standard code, the HTTP status, and
// a message. Handlers return these (or meta errors, which mapError
// translates) and the writers render the XML envelope.
type s3Error struct {
	Code    string
	Status  int
	Message string
}

func (e *s3Error) Error() string { return e.Code + ": " + e.Message }

var (
	errMethodNotAllowed = &s3Error{"MethodNotAllowed", http.StatusMethodNotAllowed,
		"The specified method is not allowed against this resource."}
	errNotImplemented = &s3Error{"NotImplemented", http.StatusNotImplemented,
		"This operation is not implemented yet."}
	errInvalidObjectName = &s3Error{"InvalidObjectName", http.StatusBadRequest,
		"Object keys must not contain the NUL byte."}
	errKeyTooLong = &s3Error{"KeyTooLongError", http.StatusBadRequest,
		"Object keys may be at most 1024 bytes."}
	errContentSHA256Mismatch = &s3Error{"XAmzContentSHA256Mismatch", http.StatusBadRequest,
		"The provided x-amz-content-sha256 does not match what was computed."}
	errInternal = &s3Error{"InternalError", http.StatusInternalServerError,
		"An internal error occurred."}
	errNoSuchKey = &s3Error{"NoSuchKey", http.StatusNotFound,
		"The specified key does not exist."}
	errInvalidArgument = &s3Error{"InvalidArgument", http.StatusBadRequest,
		"Invalid request argument."}
	errMalformedXML = &s3Error{"MalformedXML", http.StatusBadRequest,
		"The XML you provided was not well-formed or did not validate against the schema."}
	errInvalidRequest = &s3Error{"InvalidRequest", http.StatusBadRequest,
		"Copying an object to itself requires the REPLACE metadata directive."}
	errInvalidDigest = &s3Error{"InvalidDigest", http.StatusBadRequest,
		"The Content-MD5 you specified is not a valid base64 MD5 digest."}
	errBadDigest = &s3Error{"BadDigest", http.StatusBadRequest,
		"The Content-MD5 you specified did not match what we received."}
	errEntityTooLarge = &s3Error{"EntityTooLarge", http.StatusBadRequest,
		"The payload exceeds the maximum size this operation allows."}
	errMissingContentLength = &s3Error{"MissingContentLength", http.StatusLengthRequired,
		"You must provide the Content-Length HTTP header."}
	errObjectLockNotFound = &s3Error{"ObjectLockConfigurationNotFoundError", http.StatusNotFound,
		"Object Lock configuration does not exist for this bucket."}
	errObjectLockNotEnabled = &s3Error{"InvalidRequest", http.StatusBadRequest,
		"Object Lock configuration cannot be set on a bucket where it is not enabled; enable object lock when creating the bucket."}
	errNoRetention = &s3Error{"NoSuchObjectLockConfiguration", http.StatusNotFound,
		"The specified object does not have a retention configuration."}
	errInvalidSSE = &s3Error{"InvalidArgument", http.StatusBadRequest,
		"The server-side encryption value is invalid; this server supports only AES256 (SSE-S3)."}
	errSSEKMSNotSupported = &s3Error{"NotImplemented", http.StatusNotImplemented,
		"SSE-KMS is not supported; this server provides SSE-S3 (AES256)."}
	errSSECNotSupported = &s3Error{"NotImplemented", http.StatusNotImplemented,
		"SSE-C (customer-provided encryption keys) is not supported."}
	errSSENotEnabled = &s3Error{"NotImplemented", http.StatusNotImplemented,
		"Server-side encryption is not enabled on this server; encryption at rest is a cluster feature an operator turns on (cluster encrypt)."}
)

// ErrNoSuchKey is the missing-key miss, exported for object backends.
var ErrNoSuchKey error = errNoSuchKey

// ErrUnavailable is the backend saying "not now, try again": the cluster
// is below its durability floor, or this node cannot commit (it is not
// the Raft leader and v0.3 does not forward proposals). Mapped to the S3
// SlowDown family (503) — clients retry it.
var ErrUnavailable = errors.New("gateway: service temporarily unavailable")

// ErrTooManyRequests is the backend saying "at capacity, retry": the node's
// adaptive load shedder refused a new request at admission (ADR-0039 part 4).
// Mapped to HTTP 429 Too Many Requests with Retry-After — kept distinct from
// ErrUnavailable's 503 SlowDown, which means "cannot write safely right now"
// (the durability floor or a non-leader), a different condition from "at
// capacity." A 429 is always safe and fully retryable.
var ErrTooManyRequests = errors.New("gateway: too many requests; retry")

// shedRetryAfterSeconds is the Retry-After a 429 advertises: a short backoff,
// since a shed clears as in-flight operations drain (often within a second).
const shedRetryAfterSeconds = 1

// mapError translates a metadata-layer error into its S3 wire form.
func mapError(err error) *s3Error {
	var s3e *s3Error
	if errors.As(err, &s3e) {
		return s3e
	}
	switch {
	case errors.Is(err, ErrTooManyRequests):
		return &s3Error{"TooManyRequests", http.StatusTooManyRequests, "The node is at capacity; retry."}
	case errors.Is(err, ErrUnavailable):
		return &s3Error{"SlowDown", http.StatusServiceUnavailable, "The service is temporarily unable to commit writes; retry."}
	case errors.Is(err, meta.ErrNoSuchBucket):
		return &s3Error{"NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist."}
	case errors.Is(err, meta.ErrBucketExists):
		return &s3Error{"BucketAlreadyOwnedByYou", http.StatusConflict, "The bucket already exists."}
	case errors.Is(err, meta.ErrBucketNotEmpty):
		return &s3Error{"BucketNotEmpty", http.StatusConflict, "The bucket is not empty."}
	case errors.Is(err, meta.ErrInvalidBucketName):
		return &s3Error{"InvalidBucketName", http.StatusBadRequest, "The bucket name is not valid."}
	case errors.Is(err, meta.ErrInvalidObjectKey):
		return errInvalidObjectName
	case errors.Is(err, meta.ErrNoSuchVersion):
		return &s3Error{"NoSuchVersion", http.StatusNotFound, "The specified version does not exist."}
	case errors.Is(err, meta.ErrInvalidVersioningState):
		return &s3Error{"InvalidBucketState", http.StatusConflict,
			"The versioning state cannot be changed: an object lock configuration is present, or the requested state is invalid."}
	case errors.Is(err, meta.ErrObjectLocked):
		return &s3Error{"AccessDenied", http.StatusForbidden, "The object is protected by object lock."}
	case errors.Is(err, meta.ErrInvalidRetention):
		return &s3Error{"InvalidRequest", http.StatusBadRequest, "The object lock retention is not valid."}
	case errors.Is(err, meta.ErrNoSuchUpload):
		return &s3Error{"NoSuchUpload", http.StatusNotFound,
			"The specified multipart upload does not exist."}
	case errors.Is(err, meta.ErrInvalidPartNumber):
		return &s3Error{"InvalidArgument", http.StatusBadRequest,
			"Part numbers must be integers between 1 and 10000."}
	case errors.Is(err, meta.ErrInvalidPart):
		return &s3Error{"InvalidPart", http.StatusBadRequest,
			"One or more of the specified parts could not be found, or an ETag did not match."}
	case errors.Is(err, meta.ErrInvalidPartOrder):
		return &s3Error{"InvalidPartOrder", http.StatusBadRequest,
			"The list of parts was not in ascending part number order."}
	case errors.Is(err, meta.ErrPartTooSmall):
		return &s3Error{"EntityTooSmall", http.StatusBadRequest,
			"Each part except the last must be at least 5 MiB."}
	default:
		return errInternal
	}
}

// mapAuthError translates a sigv4 verification error. Codes follow what
// real S3 returns for each failure, because SDK retry logic switches on
// them (S3-API.md).
func mapAuthError(err error) *s3Error {
	switch {
	case errors.Is(err, sigv4.ErrMissingAuthentication):
		return &s3Error{"AccessDenied", http.StatusForbidden, "Anonymous access is disabled."}
	case errors.Is(err, sigv4.ErrUnknownAccessKey):
		return &s3Error{"InvalidAccessKeyId", http.StatusForbidden, "The access key ID does not exist."}
	case errors.Is(err, sigv4.ErrSignatureMismatch):
		return &s3Error{"SignatureDoesNotMatch", http.StatusForbidden,
			"The request signature does not match the signature you provided."}
	case errors.Is(err, sigv4.ErrTimeSkewed):
		return &s3Error{"RequestTimeTooSkewed", http.StatusForbidden,
			"The difference between the request time and the server's time is too large."}
	case errors.Is(err, sigv4.ErrExpired):
		return &s3Error{"AccessDenied", http.StatusForbidden, "Request has expired."}
	case errors.Is(err, sigv4.ErrCredentialScope):
		return &s3Error{"AuthorizationHeaderMalformed", http.StatusBadRequest,
			"The credential scope does not match this server's region."}
	default:
		return &s3Error{"AuthorizationHeaderMalformed", http.StatusBadRequest,
			"The authorization is malformed."}
	}
}

// errorResponse is the S3 XML error envelope.
type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

func writeError(w http.ResponseWriter, r *http.Request, err error) {
	s3e := mapError(err)
	// A 429 carries Retry-After, the throttling backoff hint (ADR-0039 part 4) —
	// kept distinct from the 503 SlowDown, which carries none.
	if s3e.Status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", strconv.Itoa(shedRetryAfterSeconds))
	}
	// HEAD responses carry no body, per HTTP and S3 alike.
	if r.Method == http.MethodHead {
		w.WriteHeader(s3e.Status)
		return
	}
	writeXML(w, s3e.Status, errorResponse{
		Code:      s3e.Code,
		Message:   s3e.Message,
		Resource:  r.URL.Path,
		RequestID: w.Header().Get("x-amz-request-id"),
	})
}

func writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	writeError(w, r, mapAuthError(err))
}

// writeXML renders one XML response. Encoding into a buffer first would
// allow turning encode failures into InternalError, but these structs
// cannot fail to encode; stream directly.
func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(v)
}
