package gateway

import (
	"encoding/xml"
	"errors"
	"net/http"

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
)

// mapError translates a metadata-layer error into its S3 wire form.
func mapError(err error) *s3Error {
	var s3e *s3Error
	if errors.As(err, &s3e) {
		return s3e
	}
	switch {
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
	case errors.Is(err, meta.ErrObjectLocked):
		return &s3Error{"AccessDenied", http.StatusForbidden, "The object is protected by object lock."}
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
