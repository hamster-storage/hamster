package gateway

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
)

const s3Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"

// iso8601 renders the millisecond timestamp form S3 listings use.
func iso8601(unixMS int64) string {
	return time.UnixMilli(unixMS).UTC().Format("2006-01-02T15:04:05.000Z")
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listAllMyBucketsResult struct {
	XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
	Xmlns   string        `xml:"xmlns,attr"`
	OwnerID string        `xml:"Owner>ID"`
	Buckets []bucketEntry `xml:"Buckets>Bucket"`
}

func (g *Gateway) listBuckets(w http.ResponseWriter, r *http.Request) {
	configs := g.cfg.Meta.ListBuckets()

	out := listAllMyBucketsResult{Xmlns: s3Xmlns, OwnerID: "hamster"}
	for _, c := range configs {
		out.Buckets = append(out.Buckets, bucketEntry{Name: c.Name, CreationDate: iso8601(c.CreatedUnixMS)})
	}
	writeXML(w, http.StatusOK, out)
}

func (g *Gateway) createBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	// Object-lock-enabled buckets need the retention surface (v0.6) to be
	// usable; refusing the flag is honest, silently dropping it would be a
	// compliance bug.
	if strings.EqualFold(r.Header.Get("x-amz-bucket-object-lock-enabled"), "true") {
		writeError(w, r, errNotImplemented)
		return
	}
	applyErr := g.cfg.Meta.ApplyCreateBucket(meta.CreateBucket{
		ProposedAtUnixMS: g.cfg.Clock.Now().UnixMilli(),
		Bucket:           bucket,
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (g *Gateway) deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	applyErr := g.cfg.Meta.ApplyDeleteBucket(meta.DeleteBucket{
		ProposedAtUnixMS: g.cfg.Clock.Now().UnixMilli(),
		Bucket:           bucket,
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) headBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, ok := g.cfg.Meta.GetBucket(bucket); !ok {
		writeError(w, r, meta.ErrNoSuchBucket)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

func (g *Gateway) getBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, ok := g.cfg.Meta.GetBucket(bucket); !ok {
		writeError(w, r, meta.ErrNoSuchBucket)
		return
	}
	// S3 quirk: us-east-1 is the empty constraint.
	region := g.cfg.Region
	if region == "us-east-1" {
		region = ""
	}
	writeXML(w, http.StatusOK, locationConstraint{Xmlns: s3Xmlns, Value: region})
}

// listing is the delimiter-grouped result both ListObjects versions share.
type listing struct {
	objects   []meta.ObjectListing
	prefixes  []string
	truncated bool
	// next is the resume point when truncated: the last emitted key, or
	// the last emitted common prefix (it ends with the delimiter, which is
	// how a resume recognizes it as a group marker).
	next string
}

const listBatch = 1000

// collectListing walks the current-version index, grouping by delimiter and
// stopping at max entries. after is the resume point: a key (exclusive) or
// a previously emitted common prefix, whose remaining group members are
// skipped.
func collectListing(s Metadata, bucket, prefix, delimiter, after string, max int) (listing, error) {
	if _, ok := s.GetBucket(bucket); !ok {
		return listing{}, meta.ErrNoSuchBucket
	}
	var out listing
	if max <= 0 {
		return out, nil
	}
	lastGroup := ""
	if delimiter != "" && after != "" && strings.HasPrefix(after, prefix) && strings.HasSuffix(after, delimiter) {
		lastGroup = after
	}
	count := 0
	scanAfter := after
	for {
		batch := s.ListObjects(bucket, prefix, scanAfter, listBatch)
		for _, o := range batch {
			scanAfter = o.Key
			if lastGroup != "" && strings.HasPrefix(o.Key, lastGroup) {
				continue // remainder of an already-emitted group
			}
			if delimiter != "" {
				if i := strings.Index(o.Key[len(prefix):], delimiter); i >= 0 {
					group := o.Key[:len(prefix)+i+len(delimiter)]
					if count == max {
						out.truncated = true
						return out, nil
					}
					out.prefixes = append(out.prefixes, group)
					out.next = group
					lastGroup = group
					count++
					continue
				}
			}
			if count == max {
				out.truncated = true
				return out, nil
			}
			out.objects = append(out.objects, o)
			out.next = o.Key
			count++
		}
		if len(batch) < listBatch {
			return out, nil
		}
	}
}

type contentsEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefixEntry struct {
	Prefix string `xml:"Prefix"`
}

type listBucketResultV2 struct {
	XMLName               xml.Name            `xml:"ListBucketResult"`
	Xmlns                 string              `xml:"xmlns,attr"`
	Name                  string              `xml:"Name"`
	Prefix                string              `xml:"Prefix"`
	Delimiter             string              `xml:"Delimiter,omitempty"`
	StartAfter            string              `xml:"StartAfter,omitempty"`
	ContinuationToken     string              `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string              `xml:"NextContinuationToken,omitempty"`
	KeyCount              int                 `xml:"KeyCount"`
	MaxKeys               int                 `xml:"MaxKeys"`
	EncodingType          string              `xml:"EncodingType,omitempty"`
	IsTruncated           bool                `xml:"IsTruncated"`
	Contents              []contentsEntry     `xml:"Contents"`
	CommonPrefixes        []commonPrefixEntry `xml:"CommonPrefixes"`
}

func (g *Gateway) listObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	encode := q.Get("encoding-type") == "url"
	maxKeys, ok := parseMaxKeys(q.Get("max-keys"))
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}

	// continuation-token wins over start-after, as on AWS. The token is
	// the opaque (to clients) base64 of the resume point.
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		raw, err := base64.StdEncoding.DecodeString(tok)
		if err != nil {
			writeError(w, r, errInvalidArgument)
			return
		}
		after = string(raw)
	}

	l, err := collectListing(g.cfg.Meta, bucket, prefix, delimiter, after, maxKeys)
	if err != nil {
		writeError(w, r, err)
		return
	}

	out := listBucketResultV2{
		Xmlns:             s3Xmlns,
		Name:              bucket,
		Prefix:            listEncode(prefix, encode),
		Delimiter:         listEncode(delimiter, encode),
		StartAfter:        listEncode(q.Get("start-after"), encode),
		ContinuationToken: q.Get("continuation-token"),
		KeyCount:          len(l.objects) + len(l.prefixes),
		MaxKeys:           maxKeys,
		IsTruncated:       l.truncated,
	}
	if encode {
		out.EncodingType = "url"
	}
	if l.truncated {
		out.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(l.next))
	}
	fillListing(&out.Contents, &out.CommonPrefixes, l, encode)
	writeXML(w, http.StatusOK, out)
}

type listBucketResultV1 struct {
	XMLName        xml.Name            `xml:"ListBucketResult"`
	Xmlns          string              `xml:"xmlns,attr"`
	Name           string              `xml:"Name"`
	Prefix         string              `xml:"Prefix"`
	Marker         string              `xml:"Marker"`
	NextMarker     string              `xml:"NextMarker,omitempty"`
	Delimiter      string              `xml:"Delimiter,omitempty"`
	MaxKeys        int                 `xml:"MaxKeys"`
	EncodingType   string              `xml:"EncodingType,omitempty"`
	IsTruncated    bool                `xml:"IsTruncated"`
	Contents       []contentsEntry     `xml:"Contents"`
	CommonPrefixes []commonPrefixEntry `xml:"CommonPrefixes"`
}

func (g *Gateway) listObjectsV1(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	marker := q.Get("marker")
	encode := q.Get("encoding-type") == "url"
	maxKeys, ok := parseMaxKeys(q.Get("max-keys"))
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}

	l, err := collectListing(g.cfg.Meta, bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		writeError(w, r, err)
		return
	}

	out := listBucketResultV1{
		Xmlns:       s3Xmlns,
		Name:        bucket,
		Prefix:      listEncode(prefix, encode),
		Marker:      listEncode(marker, encode),
		Delimiter:   listEncode(delimiter, encode),
		MaxKeys:     maxKeys,
		IsTruncated: l.truncated,
	}
	if encode {
		out.EncodingType = "url"
	}
	// AWS sets NextMarker only when a delimiter was given; without one,
	// clients resume from the last returned key.
	if l.truncated && delimiter != "" {
		out.NextMarker = listEncode(l.next, encode)
	}
	fillListing(&out.Contents, &out.CommonPrefixes, l, encode)
	writeXML(w, http.StatusOK, out)
}

func fillListing(contents *[]contentsEntry, prefixes *[]commonPrefixEntry, l listing, encode bool) {
	for _, o := range l.objects {
		*contents = append(*contents, contentsEntry{
			Key:          listEncode(o.Key, encode),
			LastModified: iso8601(o.Current.CreatedUnixMS),
			ETag:         objectETag(o.Current.ETag, int(o.Current.PartCount)),
			Size:         o.Current.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, p := range l.prefixes {
		*prefixes = append(*prefixes, commonPrefixEntry{Prefix: listEncode(p, encode)})
	}
}

func parseMaxKeys(s string) (int, bool) {
	if s == "" {
		return 1000, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, false
	}
	return min(n, 1000), true
}

// listEncode applies encoding-type=url: percent-encode UTF-8 bytes outside
// the unreserved set, keeping '/' literal — the form S3 returns and every
// SDK decodes.
func listEncode(s string, encode bool) string {
	if !encode {
		return s
	}
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~', c == '/':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
		}
	}
	return b.String()
}
