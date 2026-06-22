package cluster

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/gateway"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/raftnode"
	"github.com/hamster-storage/hamster/internal/sys"
)

// The v0.3 cluster S3 preview: the gateway over the cluster's metadata
// and data planes. Mutations are Raft proposals, so they commit only on
// the leader — v0.3 does not forward proposals, and a non-leader node
// answers writes with SlowDown (503), which S3 clients retry elsewhere.
// Reads serve from the local replica (linearizable reads via ReadIndex
// arrive with the serving hardening). Multipart and server-side copy are
// refused by the gateway on this path until their erasure-coded design
// lands.

// S3Config configures a cluster node's S3 listener.
type S3Config struct {
	Listen    string // host:port for the S3 API
	Region    string
	Domain    string // virtual-hosted base domain; empty disables
	AccessKey string
	SecretKey string
}

// s3Server is the running S3 front end of a cluster node.
type s3Server struct {
	ln  net.Listener
	srv *http.Server
}

func (s *s3Server) stop() {
	_ = s.srv.Close()
}

// ServeS3 starts the S3 API on a running cluster node. Call once, after
// Run; the listener stops with the node.
func (n *Node) ServeS3(cfg S3Config) (addr string, err error) {
	g := gateway.New(gateway.Config{
		Region: cfg.Region,
		Domain: cfg.Domain,
		Lookup: func(akid string) (string, bool) {
			if akid == cfg.AccessKey {
				return cfg.SecretKey, true
			}
			return "", false
		},
		Clock:   sys.Clock{},
		Meta:    &clusterMetadata{n: n},
		Blobs:   refuseBlobs{}, // every object rides the cluster path
		Objects: &clusterObjects{n: n},
		// The SSE-S3 surface (ADR-0021): report the cluster's replicated
		// encryption posture so the gateway echoes the header and refuses an
		// AES256 request the cluster cannot honor.
		EncryptionEnabled: func() bool {
			return n.raft.Store().EncryptionAlgorithm() != meta.EncNone
		},
	})
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return "", fmt.Errorf("cluster: S3 listener on %s: %w", cfg.Listen, err)
	}
	n.s3 = &s3Server{ln: ln, srv: &http.Server{Handler: n.instrumentS3(g)}}
	go func() { _ = n.s3.srv.Serve(ln) }()
	return ln.Addr().String(), nil
}

// instrumentS3 wraps the gateway handler to count requests by method and HTTP
// status (ADR-0035) — the data-plane request-rate and error signal. A counter
// Inc is deterministic (no clock or randomness); request-latency histograms are a
// follow-on increment.
func (n *Node) instrumentS3(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &s3StatusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		n.s3Requests.Inc(r.Method, strconv.Itoa(rec.status))
	})
}

// s3StatusRecorder captures the response status for the request counter.
type s3StatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *s3StatusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// on runs fn on the node's loop and waits.
func (n *Node) on(fn func()) {
	done := make(chan struct{})
	n.loop.Post(func() {
		defer close(done)
		fn()
	})
	<-done
}

// propose commits one metadata proposal. On the leader it proposes through the
// local Raft and returns the committed result. On a non-leader it forwards only
// the small commit to the leader (ADR-0037) — the data plane already ran on this
// node, so the bytes stay here and just the commit crosses the hop. Either way
// metadata is written solely by the leader through Raft.
func (n *Node) propose(p any) (any, error) {
	res, err := n.proposeLocal(p)
	if !errors.Is(err, raftnode.ErrNotLeader) {
		return res, err // committed here, or a real apply error (typed, identity intact)
	}
	return n.forward(p)
}

// proposeLocal submits a proposal through this node's local Raft and waits for
// commit, returning raftnode.ErrNotLeader unwrapped when this node is not the
// leader — propose forwards on that signal, and a forwarded commit landing on a
// non-leader reports it so the forwarder retries. A commit timeout maps to the
// gateway's retryable unavailability.
func (n *Node) proposeLocal(p any) (any, error) {
	type outcome struct {
		res any
		err error
	}
	ch := make(chan outcome, 1)
	n.loop.Post(func() {
		n.raft.Propose(p, func(res any, err error) {
			ch <- outcome{res, err}
		})
	})
	select {
	case out := <-ch:
		return out.res, out.err
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("%w: proposal timed out", gateway.ErrUnavailable)
	}
}

// clusterMetadata is gateway.Metadata over the cluster: reads from the
// local replica on the loop, mutations as Raft proposals.
type clusterMetadata struct{ n *Node }

func (c *clusterMetadata) MintVersionID() (vid meta.VersionID, now time.Time) {
	c.n.on(func() {
		now = time.Now()
		vid = meta.NewVersionID(now, c.n.rng)
	})
	return
}

func (c *clusterMetadata) GetBucket(name string) (cfg meta.BucketConfig, ok bool) {
	c.n.on(func() { cfg, ok = c.n.raft.Store().GetBucket(name) })
	return
}

func (c *clusterMetadata) ListBuckets() (out []meta.BucketConfig) {
	c.n.on(func() { out = c.n.raft.Store().ListBuckets() })
	return
}

func (c *clusterMetadata) Current(bucket, key string) (cur meta.CurrentRecord, ok bool) {
	c.n.on(func() { cur, ok = c.n.raft.Store().Current(bucket, key) })
	return
}

func (c *clusterMetadata) GetVersion(bucket, key string, vid meta.VersionID) (e meta.VersionEntry, ok bool) {
	c.n.on(func() { e, ok = c.n.raft.Store().GetVersion(bucket, key, vid) })
	return
}

func (c *clusterMetadata) ListVersions(bucket, key string) (out []meta.VersionEntry) {
	c.n.on(func() { out = c.n.raft.Store().ListVersions(bucket, key) })
	return
}

func (c *clusterMetadata) ListObjectVersions(bucket, prefix, keyMarker string, versionIDMarker meta.VersionID, max int) (out []meta.VersionListing, truncated bool) {
	c.n.on(func() {
		out, truncated = c.n.raft.Store().ListObjectVersions(bucket, prefix, keyMarker, versionIDMarker, max)
	})
	return
}

func (c *clusterMetadata) ListObjects(bucket, prefix, startAfter string, max int) (out []meta.ObjectListing) {
	c.n.on(func() { out = c.n.raft.Store().ListObjects(bucket, prefix, startAfter, max) })
	return
}

func (c *clusterMetadata) GetUpload(bucket, key string, uid meta.VersionID) (up meta.UploadRecord, ok bool) {
	c.n.on(func() { up, ok = c.n.raft.Store().GetUpload(bucket, key, uid) })
	return
}

func (c *clusterMetadata) ListUploads(bucket, prefix, keyMarker string, uploadMarker meta.VersionID, max int) (out []meta.UploadListing) {
	c.n.on(func() { out = c.n.raft.Store().ListUploads(bucket, prefix, keyMarker, uploadMarker, max) })
	return
}

func (c *clusterMetadata) ListUploadParts(bucket, key string, uid meta.VersionID, afterPart uint32, max int) (parts []meta.PartRecord, ok bool) {
	c.n.on(func() { parts, ok = c.n.raft.Store().ListUploadParts(bucket, key, uid, afterPart, max) })
	return
}

func (c *clusterMetadata) ApplyCreateBucket(p meta.CreateBucket) error {
	_, err := c.n.propose(p)
	return err
}

func (c *clusterMetadata) ApplyDeleteBucket(p meta.DeleteBucket) error {
	_, err := c.n.propose(p)
	return err
}

func (c *clusterMetadata) ApplySetBucketVersioning(p meta.SetBucketVersioning) error {
	_, err := c.n.propose(p)
	return err
}

func (c *clusterMetadata) ApplySetObjectLockConfiguration(p meta.SetObjectLockConfiguration) error {
	_, err := c.n.propose(p)
	return err
}

func (c *clusterMetadata) ApplyPutObject(p meta.PutObject) (meta.PutResult, error) {
	res, err := c.n.propose(p)
	if err != nil {
		return meta.PutResult{}, err
	}
	return res.(meta.PutResult), nil
}

func (c *clusterMetadata) ApplyDeleteObject(p meta.DeleteObject) (meta.DeleteObjectResult, error) {
	res, err := c.n.propose(p)
	if err != nil {
		return meta.DeleteObjectResult{}, err
	}
	return res.(meta.DeleteObjectResult), nil
}

func (c *clusterMetadata) ApplyDeleteVersion(p meta.DeleteVersion) (meta.DeleteVersionResult, error) {
	res, err := c.n.propose(p)
	if err != nil {
		return meta.DeleteVersionResult{}, err
	}
	return res.(meta.DeleteVersionResult), nil
}

func (c *clusterMetadata) ApplyUpdateRetention(p meta.UpdateRetention) error {
	_, err := c.n.propose(p)
	return err
}

func (c *clusterMetadata) ApplyUpdateLegalHold(p meta.UpdateLegalHold) error {
	_, err := c.n.propose(p)
	return err
}

func (c *clusterMetadata) ApplyCreateMultipartUpload(p meta.CreateMultipartUpload) error {
	_, err := c.n.propose(p)
	return err
}

func (c *clusterMetadata) ApplyUploadPart(p meta.UploadPart) (meta.UploadPartResult, error) {
	res, err := c.n.propose(p)
	if err != nil {
		return meta.UploadPartResult{}, err
	}
	return res.(meta.UploadPartResult), nil
}

func (c *clusterMetadata) ApplyCompleteMultipartUpload(p meta.CompleteMultipartUpload) (meta.CompleteResult, error) {
	res, err := c.n.propose(p)
	if err != nil {
		return meta.CompleteResult{}, err
	}
	return res.(meta.CompleteResult), nil
}

func (c *clusterMetadata) ApplyAbortMultipartUpload(p meta.AbortMultipartUpload) (meta.AbortResult, error) {
	res, err := c.n.propose(p)
	if err != nil {
		return meta.AbortResult{}, err
	}
	return res.(meta.AbortResult), nil
}

// clusterObjects is gateway.ObjectBackend over the coordinator.
type clusterObjects struct{ n *Node }

func (c *clusterObjects) Put(bucket, key string, body io.Reader, size int64, opts gateway.PutObjectOptions) ([]byte, meta.VersionID, error) {
	c.n.putInflight.Add(1)
	defer c.n.putInflight.Add(-1)
	type outcome struct {
		res coord.PutResult
		err error
	}
	resCh := make(chan outcome, 1)
	finished := make(chan struct{})
	// want signals from the coordinator, sent on the loop — buffered to the
	// backpressure window so the want callback never blocks the loop.
	wantCh := make(chan struct{}, c.n.coord.PutMaxOutstanding()+1)
	startCh := make(chan *coord.PutHandle, 1)
	coordOpts := coord.PutOptions{
		ContentType:       opts.ContentType,
		UserMetadata:      opts.UserMetadata,
		RetentionMode:     opts.RetentionMode,
		RetainUntilUnixMS: opts.RetainUntilUnixMS,
		LegalHold:         opts.LegalHold,
	}
	c.n.loop.Post(func() {
		h := c.n.coord.PutStream(bucket, key, size, coordOpts,
			func() { wantCh <- struct{}{} },
			func(res coord.PutResult, err error) {
				resCh <- outcome{res, err}
				close(finished)
			})
		startCh <- h
	})
	h := <-startCh
	if h == nil {
		out := <-resCh // setup failed; done already fired
		return nil, meta.VersionID{}, mapCoordErr(out.err)
	}

	feedErr := c.runFeeder(body, h, wantCh, finished)
	out := <-resCh
	if feedErr != nil {
		return nil, meta.VersionID{}, feedErr // raw, so the gateway classifies auth vs other
	}
	if out.err != nil {
		return nil, meta.VersionID{}, mapCoordErr(out.err)
	}
	c.n.putBytes.Add(float64(size))
	return out.res.ETag, out.res.VersionID, nil
}

// runFeeder reads body off-loop in window-sized chunks and hands each to the
// coordinator on the loop, but only when it asks (the want signal) — so the
// body is never buffered beyond the backpressure window regardless of object
// size. A read error (a truncation or a validation failure surfaced at EOF)
// aborts the in-flight write and is returned raw; a coordinator-side completion
// (the ack-rule refusal) ends the feeder via finished, so it never blocks
// forever waiting for a want that will not come. Shared by whole-object Put and
// multipart PutPart — the part stream is fed identically.
func (c *clusterObjects) runFeeder(body io.Reader, h *coord.PutHandle, wantCh, finished chan struct{}) error {
	chunkSize := c.n.coord.PutChunkSize()
	var feedErr error
feedLoop:
	for {
		// Acquire one want credit. If none is ready, the coordinator is
		// shard-bound (its shard-stream windows are full) and the feeder is
		// throttled — count the stall, then wait for a credit or completion.
		select {
		case <-finished:
			break feedLoop
		case <-wantCh:
		default:
			c.n.putBackpressureWaits.Inc()
			select {
			case <-finished:
				break feedLoop
			case <-wantCh:
			}
		}
		buf := make([]byte, chunkSize)
		n, rerr := io.ReadFull(body, buf)
		switch {
		case rerr == nil:
			chunk := buf[:n]
			c.n.loop.Post(func() { h.Feed(chunk) })
		case rerr == io.EOF || rerr == io.ErrUnexpectedEOF:
			if n > 0 {
				chunk := buf[:n]
				c.n.loop.Post(func() { h.Feed(chunk) })
			}
			c.n.loop.Post(func() { h.FeedEOF() })
			break feedLoop
		default:
			feedErr = rerr
			c.n.loop.Post(func() { h.Abort(rerr) })
			break feedLoop
		}
	}
	return feedErr
}

// PutPart streams one multipart part through the coordinator's PutPartStream,
// which erasure-codes the part independently and commits its UploadPart row —
// the same fed-with-backpressure machinery as a whole PUT.
func (c *clusterObjects) PutPart(bucket, key string, uploadID meta.VersionID, partNumber uint32, body io.Reader, size int64) ([]byte, error) {
	c.n.putInflight.Add(1)
	defer c.n.putInflight.Add(-1)
	type outcome struct {
		res coord.PartResult
		err error
	}
	resCh := make(chan outcome, 1)
	finished := make(chan struct{})
	wantCh := make(chan struct{}, c.n.coord.PutMaxOutstanding()+1)
	startCh := make(chan *coord.PutHandle, 1)
	c.n.loop.Post(func() {
		h := c.n.coord.PutPartStream(bucket, key, uploadID, partNumber, size,
			func() { wantCh <- struct{}{} },
			func(res coord.PartResult, err error) {
				resCh <- outcome{res, err}
				close(finished)
			})
		startCh <- h
	})
	h := <-startCh
	if h == nil {
		out := <-resCh // setup failed; done already fired
		return nil, mapCoordErr(out.err)
	}
	feedErr := c.runFeeder(body, h, wantCh, finished)
	out := <-resCh
	if feedErr != nil {
		return nil, feedErr // raw, so the gateway classifies auth vs other
	}
	if out.err != nil {
		return nil, mapCoordErr(out.err)
	}
	c.n.putBytes.Add(float64(size))
	return out.res.ETag, nil
}

func (c *clusterObjects) GetRange(entry meta.VersionEntry, off, length int64) ([]byte, error) {
	type outcome struct {
		data []byte
		err  error
	}
	ch := make(chan outcome, 1)
	c.n.loop.Post(func() {
		c.n.coord.GetEntry(entry, off, length, func(b []byte, err error) { ch <- outcome{b, err} })
	})
	out := <-ch
	if out.err != nil {
		return nil, mapCoordErr(out.err)
	}
	return out.data, nil
}

func (c *clusterObjects) DeleteShards(e meta.VersionEntry) {
	c.n.loop.Post(func() { c.n.coord.DeleteShards(e) })
}

// mapCoordErr turns coordinator refusals into the gateway's retryable
// unavailability; everything else passes through for the 500 it is.
func mapCoordErr(err error) error {
	if errors.Is(err, coord.ErrRefused) || errors.Is(err, coord.ErrUnreadable) {
		return fmt.Errorf("%w: %v", gateway.ErrUnavailable, err)
	}
	return err
}

// refuseBlobs is the BlobStore of a node with no v0.1 blob path: every
// object rides the erasure-coded backend, so any blob call is a bug or an
// entry from a different deployment shape.
type refuseBlobs struct{}

func (refuseBlobs) Put(meta.VersionID, io.Reader) (int64, error) {
	return 0, errors.New("cluster: no blob store on a cluster node")
}
func (refuseBlobs) Get(meta.VersionID) ([]byte, error) {
	return nil, errors.New("cluster: no blob store on a cluster node")
}
func (refuseBlobs) Remove(meta.VersionID) error {
	return errors.New("cluster: no blob store on a cluster node")
}

// Verify gateway interface conformance at compile time.
var (
	_ gateway.Metadata      = (*clusterMetadata)(nil)
	_ gateway.ObjectBackend = (*clusterObjects)(nil)
	_ gateway.BlobStore     = refuseBlobs{}
)
