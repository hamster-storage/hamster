package cluster

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/hamster-storage/hamster/internal/gateway"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/raftnode"
)

// forwardAttempts bounds how many times a non-leader re-resolves the leader and
// re-sends a forwarded commit before giving up with unavailability. A leadership
// change costs at most one re-resolve per attempt; exhausting them means no
// leader settled in time, which the client retries as a 503.
const forwardAttempts = 5

// forward sends a prepared metadata commit to the current leader over the
// control channel and returns the reconstructed result (ADR-0037). The data
// plane already ran on this node; only this small commit crosses the hop. A
// leadership change is a safe retry: an ErrNotLeader answer means the target
// never committed, so it re-resolves the leader and re-sends. Other transport
// failures are reported as unavailability for the client to retry.
func (n *Node) forward(p any) (any, error) {
	req := encodeRequest(reqForward, meta.EncodeProposal(p))
	for attempt := 0; attempt < forwardAttempts; attempt++ {
		addr := n.leaderDial()
		if addr == "" {
			time.Sleep(controlSettlePoll) // no leader known yet; wait for one to emerge
			continue
		}
		buf, err := controlRoundTrip(addr, *n.leaf.Load(), n.trust.Load(), req)
		if err != nil {
			time.Sleep(controlSettlePoll) // transport failure: re-resolve and retry
			continue
		}
		fr, derr := decodeForwardResponse(buf)
		if derr != nil {
			return nil, fmt.Errorf("%w: malformed forward response", gateway.ErrUnavailable)
		}
		switch cerr := forwardErr(fr.errCode, fr.errMsg); {
		case errors.Is(cerr, raftnode.ErrNotLeader):
			time.Sleep(controlSettlePoll) // target moved; nothing committed, re-resolve
			continue
		case cerr != nil:
			return nil, cerr // a real apply error, identity preserved across the hop
		default:
			return forwardResultFor(p, fr), nil
		}
	}
	return nil, fmt.Errorf("%w: no leader accepted the forwarded commit", gateway.ErrUnavailable)
}

// forwardingProposer is the coordinator's metadata plane on a cluster node
// (coord.Proposer): it commits a proposal the coordinator prepared after running
// the data plane — a PutObject or an UploadPart — through the local Raft when
// this node leads, or forwards it to the leader otherwise (ADR-0037). Propose is
// called on the loop; the forward hop runs off-loop and posts its callback back,
// so the loop never blocks on the network. Store and Leader read the local
// replica, unchanged.
type forwardingProposer struct{ n *Node }

func (f forwardingProposer) Store() *meta.Store     { return f.n.raft.Store() }
func (f forwardingProposer) Leader() (uint64, bool) { return f.n.raft.Leader() }

func (f forwardingProposer) Propose(p any, done func(any, error)) {
	// On the loop. If we lead, propose locally and the callback fires on the loop
	// as the commit applies. If we lose leadership between this check and the
	// commit, Raft answers ErrNotLeader and we fall through to forwarding — so a
	// PUT mid-election still lands rather than failing.
	if _, isLeader := f.n.raft.Leader(); isLeader {
		f.n.raft.Propose(p, func(res any, err error) {
			if errors.Is(err, raftnode.ErrNotLeader) {
				f.forwardAsync(p, done)
				return
			}
			done(res, err)
		})
		return
	}
	f.forwardAsync(p, done)
}

// forwardAsync runs the forward hop off the loop (it blocks on the network) and
// posts the result back onto the loop, where the coordinator's completion runs.
func (f forwardingProposer) forwardAsync(p any, done func(any, error)) {
	go func() {
		res, err := f.n.forward(p)
		f.n.loop.Post(func() { done(res, err) })
	}()
}

// handleForward serves a forwarded metadata commit (ADR-0037): a non-leader ran
// the data plane and sends the prepared proposal here for the leader to commit.
// It requires a cluster certificate like every control mutation, decodes the
// proposal, and proposes it through the local Raft. A non-leader answers
// fwdErrNotLeader so the forwarder retries the real leader — nothing is
// committed in that case. The committed result is flattened into the response.
func (n *Node) handleForward(conn *tls.Conn, payload []byte) []byte {
	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return encodeForwardResponse(forwardResponse{errCode: fwdErrOther, errMsg: "forward requires a cluster certificate"})
	}
	p, err := meta.DecodeProposal(payload)
	if err != nil {
		return encodeForwardResponse(forwardResponse{errCode: fwdErrOther, errMsg: "malformed forwarded proposal"})
	}
	res, perr := n.proposeLocal(p)
	if perr != nil {
		code, msg := forwardErrCode(perr)
		return encodeForwardResponse(forwardResponse{errCode: code, errMsg: msg})
	}
	fr := forwardResultOf(res)
	fr.errCode = fwdErrNone
	return encodeForwardResponse(fr)
}

// Proposal forwarding (ADR-0037): a non-leader runs the data plane locally —
// placement, erasure coding, shard transfer, all leadership-independent — then
// forwards only the small metadata commit to the leader over the control
// channel. The leader proposes it through Raft and returns the typed result;
// the follower reconstructs the result and completes the S3 response. Object
// bytes never cross this hop: only the commit does, and metadata is still
// written solely by the leader through Raft.
//
// The forwarded request payload is the encoded proposal (meta.EncodeProposal).
// The response is a forwardResponse: the proposal's typed result flattened into
// a small union, plus the apply error mapped to a stable code so it keeps its
// identity across the hop — the gateway maps the reconstructed meta sentinel to
// the right S3 error exactly as a local apply would.

// forwardResponse is the wire form of a forwarded commit's outcome. The result
// fields are a union over what each Apply returns; the follower selects the
// concrete result type by switching on the proposal it sent.
type forwardResponse struct {
	errCode       uint64           // a forwardErr* code; fwdErrNone on success
	errMsg        string           // carried verbatim only for fwdErrOther
	resultID      meta.VersionID   // a VersionID the result carries (committed version, marker ID)
	freedIDs      []meta.VersionID // data addresses the commit displaced, for the follower to reclaim
	removed       bool             // DeleteObjectResult/DeleteVersionResult.Removed
	markerCreated bool             // DeleteObjectResult.MarkerCreated
}

// Forward error codes. fwdErrNotLeader is distinct so the follower knows the
// leader candidate never committed and a retry is safe; fwdErrOther carries an
// unrecognized error's message for best-effort display.
const (
	fwdErrNone      = 0
	fwdErrNotLeader = 1
	fwdErrOther     = 2
)

// forwardErrors maps each meta apply sentinel a forwarded gateway proposal can
// return to a stable wire code, so the follower reconstructs the same sentinel
// and the gateway's errors.Is checks still fire. Append only — codes are a
// wire contract (invariant 2). Codes 0–2 are reserved (the fwdErr* constants).
var forwardErrors = []struct {
	code uint64
	err  error
}{
	{10, meta.ErrInvalidBucketName},
	{11, meta.ErrBucketExists},
	{12, meta.ErrNoSuchBucket},
	{13, meta.ErrBucketNotEmpty},
	{14, meta.ErrInvalidObjectKey},
	{15, meta.ErrNoSuchVersion},
	{16, meta.ErrObjectLocked},
	{17, meta.ErrInvalidVersioningState},
	{18, meta.ErrInvalidRetention},
	{19, meta.ErrNoSuchUpload},
	{20, meta.ErrUploadExists},
	{21, meta.ErrInvalidPartNumber},
	{22, meta.ErrInvalidPart},
	{23, meta.ErrInvalidPartOrder},
	{24, meta.ErrPartTooSmall},
	{25, meta.ErrPersist},
}

// forwardErrCode classifies a leader-side apply error into a wire code (and a
// message only when the error is unrecognized).
func forwardErrCode(err error) (uint64, string) {
	if err == nil {
		return fwdErrNone, ""
	}
	if errors.Is(err, raftnode.ErrNotLeader) {
		return fwdErrNotLeader, ""
	}
	for _, e := range forwardErrors {
		if errors.Is(err, e.err) {
			return e.code, ""
		}
	}
	return fwdErrOther, err.Error()
}

// forwardErr reconstructs the error a wire code names. A known sentinel is
// returned by identity so the gateway's errors.Is checks fire; fwdErrNotLeader
// becomes raftnode.ErrNotLeader so the follower's forward loop retries; an
// unknown code (a newer leader) falls back to the carried message.
func forwardErr(code uint64, msg string) error {
	switch code {
	case fwdErrNone:
		return nil
	case fwdErrNotLeader:
		return raftnode.ErrNotLeader
	case fwdErrOther:
		return errors.New(msg)
	}
	for _, e := range forwardErrors {
		if e.code == code {
			return e.err
		}
	}
	return errors.New(msg) // unknown code: best-effort
}

// forwardResultOf flattens a leader-side proposal result into the wire union.
// A void-result proposal (bucket ops, retention, legal hold, create-upload)
// yields the zero forwardResponse.
func forwardResultOf(res any) forwardResponse {
	switch r := res.(type) {
	case meta.PutResult:
		return forwardResponse{resultID: r.VersionID, freedIDs: r.ReplacedDataIDs}
	case meta.DeleteObjectResult:
		return forwardResponse{
			removed: r.Removed, markerCreated: r.MarkerCreated,
			resultID: r.MarkerID, freedIDs: r.RemovedDataIDs,
		}
	case meta.DeleteVersionResult:
		return forwardResponse{removed: r.Removed}
	case meta.UploadPartResult:
		var freed []meta.VersionID
		if !r.ReplacedDataID.IsZero() {
			freed = []meta.VersionID{r.ReplacedDataID}
		}
		return forwardResponse{freedIDs: freed}
	case meta.CompleteResult:
		return forwardResponse{resultID: r.VersionID, freedIDs: r.DiscardedDataIDs}
	case meta.AbortResult:
		return forwardResponse{freedIDs: r.PartDataIDs}
	default:
		return forwardResponse{}
	}
}

// forwardResultFor reconstructs the concrete result type the proposal's caller
// expects from the wire union — the follower knows the proposal it sent, so it
// selects the matching result. A void-result proposal returns nil (its caller
// discards the result).
func forwardResultFor(p any, fr forwardResponse) any {
	switch p.(type) {
	case meta.PutObject:
		return meta.PutResult{VersionID: fr.resultID, ReplacedDataIDs: fr.freedIDs}
	case meta.DeleteObject:
		return meta.DeleteObjectResult{
			Removed: fr.removed, MarkerCreated: fr.markerCreated,
			MarkerID: fr.resultID, RemovedDataIDs: fr.freedIDs,
		}
	case meta.DeleteVersion:
		return meta.DeleteVersionResult{Removed: fr.removed}
	case meta.UploadPart:
		var replaced meta.VersionID
		if len(fr.freedIDs) > 0 {
			replaced = fr.freedIDs[0]
		}
		return meta.UploadPartResult{ReplacedDataID: replaced}
	case meta.CompleteMultipartUpload:
		return meta.CompleteResult{VersionID: fr.resultID, DiscardedDataIDs: fr.freedIDs}
	case meta.AbortMultipartUpload:
		return meta.AbortResult{PartDataIDs: fr.freedIDs}
	default:
		return nil
	}
}

// encodeForwardResponse and decodeForwardResponse are the control-channel wire
// codec for a forwarded commit's outcome, in the same handwritten protowire
// shape as the rest of the protocol.
func encodeForwardResponse(fr forwardResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putUint(b, 2, fr.errCode)
	if fr.errMsg != "" {
		b = putString(b, 3, fr.errMsg)
	}
	if !fr.resultID.IsZero() {
		b = putBytes(b, 4, fr.resultID[:])
	}
	for _, id := range fr.freedIDs {
		b = putBytes(b, 5, id[:])
	}
	b = putBool(b, 6, fr.removed)
	return putBool(b, 7, fr.markerCreated)
}

func decodeForwardResponse(buf []byte) (forwardResponse, error) {
	var fr forwardResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			fr.errCode = f.u
		case 3:
			fr.errMsg = string(f.b)
		case 4:
			copy(fr.resultID[:], f.b)
		case 5:
			var id meta.VersionID
			copy(id[:], f.b)
			fr.freedIDs = append(fr.freedIDs, id)
		case 6:
			fr.removed = f.u != 0
		case 7:
			fr.markerCreated = f.u != 0
		}
		return nil
	})
	return fr, err
}
