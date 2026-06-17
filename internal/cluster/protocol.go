// Package cluster is the operational layer of a Hamster metadata cluster:
// the on-disk node identity (cluster directory, certificates, node.conf),
// the join protocol (token-authenticated certificate issuance, ADR-0022),
// and the composition that runs a node — raftnode over the production
// adapters (internal/sys) with mTLS transport.
//
// The join flow, end to end: `cluster init` mints the cluster CA and the
// first node's certificate. `cluster token` (on a node holding the CA key)
// mints a short-lived single-use token that pins the CA certificate's hash
// and the issuer's join address. `cluster join` dials that address over
// TLS, authenticates the server against the pinned hash — the joiner
// trusts nothing else yet — presents the token, and receives its identity:
// the CA certificate, its own node certificate, its Raft ID, and the
// address book. From there the node runs and admission is joiner-driven
// (raftnode admit messages over the normal mTLS transport) until the
// replicated configuration includes it.
//
// In v0.2 a cluster node carries the replicated metadata plane only: the
// S3 gateway stays on the single-node path until the erasure-coded data
// path arrives (v0.3) — serving S3 from a cluster whose blobs do not
// replicate would lose objects on failover.
package cluster

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
)

// The join/status listener speaks length-framed (4-byte big-endian),
// versioned protobuf — one request, one response, one connection:
//
//	message Request {
//	  uint32 format_version = 1;
//	  uint32 kind = 2;     // 1 join, 2 status
//	  bytes payload = 3;
//	}
//
//	message JoinRequest {
//	  uint32 format_version = 1;
//	  string token = 2;
//	  string node_id = 3;
//	  string cluster_addr = 4;  // where the joiner's transport will listen
//	  string host = 5;          // failure-domain labels (ADR-0016)
//	  string zone = 6;
//	  uint32 capacity = 7;      // relative weight (ADR-0004)
//	  string replaces = 8;      // the member this node replaces, if any (ADR-0004)
//	}
//
//	message JoinResponse {
//	  uint32 format_version = 1;
//	  string error = 2;         // set on refusal; all other fields empty
//	  string cluster = 3;
//	  uint64 raft_id = 4;
//	  bytes ca_pem = 5;
//	  bytes cert_pem = 6;
//	  bytes key_pem = 7;
//	  repeated Member members = 8;
//	}
//
//	message StatusResponse {
//	  uint32 format_version = 1;
//	  string error = 2;
//	  repeated Member members = 3;
//	}
//
//	message Member {
//	  uint64 raft_id = 1;
//	  string node_id = 2;
//	  string dial = 3;
//	  bool learner = 4;
//	  bool leader = 5;
//	  string host = 6;
//	  string zone = 7;
//	  uint32 capacity = 8;
//	  bool down = 9;        // the answering node's local liveness view
//	  bool draining = 10;   // committed: operator is removing this node
//	}
//
//	message DrainRequest {
//	  uint32 format_version = 1;
//	  string node_id = 2;
//	  bool draining = 3;        // true to drain, false to clear
//	}
//
//	message DrainResponse {
//	  uint32 format_version = 1;
//	  string error = 2;         // set on refusal
//	  string leader = 3;        // the leader's dial address when this node is not it
//	}
//
//	message RemoveRequest {
//	  uint32 format_version = 1;
//	  string node_id = 2;
//	}
//
//	message RemoveResponse {
//	  uint32 format_version = 1;
//	  string error = 2;         // set on refusal
//	  string leader = 3;        // the leader's dial address when this node is not it
//	}
const (
	protocolVersion = 1
	reqJoin         = 1
	reqStatus       = 2
	reqDrain        = 3
	reqRemove       = 4
	reqOptimize     = 5
	reqEncrypt      = 6
	reqRotateKey    = 7
	reqRotateCA     = 8
	reqReissue      = 9
	reqCanStop      = 10
)

// maxFrame caps a protocol frame: certificates and member lists are small.
const maxFrame = 1 << 20

// Member is one cluster member as the protocol reports it. Host and Zone are
// its failure-domain labels (ADR-0016): the machine identity and the domain
// above it; placement spreads shards across them. Capacity is its relative
// weight (ADR-0004): zero means equal. Down is the answering node's local,
// best-effort liveness view — a peer it currently treats as down (a PUT skips
// it to avoid the write timeout). It is not a committed cluster fact; a
// different node may report a different view.
type Member struct {
	RaftID   uint64
	NodeID   string
	Dial     string
	Learner  bool
	Leader   bool
	Host     string
	Zone     string
	Capacity uint32
	Down     bool
	// Draining is a committed cluster fact (ADR-0004): the operator has marked
	// this node for removal. Unlike Down (a local liveness view), every node
	// reports the same value, read from the cluster layout.
	Draining bool
	// BinaryVersion and Generation are the member's advertised version (ADR-0034),
	// read from the replicated NodeRecord: the release string for display and the
	// declared protocol generation. Empty/zero until the leader's version monitor
	// records them.
	BinaryVersion string
	Generation    uint32
}

type joinRequest struct {
	Token       string
	NodeID      string
	ClusterAddr string
	Host        string
	Zone        string
	Capacity    uint32
	Replaces    string // the member this node takes the place of (ADR-0004), if any
}

type joinResponse struct {
	Error   string
	Cluster string
	RaftID  uint64
	CAPEM   []byte
	CertPEM []byte
	KeyPEM  []byte
	Members []Member
}

type statusResponse struct {
	Error   string
	Members []Member
	// Encryption is the cluster's encryption-at-rest posture (ADR-0021): the
	// algorithm name (e.g. "AES256GCM") or "" when the cluster does not encrypt.
	Encryption string
	// KEKFingerprint is the cluster's current master-key fingerprint (ADR-0032),
	// hex, when encrypting. RotatingTo is the target key's fingerprint while a
	// rotation is open (empty otherwise), and Remaining is how many versions are
	// still on the old key — the rotation's observable progress, read from the
	// answering node's local replica.
	KEKFingerprint string
	RotatingTo     string
	Remaining      uint64
	// CA trust (ADR-0033): the trust-bundle generation this node is on, and —
	// while a CA rotation is open (the bundle holds more than one CA) — how many
	// members still hold a leaf from the old CA.
	TrustVersion uint64
	CARotating   bool
	CAStragglers uint64
	// Version advertisement (ADR-0034). LocalBinaryVersion and LocalGeneration are
	// the answering node's *own* build — what its binary owns, read fresh at boot
	// — which the leader's version monitor reads from each peer to keep the
	// registry current across an in-place upgrade. EffectiveGeneration is the
	// cluster's effective generation (the min across live members, etcd-style),
	// computed from the replicated registry by the answering node.
	LocalBinaryVersion  string
	LocalGeneration     uint32
	EffectiveGeneration uint32
}

// reissueRequest carries a node certificate the rotation driver signed under the
// new CA for the receiving member (ADR-0033): the member adopts it if it is for
// that member's identity and chains to the bundle's issuing CA. The key never
// travels except over the established mTLS channel, the same as a join.
type reissueRequest struct {
	CertPEM []byte
	KeyPEM  []byte
}

type reissueResponse struct {
	Error string
}

// rotateCAResponse reports a CA rotation (ADR-0033): a leader-driven dual-trust
// rollover. A non-leader answers with the leader's dial address. Reissued is how
// many members were moved onto the new CA, and Completed whether the old CA was
// dropped — at which point it may be retired.
type rotateCAResponse struct {
	Error     string
	Leader    string
	Reissued  uint64
	Completed bool
}

// encryptResponse reports enabling encryption at rest (ADR-0021): a
// leader-only posture proposal. A non-leader answers with the leader's dial
// address so the client retries there, like the other control commands.
type encryptResponse struct {
	Error      string
	Leader     string
	Encryption string // the posture in effect after the call
}

// rotateKeyResponse reports a master-key rotation (ADR-0032): a leader-only
// rewrap sweep. A non-leader answers with the leader's dial address. Rewrapped
// is how many versions this rotation moved onto the new key, Remaining how many
// are still on the old one (zero on success), and Completed whether the rotation
// closed — at which point the old key may be retired.
type rotateKeyResponse struct {
	Error     string
	Leader    string
	Rewrapped uint64
	Remaining uint64
	Completed bool
}

type drainRequest struct {
	NodeID   string
	Draining bool
}

type drainResponse struct {
	Error  string
	Leader string // the leader's dial address when the asked node is not the leader
}

// canStopRequest asks whether taking NodeID down for maintenance/upgrade is safe
// (ADR-0034). Advisory: it informs the operator's decision; it never stops a node.
type canStopRequest struct {
	NodeID string
}

// canStopResponse is the interlock's verdict (ADR-0034): Safe is the answer, and
// Reason explains it either way (for an operator or an automated roll). Error is
// set only on a malformed request or an unknown node.
type canStopResponse struct {
	Error  string
	Safe   bool
	Reason string
}

type removeRequest struct {
	NodeID string
}

type removeResponse struct {
	Error  string
	Leader string // the leader's dial address when the asked node is not the leader
}

// optimizeResponse reports one optimize sweep (ADR-0004, ADR-0031): how many
// objects it examined and how many it re-encoded up to the active profile.
type optimizeResponse struct {
	Error     string
	Leader    string // the leader's dial address when the asked node is not the leader
	Objects   uint64
	ReEncoded uint64
	// Retry marks a refusal the caller should wait out and re-ask (the cluster is
	// converging a membership change), as opposed to one it must resolve first.
	Retry bool
}

// writeFrame writes one length-framed message.
func writeFrame(w io.Writer, msg []byte) error {
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(msg)))
	_, err := w.Write(append(frame, msg...))
	return err
}

// readFrame reads one length-framed message.
func readFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size > maxFrame {
		return nil, fmt.Errorf("cluster: frame of %d bytes exceeds the cap", size)
	}
	msg := make([]byte, size)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// Encoding helpers: fields with zero values are omitted, protobuf-style.

func putUint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func putBool(b []byte, num protowire.Number, v bool) []byte {
	if !v {
		return b
	}
	return putUint(b, num, 1)
}

func putString(b []byte, num protowire.Number, s string) []byte {
	if s == "" {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func putBytes(b []byte, num protowire.Number, v []byte) []byte {
	if len(v) == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

// field is one decoded protobuf field: u for varints, b for bytes.
type field struct {
	num protowire.Number
	u   uint64
	b   []byte
}

// forEachField walks a message, invoking fn per varint or bytes field and
// skipping (without error) any other wire type — additive evolution.
func forEachField(buf []byte, fn func(f field) error) error {
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return protowire.ParseError(n)
		}
		buf = buf[n:]
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return protowire.ParseError(n)
			}
			buf = buf[n:]
			if err := fn(field{num: num, u: v}); err != nil {
				return err
			}
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return protowire.ParseError(n)
			}
			buf = buf[n:]
			if err := fn(field{num: num, b: v}); err != nil {
				return err
			}
		default:
			n := protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return protowire.ParseError(n)
			}
			buf = buf[n:]
		}
	}
	return nil
}

func encodeRequest(kind uint64, payload []byte) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putUint(b, 2, kind)
	return putBytes(b, 3, payload)
}

func decodeRequest(buf []byte) (kind uint64, payload []byte, err error) {
	err = forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			kind = f.u
		case 3:
			payload = f.b
		}
		return nil
	})
	return kind, payload, err
}

func encodeMemberMsg(m Member) []byte {
	b := putUint(nil, 1, m.RaftID)
	b = putString(b, 2, m.NodeID)
	b = putString(b, 3, m.Dial)
	b = putBool(b, 4, m.Learner)
	b = putBool(b, 5, m.Leader)
	b = putString(b, 6, m.Host)
	b = putString(b, 7, m.Zone)
	b = putUint(b, 8, uint64(m.Capacity))
	b = putBool(b, 9, m.Down)
	b = putBool(b, 10, m.Draining)
	b = putString(b, 11, m.BinaryVersion)
	return putUint(b, 12, uint64(m.Generation))
}

func decodeMemberMsg(buf []byte) (Member, error) {
	var m Member
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 1:
			m.RaftID = f.u
		case 2:
			m.NodeID = string(f.b)
		case 3:
			m.Dial = string(f.b)
		case 4:
			m.Learner = f.u != 0
		case 5:
			m.Leader = f.u != 0
		case 6:
			m.Host = string(f.b)
		case 7:
			m.Zone = string(f.b)
		case 8:
			m.Capacity = uint32(f.u)
		case 9:
			m.Down = f.u != 0
		case 10:
			m.Draining = f.u != 0
		case 11:
			m.BinaryVersion = string(f.b)
		case 12:
			m.Generation = uint32(f.u)
		}
		return nil
	})
	return m, err
}

func encodeJoinRequest(r joinRequest) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Token)
	b = putString(b, 3, r.NodeID)
	b = putString(b, 4, r.ClusterAddr)
	b = putString(b, 5, r.Host)
	b = putString(b, 6, r.Zone)
	b = putUint(b, 7, uint64(r.Capacity))
	return putString(b, 8, r.Replaces)
}

func decodeJoinRequest(buf []byte) (joinRequest, error) {
	var r joinRequest
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Token = string(f.b)
		case 3:
			r.NodeID = string(f.b)
		case 4:
			r.ClusterAddr = string(f.b)
		case 5:
			r.Host = string(f.b)
		case 6:
			r.Zone = string(f.b)
		case 7:
			r.Capacity = uint32(f.u)
		case 8:
			r.Replaces = string(f.b)
		}
		return nil
	})
	return r, err
}

func encodeJoinResponse(r joinResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	b = putString(b, 3, r.Cluster)
	b = putUint(b, 4, r.RaftID)
	b = putBytes(b, 5, r.CAPEM)
	b = putBytes(b, 6, r.CertPEM)
	b = putBytes(b, 7, r.KeyPEM)
	for _, m := range r.Members {
		b = putBytes(b, 8, encodeMemberMsg(m))
	}
	return b
}

func decodeJoinResponse(buf []byte) (joinResponse, error) {
	var r joinResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Cluster = string(f.b)
		case 4:
			r.RaftID = f.u
		case 5:
			r.CAPEM = append([]byte(nil), f.b...)
		case 6:
			r.CertPEM = append([]byte(nil), f.b...)
		case 7:
			r.KeyPEM = append([]byte(nil), f.b...)
		case 8:
			m, err := decodeMemberMsg(f.b)
			if err != nil {
				return err
			}
			r.Members = append(r.Members, m)
		}
		return nil
	})
	return r, err
}

func encodeDrainRequest(r drainRequest) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.NodeID)
	return putBool(b, 3, r.Draining)
}

func decodeDrainRequest(buf []byte) (drainRequest, error) {
	var r drainRequest
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.NodeID = string(f.b)
		case 3:
			r.Draining = f.u != 0
		}
		return nil
	})
	return r, err
}

func encodeCanStopRequest(r canStopRequest) []byte {
	b := putUint(nil, 1, protocolVersion)
	return putString(b, 2, r.NodeID)
}

func decodeCanStopRequest(buf []byte) (canStopRequest, error) {
	var r canStopRequest
	err := forEachField(buf, func(f field) error {
		if f.num == 2 {
			r.NodeID = string(f.b)
		}
		return nil
	})
	return r, err
}

func encodeCanStopResponse(r canStopResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	b = putBool(b, 3, r.Safe)
	return putString(b, 4, r.Reason)
}

func decodeCanStopResponse(buf []byte) (canStopResponse, error) {
	var r canStopResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Safe = f.u != 0
		case 4:
			r.Reason = string(f.b)
		}
		return nil
	})
	return r, err
}

func encodeDrainResponse(r drainResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	return putString(b, 3, r.Leader)
}

func decodeDrainResponse(buf []byte) (drainResponse, error) {
	var r drainResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Leader = string(f.b)
		}
		return nil
	})
	return r, err
}

func encodeRemoveRequest(r removeRequest) []byte {
	b := putUint(nil, 1, protocolVersion)
	return putString(b, 2, r.NodeID)
}

func decodeRemoveRequest(buf []byte) (removeRequest, error) {
	var r removeRequest
	err := forEachField(buf, func(f field) error {
		if f.num == 2 {
			r.NodeID = string(f.b)
		}
		return nil
	})
	return r, err
}

func encodeRemoveResponse(r removeResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	return putString(b, 3, r.Leader)
}

func decodeRemoveResponse(buf []byte) (removeResponse, error) {
	var r removeResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Leader = string(f.b)
		}
		return nil
	})
	return r, err
}

func encodeOptimizeResponse(r optimizeResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	b = putString(b, 3, r.Leader)
	b = putUint(b, 4, r.Objects)
	b = putUint(b, 5, r.ReEncoded)
	return putBool(b, 6, r.Retry)
}

func decodeOptimizeResponse(buf []byte) (optimizeResponse, error) {
	var r optimizeResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Leader = string(f.b)
		case 4:
			r.Objects = f.u
		case 5:
			r.ReEncoded = f.u
		case 6:
			r.Retry = f.u != 0
		}
		return nil
	})
	return r, err
}

func encodeStatusResponse(r statusResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	for _, m := range r.Members {
		b = putBytes(b, 3, encodeMemberMsg(m))
	}
	b = putString(b, 4, r.Encryption)
	b = putString(b, 5, r.KEKFingerprint)
	b = putString(b, 6, r.RotatingTo)
	b = putUint(b, 7, r.Remaining)
	b = putUint(b, 8, r.TrustVersion)
	b = putBool(b, 9, r.CARotating)
	b = putUint(b, 10, r.CAStragglers)
	b = putString(b, 11, r.LocalBinaryVersion)
	b = putUint(b, 12, uint64(r.LocalGeneration))
	return putUint(b, 13, uint64(r.EffectiveGeneration))
}

func decodeStatusResponse(buf []byte) (statusResponse, error) {
	var r statusResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			m, err := decodeMemberMsg(f.b)
			if err != nil {
				return err
			}
			r.Members = append(r.Members, m)
		case 4:
			r.Encryption = string(f.b)
		case 5:
			r.KEKFingerprint = string(f.b)
		case 6:
			r.RotatingTo = string(f.b)
		case 7:
			r.Remaining = f.u
		case 8:
			r.TrustVersion = f.u
		case 9:
			r.CARotating = f.u != 0
		case 10:
			r.CAStragglers = f.u
		case 11:
			r.LocalBinaryVersion = string(f.b)
		case 12:
			r.LocalGeneration = uint32(f.u)
		case 13:
			r.EffectiveGeneration = uint32(f.u)
		}
		return nil
	})
	return r, err
}

func encodeReissueRequest(r reissueRequest) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putBytes(b, 2, r.CertPEM)
	return putBytes(b, 3, r.KeyPEM)
}

func decodeReissueRequest(buf []byte) (reissueRequest, error) {
	var r reissueRequest
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.CertPEM = append([]byte(nil), f.b...)
		case 3:
			r.KeyPEM = append([]byte(nil), f.b...)
		}
		return nil
	})
	return r, err
}

func encodeReissueResponse(r reissueResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	return putString(b, 2, r.Error)
}

func decodeReissueResponse(buf []byte) (reissueResponse, error) {
	var r reissueResponse
	err := forEachField(buf, func(f field) error {
		if f.num == 2 {
			r.Error = string(f.b)
		}
		return nil
	})
	return r, err
}

func encodeRotateCAResponse(r rotateCAResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	b = putString(b, 3, r.Leader)
	b = putUint(b, 4, r.Reissued)
	return putBool(b, 5, r.Completed)
}

func decodeRotateCAResponse(buf []byte) (rotateCAResponse, error) {
	var r rotateCAResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Leader = string(f.b)
		case 4:
			r.Reissued = f.u
		case 5:
			r.Completed = f.u != 0
		}
		return nil
	})
	return r, err
}

func encodeRotateKeyResponse(r rotateKeyResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	b = putString(b, 3, r.Leader)
	b = putUint(b, 4, r.Rewrapped)
	b = putUint(b, 5, r.Remaining)
	return putBool(b, 6, r.Completed)
}

func decodeRotateKeyResponse(buf []byte) (rotateKeyResponse, error) {
	var r rotateKeyResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Leader = string(f.b)
		case 4:
			r.Rewrapped = f.u
		case 5:
			r.Remaining = f.u
		case 6:
			r.Completed = f.u != 0
		}
		return nil
	})
	return r, err
}

func encodeEncryptResponse(r encryptResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	b = putString(b, 3, r.Leader)
	return putString(b, 4, r.Encryption)
}

func decodeEncryptResponse(buf []byte) (encryptResponse, error) {
	var r encryptResponse
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			r.Error = string(f.b)
		case 3:
			r.Leader = string(f.b)
		case 4:
			r.Encryption = string(f.b)
		}
		return nil
	})
	return r, err
}
