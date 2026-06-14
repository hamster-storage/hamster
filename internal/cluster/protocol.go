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
//	}
const (
	protocolVersion = 1
	reqJoin         = 1
	reqStatus       = 2
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
}

type joinRequest struct {
	Token       string
	NodeID      string
	ClusterAddr string
	Host        string
	Zone        string
	Capacity    uint32
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
	return putBool(b, 9, m.Down)
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
	return putUint(b, 7, uint64(r.Capacity))
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

func encodeStatusResponse(r statusResponse) []byte {
	b := putUint(nil, 1, protocolVersion)
	b = putString(b, 2, r.Error)
	for _, m := range r.Members {
		b = putBytes(b, 3, encodeMemberMsg(m))
	}
	return b
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
		}
		return nil
	})
	return r, err
}
