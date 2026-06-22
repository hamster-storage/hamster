package cluster

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/raftnode"
)

func vid(b byte) meta.VersionID {
	var id meta.VersionID
	id[0] = b
	return id
}

// TestForwardResultRoundTrip proves a leader-side result survives the wire and
// the proposal→result reconstruction: encode the result, decode it, and rebuild
// the concrete type the proposal's caller asserts — for every typed-result
// proposal and a void-result one.
func TestForwardResultRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		prop any
		res  any
	}{
		{"PutObject", meta.PutObject{}, meta.PutResult{VersionID: vid(1), ReplacedDataIDs: []meta.VersionID{vid(2), vid(3)}}},
		{"PutObject_noReplace", meta.PutObject{}, meta.PutResult{VersionID: vid(9)}},
		{"DeleteObject_marker", meta.DeleteObject{}, meta.DeleteObjectResult{MarkerCreated: true, MarkerID: vid(4), RemovedDataIDs: []meta.VersionID{vid(5)}}},
		{"DeleteObject_removed", meta.DeleteObject{}, meta.DeleteObjectResult{Removed: true, RemovedDataIDs: []meta.VersionID{vid(6)}}},
		{"DeleteVersion", meta.DeleteVersion{}, meta.DeleteVersionResult{Removed: true}},
		{"UploadPart_replaced", meta.UploadPart{}, meta.UploadPartResult{ReplacedDataID: vid(7)}},
		{"UploadPart_first", meta.UploadPart{}, meta.UploadPartResult{}},
		{"Complete", meta.CompleteMultipartUpload{}, meta.CompleteResult{VersionID: vid(8), DiscardedDataIDs: []meta.VersionID{vid(1), vid(2)}}},
		{"Abort", meta.AbortMultipartUpload{}, meta.AbortResult{PartDataIDs: []meta.VersionID{vid(3), vid(4)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fr := forwardResultOf(c.res)
			fr.errCode = fwdErrNone
			decoded, err := decodeForwardResponse(encodeForwardResponse(fr))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			got := forwardResultFor(c.prop, decoded)
			if !reflect.DeepEqual(got, c.res) {
				t.Fatalf("round trip:\n got %+v\nwant %+v", got, c.res)
			}
		})
	}
}

// TestForwardVoidResult: a void-result proposal reconstructs as nil — its caller
// discards the result, so nothing need cross the wire.
func TestForwardVoidResult(t *testing.T) {
	for _, p := range []any{
		meta.CreateBucket{}, meta.DeleteBucket{}, meta.SetBucketVersioning{},
		meta.SetObjectLockConfiguration{}, meta.UpdateRetention{}, meta.UpdateLegalHold{},
		meta.CreateMultipartUpload{},
	} {
		fr, _ := decodeForwardResponse(encodeForwardResponse(forwardResultOf(nil)))
		if got := forwardResultFor(p, fr); got != nil {
			t.Fatalf("%T: void result reconstructed as %+v, want nil", p, got)
		}
	}
}

// TestForwardErrorIdentity: every mapped apply sentinel keeps its identity
// across the wire, so the gateway's errors.Is checks still fire. ErrNotLeader
// reconstructs to the retry sentinel; an unrecognized error degrades to its
// message.
func TestForwardErrorIdentity(t *testing.T) {
	for _, e := range forwardErrors {
		code, msg := forwardErrCode(e.err)
		if got := forwardErr(code, msg); !errors.Is(got, e.err) {
			t.Fatalf("%v: round-tripped to %v (code %d), lost identity", e.err, got, code)
		}
	}
	// ErrNotLeader is distinct and reconstructs to the retry sentinel.
	code, _ := forwardErrCode(raftnode.ErrNotLeader)
	if code != fwdErrNotLeader {
		t.Fatalf("ErrNotLeader mapped to code %d, want fwdErrNotLeader", code)
	}
	if !errors.Is(forwardErr(code, ""), raftnode.ErrNotLeader) {
		t.Fatal("fwdErrNotLeader did not reconstruct ErrNotLeader")
	}
	// An unrecognized error carries its message verbatim under fwdErrOther.
	code, msg := forwardErrCode(errors.New("something internal"))
	if code != fwdErrOther || msg != "something internal" {
		t.Fatalf("unrecognized error: code %d msg %q", code, msg)
	}
	// Success is the zero code.
	if code, _ := forwardErrCode(nil); code != fwdErrNone {
		t.Fatalf("nil error mapped to code %d, want fwdErrNone", code)
	}
}

// TestForwardErrorWireRoundTrip: the error code and message survive the
// response codec.
func TestForwardErrorWireRoundTrip(t *testing.T) {
	code, msg := forwardErrCode(meta.ErrObjectLocked)
	fr, err := decodeForwardResponse(encodeForwardResponse(forwardResponse{errCode: code, errMsg: msg}))
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(forwardErr(fr.errCode, fr.errMsg), meta.ErrObjectLocked) {
		t.Fatal("ErrObjectLocked lost across the response wire")
	}
}
