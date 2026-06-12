package ec

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/hamster-storage/hamster/internal/stream"
)

// TestPipelineStreamThroughShards is the v0.3 data path end to end in
// miniature: object bytes → framed stream → erasure-coded shards, then
// nodes die and a Range read comes in — shards → reassembled frame →
// CRC-verified plaintext. The frame is opaque to the EC layer and the
// shards are opaque to the frame; this test is the proof the two designs
// actually compose.
func TestPipelineStreamThroughShards(t *testing.T) {
	rng := rand.New(rand.NewPCG(2026, 6))
	const (
		k, m      = 4, 2
		chunkSize = 4096 // frame chunks; slices are 1024 in tests
	)
	for _, size := range []int{0, 1, 5000, 100_000} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(rng.Uint32())
		}

		// PUT: plaintext streams through the framer into the encoder.
		frameSize := stream.FrameSize(int64(size), chunkSize)
		bufs := make([]bytes.Buffer, k+m)
		sinks := make([]io.Writer, k+m)
		for i := range bufs {
			sinks[i] = &bufs[i]
		}
		ecw, err := NewWriter(testID, k, m, frameSize, sinks)
		if err != nil {
			t.Fatal(err)
		}
		fw, err := stream.NewWriter(ecw, int64(size), chunkSize)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fw.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := fw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := ecw.Close(); err != nil {
			t.Fatal(err)
		}

		// Disaster: m nodes gone. GET anyway.
		shards := make([]io.ReaderAt, k+m)
		for i := range bufs {
			if i != 0 && i != 5 {
				shards[i] = bytes.NewReader(bufs[i].Bytes())
			}
		}
		er, err := NewReader(shards)
		if err != nil {
			t.Fatal(err)
		}
		fr, err := stream.NewReader(er, er.FrameSize())
		if err != nil {
			t.Fatal(err)
		}
		if fr.Size() != int64(size) {
			t.Fatalf("size %d: plaintext size %d through the pipeline", size, fr.Size())
		}
		got, err := io.ReadAll(io.NewSectionReader(fr, 0, fr.Size()))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("size %d: pipeline round trip diverged", size)
		}

		// And a Range read straight into the middle.
		if size > 6000 {
			p := make([]byte, 3000)
			if _, err := fr.ReadAt(p, 2500); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(p, payload[2500:5500]) {
				t.Fatalf("size %d: range read through the pipeline diverged", size)
			}
		}
	}
}
