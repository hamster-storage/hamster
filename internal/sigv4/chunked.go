package sigv4

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"strconv"
	"strings"
)

// ChunkedBody unwraps a STREAMING-AWS4-HMAC-SHA256-PAYLOAD request body,
// verifying every chunk's signature against the rolling chain seeded by
// the request signature. Reads return the decoded payload bytes.
//
// The caller must read to io.EOF: the final chunk's signature and the
// zero-length terminating chunk are verified on the way there, and
// stopping early skips them. A verification failure surfaces as
// ErrSignatureMismatch from Read.
//
// ChunkedBody panics if the identity is not a streaming one — that is a
// caller bug, not request data.
func (id *Identity) ChunkedBody(body io.Reader) io.Reader {
	if !id.Streaming {
		panic("sigv4: ChunkedBody on a non-streaming identity")
	}
	return &chunkedReader{
		br:      bufio.NewReader(body),
		key:     id.signingKey,
		amzDate: id.amzDate,
		scope:   id.scope,
		prevSig: id.seedSig,
		hash:    sha256.New(),
	}
}

type chunkedReader struct {
	br      *bufio.Reader
	key     []byte
	amzDate string
	scope   string
	prevSig string
	hash    hash.Hash // sha256 of the current chunk's data so far

	declared  string // current chunk's declared signature
	remaining uint64
	inChunk   bool
	done      bool
	err       error
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	for c.remaining == 0 {
		if c.done {
			return 0, io.EOF
		}
		if err := c.advance(); err != nil {
			c.err = err
			return 0, err
		}
	}
	n := len(p)
	if uint64(n) > c.remaining {
		n = int(c.remaining)
	}
	n, err := c.br.Read(p[:n])
	c.hash.Write(p[:n])
	c.remaining -= uint64(n)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF // body truncated mid-chunk
		}
		c.err = err
		if n > 0 {
			return n, nil
		}
		return 0, c.err
	}
	return n, nil
}

// advance crosses a chunk boundary: it settles the chunk just finished
// (trailing CRLF plus signature check), then parses the next header. The
// zero-length chunk closes the stream after its own verification.
func (c *chunkedReader) advance() error {
	if c.inChunk {
		if err := c.expectCRLF(); err != nil {
			return err
		}
		if err := c.verifyChunk(); err != nil {
			return err
		}
		c.inChunk = false
	}

	line, err := c.br.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	sizeHex, rest, ok := strings.Cut(strings.TrimSuffix(line, "\r\n"), ";")
	if !ok {
		return ErrMalformed
	}
	size, err := strconv.ParseUint(sizeHex, 16, 63)
	if err != nil {
		return ErrMalformed
	}
	declared, ok := strings.CutPrefix(rest, "chunk-signature=")
	if !ok || len(declared) != 64 {
		return ErrMalformed
	}

	c.declared = declared
	c.hash.Reset()
	if size == 0 {
		// The terminator signs an empty payload; verify it, then the
		// stream is complete (trailers are not supported in v0.1).
		if err := c.verifyChunk(); err != nil {
			return err
		}
		c.done = true
		return nil
	}
	c.remaining = size
	c.inChunk = true
	return nil
}

func (c *chunkedReader) verifyChunk() error {
	stringToSign := strings.Join([]string{
		algorithm + "-PAYLOAD",
		c.amzDate,
		c.scope,
		c.prevSig,
		emptySHA256,
		hex.EncodeToString(c.hash.Sum(nil)),
	}, "\n")
	want := hex.EncodeToString(hmacSHA256(c.key, stringToSign))
	if !signaturesEqual(want, c.declared) {
		return ErrSignatureMismatch
	}
	c.prevSig = c.declared
	return nil
}

func (c *chunkedReader) expectCRLF() error {
	var crlf [2]byte
	if _, err := io.ReadFull(c.br, crlf[:]); err != nil {
		return io.ErrUnexpectedEOF
	}
	if crlf != [2]byte{'\r', '\n'} {
		return ErrMalformed
	}
	return nil
}
