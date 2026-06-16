package stream

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
)

// DEKLen is the data-encryption-key length the encrypted-frame transform
// requires: 256 bits, for AES-256-GCM. The DEK is supplied per object by
// the caller (docs/DATA-STREAM.md, ADR-0021); this layer never mints or
// stores it.
const DEKLen = 32

// AES-256-GCM parameters this code depends on. They are the standard GCM
// sizes; the constructor asserts them so a future stdlib change cannot
// silently shift the on-disk layout.
const (
	gcmNonceLen = 12 // GCM standard nonce
	gcmTagLen   = 16 // GCM authentication tag, appended to each chunk's ciphertext
)

// chunkCrypter is the per-chunk AES-256-GCM transform. Each chunk is
// sealed independently so a Range read can decrypt only the chunks it
// touches, and the chunk index is the nonce — safe because the DEK is
// unique per object (ADR-0021): distinct objects carry distinct keys and
// distinct chunks distinct indices, so a (key, nonce) pair never repeats.
// Reusing a DEK across two objects would reuse nonces and break GCM; that
// invariant is the caller's to keep, and the per-object-DEK rule keeps it.
type chunkCrypter struct {
	aead cipher.AEAD
}

// newChunkCrypter builds the transform for a 32-byte DEK.
func newChunkCrypter(dek []byte) (*chunkCrypter, error) {
	if len(dek) != DEKLen {
		return nil, fmt.Errorf("stream: DEK must be %d bytes, got %d", DEKLen, len(dek))
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("stream: AES init: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("stream: GCM init: %w", err)
	}
	if aead.NonceSize() != gcmNonceLen || aead.Overhead() != gcmTagLen {
		return nil, fmt.Errorf("stream: unexpected GCM geometry (nonce %d, tag %d)", aead.NonceSize(), aead.Overhead())
	}
	return &chunkCrypter{aead: aead}, nil
}

// nonce derives chunk i's 12-byte GCM nonce: the chunk index big-endian in
// the low 8 bytes, the high 4 bytes zero. Deterministic and unique per
// chunk within an object.
func chunkNonce(i int64) [gcmNonceLen]byte {
	var n [gcmNonceLen]byte
	binary.BigEndian.PutUint64(n[gcmNonceLen-8:], uint64(i))
	return n
}

// seal encrypts plaintext for chunk i, appending the tag, into dst.
func (c *chunkCrypter) seal(dst, plaintext []byte, i int64) []byte {
	nonce := chunkNonce(i)
	return c.aead.Seal(dst, nonce[:], plaintext, nil)
}

// open decrypts and authenticates chunk i's ciphertext into dst. It fails
// if the bytes were tampered with — the GCM tag, alongside the frame's
// per-chunk CRC, makes every single-byte change to a chunk detectable.
func (c *chunkCrypter) open(dst, ciphertext []byte, i int64) ([]byte, error) {
	nonce := chunkNonce(i)
	return c.aead.Open(dst, nonce[:], ciphertext, nil)
}
