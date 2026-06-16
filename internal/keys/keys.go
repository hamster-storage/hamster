// Package keys implements Hamster's envelope-encryption key handling
// (ADR-0021): per-object data-encryption keys (DEKs) and the cluster
// key-encryption key (KEK) that wraps them.
//
// The split is the standard one. Each object gets a fresh random DEK that
// encrypts its bytes (the AES-256-GCM transform in internal/stream); the
// DEK is then wrapped — encrypted — under the KEK and stored, wrapped,
// beside the object's metadata. Only wrapped DEKs and ciphertext ever
// reach disk, so a stolen disk yields nothing without the KEK, which lives
// only in a running node's memory (loaded from an operator-provided source
// at startup) and is never persisted or sent over the network.
//
// The KEK is symmetric: the same key wraps and unwraps. There is no
// public/private split, because every node both encrypts on PUT and
// decrypts on GET, so a write-only key would buy nothing.
//
// This package is pure given its inputs — the entropy source is an
// injected io.Reader (crypto/rand in production, a seeded reader under the
// simulator), and key material arrives as bytes the caller already read.
// Reading a key file or running a key command is the boot layer's job, not
// this package's, so no seam lives here.
package keys

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
)

// Key lengths, in bytes. KEK and DEK are both AES-256 keys.
const (
	KEKLen = 32
	DEKLen = 32

	// WrapNonceLen is the GCM nonce prefixed to a wrapped DEK. The caller
	// supplies a unique nonce of this length to Wrap (derived from the
	// object's version ID, not random).
	WrapNonceLen = 12
	wrapTagLen   = 16 // GCM tag

	// WrappedLen is the exact size of a wrapped DEK: nonce, ciphertext (a
	// DEK is DEKLen bytes), and the GCM tag.
	WrappedLen = WrapNonceLen + DEKLen + wrapTagLen

	// FingerprintLen is the length of a KEK fingerprint in bytes. A
	// fingerprint identifies a KEK by its content without revealing it, so a
	// version's record can name the key that wraps its DEK and a node can
	// detect a mismatched master key (ADR-0032). 64 bits is ample to tell
	// apart the handful of keys a cluster sees over its lifetime; collisions
	// are irrelevant because the operator controls that small key set.
	FingerprintLen = 8
)

// fingerprintLabel domain-separates the KEK fingerprint from any other use
// of the key. It is part of the on-disk-format contract: changing it would
// change every stored fingerprint, so it is pinned forever (ADR-0032).
const fingerprintLabel = "hamster-kek-fingerprint-v1"

// A DEK is a per-object data-encryption key.
type DEK [DEKLen]byte

// Bytes returns the DEK as a slice, for the stream transform that consumes
// a []byte key. The slice aliases the DEK, so do not retain it past the
// DEK's own lifetime.
func (d DEK) Bytes() []byte { return d[:] }

// NewDEK reads a fresh DEK from the entropy source. In production entropy
// is crypto/rand.Reader; under the simulator it is a seeded reader, which
// keeps the encrypted write path deterministic — the DEK is the only
// random input to it (ADR-0021).
func NewDEK(entropy io.Reader) (DEK, error) {
	var d DEK
	if _, err := io.ReadFull(entropy, d[:]); err != nil {
		return DEK{}, fmt.Errorf("keys: generating DEK: %w", err)
	}
	return d, nil
}

// A Fingerprint identifies a KEK by its content without revealing it. It is
// a truncated HMAC-SHA-256 over a fixed label keyed by the master-key
// material — a key-check value: preimage resistant, so storing it in
// replicated metadata or printing it in status leaks nothing usable about
// the key (ADR-0032). The zero Fingerprint means "none recorded": a version
// without a fingerprint was wrapped under the cluster's founding KEK,
// before rotation began.
type Fingerprint [FingerprintLen]byte

// IsZero reports whether the fingerprint is unset (the founding-KEK / no
// fingerprint recorded case).
func (f Fingerprint) IsZero() bool { return f == Fingerprint{} }

// Uint64 returns the fingerprint as a big-endian unsigned integer, the
// compact form the metadata codec stores (a single varint field). A zero
// value round-trips as "no fingerprint recorded".
func (f Fingerprint) Uint64() uint64 { return binary.BigEndian.Uint64(f[:]) }

// FingerprintFromUint64 rebuilds a Fingerprint from its stored integer form.
func FingerprintFromUint64(v uint64) Fingerprint {
	var f Fingerprint
	binary.BigEndian.PutUint64(f[:], v)
	return f
}

// String renders the fingerprint as hex, for status and logs.
func (f Fingerprint) String() string { return hex.EncodeToString(f[:]) }

// A KEK is the cluster key-encryption key, held only in memory. It is
// built once from raw key material and reused to wrap and unwrap every
// object's DEK.
type KEK struct {
	aead cipher.AEAD
	fp   Fingerprint
}

// LoadKEK builds a KEK from raw key material. The material is the 32 key
// bytes directly, or a hex or base64 encoding of them (whitespace trimmed)
// — so a Kubernetes Secret can hold the key in whichever form is
// convenient. Any other length is rejected: a KEK must be exactly 256 bits.
func LoadKEK(material []byte) (KEK, error) {
	raw, err := decodeKeyMaterial(material)
	if err != nil {
		return KEK{}, err
	}
	if len(raw) != KEKLen {
		return KEK{}, fmt.Errorf("keys: KEK must be %d bytes, got %d", KEKLen, len(raw))
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return KEK{}, fmt.Errorf("keys: KEK cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return KEK{}, fmt.Errorf("keys: KEK GCM: %w", err)
	}
	if aead.NonceSize() != WrapNonceLen || aead.Overhead() != wrapTagLen {
		return KEK{}, fmt.Errorf("keys: unexpected GCM geometry (nonce %d, tag %d)", aead.NonceSize(), aead.Overhead())
	}
	return KEK{aead: aead, fp: fingerprint(raw)}, nil
}

// Fingerprint returns the content fingerprint of the loaded KEK. The zero
// KEK (none loaded) returns the zero fingerprint.
func (k KEK) Fingerprint() Fingerprint { return k.fp }

// fingerprint computes a KEK's content fingerprint: the first FingerprintLen
// bytes of HMAC-SHA-256(material, fingerprintLabel). Keying the HMAC with
// the secret and hashing a fixed public label is the standard key-check-value
// construction — deterministic across nodes and builds (so every node maps a
// given key to the identical fingerprint), and preimage resistant (so the
// fingerprint reveals nothing about the key).
func fingerprint(material []byte) Fingerprint {
	mac := hmac.New(sha256.New, material)
	mac.Write([]byte(fingerprintLabel))
	sum := mac.Sum(nil)
	var f Fingerprint
	copy(f[:], sum[:FingerprintLen])
	return f
}

// Loaded reports whether the KEK holds a key. The zero KEK is not loaded;
// a node that could not obtain its key carries one and refuses encrypted
// work rather than operating without it.
func (k KEK) Loaded() bool { return k.aead != nil }

// decodeKeyMaterial accepts raw bytes, hex, or base64 key material. Raw
// (exactly KEKLen bytes) is taken as-is; otherwise the trimmed text is
// tried as hex then standard base64. This lets a Secret hold the key in
// whatever form the tooling produced.
func decodeKeyMaterial(material []byte) ([]byte, error) {
	if len(material) == KEKLen {
		return material, nil
	}
	text := string(bytes.TrimSpace(material))
	if raw, err := hex.DecodeString(text); err == nil && len(raw) == KEKLen {
		return raw, nil
	}
	if raw, err := base64.StdEncoding.DecodeString(text); err == nil && len(raw) == KEKLen {
		return raw, nil
	}
	return nil, fmt.Errorf("keys: key material is not %d raw bytes, nor hex/base64 of them", KEKLen)
}

// Wrap encrypts dek under the KEK, returning the self-contained wrapped
// blob: the nonce followed by the AES-256-GCM ciphertext and tag.
//
// nonce must be WrapNonceLen bytes and unique per wrap under a given KEK.
// The caller derives it from the object's globally-unique version ID
// rather than at random, so the (KEK, nonce) pair never repeats no matter
// how many objects are stored — sidestepping the ~2^32-message bound that
// random GCM nonces carry — and so the wrap is deterministic under the
// simulator.
func (k KEK) Wrap(dek DEK, nonce []byte) ([]byte, error) {
	if !k.Loaded() {
		return nil, fmt.Errorf("keys: cannot wrap: no KEK loaded")
	}
	if len(nonce) != WrapNonceLen {
		return nil, fmt.Errorf("keys: wrap nonce must be %d bytes, got %d", WrapNonceLen, len(nonce))
	}
	out := make([]byte, WrapNonceLen, WrappedLen)
	copy(out, nonce)
	return k.aead.Seal(out, nonce, dek[:], nil), nil
}

// Unwrap decrypts a wrapped DEK under the KEK. The wrapped blob is
// self-contained — it carries its own nonce — so unwrap needs only the
// blob and the KEK. A wrong KEK or a tampered blob fails authentication
// rather than returning a bad key.
func (k KEK) Unwrap(wrapped []byte) (DEK, error) {
	if !k.Loaded() {
		return DEK{}, fmt.Errorf("keys: cannot unwrap: no KEK loaded")
	}
	if len(wrapped) != WrappedLen {
		return DEK{}, fmt.Errorf("keys: wrapped DEK must be %d bytes, got %d", WrappedLen, len(wrapped))
	}
	nonce := wrapped[:WrapNonceLen]
	plain, err := k.aead.Open(nil, nonce, wrapped[WrapNonceLen:], nil)
	if err != nil {
		return DEK{}, fmt.Errorf("keys: unwrap failed: wrong KEK or corrupt wrapped DEK: %w", err)
	}
	if len(plain) != DEKLen {
		return DEK{}, fmt.Errorf("keys: unwrapped DEK is %d bytes, want %d", len(plain), DEKLen)
	}
	var d DEK
	copy(d[:], plain)
	return d, nil
}
