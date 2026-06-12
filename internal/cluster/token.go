package cluster

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Join tokens (ADR-0022): short-lived, single-use, minted by a node that
// holds the CA key. The token string carries everything a joiner needs to
// reach and authenticate the cluster; the issuing node keeps only a hash
// of the secret, one file per outstanding token, deleted on use — no
// read-modify-write races between the minting CLI and the running node.
//
//	message Token {
//	  uint32 format_version = 1;
//	  string join_addr = 2;   // where to present it
//	  bytes ca_hash = 3;      // SHA-256 of the cluster CA certificate (DER)
//	  string id = 4;          // names the server-side record
//	  bytes secret = 5;
//	}
//
//	message TokenRecord {  // tokens/<id> on the issuing node
//	  uint32 format_version = 1;
//	  bytes secret_hash = 2;  // SHA-256 of the secret
//	  uint64 expires_unix_ms = 3;
//	}
const (
	tokenVersion = 1
	tokenPrefix  = "hamster-join-"
)

// token is the decoded join token.
type token struct {
	JoinAddr string
	CAHash   [32]byte
	ID       string
	Secret   []byte
}

func encodeToken(t token) string {
	b := putUint(nil, 1, tokenVersion)
	b = putString(b, 2, t.JoinAddr)
	b = putBytes(b, 3, t.CAHash[:])
	b = putString(b, 4, t.ID)
	b = putBytes(b, 5, t.Secret)
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(b)
}

func decodeToken(s string) (token, error) {
	raw, ok := strings.CutPrefix(strings.TrimSpace(s), tokenPrefix)
	if !ok {
		return token{}, fmt.Errorf("cluster: not a join token (expected a %q prefix)", tokenPrefix)
	}
	buf, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return token{}, fmt.Errorf("cluster: malformed join token: %w", err)
	}
	var t token
	err = forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			t.JoinAddr = string(f.b)
		case 3:
			if len(f.b) != len(t.CAHash) {
				return errors.New("CA hash is not 32 bytes")
			}
			copy(t.CAHash[:], f.b)
		case 4:
			t.ID = string(f.b)
		case 5:
			t.Secret = append([]byte(nil), f.b...)
		}
		return nil
	})
	if err != nil {
		return token{}, fmt.Errorf("cluster: malformed join token: %w", err)
	}
	if t.JoinAddr == "" || t.ID == "" || len(t.Secret) == 0 {
		return token{}, errors.New("cluster: join token is missing fields")
	}
	return t, nil
}

func tokensDir(dir string) string { return filepath.Join(dir, "tokens") }

// mintToken creates a token valid for ttl and records it on disk.
func mintToken(dir, joinAddr string, caHash [32]byte, now time.Time, ttl time.Duration) (string, error) {
	var idBytes [8]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return "", err
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	t := token{JoinAddr: joinAddr, CAHash: caHash, ID: hex.EncodeToString(idBytes[:]), Secret: secret}

	hash := sha256.Sum256(secret)
	rec := putUint(nil, 1, tokenVersion)
	rec = putBytes(rec, 2, hash[:])
	rec = putUint(rec, 3, uint64(now.Add(ttl).UnixMilli()))
	if err := os.MkdirAll(tokensDir(dir), 0o700); err != nil {
		return "", fmt.Errorf("cluster: creating tokens directory: %w", err)
	}
	path := filepath.Join(tokensDir(dir), t.ID)
	if err := os.WriteFile(path, rec, 0o600); err != nil {
		return "", fmt.Errorf("cluster: recording token: %w", err)
	}
	return encodeToken(t), nil
}

// consumeToken validates a presented token against its record and burns
// it: validation failures leave the record (an attacker must not be able
// to void a token by guessing its ID), success deletes it first — a token
// admits one node, ever.
func consumeToken(dir, id string, secret []byte, now time.Time) error {
	path := filepath.Join(tokensDir(dir), filepath.Base(id)) // Base: the ID names a file, never a path
	buf, err := os.ReadFile(path)
	if err != nil {
		return errors.New("cluster: unknown or already-used join token")
	}
	var secretHash []byte
	var expiresMS uint64
	err = forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			secretHash = append([]byte(nil), f.b...)
		case 3:
			expiresMS = f.u
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cluster: corrupt token record: %w", err)
	}
	presented := sha256.Sum256(secret)
	if subtle.ConstantTimeCompare(presented[:], secretHash) != 1 {
		return errors.New("cluster: join token rejected")
	}
	if now.UnixMilli() > int64(expiresMS) {
		return errors.New("cluster: join token expired")
	}
	if err := os.Remove(path); err != nil {
		// Lost a race with another use of the same token: single-use means
		// whoever removed it wins.
		return errors.New("cluster: unknown or already-used join token")
	}
	return nil
}
