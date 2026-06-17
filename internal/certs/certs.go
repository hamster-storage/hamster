// Package certs mints the cluster-internal certificate authority and the
// node certificates it issues (ADR-0022). Everything is stdlib crypto: an
// Ed25519 CA, node certificates bound to node IDs (common name and DNS
// SAN), TLS 1.3 material on both ends.
//
// Time is a parameter everywhere — validity windows come from the caller's
// clock, never an ambient read (CLAUDE.md). Randomness is crypto/rand by
// nature: keys are the one place determinism must not apply.
package certs

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

// Validity windows. The CA is cluster identity and lives long (rotation is
// owed before v1, designed with KEK rotation per ADR-0022); node
// certificates are long-lived too until the renewal machinery arrives with
// cluster join — at which point they shorten drastically.
const (
	caValidity   = 20 * 365 * 24 * time.Hour
	nodeValidity = 10 * 365 * 24 * time.Hour
)

// CA is a cluster certificate authority: the self-signed root that every
// node certificate chains to.
type CA struct {
	cert    *x509.Certificate
	key     ed25519.PrivateKey
	certPEM []byte
}

// NewCA mints a cluster CA valid from now.
func NewCA(cluster string, now time.Time) (*CA, error) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("certs: generating CA key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cluster + " cluster CA"},
		NotBefore:             now.Add(-time.Hour), // tolerate skewed clocks
		NotAfter:              now.Add(caValidity),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		return nil, fmt.Errorf("certs: creating CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("certs: parsing CA certificate: %w", err)
	}
	return &CA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}, nil
}

// CertPEM is the CA certificate, PEM-encoded — what every node trusts.
func (ca *CA) CertPEM() []byte { return ca.certPEM }

// KeyPEM is the CA private key, PEM-encoded (PKCS#8) — the issuance
// secret, stored only where issuance happens (ADR-0022).
func (ca *CA) KeyPEM() ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(ca.key)
	if err != nil {
		return nil, fmt.Errorf("certs: marshaling CA key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// Hash is the SHA-256 of the CA certificate (DER) — what a join token
// pins, so a joining node can authenticate the cluster before it trusts
// anything else.
func (ca *CA) Hash() [32]byte { return sha256.Sum256(ca.cert.Raw) }

// LoadCA rebuilds a CA from its PEM-encoded certificate and key.
func LoadCA(certPEM, keyPEM []byte) (*CA, error) {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("certs: no PEM block in CA certificate")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("certs: parsing CA certificate: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, fmt.Errorf("certs: no PEM block in CA key")
	}
	keyAny, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("certs: parsing CA key: %w", err)
	}
	key, ok := keyAny.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("certs: CA key is %T, want Ed25519", keyAny)
	}
	return &CA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBlock.Bytes}),
	}, nil
}

// LoadCertDER extracts the DER bytes of the first certificate in a PEM
// block — for building chains and hashes from stored PEM.
func LoadCertDER(certPEM []byte) ([]byte, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("certs: no certificate PEM block")
	}
	return block.Bytes, nil
}

// PoolFromPEM builds a cert pool from a PEM-encoded CA certificate — the
// trust store on nodes that hold the CA certificate but not its key.
func PoolFromPEM(certPEM []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		return nil, fmt.Errorf("certs: no usable certificate in CA PEM")
	}
	return pool, nil
}

// PoolFromCAs builds one trust pool that accepts certificates chaining to any
// of the given CA certificates — the multi-CA trust bundle CA rotation needs
// (ADR-0033): during a rollover a node trusts both the old and the new CA at
// once, so neither side of the handshake is ever untrusted. Empty input is an
// error: a node with no trust anchor would accept nothing.
func PoolFromCAs(certPEMs [][]byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	added := 0
	for _, p := range certPEMs {
		if pool.AppendCertsFromPEM(p) {
			added++
		}
	}
	if added == 0 {
		return nil, fmt.Errorf("certs: no usable CA certificate in the trust bundle")
	}
	return pool, nil
}

// CAFingerprint identifies a CA certificate by its content (ADR-0033): the
// first 8 bytes of the SHA-256 of its DER, as a big-endian integer. A CA
// certificate is public, so the fingerprint guards nothing — it is an
// identifier, letting the trust bundle and each member's record name which CA
// signed which leaf so a rotation's progress is countable, the CA analogue of
// the per-version KEK fingerprint (ADR-0032). Zero means "none recorded".
func CAFingerprint(certPEM []byte) (uint64, error) {
	der, err := LoadCertDER(certPEM)
	if err != nil {
		return 0, err
	}
	sum := sha256.Sum256(der)
	var fp uint64
	for i := 0; i < 8; i++ {
		fp = fp<<8 | uint64(sum[i])
	}
	return fp, nil
}

// Fingerprint is CAFingerprint for this CA's own certificate.
func (ca *CA) Fingerprint() uint64 {
	sum := sha256.Sum256(ca.cert.Raw)
	var fp uint64
	for i := 0; i < 8; i++ {
		fp = fp<<8 | uint64(sum[i])
	}
	return fp
}

// CertPEMs encodes an issued certificate and its key as PEM, the inverse
// of tls.X509KeyPair.
func CertPEMs(cert tls.Certificate) (certPEM, keyPEM []byte, err error) {
	if len(cert.Certificate) == 0 {
		return nil, nil, fmt.Errorf("certs: empty certificate")
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("certs: marshaling node key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

// Pool returns a cert pool holding only this CA.
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// Issue mints a node certificate bound to nodeID: the ID is the common
// name and the DNS SAN, which is what the transport authenticates on both
// ends of a connection.
func (ca *CA) Issue(nodeID string, now time.Time) (tls.Certificate, error) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("certs: generating node key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("certs: generating serial: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: nodeID},
		DNSNames:     []string{nodeID},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(nodeValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("certs: issuing node certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
