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
