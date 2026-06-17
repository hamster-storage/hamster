package certs

import (
	"crypto/x509"
	"testing"
	"time"
)

func mustCA(t *testing.T, name string) *CA {
	t.Helper()
	ca, err := NewCA(name, time.Now())
	if err != nil {
		t.Fatalf("NewCA: %v", err)
	}
	return ca
}

// TestCAFingerprintIdentifiesContent: a CA's fingerprint is a function of its
// certificate, stable across CertPEM/Fingerprint, and distinct between CAs.
func TestCAFingerprintIdentifiesContent(t *testing.T) {
	a := mustCA(t, "alpha")
	b := mustCA(t, "beta")

	fpA, err := CAFingerprint(a.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	if fpA != a.Fingerprint() {
		t.Errorf("CAFingerprint(%x) != CA.Fingerprint(%x)", fpA, a.Fingerprint())
	}
	if fpA == 0 {
		t.Error("a real CA fingerprinted to zero (the none sentinel)")
	}
	if a.Fingerprint() == b.Fingerprint() {
		t.Error("two distinct CAs share a fingerprint")
	}
	// Reloading the same cert yields the same fingerprint.
	reFP, err := CAFingerprint(a.CertPEM())
	if err != nil || reFP != fpA {
		t.Fatalf("fingerprint not stable: %x vs %x (%v)", reFP, fpA, err)
	}
}

// TestPoolFromCAsTrustsEither: a leaf issued by any CA in the bundle verifies
// against the multi-CA pool, and a leaf from a CA outside it does not — the
// dual-trust property a rollover relies on (ADR-0033).
func TestPoolFromCAsTrustsEither(t *testing.T) {
	oldCA := mustCA(t, "old")
	newCA := mustCA(t, "new")
	stranger := mustCA(t, "stranger")

	pool, err := PoolFromCAs([][]byte{oldCA.CertPEM(), newCA.CertPEM()})
	if err != nil {
		t.Fatal(err)
	}

	verify := func(ca *CA) error {
		leaf, err := ca.Issue("n1", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		cert, err := x509.ParseCertificate(leaf.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		_, err = cert.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
		return err
	}

	if err := verify(oldCA); err != nil {
		t.Errorf("leaf from the old CA should verify against the bundle: %v", err)
	}
	if err := verify(newCA); err != nil {
		t.Errorf("leaf from the new CA should verify against the bundle: %v", err)
	}
	if err := verify(stranger); err == nil {
		t.Error("a leaf from a CA outside the bundle verified")
	}
}

// TestPoolFromCAsEmpty: a bundle with no usable CA is an error — a node with
// no trust anchor would accept nothing.
func TestPoolFromCAsEmpty(t *testing.T) {
	if _, err := PoolFromCAs(nil); err == nil {
		t.Error("empty bundle accepted")
	}
	if _, err := PoolFromCAs([][]byte{[]byte("not a pem")}); err == nil {
		t.Error("garbage bundle accepted")
	}
}
