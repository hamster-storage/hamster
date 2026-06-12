package cluster

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
)

// Init creates a new cluster: the CA, the founding node's certificate and
// identity, all under <data-dir>/cluster. The node becomes a running
// cluster the first time `cluster run` starts it (a fresh Raft log
// bootstraps a single-voter configuration).
func Init(dataDir, clusterName, nodeID, clusterAddr, joinAddr string, now time.Time) error {
	if Initialized(dataDir) {
		return fmt.Errorf("cluster: %s already holds a cluster identity", Dir(dataDir))
	}
	if nodeID == "" || clusterName == "" {
		return errors.New("cluster: a cluster name and a node ID are required")
	}
	dir := Dir(dataDir)
	if err := os.MkdirAll(tokensDir(dir), 0o700); err != nil {
		return fmt.Errorf("cluster: creating %s: %w", dir, err)
	}
	ca, err := certs.NewCA(clusterName, now)
	if err != nil {
		return err
	}
	cert, err := ca.Issue(nodeID, now)
	if err != nil {
		return err
	}
	certPEM, keyPEM, err := certs.CertPEMs(cert)
	if err != nil {
		return err
	}
	caKeyPEM, err := ca.KeyPEM()
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{
		"ca.pem": ca.CertPEM(), "ca.key": caKeyPEM,
		"node.pem": certPEM, "node.key": keyPEM,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return fmt.Errorf("cluster: writing %s: %w", name, err)
		}
	}
	return saveConfig(dir, NodeConfig{
		Cluster: clusterName, NodeID: nodeID, RaftID: 1,
		ClusterAddr: clusterAddr, JoinAddr: joinAddr,
		Members:    []Member{{RaftID: 1, NodeID: nodeID, Dial: clusterAddr}},
		NextRaftID: 2,
	})
}

// MintToken mints a join token on a node holding the CA key, valid for
// ttl, single-use. The running node (or the next `cluster run`) honors it.
func MintToken(dataDir string, ttl time.Duration, now time.Time) (string, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return "", err
	}
	ca, err := loadCA(dir)
	if err != nil {
		return "", fmt.Errorf("cluster: only a node holding the cluster CA key can mint join tokens (the init node, in v0.2): %w", err)
	}
	return mintToken(dir, cfg.JoinAddr, ca.Hash(), now, ttl)
}

// Join performs the joining side of the join protocol: dial the token's
// issuer, authenticate it against the token's pinned CA hash, present the
// token, and persist the identity it returns. The node is a cluster member
// once `cluster run` starts it and admission commits.
func Join(dataDir, nodeID, clusterAddr, joinAddr, tokenStr string) error {
	if Initialized(dataDir) {
		return fmt.Errorf("cluster: %s already holds a cluster identity", Dir(dataDir))
	}
	if nodeID == "" {
		return errors.New("cluster: a node ID is required")
	}
	tok, err := decodeToken(tokenStr)
	if err != nil {
		return err
	}
	conn, err := dialPinned(tok.JoinAddr, tok.CAHash)
	if err != nil {
		return err
	}
	defer conn.Close()
	req := encodeRequest(reqJoin, encodeJoinRequest(joinRequest{
		Token: tokenStr, NodeID: nodeID, ClusterAddr: clusterAddr,
	}))
	if err := writeFrame(conn, req); err != nil {
		return fmt.Errorf("cluster: sending join request: %w", err)
	}
	buf, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("cluster: reading join response: %w", err)
	}
	resp, err := decodeJoinResponse(buf)
	if err != nil {
		return fmt.Errorf("cluster: decoding join response: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("cluster: join refused: %s", resp.Error)
	}

	dir := Dir(dataDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("cluster: creating %s: %w", dir, err)
	}
	for name, data := range map[string][]byte{
		"ca.pem": resp.CAPEM, "node.pem": resp.CertPEM, "node.key": resp.KeyPEM,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return fmt.Errorf("cluster: writing %s: %w", name, err)
		}
	}
	members := append(resp.Members, Member{RaftID: resp.RaftID, NodeID: nodeID, Dial: clusterAddr})
	return saveConfig(dir, NodeConfig{
		Cluster: resp.Cluster, NodeID: nodeID, RaftID: resp.RaftID,
		ClusterAddr: clusterAddr, JoinAddr: joinAddr,
		Join: true, Members: members,
	})
}

// Status queries a running node's join/status listener with this node's
// own certificate. An empty addr asks the local node.
func Status(dataDir, addr string) ([]Member, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return nil, err
	}
	if addr == "" {
		addr = cfg.JoinAddr
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return nil, err
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		// The peer is authenticated as a holder of a cluster-CA
		// certificate, not by name: status may be asked of any member,
		// whose node ID the caller need not know.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: verifyChainToPool(pool),
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: dialing %s: %w", addr, err)
	}
	defer conn.Close()
	if err := writeFrame(conn, encodeRequest(reqStatus, nil)); err != nil {
		return nil, fmt.Errorf("cluster: sending status request: %w", err)
	}
	buf, err := readFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("cluster: reading status response: %w", err)
	}
	resp, err := decodeStatusResponse(buf)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("cluster: status refused: %s", resp.Error)
	}
	return resp.Members, nil
}

// dialPinned dials a join listener trusting only the token's pinned CA:
// the server must present a chain containing the certificate with that
// hash, and its leaf must verify against it. kubeadm-style bootstrap — the
// joiner has no trust store yet, the token is the trust.
func dialPinned(addr string, caHash [32]byte) (*tls.Conn, error) {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // verification happens below, against the pin
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			pool := x509.NewCertPool()
			for _, der := range rawCerts {
				if sha256.Sum256(der) == caHash {
					ca, err := x509.ParseCertificate(der)
					if err != nil {
						return fmt.Errorf("parsing pinned CA: %w", err)
					}
					pool.AddCert(ca)
				}
			}
			if len(rawCerts) == 0 {
				return errors.New("no certificate presented")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parsing server certificate: %w", err)
			}
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
				return fmt.Errorf("server is not a member of the token's cluster: %w", err)
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: dialing join address %s: %w", addr, err)
	}
	return conn, nil
}

// verifyChainToPool authenticates a peer by chain membership in a pool,
// without a name check.
func verifyChainToPool(pool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no certificate presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		_, err = leaf.Verify(x509.VerifyOptions{Roots: pool})
		return err
	}
}

// loadCA loads the CA with its key — issuance nodes only.
func loadCA(dir string) (*certs.CA, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		return nil, err
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		return nil, err
	}
	return certs.LoadCA(certPEM, keyPEM)
}

// loadNodeTLS loads a node's certificate (with the CA appended to its
// chain, so join clients can match their pin) and the CA trust pool.
func loadNodeTLS(dir string) (tls.Certificate, *x509.CertPool, []byte, error) {
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("cluster: reading ca.pem: %w", err)
	}
	pool, err := certs.PoolFromPEM(caPEM)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "node.pem"), filepath.Join(dir, "node.key"))
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("cluster: loading node certificate: %w", err)
	}
	caCert, err := certs.LoadCertDER(caPEM)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	cert.Certificate = append(cert.Certificate, caCert)
	return cert, pool, caPEM, nil
}
