package cluster

import (
	"fmt"
	"os"
	"path/filepath"
)

// The cluster directory under a node's data directory:
//
//	<data-dir>/cluster/
//	  node.conf   versioned protobuf NodeConfig (below)
//	  ca.pem      the cluster CA certificate — every node
//	  ca.key      the CA private key — issuance nodes only (the init node, in v0.2)
//	  node.pem    this node's certificate
//	  node.key    this node's private key
//	  tokens/     one file per outstanding join token, named by token ID
//
//	message NodeConfig {
//	  uint32 format_version = 1;
//	  string cluster = 2;
//	  string node_id = 3;
//	  uint64 raft_id = 4;
//	  string cluster_addr = 5;   // the mTLS transport listen/advertise address
//	  string join_addr = 6;      // the join/status listener address
//	  bool join = 7;             // this node joined; it never bootstraps
//	  repeated Member members = 8;  // the address book as of init/join
//	  uint64 next_raft_id = 9;   // issuance counter; init node only
//	  string host = 10;          // this node's machine identity (ADR-0016)
//	  string zone = 11;          // this node's failure-domain label
//	  repeated Member node_labels = 12;  // issuer's host/zone/capacity registry, by node ID
//	  uint32 capacity = 13;      // this node's relative capacity weight (ADR-0004)
//	}
const configVersion = 1

// NodeConfig is a node's durable cluster identity. Host and Zone are this
// node's own failure-domain labels (ADR-0016). NodeLabels is the issuer's
// growing registry of every member's labels (node ID → host/zone), recorded
// as nodes join — the source the layout reconcile reads to compose a labeled
// layout. Only the issuer (the init node, in v0.2/v0.3) accumulates it.
type NodeConfig struct {
	Cluster     string
	NodeID      string
	RaftID      uint64
	ClusterAddr string
	JoinAddr    string
	Join        bool
	Members     []Member
	NextRaftID  uint64
	Host        string
	Zone        string
	NodeLabels  []Member
	Capacity    uint32
}

// Dir is the cluster directory under a data directory.
func Dir(dataDir string) string { return filepath.Join(dataDir, "cluster") }

func configPath(dir string) string { return filepath.Join(dir, "node.conf") }

// Initialized reports whether a data directory holds a cluster identity.
func Initialized(dataDir string) bool {
	_, err := os.Stat(configPath(Dir(dataDir)))
	return err == nil
}

func encodeConfig(c NodeConfig) []byte {
	b := putUint(nil, 1, configVersion)
	b = putString(b, 2, c.Cluster)
	b = putString(b, 3, c.NodeID)
	b = putUint(b, 4, c.RaftID)
	b = putString(b, 5, c.ClusterAddr)
	b = putString(b, 6, c.JoinAddr)
	b = putBool(b, 7, c.Join)
	for _, m := range c.Members {
		b = putBytes(b, 8, encodeMemberMsg(m))
	}
	b = putUint(b, 9, c.NextRaftID)
	b = putString(b, 10, c.Host)
	b = putString(b, 11, c.Zone)
	for _, m := range c.NodeLabels {
		b = putBytes(b, 12, encodeMemberMsg(m))
	}
	b = putUint(b, 13, uint64(c.Capacity))
	return b
}

func decodeConfig(buf []byte) (NodeConfig, error) {
	var c NodeConfig
	err := forEachField(buf, func(f field) error {
		switch f.num {
		case 2:
			c.Cluster = string(f.b)
		case 3:
			c.NodeID = string(f.b)
		case 4:
			c.RaftID = f.u
		case 5:
			c.ClusterAddr = string(f.b)
		case 6:
			c.JoinAddr = string(f.b)
		case 7:
			c.Join = f.u != 0
		case 8:
			m, err := decodeMemberMsg(f.b)
			if err != nil {
				return err
			}
			c.Members = append(c.Members, m)
		case 9:
			c.NextRaftID = f.u
		case 10:
			c.Host = string(f.b)
		case 11:
			c.Zone = string(f.b)
		case 12:
			m, err := decodeMemberMsg(f.b)
			if err != nil {
				return err
			}
			c.NodeLabels = append(c.NodeLabels, m)
		case 13:
			c.Capacity = uint32(f.u)
		}
		return nil
	})
	return c, err
}

// loadConfig reads a node's cluster identity.
func loadConfig(dir string) (NodeConfig, error) {
	buf, err := os.ReadFile(configPath(dir))
	if err != nil {
		return NodeConfig{}, fmt.Errorf("cluster: reading node.conf (is this data directory part of a cluster?): %w", err)
	}
	c, err := decodeConfig(buf)
	if err != nil {
		return NodeConfig{}, fmt.Errorf("cluster: decoding node.conf: %w", err)
	}
	return c, nil
}

// saveConfig writes a node's cluster identity durably: temp file, sync,
// rename.
func saveConfig(dir string, c NodeConfig) error {
	tmp := configPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("cluster: writing node.conf: %w", err)
	}
	if _, err := f.Write(encodeConfig(c)); err != nil {
		f.Close()
		return fmt.Errorf("cluster: writing node.conf: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("cluster: syncing node.conf: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cluster: closing node.conf: %w", err)
	}
	if err := os.Rename(tmp, configPath(dir)); err != nil {
		return fmt.Errorf("cluster: replacing node.conf: %w", err)
	}
	return nil
}
