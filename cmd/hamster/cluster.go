package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/hamster-storage/hamster/internal/cluster"
)

const clusterUsage = `usage: hamster cluster <command> [flags]

commands:
  init     create a new cluster: mint the CA and this node's identity
  token    mint a single-use join token (on the init node)
  join     join an existing cluster with a token (identity only; run starts it)
  run      run this cluster node (v0.2 preview: the replicated metadata
           plane; S3 serving joins it with the erasure-coded data path).
           With -token, an uninitialized node joins first — one command,
           restart-safe
  status   show cluster membership from a running node
  recover  rewrite a stopped survivor into a new single-voter cluster —
           the last resort when a majority of voters is permanently lost
`

func clusterCmd(args []string) error {
	if len(args) < 1 {
		fmt.Fprint(os.Stderr, clusterUsage)
		os.Exit(2)
	}
	switch args[0] {
	case "init":
		return clusterInit(args[1:])
	case "token":
		return clusterToken(args[1:])
	case "join":
		return clusterJoin(args[1:])
	case "run":
		return clusterRun(args[1:])
	case "status":
		return clusterStatus(args[1:])
	case "recover":
		return clusterRecover(args[1:])
	default:
		fmt.Fprint(os.Stderr, clusterUsage)
		os.Exit(2)
		return nil
	}
}

func clusterInit(args []string) error {
	fs := flag.NewFlagSet("cluster init", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "directory for this node's data (required)")
	name := fs.String("cluster", "hamster", "cluster name")
	node := fs.String("node", "n1", "this node's ID")
	listenCluster := fs.String("listen-cluster", "127.0.0.1:7946", "inter-node (mTLS) listen address; peers dial it, so use a reachable one")
	listenJoin := fs.String("listen-join", "127.0.0.1:7947", "join/status listen address")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	if err := cluster.Init(*dataDir, *name, *node, *listenCluster, *listenJoin, time.Now()); err != nil {
		return err
	}
	log.Printf("cluster %q initialized: node %s, transport %s, join %s", *name, *node, *listenCluster, *listenJoin)
	log.Printf("next: hamster cluster run -data-dir %s", *dataDir)
	return nil
}

func clusterToken(args []string) error {
	fs := flag.NewFlagSet("cluster token", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	ttl := fs.Duration("ttl", 24*time.Hour, "token validity")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	tok, err := cluster.MintToken(*dataDir, *ttl, time.Now())
	if err != nil {
		return err
	}
	fmt.Println(tok)
	return nil
}

func clusterJoin(args []string) error {
	fs := flag.NewFlagSet("cluster join", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "directory for this node's data (required)")
	node := fs.String("node", "", "this node's ID (required, unique in the cluster)")
	listenCluster := fs.String("listen-cluster", "127.0.0.1:7946", "inter-node (mTLS) listen address; peers dial it, so use a reachable one")
	listenJoin := fs.String("listen-join", "127.0.0.1:7947", "join/status listen address")
	token := fs.String("token", "", "join token from `hamster cluster token` (required)")
	fs.Parse(args)
	if *dataDir == "" || *node == "" || *token == "" {
		return fmt.Errorf("-data-dir, -node, and -token are required")
	}
	if err := cluster.Join(*dataDir, *node, *listenCluster, *listenJoin, *token); err != nil {
		return err
	}
	log.Printf("joined as node %s", *node)
	log.Printf("next: hamster cluster run -data-dir %s", *dataDir)
	return nil
}

func clusterRun(args []string) error {
	fs := flag.NewFlagSet("cluster run", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	node := fs.String("node", "", "this node's ID (first boot with -token only)")
	listenCluster := fs.String("listen-cluster", "127.0.0.1:7946", "inter-node (mTLS) listen address (first boot with -token only)")
	listenJoin := fs.String("listen-join", "127.0.0.1:7947", "join/status listen address (first boot with -token only)")
	token := fs.String("token", "", "join token: an uninitialized data directory joins before running; ignored once joined, so the same command line is restart-safe")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	if !cluster.Initialized(*dataDir) {
		if *token == "" {
			return fmt.Errorf("%s is not part of a cluster: run `hamster cluster init` or `hamster cluster join`, or pass -token to join and run in one step", *dataDir)
		}
		if *node == "" {
			return fmt.Errorf("-node is required when joining with -token")
		}
		if err := cluster.Join(*dataDir, *node, *listenCluster, *listenJoin, *token); err != nil {
			return err
		}
		log.Printf("joined as node %s", *node)
	} else if *token != "" {
		log.Printf("already a cluster member; ignoring -token")
	}
	n, err := cluster.Run(*dataDir)
	if err != nil {
		return err
	}
	log.Printf("hamster cluster node: %s — transport %s, join/status %s", fullVersion(), n.Addr(), n.JoinAddr())
	log.Printf("hamster cluster node: DEV PREVIEW — metadata plane only; S3 serving stays single-node until the data path replicates (v0.3)")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Printf("hamster cluster node: shutting down")
	n.Stop()
	return nil
}

func clusterRecover(args []string) error {
	fs := flag.NewFlagSet("cluster recover", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "the surviving node's data directory (required)")
	force := fs.Bool("force", false, "confirm: the other members are gone forever and their data directories will never run again")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	if !*force {
		fmt.Fprintln(os.Stderr, `cluster recover rewrites this stopped node into a NEW single-voter cluster.

Use it only when a majority of voters is permanently lost — dead disks,
not a reboot. It is irreversible:

  - everything in this node's local log becomes the cluster's history,
    including entries the old cluster may never have acknowledged
  - every other member is removed; their data directories hold a
    competing history and MUST NEVER run again
  - the cluster then grows again with fresh join tokens

If the missing nodes might come back, start them instead - quorum will
re-form on its own. To proceed, rerun with -force.`)
		os.Exit(2)
	}
	sum, err := cluster.Recover(*dataDir)
	if err != nil {
		return err
	}
	log.Printf("recovered: a new single-voter cluster at log index %d", sum.LastIndex)
	for _, m := range sum.Removed {
		log.Printf("removed: %s (raft id %d, %s) — its data directory must never run again", m.Addr, m.ID, m.Dial)
	}
	if !cluster.CanIssue(*dataDir) {
		log.Printf("WARNING: this node does not hold the cluster CA key (ca.key lives on the init node);")
		log.Printf("WARNING: it cannot mint join tokens, so this cluster cannot grow until the CA is restored")
	}
	log.Printf("next: hamster cluster run -data-dir %s", *dataDir)
	return nil
}

func clusterStatus(args []string) error {
	fs := flag.NewFlagSet("cluster status", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	addr := fs.String("addr", "", "join/status address of the node to ask (default: this node's own)")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	members, err := cluster.Status(*dataDir, *addr)
	if err != nil {
		return err
	}
	fmt.Printf("%-8s %-16s %-22s %-8s\n", "RAFT-ID", "NODE", "ADDRESS", "ROLE")
	for _, m := range members {
		role := "voter"
		if m.Learner {
			role = "learner"
		}
		if m.Leader {
			role += " (leader)"
		}
		fmt.Printf("%-8d %-16s %-22s %-8s\n", m.RaftID, m.NodeID, m.Dial, role)
	}
	return nil
}
