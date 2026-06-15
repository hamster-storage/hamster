package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"text/tabwriter"
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
  drain    mark a node for removal: new writes steer off it, repair migrates
           its shards away (undrain reverses it)
  undrain  clear a node's drain flag
  remove   evict a drained, empty node from the cluster for good (its ID is
           tombstoned — a return needs a fresh join)
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
	case "drain":
		return clusterDrain(args[1:], true)
	case "undrain":
		return clusterDrain(args[1:], false)
	case "remove":
		return clusterRemove(args[1:])
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
	listen := fs.String("listen", "127.0.0.1:7946", "cluster listen address (mTLS peer transport + join/status); peers dial it, so use a reachable one")
	zone := fs.String("zone", "", "failure-domain label for this node — a rack or AZ (ADR-0016); defaults to the auto-detected host")
	capacity := fs.Uint("capacity", 0, "relative storage capacity weight (ADR-0004); 0 means equal — set it proportional to disk size on a heterogeneous cluster")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	if err := cluster.Init(*dataDir, *name, *node, *listen, *zone, uint32(*capacity), time.Now()); err != nil {
		return err
	}
	log.Printf("cluster %q initialized: node %s, listen %s", *name, *node, *listen)
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
	listen := fs.String("listen", "127.0.0.1:7946", "cluster listen address (mTLS peer transport + join/status); peers dial it, so use a reachable one")
	token := fs.String("token", "", "join token from `hamster cluster token` (required)")
	zone := fs.String("zone", "", "failure-domain label for this node — a rack or AZ (ADR-0016); defaults to the auto-detected host")
	capacity := fs.Uint("capacity", 0, "relative storage capacity weight (ADR-0004); 0 means equal — set it proportional to disk size on a heterogeneous cluster")
	fs.Parse(args)
	if *dataDir == "" || *node == "" || *token == "" {
		return fmt.Errorf("-data-dir, -node, and -token are required")
	}
	if err := cluster.Join(*dataDir, *node, *listen, *token, *zone, uint32(*capacity)); err != nil {
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
	listen := fs.String("listen", "127.0.0.1:7946", "cluster listen address — mTLS peer transport + join/status (first boot with -token only)")
	token := fs.String("token", "", "join token: an uninitialized data directory joins before running; ignored once joined, so the same command line is restart-safe")
	zone := fs.String("zone", "", "failure-domain label when joining with -token — a rack or AZ (ADR-0016); defaults to the auto-detected host")
	capacity := fs.Uint("capacity", 0, "relative storage capacity weight when joining with -token (ADR-0004); 0 means equal")
	s3 := fs.String("s3", "", "serve the S3 API on this address (host:port); empty disables")
	region := fs.String("region", "us-east-1", "S3 region name (with -s3)")
	domain := fs.String("domain", "", "virtual-hosted base domain (with -s3); empty serves path-style only")
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
		if err := cluster.Join(*dataDir, *node, *listen, *token, *zone, uint32(*capacity)); err != nil {
			return err
		}
		log.Printf("joined as node %s", *node)
	} else {
		if *token != "" {
			log.Printf("already a cluster member; ignoring -token")
		}
		// An explicit -listen on an already-joined node moves its listen
		// address on this restart — the local bind/advertise endpoint, not
		// identity. Without -listen the saved config is used unchanged, so a
		// plain restart stays restart-safe.
		if flagSet(fs, "listen") {
			if err := cluster.UpdateListenAddr(*dataDir, *listen); err != nil {
				return err
			}
			log.Printf("listen address set to %s", *listen)
		}
	}
	n, err := cluster.Run(*dataDir)
	if err != nil {
		return err
	}
	log.Printf("hamster cluster node: %s — listen %s (peer transport + join/status)", fullVersion(), n.Addr())
	if *s3 != "" {
		accessKey, secretKey := os.Getenv("HAMSTER_ACCESS_KEY_ID"), os.Getenv("HAMSTER_SECRET_ACCESS_KEY")
		if accessKey == "" || secretKey == "" {
			n.Stop()
			return fmt.Errorf("-s3 requires HAMSTER_ACCESS_KEY_ID and HAMSTER_SECRET_ACCESS_KEY in the environment")
		}
		addr, err := n.ServeS3(cluster.S3Config{
			Listen: *s3, Region: *region, Domain: *domain,
			AccessKey: accessKey, SecretKey: secretKey,
		})
		if err != nil {
			n.Stop()
			return err
		}
		log.Printf("hamster cluster node: S3 API on http://%s (region %s) — erasure-coded across the cluster", addr, *region)
		log.Printf("hamster cluster node: DEV PREVIEW — writes commit on the Raft leader only; multipart and copy are not yet on the cluster path")
	} else {
		log.Printf("hamster cluster node: DEV PREVIEW — pass -s3 to serve the S3 API over the cluster data path")
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	select {
	case <-stop:
		log.Printf("hamster cluster node: shutting down")
	case <-n.Done():
		// The node removed itself from the cluster (ADR-0004): exit rather than
		// linger as a stopped, tombstoned process.
		log.Printf("hamster cluster node: removed from the cluster; exiting")
	}
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
	addr := fs.String("addr", "", "cluster listen address of the node to ask (default: this node's own)")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	members, err := cluster.Status(*dataDir, *addr)
	if err != nil {
		return err
	}
	// tabwriter sizes each column to its widest cell, so labels like a full
	// hostname never overrun the next column (stdlib, no dependency).
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "RAFT-ID\tNODE\tADDRESS\tROLE\tHOST\tZONE\tCAPACITY\tSTATE")
	hosts, zones := map[string]bool{}, map[string]bool{}
	anyDown := false
	for _, m := range members {
		role := "voter"
		if m.Learner {
			role = "learner"
		}
		if m.Leader {
			role += " (leader)"
		}
		capacity := "equal"
		if m.Capacity != 0 {
			capacity = fmt.Sprintf("%d", m.Capacity)
		}
		// down (local liveness) takes precedence over draining (a committed
		// fact) — an unreachable node is the more urgent thing to surface.
		state := "up"
		if m.Draining {
			state = "draining"
		}
		if m.Down {
			state = "down"
			anyDown = true
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", m.RaftID, m.NodeID, m.Dial, role, m.Host, m.Zone, capacity, state)
		if m.Host != "" {
			hosts[m.Host] = true
		}
		if m.Zone != "" {
			zones[m.Zone] = true
		}
	}
	w.Flush()
	// Failure-domain topology (ADR-0016): state plainly when a level is
	// trivial — a single host or zone has no tolerance at that level.
	if anyDown {
		// STATE is the answering node's local, best-effort view (ADR-0027):
		// a peer it currently treats as down, not a committed cluster fact.
		fmt.Println("  note: STATE is this node's local liveness view; another node may differ")
	}
	fmt.Printf("\ntopology: %d node(s), %d host(s), %d zone(s)\n", len(members), len(hosts), len(zones))
	if len(hosts) <= 1 {
		fmt.Println("  note: one host — no host-level failure tolerance (shards can share a machine)")
	}
	if len(zones) <= 1 {
		fmt.Println("  note: one zone — no zone-level failure tolerance")
	}
	return nil
}

func clusterDrain(args []string, draining bool) error {
	cmd := "drain"
	if !draining {
		cmd = "undrain"
	}
	fs := flag.NewFlagSet("cluster "+cmd, flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	node := fs.String("node", "", "the node ID to "+cmd+" (required)")
	addr := fs.String("addr", "", "cluster address of a node to ask (default: this node's own; auto-redirects to the leader)")
	fs.Parse(args)
	if *dataDir == "" || *node == "" {
		return fmt.Errorf("-data-dir and -node are required")
	}
	if err := cluster.Drain(*dataDir, *addr, *node, draining); err != nil {
		return err
	}
	if draining {
		log.Printf("node %s is draining — new writes steer off it; repair migrates its shards away", *node)
	} else {
		log.Printf("node %s is active again (drain cleared)", *node)
	}
	return nil
}

func clusterRemove(args []string) error {
	fs := flag.NewFlagSet("cluster remove", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	node := fs.String("node", "", "the node ID to remove (required)")
	addr := fs.String("addr", "", "cluster address of a node to ask (default: this node's own; auto-redirects to the leader)")
	fs.Parse(args)
	if *dataDir == "" || *node == "" {
		return fmt.Errorf("-data-dir and -node are required")
	}
	if err := cluster.Remove(*dataDir, *addr, *node); err != nil {
		return err
	}
	log.Printf("node %s removed from the cluster; its ID is retired — a return needs a fresh join", *node)
	return nil
}

// flagSet reports whether the named flag was given explicitly on the command
// line (as opposed to left at its default) — the FlagSet records only the
// flags the user actually set.
func flagSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}
