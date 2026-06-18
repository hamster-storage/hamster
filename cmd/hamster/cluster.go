package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/hamster-storage/hamster/internal/cluster"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/keys"
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
  can-stop check whether taking a node down for maintenance/upgrade is safe now
           (ADR-0034): quorum holds, no other node is down, no transition open.
           Advisory — exits 0 (safe) or 1 (not); it never stops a node
  drain    take a node out of service: new writes steer off it, repair migrates
           its shards away. Reversible with undrain (e.g. for maintenance);
           follow with remove to decommission. If it shrinks the cluster past a
           storage-profile boundary it re-encodes the data down (prompts first;
           -reencode skips the prompt)
  undrain  return a drained node to service
  remove   evict a drained, empty node from the cluster for good (its ID is
           tombstoned — a return needs a fresh join)
  optimize re-encode existing data up to the current cluster's storage profile,
           spreading objects written when it was smaller across the nodes added
           since (run after growing the cluster — never automatic)
  encrypt  turn on encryption at rest (ADR-0021): new writes are encrypted,
           existing objects stay readable. Permanent (no disable). Every node
           must run with -master-key-file holding the same key
  rotate-key  rotate the cluster master key (ADR-0032): rewrap every object's
           key from the old to the new — metadata only, object bytes never move.
           Every node must run with -new-master-key-file holding the next key;
           on success the old key can be retired
  rotate-ca  rotate the cluster CA (ADR-0033): mint a new CA, trust it alongside
           the old, reissue every node's certificate onto it, then drop the old —
           no downtime, no trust gap. The new CA key never leaves the leader
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
	case "can-stop":
		return clusterCanStop(args[1:])
	case "drain":
		return clusterDrain(args[1:], true)
	case "undrain":
		return clusterDrain(args[1:], false)
	case "remove":
		return clusterRemove(args[1:])
	case "optimize":
		return clusterOptimize(args[1:])
	case "encrypt":
		return clusterEncrypt(args[1:])
	case "rotate-key":
		return clusterRotateKey(args[1:])
	case "rotate-ca":
		return clusterRotateCA(args[1:])
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
	replaces := fs.String("replaces", "", "replace an existing member with this node at the same cluster size (ADR-0004): the cluster migrates the old node's shards across and evicts it, profile unchanged")
	fs.Parse(args)
	if *dataDir == "" || *node == "" || *token == "" {
		return fmt.Errorf("-data-dir, -node, and -token are required")
	}
	if err := cluster.Join(*dataDir, *node, *listen, *token, *zone, uint32(*capacity), *replaces); err != nil {
		return err
	}
	if *replaces != "" {
		log.Printf("joined as node %s, replacing %s — the cluster will migrate %s's shards here and evict it", *node, *replaces, *replaces)
	} else {
		log.Printf("joined as node %s", *node)
	}
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
	replaces := fs.String("replaces", "", "when joining with -token, replace an existing member with this node at the same cluster size (ADR-0004): the old node's shards migrate here and it is evicted, profile unchanged")
	s3 := fs.String("s3", "", "serve the S3 API on this address (host:port); empty disables")
	admin := fs.String("admin", "", "serve the admin endpoints on this address (host:port): Prometheus metrics at /metrics (ADR-0035); empty disables")
	region := fs.String("region", "us-east-1", "S3 region name (with -s3)")
	domain := fs.String("domain", "", "virtual-hosted base domain (with -s3); empty serves path-style only")
	masterKeyFile := fs.String("master-key-file", "", "path to the cluster master key (KEK) for encryption at rest (ADR-0021): 32 bytes raw, or hex/base64. The same key on every node; held in memory only, never persisted. Mount it off the data disk (e.g. a Kubernetes Secret volume)")
	newMasterKeyFile := fs.String("new-master-key-file", "", "path to the incoming master key during a key rotation (ADR-0032): the node holds both this and -master-key-file so `cluster rotate-key` can rewrap onto it. Provision it on every node; held in memory only, never sent over the wire")
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
		if err := cluster.Join(*dataDir, *node, *listen, *token, *zone, uint32(*capacity), *replaces); err != nil {
			return err
		}
		if *replaces != "" {
			log.Printf("joined as node %s, replacing %s", *node, *replaces)
		} else {
			log.Printf("joined as node %s", *node)
		}
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
	// Advertise this binary's version and declared protocol generation (ADR-0034)
	// so the cluster's effective generation tracks the roll.
	runOpts := []cluster.Option{cluster.WithVersion(fullVersion(), protocolGeneration())}
	if *masterKeyFile != "" {
		material, err := os.ReadFile(*masterKeyFile)
		if err != nil {
			return fmt.Errorf("reading -master-key-file: %w", err)
		}
		kek, err := keys.LoadKEK(material)
		if err != nil {
			return fmt.Errorf("loading master key: %w", err)
		}
		runOpts = append(runOpts, cluster.WithMasterKey(kek))
		log.Printf("hamster cluster node: master key loaded — encryption at rest available")
	}
	if *newMasterKeyFile != "" {
		material, err := os.ReadFile(*newMasterKeyFile)
		if err != nil {
			return fmt.Errorf("reading -new-master-key-file: %w", err)
		}
		kek, err := keys.LoadKEK(material)
		if err != nil {
			return fmt.Errorf("loading new master key: %w", err)
		}
		runOpts = append(runOpts, cluster.WithNewMasterKey(kek))
		log.Printf("hamster cluster node: new master key loaded — `cluster rotate-key` can rewrap onto it")
	}
	n, err := cluster.Run(*dataDir, runOpts...)
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
	var adminSrv *http.Server
	if *admin != "" {
		adminSrv = startAdmin(*admin, n.Metrics())
		log.Printf("hamster cluster node: admin endpoints on http://%s (metrics at /metrics)", *admin)
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
	shutdownAdmin(adminSrv)
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
	report, err := cluster.Status(*dataDir, *addr)
	if err != nil {
		return err
	}
	members := report.Members
	// tabwriter sizes each column to its widest cell, so labels like a full
	// hostname never overrun the next column (stdlib, no dependency).
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "RAFT-ID\tNODE\tADDRESS\tROLE\tHOST\tZONE\tCAPACITY\tVERSION\tGEN\tSTATE")
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
		ver := m.BinaryVersion
		if ver == "" {
			ver = "—"
		}
		gen := "—"
		if m.Generation != 0 {
			gen = fmt.Sprintf("%d", m.Generation)
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", m.RaftID, m.NodeID, m.Dial, role, m.Host, m.Zone, capacity, ver, gen, state)
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
	// Cluster protocol generation (ADR-0034): the effective generation is the min
	// across live members and rolls forward as the last node upgrades. Surface a
	// roll in progress and flag a skew beyond one generation (the one-step rule).
	if report.EffectiveGeneration != 0 || anyGeneration(members) {
		fmt.Printf("\ncluster protocol generation: effective %d\n", report.EffectiveGeneration)
		lo, hi := generationSpread(members)
		if hi != lo {
			fmt.Printf("  upgrade in progress: members span generation %d–%d; the effective generation rolls forward when the last node upgrades\n", lo, hi)
		}
		if hi-lo > 1 {
			fmt.Printf("  WARNING: generation skew %d exceeds one step — upgrade one generation at a time (ADR-0034)\n", hi-lo)
		}
	}
	fmt.Printf("\ntopology: %d node(s), %d host(s), %d zone(s)\n", len(members), len(hosts), len(zones))
	if len(hosts) <= 1 {
		fmt.Println("  note: one host — no host-level failure tolerance (shards can share a machine)")
	}
	if len(zones) <= 1 {
		fmt.Println("  note: one zone — no zone-level failure tolerance")
	}
	if report.Encryption != "" {
		fmt.Printf("encryption at rest: %s", report.Encryption)
		if report.KEKFingerprint != "" {
			fmt.Printf(" (key %s)", report.KEKFingerprint)
		}
		fmt.Println()
		if report.RotatingTo != "" {
			fmt.Printf("  key rotation in progress → %s: %d object(s) still on the old key\n", report.RotatingTo, report.Remaining)
		}
	} else {
		fmt.Println("encryption at rest: off")
	}
	if report.TrustVersion > 0 {
		fmt.Printf("cluster CA: trust bundle generation %d", report.TrustVersion)
		if report.CARotating {
			fmt.Printf(" — CA rotation in progress: %d node(s) still on the old CA", report.CAStragglers)
		}
		fmt.Println()
	}
	return nil
}

// clusterCanStop runs the advisory health interlock (ADR-0034): it asks whether
// taking a node down for maintenance or upgrade is safe right now. It prints the
// verdict and reason and exits 0 when safe, 1 when not — so a roll script can
// gate on `hamster cluster can-stop <node> && stop-and-upgrade <node>`. Advisory
// only: it never stops a node.
func clusterCanStop(args []string) error {
	fs := flag.NewFlagSet("cluster can-stop", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	node := fs.String("node", "", "the node to check (required)")
	addr := fs.String("addr", "", "cluster listen address of the node to ask (default: this node's own)")
	fs.Parse(args)
	if *dataDir == "" || *node == "" {
		return fmt.Errorf("-data-dir and -node are required")
	}
	safe, reason, err := cluster.CanStop(*dataDir, *addr, *node)
	if err != nil {
		return err
	}
	if safe {
		fmt.Printf("can-stop %s: YES — %s\n", *node, reason)
		return nil
	}
	fmt.Printf("can-stop %s: NO — %s\n", *node, reason)
	os.Exit(1)
	return nil
}

// anyGeneration reports whether any member has advertised a protocol generation
// (ADR-0034) — false on a cluster that predates version advertisement.
func anyGeneration(ms []cluster.Member) bool {
	for _, m := range ms {
		if m.Generation != 0 {
			return true
		}
	}
	return false
}

// generationSpread returns the lowest and highest advertised generation across
// members (ignoring unrecorded zeros), for the roll-in-progress and skew notes.
func generationSpread(ms []cluster.Member) (lo, hi uint32) {
	first := true
	for _, m := range ms {
		if m.Generation == 0 {
			continue
		}
		if first || m.Generation < lo {
			lo = m.Generation
		}
		if first || m.Generation > hi {
			hi = m.Generation
		}
		first = false
	}
	return lo, hi
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
	reencode := fs.Bool("reencode", false, "proceed without the prompt when draining crosses a storage-profile boundary (re-encodes existing data to the smaller profile)")
	fs.Parse(args)
	if *dataDir == "" || *node == "" {
		return fmt.Errorf("-data-dir and -node are required")
	}

	// Draining a node that shrinks the cluster past a profile boundary re-encodes
	// every object to the smaller profile (ADR-0004, ADR-0015) — a consequential,
	// possibly durability-reducing change. Warn and confirm before committing it.
	if draining {
		if report, err := cluster.Status(*dataDir, *addr); err == nil {
			active, isMember := 0, false
			for _, m := range report.Members {
				if m.NodeID == *node {
					isMember = true
				}
				if !m.Draining {
					active++
				}
			}
			if isMember {
				if msg, downsize := downsizeWarning(*node, active); downsize && !*reencode {
					fmt.Println(msg)
					if !confirm() {
						return fmt.Errorf("drain cancelled")
					}
				}
			}
		}
	}

	if err := cluster.Drain(*dataDir, *addr, *node, draining); err != nil {
		return err
	}
	if draining {
		log.Printf("node %s is draining — new writes steer off it and its shards migrate away; undrain to return it to service, or remove to decommission", *node)
	} else {
		log.Printf("node %s is back in service (drain cleared)", *node)
	}
	return nil
}

// downsizeWarning reports whether draining one node from a cluster of `active`
// non-draining nodes crosses a storage-profile boundary — forcing every object
// to be re-encoded to the smaller profile — and, if so, the operator warning
// describing the change. `active` is the count before this drain.
func downsizeWarning(node string, active int) (msg string, isDownsize bool) {
	cur := ec.AutoProfile(active)
	if active < 1 || cur.Nodes() <= active-1 {
		return "", false // the current widest profile still fits — a same-size drain
	}
	next := ec.AutoProfile(active - 1)
	tol := fmt.Sprintf("tolerates %d failures → tolerates %d (unchanged)", cur.Parity, next.Parity)
	if next.Parity < cur.Parity {
		tol = fmt.Sprintf("tolerates %d failures → tolerates %d (REDUCED)", cur.Parity, next.Parity)
	}
	overhead := func(p ec.Profile) float64 { return float64(p.Data+p.Parity) / float64(p.Data) }
	return fmt.Sprintf(
		"Draining %s shrinks the cluster from %d to %d nodes, re-encoding all existing\n"+
			"data from %s to %s:\n"+
			"  durability:       %s\n"+
			"  storage overhead: %.2fx → %.2fx\n"+
			"The object data is unchanged; re-encoding happens in place and can take a while.\n"+
			"Once it converges, `cluster remove %s` evicts the node.",
		node, active, active-1, cur.Name, next.Name, tol, overhead(cur), overhead(next), node), true
}

// confirm reads a yes/no answer from stdin, defaulting to no.
func confirm() bool {
	fmt.Print("Proceed? [y/N]: ")
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	line = strings.ToLower(strings.TrimSpace(line))
	return line == "y" || line == "yes"
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

func clusterOptimize(args []string) error {
	fs := flag.NewFlagSet("cluster optimize", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	addr := fs.String("addr", "", "cluster address of a node to ask (default: this node's own; auto-redirects to the leader)")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	log.Printf("optimizing: waiting for any recent membership change to reconcile, then re-encoding existing data up to the current cluster's storage profile — this can take a while")
	rep, err := cluster.Optimize(*dataDir, *addr)
	if err != nil {
		return err
	}
	if rep.ReEncoded == 0 {
		log.Printf("optimize complete: all %d objects already fit the current profile — nothing to do", rep.Objects)
	} else {
		log.Printf("optimize complete: re-encoded %d of %d objects up to the current profile", rep.ReEncoded, rep.Objects)
	}
	return nil
}

func clusterEncrypt(args []string) error {
	fs := flag.NewFlagSet("cluster encrypt", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	addr := fs.String("addr", "", "cluster address of a node to ask (default: this node's own; auto-redirects to the leader)")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	label, err := cluster.Encrypt(*dataDir, *addr)
	if err != nil {
		return err
	}
	log.Printf("encryption at rest enabled (%s): new writes are now encrypted; existing objects are unchanged and stay readable. This is permanent — encryption cannot be turned off.", label)
	log.Printf("  every node must hold the same master key (-master-key-file); a node without it cannot serve encrypted reads")
	return nil
}

func clusterRotateKey(args []string) error {
	fs := flag.NewFlagSet("cluster rotate-key", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	addr := fs.String("addr", "", "cluster address of a node to ask (default: this node's own; auto-redirects to the leader)")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	log.Printf("rotating the cluster master key: rewrapping every object's key onto the new one (object bytes are not touched). This may take a while on a large cluster.")
	rep, err := cluster.RotateKey(*dataDir, *addr)
	if err != nil {
		return err
	}
	if !rep.Completed {
		return fmt.Errorf("rotation did not converge: %d object(s) still on the old key — re-run `cluster rotate-key`", rep.Remaining)
	}
	log.Printf("key rotation complete: rewrapped %d object(s) onto the new key. The old key is no longer in use — retire it and run every node with the new key as -master-key-file.", rep.Rewrapped)
	return nil
}

func clusterRotateCA(args []string) error {
	fs := flag.NewFlagSet("cluster rotate-ca", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "this node's data directory (required)")
	addr := fs.String("addr", "", "cluster address of a node to ask (default: this node's own; auto-redirects to the leader)")
	fs.Parse(args)
	if *dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	log.Printf("rotating the cluster CA: minting a new CA, trusting it alongside the old, reissuing every node onto it, then dropping the old. No downtime.")
	rep, err := cluster.RotateCA(*dataDir, *addr)
	if err != nil {
		return err
	}
	if !rep.Completed {
		return fmt.Errorf("CA rotation did not complete (reissued %d) — re-run `cluster rotate-ca`", rep.Reissued)
	}
	log.Printf("CA rotation complete: reissued %d node certificate(s) onto the new CA and dropped the old one. The old CA is retired.", rep.Reissued)
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
