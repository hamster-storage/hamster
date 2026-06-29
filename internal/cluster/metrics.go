package cluster

import (
	"crypto/tls"
	"strconv"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/metrics"
)

// Observability (ADR-0035). A node owns a metrics registry: the single in-process
// source of truth for its signals, served as Prometheus text on the admin port
// and (a later pass) as a typed snapshot for the CLI and console. This pass wires
// a first signal set proving the surface end to end — build/node identity, uptime,
// and the cluster-wide gauges any node derives from its own replica.

// Metrics returns the node's registry, for the admin /metrics endpoint.
func (n *Node) Metrics() *metrics.Registry { return n.metrics }

// handleMetrics serves a metrics snapshot request (ADR-0035): the node's registry
// as the typed wire snapshot, authenticated by a cluster certificate like status.
// Read-only and per-node, so any member answers — no leader redirect.
func (n *Node) handleMetrics(conn *tls.Conn) metricsResponse {
	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return metricsResponse{Error: "metrics requires a cluster certificate"}
	}
	return metricsResponse{Snapshot: metrics.MarshalSnapshot(n.metrics.Snapshot())}
}

// initMetrics builds the registry and registers the first signal set. Constant
// identity metrics are set once; the cluster-wide gauges are refreshed by a
// collector at scrape time, so they reflect live membership without a background
// poll. Collectors read through the loop and guard a not-yet-built raft, so a
// scrape during early startup is safe.
func (n *Node) initMetrics() {
	r := metrics.NewRegistry()
	n.metrics = r

	version := n.binaryVersion
	if version == "" {
		version = "unknown"
	}
	r.NewGauge("hamster_build_info",
		"Build and version info; constant 1, labeled by binary version and declared protocol generation.",
		"version", "generation").
		Set(1, version, strconv.FormatUint(uint64(n.generation), 10))
	r.NewGauge("hamster_node_info",
		"Node identity; constant 1, labeled by node ID and cluster name.",
		"node_id", "cluster").
		Set(1, n.cfg.NodeID, n.cfg.Cluster)

	uptime := r.NewGauge("hamster_uptime_seconds", "Seconds since this node started.")
	members := r.NewGauge("hamster_cluster_members", "Cluster members known to this node.")
	voters := r.NewGauge("hamster_cluster_voters", "Voting members known to this node.")
	isLeader := r.NewGauge("hamster_raft_is_leader", "1 if this node is the Raft leader, else 0.")
	effGen := r.NewGauge("hamster_cluster_effective_generation",
		"Cluster effective protocol generation: the minimum across live members (ADR-0034).")
	down := r.NewGauge("hamster_cluster_nodes_down",
		"Members this node currently treats as down (its local liveness view).")

	r.AddCollector(func() {
		uptime.Set(n.clock.Now().Sub(n.startAt).Seconds())
		if n.raft == nil {
			return
		}
		ms := n.members()
		members.Set(float64(len(ms)))
		v, d, leader := 0, 0, 0.0
		for _, m := range ms {
			if !m.Learner {
				v++
			}
			if m.Down {
				d++
			}
			if m.NodeID == n.cfg.NodeID && m.Leader {
				leader = 1
			}
		}
		voters.Set(float64(v))
		isLeader.Set(leader)
		effGen.Set(float64(effectiveGeneration(ms)))
		down.Set(float64(d))
	})

	// S3 request counter (ADR-0035): incremented by the ServeS3 middleware. The
	// counter family is declared here so it appears in the registry from the
	// start; a method/code series springs into being on its first request.
	n.s3Requests = r.NewCounter("hamster_s3_requests_total",
		"S3 requests served by this node, by method and HTTP status code.", "method", "code")

	// Per-operation data-plane latency (ADR-0039 part 1, the ADR-0035 follow-on):
	// the coordinator times each PUT and GET on the loop through the seam clock,
	// from admission to completion, and reports the service time via the
	// ObserveLatency hook wired in node.go — the same coord→cluster decoupling the
	// streaming load gauges use (the coordinator never imports internal/metrics).
	// Only a successful operation is observed, so the distribution is service time,
	// the baseline the load shedder's minRTT/curRTT will build on. The method label
	// matches the s3_requests_total counter's PUT/GET.
	n.s3ReqDuration = r.NewHistogram("hamster_s3_request_duration_seconds",
		"Data-plane S3 operation latency in seconds, by method (ADR-0039), from admission to completion.",
		metrics.DefaultLatencyBuckets, "method")

	// RTT gradient (ADR-0039 part 2): the coordinator maintains, per operation, a
	// no-load baseline (minRTT, a re-probing long-window minimum that can rise as
	// the floor degrades) and a short-window recent estimate (curRTT). Their ratio
	// — the gradient, ≈1 healthy and →0 as queuing grows — is the signal the load
	// shedder (part 3/4) and degradation detection (part 5) build on. Exposed the
	// same way the durability gauges are: coordinator accessors read on the loop by
	// a scrape collector, so the coordinator never imports internal/metrics.
	gradient := r.NewGauge("hamster_s3_request_gradient",
		"Latency gradient clamp(minRTT/curRTT,0..1) per operation (ADR-0039): ~1 healthy, ->0 as queuing grows.",
		"method")
	minRTT := r.NewGauge("hamster_s3_request_min_rtt_seconds",
		"Best-case service time per operation in seconds: the re-probing long-window minimum (ADR-0039).",
		"method")
	curRTT := r.NewGauge("hamster_s3_request_cur_rtt_seconds",
		"Recent service-time estimate per operation in seconds: the short-window EWMA (ADR-0039).",
		"method")
	// Materialize each op's series at the healthy default so the lines exist from
	// the first scrape, before any operation has run.
	for _, op := range coord.GradientOps() {
		gradient.Set(1, op)
		minRTT.Set(0, op)
		curRTT.Set(0, op)
	}
	r.AddCollector(func() {
		if n.coord == nil {
			return
		}
		n.on(func() {
			for _, op := range coord.GradientOps() {
				gradient.Set(n.coord.Gradient(op), op)
				minRTT.Set(n.coord.MinRTT(op), op)
				curRTT.Set(n.coord.CurRTT(op), op)
			}
		})
	})

	// Streaming-PUT load signals (ADR-0038): the headline questions for the
	// data path under load — how many writes are in flight, how much they move,
	// and how often the feeder is throttled by the shard streams (the
	// backpressure stall is the "are we shard-bound?" signal a load test wants).
	n.putInflight = r.NewGauge("hamster_put_inflight",
		"Cluster PUT operations currently streaming through this node.")
	n.putBytes = r.NewCounter("hamster_put_bytes_total",
		"Object bytes accepted by completed cluster PUTs on this node.")
	n.putBackpressureWaits = r.NewCounter("hamster_put_backpressure_waits_total",
		"Times a streaming PUT feeder stalled waiting for the coordinator to accept the next chunk (shard-bound backpressure).")
	// Materialize each series at zero so it is present from the first scrape —
	// a load-test dashboard wants the line before the first PUT, not after.
	n.putInflight.Set(0)
	n.putBytes.Add(0)
	n.putBackpressureWaits.Add(0)

	// Durability posture (ADR-0035): cluster-wide signals any node derives from its
	// own replica — the compliance-shaped store's headline question, "is my data
	// safe and at what width." Refreshed at scrape time from one loop read.
	objects := r.NewGauge("hamster_object_versions",
		"Stored object versions known to this node's replica.")
	buckets := r.NewGauge("hamster_buckets", "Buckets known to this node's replica.")
	dataShards := r.NewGauge("hamster_storage_profile_data_shards",
		"Active auto storage-profile data shards (k) at the current active node count (ADR-0015).")
	parityShards := r.NewGauge("hamster_storage_profile_parity_shards",
		"Active auto storage-profile parity shards (m): node-loss tolerance (ADR-0015).")
	transition := r.NewGauge("hamster_layout_transition_open",
		"1 while a layout migration is in progress (ADR-0004), else 0.")

	r.AddCollector(func() {
		if n.raft == nil {
			return
		}
		st := n.durabilityStats()
		objects.Set(float64(st.objectVersions))
		buckets.Set(float64(st.buckets))
		dataShards.Set(float64(st.dataShards))
		parityShards.Set(float64(st.parityShards))
		if st.transitionOpen {
			transition.Set(1)
		} else {
			transition.Set(0)
		}
	})
}

// durabilityStat is the cluster's durability posture as one node's replica sees
// it (ADR-0035): the stored version count, the bucket count, the active auto
// storage profile (k+m at the current active node count), and whether a layout
// migration is open. Shared by the metrics collector and `cluster status`.
type durabilityStat struct {
	objectVersions           uint64
	buckets                  int
	dataShards, parityShards int
	transitionOpen           bool
}

// layoutPosture reads the cheap durability posture for `cluster status` (ADR-0035):
// the active auto storage profile (k+m) and whether a layout migration is open —
// both from the layout alone, no keyspace scan, so the frequently-polled status
// path stays light. The object-version count (which does scan) lives only in the
// metrics collector, where the scrape cadence tolerates it.
func (n *Node) layoutPosture() (dataShards, parityShards int, transitionOpen bool) {
	n.on(func() {
		active := 0
		if cl, ok := n.raft.Store().ClusterLayout(); ok {
			for _, e := range cl.EffectiveNodes() {
				if !e.Draining {
					active++
				}
			}
			transitionOpen = len(cl.Previous) > 0
		}
		p := ec.AutoProfile(active)
		dataShards, parityShards = p.Data, p.Parity
	})
	return
}

// durabilityStats reads the full durability posture, including the object-version
// count (a keyspace scan), on the loop. Called off-loop from the metrics collector
// at scrape time — not from the status path, which uses the cheaper layoutPosture.
func (n *Node) durabilityStats() durabilityStat {
	var st durabilityStat
	n.on(func() {
		store := n.raft.Store()
		bs := store.ListBuckets()
		st.buckets = len(bs)
		for _, b := range bs {
			store.ScanVersions(b.Name, func(_ string, _ meta.VersionEntry) bool {
				st.objectVersions++
				return true
			})
		}
		active := 0
		if cl, ok := store.ClusterLayout(); ok {
			for _, e := range cl.EffectiveNodes() {
				if !e.Draining {
					active++
				}
			}
			st.transitionOpen = len(cl.Previous) > 0
		}
		p := ec.AutoProfile(active)
		st.dataShards, st.parityShards = p.Data, p.Parity
	})
	return st
}
