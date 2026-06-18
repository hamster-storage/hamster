package cluster

import (
	"strconv"

	"github.com/hamster-storage/hamster/internal/metrics"
)

// Observability (ADR-0035). A node owns a metrics registry: the single in-process
// source of truth for its signals, served as Prometheus text on the admin port
// and (a later pass) as a typed snapshot for the CLI and console. This pass wires
// a first signal set proving the surface end to end — build/node identity, uptime,
// and the cluster-wide gauges any node derives from its own replica.

// Metrics returns the node's registry, for the admin /metrics endpoint.
func (n *Node) Metrics() *metrics.Registry { return n.metrics }

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
}
