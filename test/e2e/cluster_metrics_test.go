//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterMetricsEndpoint proves the admin metrics surface end to end
// (ADR-0035): a node started with -admin serves the Prometheus text exposition at
// /metrics, carrying the first signal set — build/node identity, uptime, and the
// cluster-wide gauges derived from the node's own replica.
func TestClusterMetricsEndpoint(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-m", "HAMSTER_SECRET_ACCESS_KEY=e2e-m-secret"}
	root := t.TempDir()
	d1 := filepath.Join(root, "n1")
	adminAddr := freeAddr(t)
	s3Addr := freeAddr(t)

	run(t, "cluster", "init", "-data-dir", d1, "-cluster", "e2e-metrics", "-node", "n1", "-listen", freeAddr(t))
	start(t, env, "cluster", "run", "-data-dir", d1, "-s3", s3Addr, "-admin", adminAddr)
	waitStatus(t, d1, "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	// One S3 request (unsigned → 403) so the request counter has a series.
	if _, err := http.Get("http://" + s3Addr + "/"); err != nil {
		t.Fatalf("S3 probe: %v", err)
	}

	wantSignals := []string{
		"# TYPE hamster_build_info gauge",
		"hamster_build_info{version=",
		"hamster_node_info{node_id=\"n1\",cluster=\"e2e-metrics\"} 1",
		"hamster_uptime_seconds ",
		"hamster_cluster_members 1",
		"hamster_cluster_voters 1",
		"hamster_raft_is_leader 1",
		// Durability posture (a lone node is the 1+0 auto profile).
		"hamster_object_versions 0",
		"hamster_storage_profile_data_shards 1",
		"hamster_layout_transition_open 0",
		// Streaming-PUT load signals (ADR-0038), present at zero before any PUT.
		"hamster_put_inflight 0",
		"hamster_put_bytes_total 0",
		"hamster_put_backpressure_waits_total 0",
		// The S3 request counter, after the probe above.
		`hamster_s3_requests_total{method="GET",code="403"} 1`,
	}

	// The Prometheus scrape surface on the admin port.
	body := scrapeMetrics(t, adminAddr)
	for _, want := range wantSignals {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics output missing %q:\n%s", want, body)
		}
	}

	// The same registry, fetched as the typed snapshot over the control channel
	// and rendered by `cluster metrics` (ADR-0035) — proves the snapshot path the
	// web console will use.
	cli := run(t, "cluster", "metrics", "-data-dir", d1)
	for _, want := range wantSignals {
		if !strings.Contains(cli, want) {
			t.Fatalf("`cluster metrics` output missing %q:\n%s", want, cli)
		}
	}

	// The durability health summary on `cluster status` (ADR-0035).
	status := run(t, "cluster", "status", "-data-dir", d1)
	if !strings.Contains(status, "durability:") || !strings.Contains(status, "profile 1+0") {
		t.Fatalf("`cluster status` missing the durability summary:\n%s", status)
	}
}

// scrapeMetrics GETs /metrics from the admin address, retrying until the server
// is up, and returns the body.
func scrapeMetrics(t *testing.T, adminAddr string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + adminAddr + "/metrics")
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return string(body)
		}
		lastErr = nil
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("scraping /metrics on %s never returned 200 (last err %v)", adminAddr, lastErr)
	return ""
}
