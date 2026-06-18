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
	root := t.TempDir()
	d1 := filepath.Join(root, "n1")
	adminAddr := freeAddr(t)

	run(t, "cluster", "init", "-data-dir", d1, "-cluster", "e2e-metrics", "-node", "n1", "-listen", freeAddr(t))
	start(t, nil, "cluster", "run", "-data-dir", d1, "-admin", adminAddr)
	waitStatus(t, d1, "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	body := scrapeMetrics(t, adminAddr)
	for _, want := range []string{
		"# TYPE hamster_build_info gauge",
		"hamster_build_info{version=",
		"hamster_node_info{node_id=\"n1\",cluster=\"e2e-metrics\"} 1",
		"hamster_uptime_seconds ",
		"hamster_cluster_members 1",
		"hamster_cluster_voters 1",
		"hamster_raft_is_leader 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, body)
		}
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
