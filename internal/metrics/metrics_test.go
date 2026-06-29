package metrics

import (
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// TestExpositionGolden pins the Prometheus text output: families in registration
// order, series sorted by label values, integer-valued numbers without a decimal.
func TestExpositionGolden(t *testing.T) {
	r := NewRegistry()
	build := r.NewGauge("hamster_build_info", "Build and version info; always 1.", "version", "generation")
	members := r.NewGauge("hamster_cluster_members", "Cluster members known to this node.")
	reqs := r.NewCounter("hamster_s3_requests_total", "S3 requests served.", "method", "code")

	build.Set(1, "v0.10.0", "1")
	members.Set(3)
	reqs.Inc("GET", "200")
	reqs.Add(2, "GET", "200")
	reqs.Inc("PUT", "503")

	var sb strings.Builder
	if err := r.WritePrometheus(&sb); err != nil {
		t.Fatal(err)
	}
	const want = `# HELP hamster_build_info Build and version info; always 1.
# TYPE hamster_build_info gauge
hamster_build_info{version="v0.10.0",generation="1"} 1
# HELP hamster_cluster_members Cluster members known to this node.
# TYPE hamster_cluster_members gauge
hamster_cluster_members 3
# HELP hamster_s3_requests_total S3 requests served.
# TYPE hamster_s3_requests_total counter
hamster_s3_requests_total{method="GET",code="200"} 3
hamster_s3_requests_total{method="PUT",code="503"} 1
`
	if got := sb.String(); got != want {
		t.Fatalf("exposition mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// TestCollectorsRunAtScrape: a collector refreshes a derived gauge each time the
// registry is rendered.
func TestCollectorsRunAtScrape(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("hamster_uptime_seconds", "Seconds since start.")
	now := 0.0
	r.AddCollector(func() { g.Set(now) })

	now = 5
	var first strings.Builder
	r.WritePrometheus(&first)
	if !strings.Contains(first.String(), "hamster_uptime_seconds 5\n") {
		t.Fatalf("first scrape:\n%s", first.String())
	}
	now = 12
	var second strings.Builder
	r.WritePrometheus(&second)
	if !strings.Contains(second.String(), "hamster_uptime_seconds 12\n") {
		t.Fatalf("second scrape:\n%s", second.String())
	}
}

// TestValueFormatting: integers print without a decimal point, fractions print
// round-trippably.
func TestValueFormatting(t *testing.T) {
	for _, tc := range []struct {
		v    float64
		want string
	}{
		{0, "0"}, {1, "1"}, {3, "3"}, {1024, "1024"}, {1.5, "1.5"}, {0.25, "0.25"},
	} {
		if got := formatValue(tc.v); got != tc.want {
			t.Errorf("formatValue(%v) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

// TestLabelEscaping: a quote, backslash, or newline in a label value is escaped.
func TestLabelEscaping(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("hamster_thing", "A thing.", "note")
	g.Set(1, "a\"b\\c\nd")
	var sb strings.Builder
	r.WritePrometheus(&sb)
	if !strings.Contains(sb.String(), `hamster_thing{note="a\"b\\c\nd"} 1`) {
		t.Fatalf("escaping:\n%s", sb.String())
	}
}

// TestConcurrentRecording: concurrent Inc on one counter series totals correctly
// (run under -race to prove no data race).
func TestConcurrentRecording(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("hamster_events_total", "Events.")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	var sb strings.Builder
	r.WritePrometheus(&sb)
	if !strings.Contains(sb.String(), "hamster_events_total 50000\n") {
		t.Fatalf("concurrent total wrong:\n%s", sb.String())
	}
}

// TestLabelCountMismatchPanics: a wrong label-value count is a programming error.
func TestLabelCountMismatchPanics(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("hamster_x", "x", "a", "b")
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic on label-count mismatch")
		}
	}()
	g.Set(1, "only-one")
}

// TestSnapshotRoundTrip: a registry's snapshot survives the wire codec and
// re-renders to the same Prometheus text.
func TestSnapshotRoundTrip(t *testing.T) {
	r := NewRegistry()
	r.NewGauge("hamster_build_info", "Build info.", "version", "generation").Set(1, "v0.10.0", "1")
	c := r.NewCounter("hamster_s3_requests_total", "Requests.", "method", "code")
	c.Add(3, "GET", "200")
	c.Inc("PUT", "503")
	g := r.NewGauge("hamster_frac", "A fraction.")
	g.Set(0.25)

	snap := r.Snapshot()
	wire := MarshalSnapshot(snap)
	back, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}

	var a, b strings.Builder
	if err := RenderText(&a, snap); err != nil {
		t.Fatal(err)
	}
	if err := RenderText(&b, back); err != nil {
		t.Fatal(err)
	}
	if a.String() != b.String() {
		t.Fatalf("snapshot diverged across the wire:\n--- before ---\n%s\n--- after ---\n%s", a.String(), b.String())
	}
	if !strings.Contains(b.String(), `hamster_s3_requests_total{method="GET",code="200"} 3`) {
		t.Fatalf("decoded snapshot lost a sample:\n%s", b.String())
	}
}

// TestHistogramObserve: observations land in the right cumulative buckets, the
// sum and count add up, and values above the top boundary fall into +Inf.
func TestHistogramObserve(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("hamster_op_latency_seconds", "Operation latency.", []float64{0.1, 0.5, 1})
	// Exactly-representable values (multiples of 1/16) so the running sum is exact.
	for _, v := range []float64{0.0625, 0.25, 0.75, 2.0} {
		h.Observe(v)
	}

	fams := r.Snapshot()
	if len(fams) != 1 || len(fams[0].Samples) != 1 {
		t.Fatalf("families = %+v", fams)
	}
	hv := fams[0].Samples[0].Histogram
	if hv == nil {
		t.Fatal("expected a histogram sample value")
	}
	// Cumulative: le=0.1 → 1, le=0.5 → 2, le=1 → 3, +Inf → 4.
	wantCounts := []uint64{1, 2, 3, 4}
	if len(hv.Counts) != len(wantCounts) {
		t.Fatalf("counts = %v, want %v", hv.Counts, wantCounts)
	}
	for i, c := range wantCounts {
		if hv.Counts[i] != c {
			t.Fatalf("counts = %v, want %v", hv.Counts, wantCounts)
		}
	}
	if hv.Count != 4 {
		t.Fatalf("count = %d, want 4", hv.Count)
	}
	if hv.Sum != 3.0625 {
		t.Fatalf("sum = %v, want 3.0625", hv.Sum)
	}
}

// TestHistogramExpositionGolden pins the Prometheus histogram text: cumulative
// _bucket lines including le="+Inf", then _sum and _count.
func TestHistogramExpositionGolden(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("hamster_op_latency_seconds", "Operation latency.", []float64{0.1, 0.5, 1})
	// Exactly-representable values (multiples of 1/16) so the rendered sum is stable.
	for _, v := range []float64{0.0625, 0.25, 0.75, 2.0} {
		h.Observe(v)
	}

	var sb strings.Builder
	if err := r.WritePrometheus(&sb); err != nil {
		t.Fatal(err)
	}
	const want = `# HELP hamster_op_latency_seconds Operation latency.
# TYPE hamster_op_latency_seconds histogram
hamster_op_latency_seconds_bucket{le="0.1"} 1
hamster_op_latency_seconds_bucket{le="0.5"} 2
hamster_op_latency_seconds_bucket{le="1"} 3
hamster_op_latency_seconds_bucket{le="+Inf"} 4
hamster_op_latency_seconds_sum 3.0625
hamster_op_latency_seconds_count 4
`
	if got := sb.String(); got != want {
		t.Fatalf("histogram exposition mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// TestHistogramLabeledExposition: a labeled histogram carries the family labels on
// every line and the synthetic le on the _bucket lines.
func TestHistogramLabeledExposition(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("hamster_op_latency_seconds", "Operation latency.", []float64{0.1, 1}, "op")
	h.Observe(0.0625, "get") // ≤ 0.1
	h.Observe(5, "get")      // > 1 → +Inf

	var sb strings.Builder
	if err := r.WritePrometheus(&sb); err != nil {
		t.Fatal(err)
	}
	got := sb.String()
	for _, want := range []string{
		`hamster_op_latency_seconds_bucket{op="get",le="0.1"} 1`,
		`hamster_op_latency_seconds_bucket{op="get",le="1"} 1`,
		`hamster_op_latency_seconds_bucket{op="get",le="+Inf"} 2`,
		`hamster_op_latency_seconds_sum{op="get"} 5.0625`,
		`hamster_op_latency_seconds_count{op="get"} 2`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

// TestHistogramSnapshotRoundTrip: a histogram survives the wire codec and
// re-renders to the same Prometheus text.
func TestHistogramSnapshotRoundTrip(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("hamster_s3_requests_total", "Requests.", "method").Inc("GET")
	h := r.NewHistogram("hamster_op_latency_seconds", "Operation latency.", DefaultLatencyBuckets, "op")
	for _, v := range []float64{0.003, 0.04, 0.4, 7, 20} {
		h.Observe(v, "put")
	}

	snap := r.Snapshot()
	wire := MarshalSnapshot(snap)
	back, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}

	var a, b strings.Builder
	if err := RenderText(&a, snap); err != nil {
		t.Fatal(err)
	}
	if err := RenderText(&b, back); err != nil {
		t.Fatal(err)
	}
	if a.String() != b.String() {
		t.Fatalf("histogram snapshot diverged across the wire:\n--- before ---\n%s\n--- after ---\n%s", a.String(), b.String())
	}
	if !strings.Contains(b.String(), `hamster_op_latency_seconds_bucket{op="put",le="+Inf"} 5`) {
		t.Fatalf("decoded snapshot lost the histogram:\n%s", b.String())
	}
}

// TestSnapshotWithoutHistogramDecodes: a snapshot carrying only counters/gauges
// (no histogram field on its samples) decodes with nil Histogram — the additive
// field is back-compatible in both directions.
func TestSnapshotWithoutHistogramDecodes(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("hamster_events_total", "Events.").Add(7)
	wire := MarshalSnapshot(r.Snapshot())
	fams, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}
	if len(fams) != 1 || len(fams[0].Samples) != 1 {
		t.Fatalf("families = %+v", fams)
	}
	if fams[0].Samples[0].Histogram != nil {
		t.Fatalf("expected nil Histogram on a counter sample, got %+v", fams[0].Samples[0].Histogram)
	}
	if fams[0].Samples[0].Value != 7 {
		t.Fatalf("value = %v, want 7", fams[0].Samples[0].Value)
	}
}

// TestConcurrentObserve: concurrent Observe on one histogram series totals
// correctly (run under -race to prove no data race).
func TestConcurrentObserve(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("hamster_op_latency_seconds", "Operation latency.", []float64{0.1, 0.5, 1})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				h.Observe(0.25) // every observation lands in the le=0.5 bucket; 0.25 is exact, so repeated adds stay exact
			}
		}()
	}
	wg.Wait()

	hv := r.Snapshot()[0].Samples[0].Histogram
	if hv.Count != 50000 {
		t.Fatalf("count = %d, want 50000", hv.Count)
	}
	// Cumulative: le=0.1 → 0, le=0.5 → 50000, le=1 → 50000, +Inf → 50000.
	if hv.Counts[0] != 0 || hv.Counts[1] != 50000 || hv.Counts[3] != 50000 {
		t.Fatalf("counts = %v", hv.Counts)
	}
	if hv.Sum != 0.25*50000 {
		t.Fatalf("sum = %v, want %v", hv.Sum, 0.25*50000)
	}
}

// TestUnmarshalSnapshotSkipsUnknownField: a future field is skipped, not an error.
func TestUnmarshalSnapshotSkipsUnknownField(t *testing.T) {
	r := NewRegistry()
	r.NewGauge("hamster_x", "x.").Set(7)
	wire := MarshalSnapshot(r.Snapshot())
	// Append an unknown top-level field (number 15, varint) a newer encoder might add.
	wire = protowire.AppendTag(wire, 15, protowire.VarintType)
	wire = protowire.AppendVarint(wire, 1)
	fams, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("unknown field should be skipped: %v", err)
	}
	if len(fams) != 1 || fams[0].Name != "hamster_x" {
		t.Fatalf("families = %+v", fams)
	}
}
