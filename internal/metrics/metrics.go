// Package metrics is Hamster's hand-rolled metrics registry (ADR-0035): the one
// in-process source of truth for a node's quantitative signals, rendered many
// ways. This file holds the registry, the metric types, and the Prometheus text
// exposition; a typed snapshot for the CLI and web console renders the same
// registry (a later pass).
//
// Hand-rolled rather than prometheus/client_golang to keep the module graph small
// (ADR-0002, ADR-0011): the exposition format is simple and stable. The registry
// is pure — no clock, no I/O, no randomness — so it carries no seam imports; a
// caller that wants a duration computes it through the seam clock and records the
// value (ADR-0009 determinism). Metrics are observability only: a value here is a
// side effect, never an input to replicated state or control flow.
package metrics

import (
	"bufio"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds a node's metric families. Counters and gauges are created up
// front with their help text and label names; collectors registered with
// AddCollector run at scrape time to refresh gauges derived from live state
// (membership, durability), the standard "collect at gather" pattern. Safe for
// concurrent use.
type Registry struct {
	mu         sync.Mutex
	order      []string // family names in registration order — deterministic output
	families   map[string]*metric
	collectors []func()
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{families: map[string]*metric{}}
}

// Counter is a monotonically increasing metric (request counts, bytes moved).
type Counter struct{ m *metric }

// Gauge is a value that can go up or down (members, queue depth, a derived health
// count, build info as a constant 1).
type Gauge struct{ m *metric }

// NewCounter declares a counter family. labelNames may be empty for a single
// unlabeled series. Re-declaring a name returns the existing family.
func (r *Registry) NewCounter(name, help string, labelNames ...string) *Counter {
	return &Counter{m: r.declare(name, help, "counter", labelNames)}
}

// NewGauge declares a gauge family.
func (r *Registry) NewGauge(name, help string, labelNames ...string) *Gauge {
	return &Gauge{m: r.declare(name, help, "gauge", labelNames)}
}

// AddCollector registers a callback run at scrape time, just before exposition,
// so gauges derived from live state are fresh when read. Collectors run in
// registration order.
func (r *Registry) AddCollector(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, fn)
}

func (r *Registry) declare(name, help, typ string, labelNames []string) *metric {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.families[name]; ok {
		return m
	}
	m := &metric{name: name, help: help, typ: typ, labelNames: append([]string(nil), labelNames...), series: map[string]*series{}}
	r.families[name] = m
	r.order = append(r.order, name)
	return m
}

// Inc adds one to the series identified by labelValues (in declared order).
func (c *Counter) Inc(labelValues ...string) { c.m.series_(labelValues).add(1) }

// Add adds v (v >= 0 by counter convention, not enforced) to the series.
func (c *Counter) Add(v float64, labelValues ...string) { c.m.series_(labelValues).add(v) }

// Set sets the series value.
func (g *Gauge) Set(v float64, labelValues ...string) { g.m.series_(labelValues).set(v) }

// Add adjusts the series value by v (may be negative).
func (g *Gauge) Add(v float64, labelValues ...string) { g.m.series_(labelValues).add(v) }

// metric is one family: a name, type, label schema, and the per-label-tuple
// series. New series appear on first use of a label tuple.
type metric struct {
	name, help, typ string
	labelNames      []string
	mu              sync.Mutex
	series          map[string]*series
}

type series struct {
	labelValues []string
	bits        atomic.Uint64 // math.Float64bits of the value
}

func (s *series) set(v float64) { s.bits.Store(math.Float64bits(v)) }
func (s *series) get() float64  { return math.Float64frombits(s.bits.Load()) }
func (s *series) add(v float64) {
	for {
		old := s.bits.Load()
		nw := math.Float64bits(math.Float64frombits(old) + v)
		if s.bits.CompareAndSwap(old, nw) {
			return
		}
	}
}

// series_ finds or creates the series for labelValues, which must match the
// declared label count.
func (m *metric) series_(labelValues []string) *series {
	if len(labelValues) != len(m.labelNames) {
		panic("metrics: " + m.name + ": " + strconv.Itoa(len(labelValues)) + " label values, want " + strconv.Itoa(len(m.labelNames)))
	}
	key := strings.Join(labelValues, "\x00")
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.series[key]
	if !ok {
		s = &series{labelValues: append([]string(nil), labelValues...)}
		m.series[key] = s
	}
	return s
}

// Sample is one labeled value: Labels are the values for the family's label
// names, in order.
type Sample struct {
	Labels []string
	Value  float64
}

// Family is a metric family — a name, help, type, label schema, and its samples —
// in a snapshot. This is the typed model the CLI and the web console render, and
// the wire snapshot (snapshot.go) serializes.
type Family struct {
	Name, Help, Type string
	LabelNames       []string
	Samples          []Sample
}

// Snapshot runs the collectors and returns the registry as typed families,
// deterministic: families in registration order, samples sorted by label values.
// It is the one gather the Prometheus text and the wire snapshot both render.
func (r *Registry) Snapshot() []Family {
	r.mu.Lock()
	collectors := append([]func(){}, r.collectors...)
	order := append([]string(nil), r.order...)
	fams := make(map[string]*metric, len(r.families))
	for k, v := range r.families {
		fams[k] = v
	}
	r.mu.Unlock()

	for _, fn := range collectors {
		fn()
	}

	out := make([]Family, 0, len(order))
	for _, name := range order {
		m := fams[name]
		m.mu.Lock()
		all := make([]*series, 0, len(m.series))
		for _, s := range m.series {
			all = append(all, s)
		}
		m.mu.Unlock()
		if len(all) == 0 {
			continue
		}
		sort.Slice(all, func(i, j int) bool { return less(all[i].labelValues, all[j].labelValues) })
		f := Family{Name: m.name, Help: m.help, Type: m.typ, LabelNames: append([]string(nil), m.labelNames...)}
		for _, s := range all {
			f.Samples = append(f.Samples, Sample{Labels: append([]string(nil), s.labelValues...), Value: s.get()})
		}
		out = append(out, f)
	}
	return out
}

// WritePrometheus writes the registry in the Prometheus text exposition format,
// after running collectors. Output is deterministic, so a golden test can pin it.
func (r *Registry) WritePrometheus(w io.Writer) error {
	return RenderText(w, r.Snapshot())
}

// RenderText writes families in the Prometheus text exposition format. Shared by
// the live /metrics endpoint (from a registry's Snapshot) and `cluster metrics`
// (from a snapshot fetched over the wire), so both render identically.
func RenderText(w io.Writer, families []Family) error {
	bw := bufio.NewWriter(w)
	for _, f := range families {
		bw.WriteString("# HELP ")
		bw.WriteString(f.Name)
		bw.WriteByte(' ')
		bw.WriteString(escapeHelp(f.Help))
		bw.WriteByte('\n')
		bw.WriteString("# TYPE ")
		bw.WriteString(f.Name)
		bw.WriteByte(' ')
		bw.WriteString(f.Type)
		bw.WriteByte('\n')
		for _, s := range f.Samples {
			bw.WriteString(f.Name)
			writeLabels(bw, f.LabelNames, s.Labels)
			bw.WriteByte(' ')
			bw.WriteString(formatValue(s.Value))
			bw.WriteByte('\n')
		}
	}
	return bw.Flush()
}

func writeLabels(bw *bufio.Writer, names, values []string) {
	if len(names) == 0 {
		return
	}
	bw.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			bw.WriteByte(',')
		}
		bw.WriteString(n)
		bw.WriteString(`="`)
		bw.WriteString(escapeLabelValue(values[i]))
		bw.WriteByte('"')
	}
	bw.WriteByte('}')
}

// formatValue renders a metric value: a whole number without a decimal point, an
// integer-valued float likewise, otherwise the shortest round-trippable form.
func formatValue(v float64) string {
	if v == math.Trunc(v) && !math.IsInf(v, 0) && math.Abs(v) < 1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func less(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// escapeLabelValue escapes a label value per the exposition format: backslash,
// double-quote, and newline.
func escapeLabelValue(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}

// escapeHelp escapes a HELP line: backslash and newline (not quotes — HELP is not
// quoted).
func escapeHelp(s string) string {
	if !strings.ContainsAny(s, "\\\n") {
		return s
	}
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}
