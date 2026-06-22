// Command hamster is the Hamster server binary: one self-hosted, S3-compatible
// object store with a flat command surface. A node is a one-node cluster —
// `init` founds one, `join`+`serve` grow it, `serve` runs a node and serves the
// S3 API over the erasure-coded data path (ADR-0036). The rest are the operate,
// security, and recovery verbs. Every command is a row in commandGroups, the one
// source of truth for both dispatch and help.
//
// Still a development preview: v0 formats may change between releases
// (ROADMAP.md).
package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// version is stamped by the release build:
//
//	go build -ldflags "-X main.version=v0.1.0" ./cmd/hamster
var version = "dev"

// protocolGenerationStr is the declared protocol generation (ADR-0034): the
// monotonic integer this binary owns, advanced only by a coordinated format
// change (not every release). The cluster's effective generation is the minimum
// across live members, etcd-style, and a gate (when one first lands) compares
// against it. Stamped like version — `-ldflags "-X main.protocolGenerationStr=2"`
// — so the upgrade test can build two generations from one source; defaults to
// the baseline generation 1.
var protocolGenerationStr = "1"

// protocolGeneration parses protocolGenerationStr, falling back to the baseline
// on a malformed stamp (a build-time string, so this is defensive only).
func protocolGeneration() uint32 {
	g, err := strconv.ParseUint(protocolGenerationStr, 10, 32)
	if err != nil {
		return 1
	}
	return uint32(g)
}

// fullVersion is what banners and `hamster version` print: the stamped
// release version as-is, or — for a plain `go build` — the commit Go
// embedded in the binary (git tags are not part of Go's VCS stamping, so
// a dev build can name its commit but not the latest tag).
func fullVersion() string {
	if version != "dev" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision == "" {
		return version
	}
	v := "dev (" + revision[:min(7, len(revision))]
	if modified == "true" {
		v += ", modified"
	}
	return v + ")"
}

// command is one top-level verb: its name, its handler, and the one-line help
// the generated usage prints. The dispatch table and the help are one source of
// truth — a command cannot exist without a help line, nor can help drift from
// what is dispatched (a property the flatten-drift test pins).
type command struct {
	name  string
	run   func(args []string) error
	short string
}

// commandGroups is every command, grouped the way an operator reaches for them.
// main dispatches by flattening it; usageText renders it. Adding a command means
// adding a row here — there is no second list to keep in sync.
var commandGroups = []struct {
	title    string
	commands []command
}{
	{"Run a node", []command{
		{"init", clusterInit, "create a new cluster: mint the CA and this node's identity"},
		{"join", clusterJoin, "join an existing cluster with a token (identity only; serve starts it)"},
		{"token", clusterToken, "mint a single-use join token (on an existing node)"},
		{"serve", serve, "run this node, serving the S3 API (-no-s3 for a headless storage node)"},
	}},
	{"Operate", []command{
		{"status", clusterStatus, "show cluster membership and health from a running node"},
		{"metrics", clusterMetrics, "print a node's metrics snapshot (ADR-0035) in Prometheus text form"},
		{"can-stop", clusterCanStop, "check whether taking a node down for maintenance/upgrade is safe now (ADR-0034)"},
		{"drain", func(a []string) error { return clusterDrain(a, true) }, "take a node out of service: writes steer off it, repair migrates its shards away"},
		{"undrain", func(a []string) error { return clusterDrain(a, false) }, "return a drained node to service"},
		{"remove", clusterRemove, "evict a drained, empty node from the cluster for good"},
		{"optimize", clusterOptimize, "re-encode existing data up to the current storage profile (after growing the cluster)"},
	}},
	{"Security", []command{
		{"encrypt", clusterEncrypt, "turn on encryption at rest (ADR-0021): new writes encrypted; permanent"},
		{"rotate-key", clusterRotateKey, "rotate the cluster master key (ADR-0032): rewrap every object, metadata only"},
		{"rotate-ca", clusterRotateCA, "rotate the cluster CA (ADR-0033): dual-trust rollover, no downtime"},
	}},
	{"Recover", []command{
		{"recover", clusterRecover, "rewrite a stopped survivor into a new single-voter cluster (last resort)"},
	}},
	{"Other", []command{
		{"version", func([]string) error { fmt.Println("hamster", fullVersion()); return nil }, "print the version"},
	}},
}

// usageText renders the top-level help from commandGroups, so help can never
// list a command the binary does not dispatch, or omit one it does.
func usageText() string {
	var b strings.Builder
	b.WriteString(`hamster is a self-hosted, S3-compatible object store in a single binary.

Learn more: https://github.com/hamster-storage/hamster

Usage:
  hamster <command> [flags]
`)
	for _, g := range commandGroups {
		fmt.Fprintf(&b, "\n%s:\n", g.title)
		for _, c := range g.commands {
			fmt.Fprintf(&b, "  %-11s %s\n", c.name, c.short)
		}
	}
	b.WriteString("\nUse \"hamster <command> -h\" for a command's flags.\n")
	return b.String()
}

// lookupCommand finds a command by name across every group.
func lookupCommand(name string) (command, bool) {
	for _, g := range commandGroups {
		for _, c := range g.commands {
			if c.name == name {
				return c, true
			}
		}
	}
	return command{}, false
}

func main() {
	setupLogging()
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usageText())
		os.Exit(2)
	}
	switch os.Args[1] {
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usageText())
		return
	}
	cmd, ok := lookupCommand(os.Args[1])
	if !ok {
		fmt.Fprint(os.Stderr, usageText())
		os.Exit(2)
	}
	if err := cmd.run(os.Args[2:]); err != nil {
		log.Fatalf("hamster: %v", err)
	}
}

// setupLogging configures the process logger. The default is plain, timestamped
// lines for a human at a terminal; HAMSTER_LOG_FORMAT=json switches to one JSON
// record per line for log shippers in production. Both carry a timestamp — the
// previous default carried none.
func setupLogging() {
	if strings.EqualFold(os.Getenv("HAMSTER_LOG_FORMAT"), "json") {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		// Route the standard log package (log.Printf throughout the server)
		// through slog so every line becomes one JSON record, without
		// rewriting the call sites.
		log.SetFlags(0)
		log.SetOutput(slogLogWriter{})
		return
	}
	log.SetFlags(log.LstdFlags)
}

// slogLogWriter forwards standard-library log output to slog as Info records,
// so existing log.Printf calls emit JSON in production mode.
type slogLogWriter struct{}

func (slogLogWriter) Write(p []byte) (int, error) {
	slog.Default().Info(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
