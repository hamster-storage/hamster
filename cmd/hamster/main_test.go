package main

import (
	"strings"
	"testing"
)

// TestCommandHelpInSync pins the property the flat CLI is built on (ADR-0036):
// commandGroups is the one source of truth for both dispatch and help, so help
// can never list a command the binary does not run, nor omit one it does. Every
// command must be well-formed, uniquely named, dispatchable, and rendered into
// the help.
func TestCommandHelpInSync(t *testing.T) {
	help := usageText()
	seen := map[string]bool{}
	for _, g := range commandGroups {
		if g.title == "" {
			t.Errorf("command group has no title")
		}
		for _, c := range g.commands {
			if c.name == "" {
				t.Errorf("group %q has a command with no name", g.title)
			}
			if c.run == nil {
				t.Errorf("command %q has no handler", c.name)
			}
			if strings.TrimSpace(c.short) == "" {
				t.Errorf("command %q has no help description", c.name)
			}
			if seen[c.name] {
				t.Errorf("command %q is listed more than once", c.name)
			}
			seen[c.name] = true

			// Help renders the command name and its description, so help cannot
			// drift from the dispatch table.
			if !strings.Contains(help, c.name) {
				t.Errorf("help does not list command %q", c.name)
			}
			if !strings.Contains(help, c.short) {
				t.Errorf("help does not carry %q's description", c.name)
			}
			// The command is dispatchable by exactly the name help advertises.
			if _, ok := lookupCommand(c.name); !ok {
				t.Errorf("command %q in the table is not dispatchable", c.name)
			}
		}
	}

	// The verbs an operator must be able to find — guards against a future
	// refactor silently dropping one from the table.
	for _, must := range []string{"init", "join", "token", "serve", "status", "drain", "remove", "recover", "version"} {
		if !seen[must] {
			t.Errorf("core command %q is missing from commandGroups", must)
		}
	}
}
