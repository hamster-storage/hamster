# Hamster builds with CGO_ENABLED=0, always (CLAUDE.md, ADR-0002). The one
# exception is the race detector, whose runtime needs cgo — race builds are
# test-only and never shipped.
export CGO_ENABLED = 0

.PHONY: build test test-race check

build:
	go build ./...

test:
	go test ./...

test-race:
	CGO_ENABLED=1 go test -race ./...

check:
	go vet ./...
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi
