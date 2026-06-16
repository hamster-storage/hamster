# TODO
I created this to track items that I find but not sure which version they should go in or just work on in between phases of the PLAN.md. When an item is completed, remove it from this doc.

## Observability

### Loop queue-depth signal (`internal/sys/loop.go`)
The control-plane loop's queue is intentionally unbounded — `Post` must never
block (a loop fn posting to its own loop against a bounded queue self-deadlocks),
and the runtime can't safely choose what to drop (dropping a Raft apply corrupts
state; dropping an S3 request is harmless). Backpressure is pushed upstream and
in practice the producers are self-throttling (the gateway is request/response;
the data path is windowed; Raft has flow control). But there is **no signal** if
something does outrun the consumer: it surfaces as creeping memory + latency, not
a clean warning. Add observability — a queue-depth gauge and/or a logged
high-water mark — so a pathological producer is visible early. Not a hard cap
(that's the wrong fix, see above).
