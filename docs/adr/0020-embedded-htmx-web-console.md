# ADR-0020: Embedded web console on the admin port, server-rendered with htmx

## Status

Accepted

## Context

v1.0 promises a web console, and several earlier decisions constrain its shape before any of it is built:

- The single-binary promise ([ADR-0002](0002-single-binary-no-external-dependencies.md)) rules out a separate console process, and the spirit of the rule — keep the toolchain and module graph small — weighs against a JavaScript build pipeline in the repository.
- With path-style S3 addressing ([S3-API.md](../S3-API.md)), every path on the S3 listener is potentially a valid bucket/object request. There is no safe URL prefix to carve out for a console: a bucket named `console` must keep working.
- The committed CLI commands (`hamster cluster status`, `set-profile`, `recover`, `admin key create`) already imply a machine-readable admin protocol to a running node, and the console needs cluster details the S3 API cannot express: node membership, voter/learner status, the active storage profile, repair progress, data-below-profile reporting, key management.
- The target user has compliance-shaped needs: the management plane should be separately bindable and firewallable, and admin actions should be auditable in one place.

## Decision

1. **The console is served by the same `hamster` process on its own listener** — the admin port, separate from the S3 port. All assets are embedded with `go:embed`. The admin listener can be bound to localhost or a management interface, or disabled outright.
2. **The UI is server-rendered HTML using `html/template` and htmx.** Handlers return HTML fragments; the browser holds no application state and consumes no JSON. htmx ships as a single embedded, permissively licensed (BSD-family) static JavaScript file — it is an asset, not a dependency in the module graph. There is no Node toolchain, no npm, no bundler, no SPA.
3. **One admin core, two presentations.** Admin operations are plain Go functions on the node (backed by replicated metadata, callable from any node). The CLI reaches them through JSON-over-HTTP endpoints on the admin port; the console handlers call the same functions in-process and render HTML. Authorization and audit logging live in the admin core, below both presentations, so neither surface can do what the other cannot and every admin action is logged once, identically.
4. **Object bytes never traverse the admin port.** The console renders bucket and object listings server-side through the same in-process paths, but downloads and uploads use presigned S3 URLs ([ADR-0018](0018-sigv4-auth.md)) against the S3 listener.
5. **Admin credentials are distinct from S3 access keys.** "Can write objects" and "can remove a node" are different powers. The admin authentication design is deferred to its own ADR when the admin API lands.

## Consequences

- No frontend toolchain, ever: contributors build the console with Go templates, and `go build` produces the whole product, console included.
- The console cannot drift from the CLI — both are skins over the same admin core, and audit coverage is structural rather than per-endpoint discipline.
- htmx sets an interactivity ceiling. That ceiling is fine for an admin console; live-updating views (repair progress, cluster health) use polling or server-sent events, both of which htmx handles natively.
- The admin JSON API becomes a real, versioned surface for the CLI. During v0.x it may change freely; at v1.0 it joins the compatibility promise ([ADR-0010](0010-v1-compatibility-policy.md)).

## Alternatives considered

- **A JavaScript SPA (React/Vue) over the JSON admin API.** Rejected: it imports a second toolchain and dependency ecosystem into a project whose identity is a small, auditable, single-binary build — and SPA dependency churn is real operational and supply-chain weight for no capability an admin console needs.
- **Console on the S3 port under a reserved path.** Rejected: collides with path-style addressing. Any reserved prefix silently breaks a legal bucket name.
- **A separate console binary or container.** Rejected: violates the single-binary promise outright, and the industry precedent ran the same direction — consoles that started separate got folded back into the server.
- **Browser-side S3 calls with SigV4 signed in JavaScript.** Rejected for console operations: it puts key material and signing logic in the browser and forces CORS onto the S3 listener. Presigned URLs cover the data-transfer cases without either.
- **CLI only, no web console.** Rejected: the v1.0 promise stands. The operators Hamster targets evaluate storage with their eyes as well as their terminals.
