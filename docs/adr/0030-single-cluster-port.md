# ADR-0030: One cluster port — join/status shares the peer transport listener, split by ALPN

## Status

Accepted

## Context

A v0.2/v0.3 node bound two TLS listeners on the cluster CA, plus the S3 port:

- the **peer transport** ([ADR-0022](0022-cluster-mtls.md)) — the mutually
  authenticated mTLS channel carrying Raft and shard traffic
  ([ADR-0027](0027-v03-distributed-data-path.md)), `RequireAndVerifyClientCert`;
- a separate **join/status** listener, `VerifyClientCertIfGiven` — because the
  join handshake necessarily arrives *without* a client certificate (the joiner
  has no trust material until the cluster issues it), so it cannot satisfy a
  listener that requires one.

That is three ports per node (cluster, join, S3), each set independently and, on
a single machine, incremented by hand per node. It is a real first-run friction:
forgetting to bump one collides with another node, and — worse — the failed bind
surfaces *after* the join has already consumed its single-use token and written
the bad address into `node.conf`. Two ports for what is, to an operator, "the
cluster" is one port too many. The peer transport and the join/status protocol
are the same cluster-CA TLS to the same audience (cluster members and would-be
members); only their client-auth posture differs.

## Decision

**Serve the join/status protocol on the peer transport's port. One cluster
listen address per node, plus the separate S3 port — two, not three.**

One TLS listener, `ClientAuth: VerifyClientCertIfGiven`, demultiplexes by ALPN:

- The peer transport dials and listens with the ALPN protocol `hamster/peer`. A
  connection that negotiates it is a peer stream and flows to the transport's
  read loop (Raft/data frames).
- A connection that does *not* negotiate `hamster/peer` — a join or status
  client, which offers no ALPN — is handed to an `OnControl` callback the
  cluster layer wires to its existing join/status handler.

The mutual-authentication guarantee for peer traffic is preserved by the demux,
not by the TLS layer: a presented certificate is still verified against the
cluster CA (an invalid one fails the handshake), and a peer stream that arrives
without a verified certificate is dropped before any frame is delivered. The
listener admits certless clients *only* so the certless join handshake can reach
`OnControl`; such a connection can never reach the peer path. The trust model of
[ADR-0022](0022-cluster-mtls.md) is unchanged — this ADR changes the listener
topology, not who is trusted.

The CLI collapses `-listen-cluster` and `-listen-join` to a single `-listen`.
`NodeConfig` keeps both `cluster_addr` and `join_addr` fields (formats are only
ever added to — [ADR-0008](0008-versioned-formats-rolling-upgrades.md)); both are
written equal to the one listen address.

## Consequences

- **One fewer port to configure and to collide.** The dominant first-run friction
  on a single machine is halved; firewall rules shrink to "the cluster port and
  the S3 port."
- **The join handshake arrives on the shared listener before the node has finished
  building** (the transport accepts from the moment it exists, which precedes
  Raft/handler construction on the loop). The control handler therefore waits on
  a readiness signal, bounded by a deadline, before serving — a pre-build
  connection cannot pin a goroutine, and a join is served correctly once the node
  is up.
- **S3 stays on its own port.** Mixing the public object API onto the consensus
  port would be a worse firewall and TLS-policy story; the consolidation stops at
  the two cluster-CA protocols.
- **Peer authentication now has a code-path obligation, not only a TLS-layer one.**
  The "drop a peer stream without a verified certificate" check is load-bearing
  security, not defensive belt-and-suspenders; it is covered by a transport test
  (a non-peer connection reaches `OnControl`, never delivery) and the rogue-cert
  test (an off-CA certificate still fails the handshake).
- This does **not** fix a node that joined while advertising the wrong dial
  address: the advertised address is frozen at admission and v0.3 does not
  propagate a changed dial. That is a separate concern (a member re-advertise
  path), tracked outside this ADR.

## Alternatives considered

- **Keep two listeners.** The status quo. Rejected: the second cluster port buys
  nothing an operator benefits from — it exists only because the join handshake
  is certless, which the ALPN split handles on one port without weakening peer
  auth.
- **A magic first-byte prefix instead of ALPN.** Peer streams and join requests
  are both length-framed, so they cannot be told apart by shape; a sentinel
  prefix would work but is a bespoke protocol detail where ALPN is the idiomatic,
  already-negotiated TLS mechanism. Rejected as reinventing ALPN.
- **Run join/status as a channel on the ADR-0027 envelope** (like Raft and data).
  Rejected: the envelope rides *peer* connections, which require a verified
  member certificate; the joiner has none, so the certless handshake must be
  admitted at the listener, which is exactly what the ALPN split does.
- **Collapse S3 onto the cluster port too** (one port total). Rejected: the public
  object API and the inter-node consensus plane want different TLS policy and
  firewall treatment; merging them trades a real operational separation for a
  cosmetic win.
