# ADR-0022: Mutual TLS for all inter-node traffic, cluster CA minted at init

## Status

Proposed (design only; ships with clustering — see the [roadmap](../ROADMAP.md))

## Context

From v0.2 onward, Hamster nodes exchange Raft messages and erasure-coded shards over real networks. Two distinct needs arrive together: **confidentiality and integrity** of inter-node traffic (shard contents are customer data; metadata contains key names and, under [ADR-0021](0021-envelope-encryption-at-rest.md), wrapped DEKs), and **node authentication** — without it, any process that can reach a port can join the cluster, receive shards, and vote. These are one problem: an authenticated encrypted channel.

The constraints are the usual ones: pure Go, no external services, operable by one person without a platform team, and the transport adapters must stay logic-free below the simulator seam ([SIMULATION.md](../SIMULATION.md)).

## Decision

**All inter-node traffic — Raft and data plane alike — runs over mutual TLS, always.** There is no plaintext cluster mode and no flag to create one: a configuration that exists gets deployed, and "we accidentally ran production unencrypted" is not a state this design permits.

Mechanics:

1. **`cluster init` mints a cluster-internal CA** (Ed25519, long-lived). The CA key is part of the cluster's identity, stored alongside the first node's data — operators who prefer to hold issuance elsewhere can supply their own CA material instead.
2. **Nodes join with a join token** (issued by `hamster admin`, short-lived, single-use) and receive a node certificate bound to their node ID. Certificates renew automatically well before expiry; renewal is a local exchange with the metadata leader, not an operator chore.
3. **Both ends verify**: client certificates are required, peer node identity is matched against cluster membership, and a removed node's certificate stops working when its membership does.
4. **Everything is stdlib `crypto/tls` + `crypto/x509`.** No new dependencies.
5. **TLS lives in the `sys` transport adapter, below the seam.** The simulator continues to exchange plain bytes deterministically — it tests distributed logic, not OpenSSL's job. The e2e suite ([ADR-0009](0009-deterministic-simulation-testing.md)) runs real binaries over real mTLS loopback connections and validates the certificate machinery: join, renewal, revocation by membership removal.
6. **Client-facing listeners (S3 API, admin/console) are separate decisions with softer defaults**: TLS via operator-supplied cert files, off by default because homelab reality is a reverse proxy or plain HTTP on a LAN — and because every S3 client supports both. ACME may arrive later; it is convenience, not architecture.

## Consequences

- Cluster security is not a configuration achievement: a freshly initialized cluster is encrypted and authenticated node-to-node before the operator thinks about it. Zero flags in the happy path — init mints, join hands over a token, done.
- The join token becomes the cluster's security boundary and gets treated accordingly: short-lived, single-use, revocable, and audit-logged.
- CPU cost is real but small relative to erasure coding, and node-per-disk deployments pay it on loopback too. Accepted: one code path that is always exercised beats two paths where the fast one wins production by default.
- Certificate machinery (issuance, renewal, clock skew on validity windows) is new surface that must be tested — by the e2e suite, since it deliberately sits below the simulator.
- A compromised CA key is a compromised cluster. The CA key gets the same protection story as the encryption KEK ([ADR-0021](0021-envelope-encryption-at-rest.md)), and CA rotation is owed before v1, designed alongside KEK rotation.

## Alternatives considered

- **Optional TLS (a `--secure` flag).** The insecure configuration becomes the de facto default, every tutorial skips the flag, and the test matrix doubles. Rejected — always-on is simpler to build, test, and trust.
- **A shared cluster secret (symmetric, Noise-style).** Fewer moving parts than X.509, but no per-node identity (any holder of the secret is every node), no story for removing one node's access, and awkward rotation. The membership-bound certificate is the feature, not the overhead. Rejected.
- **TLS-PSK.** Same identity problem as the shared secret, and Go's `crypto/tls` PSK support is not a paved road. Rejected.
- **Delegating to a service mesh or external proxy (Tailscale, WireGuard, Istio).** Genuinely fine as *defense in depth*, and nothing prevents running Hamster over them — but requiring one breaks the no-external-services promise and outsources node authentication to something the cluster cannot see. Rejected as the answer, compatible as an addition.
- **Plaintext with application-layer signing (HMAC per message).** Authenticates but does not encrypt shard contents, and hand-rolling a secure channel protocol to avoid TLS is how projects end up in CVE databases. Rejected.
