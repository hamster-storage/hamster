# ADR-0023: Hand-written protowire codecs for metadata records

## Status

Accepted

## Context

Every persistent and networked format is additively versioned protobuf (CLAUDE.md invariant 2, [ADR-0008](0008-versioned-formats-rolling-upgrades.md)), and metadata persistence ([ADR-0005](0005-metadata-badgerdb-raft.md)) is the moment the records in [METADATA.md](../METADATA.md) stop being Go structs and become bytes on disk. Something has to produce those bytes.

The default answer is `protoc` code generation: write `.proto` files, run `protoc-gen-go`, commit the generated types. That buys a battle-tested codec, but at real cost here: a code-generation toolchain as a development dependency; a parallel set of generated struct types alongside `internal/meta`'s records (which carry methods and invariants the generated types cannot); and a hand-written conversion layer between the two for every record in both directions — which is exactly the hand-written, field-by-field code the generator was supposed to save, now doubled.

Two properties matter more to Hamster than codec convenience:

- **Deterministic encoding.** Replicated state machines and snapshot comparison want the same record to encode to the same bytes on every node, every time. Standard protobuf serializers do not promise canonical output (map ordering in particular).
- **Unknown-field preservation.** Rolling upgrades mean an older node may rewrite a record a newer node wrote (a retention update rewrites a `VersionEntry`). Dropping fields it does not know would silently destroy a newer feature's data.

## Decision

The codecs are **written by hand in `internal/meta` (codec.go), on `google.golang.org/protobuf/encoding/protowire`** — the official low-level wire-format helpers (varints, tags, length-delimited fields), not the full runtime and not code generation. The output is the real protobuf wire format; the message sketches in METADATA.md are the schema document, and the field numbers in code match them exactly.

Stated plainly, because the combination surprises people: protobuf is three separable things — `.proto` schema files, the `protoc` code generator, and the wire format — and Hamster keeps only the wire format. There are no `.proto` files in the repo and no generation step in the build, but the bytes on disk are ordinary protobuf, bit-for-bit: any protobuf implementation, in any language, given a `.proto` transcribed from METADATA.md, decodes Hamster's records today. These are two independent decisions that happen to meet here. The *format* is protobuf because additive evolution across mixed-version clusters is the property the whole upgrade story rests on (invariant 2, ADR-0008), and numbered tags with self-describing wire types is precisely the mechanism that lets old code skip — and preserve — fields it does not understand. The *implementation* is hand-written because the messages are few and small, and because the two guarantees below are ones the generated code does not give.

The hand-written layer guarantees, by construction:

- **Determinism**: fields encode in number order; map entries encode sorted by key; same record, same bytes, everywhere.
- **Unknown-field preservation**: each record carries an opaque `unknown` byte buffer; decode routes unrecognized fields into it, encode re-emits it, and a rewrite by older code keeps newer fields intact.
- **Proto3 zero-value omission**, so absent and zero are the same thing and old records decode forward cleanly.

Golden conformance tests pin the exact encoded bytes of representative records. A change in those bytes is a format change: deliberate and expected during v0 ([ROADMAP.md](../ROADMAP.md) reserves that right), a compatibility bug after v1 ([ADR-0010](0010-v1-compatibility-policy.md)).

## Consequences

- No code-generation step anywhere: `go build` is the whole build, and the format's source of truth is readable Go next to the structs it encodes.
- `internal/meta` gains its first import beyond the standard library — `protowire` alone, a leaf package with no further dependencies. The package remains seam-free and deterministic.
- Each new record field is added by hand in three places (struct, marshal, unmarshal) plus the METADATA.md sketch. The codec tests — round-trip, golden bytes, determinism, unknown-field survival — are the guard rail; a field added to the struct but not the codec fails the persistence-equivalence tests immediately.
- The wire format stays interoperable: any protobuf implementation given the METADATA.md schema can read Hamster's records, which keeps future tooling (offline inspection, migration utilities) honest.

## Alternatives considered

The yardstick for every alternative: (1) additive evolution — old code skips fields it does not understand; (2) unknown-field preservation — old code *rewrites* a record without shedding those fields; (3) deterministic bytes; (4) readable by external tooling over a decade-plus; (5) pure Go, permissively licensed. Note what the records are: control-plane entries of a few hundred bytes, decoded fully into Go structs. Object data never passes through these codecs — so encoding speed and zero-copy access, the axes most format comparisons turn on, carry no weight here.

- **`protoc-gen-go` code generation.** The standard route. Rejected for the doubled type system: generated structs cannot carry `internal/meta`'s methods and invariants, so every record needs hand-written conversions anyway — all of the manual field-by-field work, plus a toolchain, plus generated files in review diffs. Determinism and unknown-field handling would come free, but they are a few dozen lines here and now explicit rather than implicit.
- **The full `google.golang.org/protobuf` runtime with `dynamicpb`.** Reflection-driven, no codegen, but heavyweight at runtime, still non-deterministic by default, and the schema would live in descriptors rather than code. Rejected.
- **MessagePack.** Binary JSON: self-describing values, no field tags. The two encoding idioms both fail the yardstick. Positional arrays make every field load-bearing forever, with no way to express an absent middle field — additive evolution in name only. String-keyed maps allow skipping and even preserving unknown keys, but spell every field name into every record, leave determinism to hand-sorted keys anyway, and — decisively — come with no schema-evolution standard: the upgrade rules would be conventions Hamster invents and enforces alone, a private format wearing a standard's name. Good for ephemeral RPC where both ends deploy together; these records outlive every deployment. Rejected.
- **FlatBuffers (and Cap'n Proto).** Zero-copy designs: vtables, offsets, and padding so a reader can access one field of a large buffer without parsing it. That solves a problem these records do not have — they are a few hundred bytes and decoded fully — while the machinery costs on every axis that matters here: vtables and alignment padding dwarf small records, encoding is builder-order-dependent (determinism gets harder, not easier), there is no unknown-field preservation (old code rebuilding a buffer through its own schema drops what it did not know), and `flatc` reintroduces the codegen toolchain. FlatBuffers does live in Hamster's module graph — Badger uses it internally for its tables, a fine fit for large, read-partially, never-rewritten-by-old-code files. Right tool, different problem. Cap'n Proto fails identically with a thinner Go ecosystem. Rejected.
- **`encoding/gob`, JSON, CBOR, or a bespoke binary format.** All violate invariant 2 — the formats are protobuf, full stop. Gob is also Go-private, which would wall off every non-Go tool forever. A bespoke format would have to re-derive tagged fields with self-describing lengths to satisfy (1) and (2) — protobuf's wire format minus the ecosystem. Rejected without much grief.
