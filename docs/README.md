# Documentation Index

- [`distributed-scalable-design.md`](./distributed-scalable-design.md): current system mechanics, distributed upgrade path, Kafka-partitioned architecture, Redis coordination patterns, and aggressive glossary.
- [`phase-1-implementation-checklist.md`](./phase-1-implementation-checklist.md): concrete Phase 1 execution checklist for durability and optimistic concurrency.

## Diagrams

Pre-rendered diagram assets and Mermaid sources live in:

- [`diagrams/`](./diagrams/)

Each diagram has both:

- `*.mmd` source
- `*.svg` rendered output for markdown preview compatibility

Notable diagram assets include:

- `redis-lock-lifecycle.mmd` / `redis-lock-lifecycle.svg`
- `hybrid-consistency-boundary.mmd` / `hybrid-consistency-boundary.svg`
