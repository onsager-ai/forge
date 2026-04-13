# Forge

Production line subsystem of the Onsager AI factory. Drives artifacts through their lifecycle.

## Architecture

Forge is a **binary crate** (service). It depends on the `onsager` library crate for shared types (artifact model, factory events, protocol types) and event spine access.

See `../onsager/specs/forge-v0.1.md` for the full specification.

## Core Concepts

- **Core loop** — decide → gate → dispatch → advance → route
- **Scheduling kernel** — pluggable decision-maker (`BaselineKernel` in v0.1)
- **Artifact store** — Forge's read/write access to artifact state (single writer)
- **Consumer router** — dispatches released artifacts to consumer sinks

## Build & Test

```bash
cargo build              # Build
cargo test               # Run all tests
cargo clippy -- -D warnings  # Lint
cargo fmt --check        # Format check
```

## Running

```bash
DATABASE_URL=postgres://user:pass@localhost/onsager cargo run
```

## Environment Variables

- `DATABASE_URL` — PostgreSQL connection URL (required)
- `FORGE_IDLE_TICK_MS` — Kernel tick interval when idle (default: 1000)
- `FORGE_IDLE_EVENT_MS` — Idle event emission interval (default: 30000)
- `FORGE_MAX_IN_FLIGHT` — Max concurrent shaping requests (default: 10)

## Conventions

- Rust edition 2021, rustfmt formatting, clippy with warnings-as-errors
- thiserror for library errors, anyhow for application errors
- Small focused commits, imperative mood, under 72 characters
