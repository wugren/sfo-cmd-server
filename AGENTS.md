# AGENTS.md

Guidance for coding agents working in `sfo-cmd-server`.

## Scope

- This repo is a Rust library crate: `sfo-cmd-server`.
- Edition is Rust 2024.
- Main code is under `src/`.
- Runnable examples are under `examples/`.
- There is currently no dedicated `tests/` directory.

## Repository Layout

- `src/lib.rs` re-exports core modules.
- `src/cmd.rs` contains protocol primitives (`CmdHeader`, `CmdBody`, handlers).
- `src/client/` contains client abstractions and implementations.
- `src/server/` contains server abstractions and implementations.
- `src/node/` contains node-level abstractions and implementations.
- `src/errors.rs` defines `CmdErrorCode`, `CmdResult`, and error helpers.
- `examples/tcp_cmd_server.rs` and `examples/tcp_cmd_client.rs` are integration demos.

## Rules Files (Cursor / Copilot)

- No `.cursorrules` file was found.
- No `.cursor/rules/` directory was found.
- No `.github/copilot-instructions.md` file was found.
- If any of these files are added later, treat them as higher-priority constraints.

## Build / Lint / Test Commands

Run from repository root.

### Build

- `cargo build`
- `cargo build --release`
- `cargo check`

### Format

- Apply formatting: `cargo fmt --all`
- Check formatting only: `cargo fmt --all -- --check`

### Lint

- Standard lint pass: `cargo clippy --all-targets --all-features`
- Strict mode (currently fails on existing codebase):
  - `cargo clippy --all-targets --all-features -- -D warnings`

### Tests

- Run all tests: `cargo test`
- Run library tests only: `cargo test --lib`
- Run doc tests only: `cargo test --doc`
- Run examples build check via tests: `cargo test --examples`

### Run a Single Test (important)

- By test name substring:
  - `cargo test <test_name_substring>`
- Exact test name:
  - `cargo test <exact_test_name> -- --exact`
- Single-threaded run for debugging:
  - `cargo test <test_name_substring> -- --exact --nocapture --test-threads=1`
- From a specific integration test target:
  - `cargo test --test <test_file_stem> <test_name_substring>`
- Single doc test by name substring:
  - `cargo test --doc <name_substring>`

Note: the current repository has no unit/integration tests checked in yet, so single-test
commands are templates for new tests.

### Examples

- Run server example: `cargo run --example tcp_cmd_server`
- Run client example: `cargo run --example tcp_cmd_client`

## Coding Conventions

Follow existing style in this repo first; apply these rules when adding or editing code.

### Imports

- Keep `std` imports explicit and near the top.
- Keep external crate imports explicit; avoid wildcard imports.
- Keep `crate::...` imports grouped together.
- Prefer stable import ordering and let `rustfmt` normalize details.
- Remove unused imports when touching a file.

### Formatting

- Always run `cargo fmt --all` after edits.
- Use semicolons consistently for statements.
- Prefer multi-line formatting for long generic bounds and function signatures.
- Do not manually align spacing; trust formatter output.

### Types and Traits

- This codebase is strongly trait/generic driven; preserve generic constraints.
- Add bounds in `where` clauses when signatures become long.
- Prefer concrete trait bounds used elsewhere (e.g., `RawEncode`, `RawDecode`, `RawFixedBytes`).
- Keep async trait method signatures consistent with existing public traits.
- Prefer small helper type aliases for very complex types when clarity improves.

### Naming

- Types/traits/enums: `CamelCase`.
- Functions/methods/modules/variables: `snake_case`.
- Constants/statics: `UPPER_SNAKE_CASE`.
- Error code variants follow concise PascalCase names (`IoError`, `InvalidParam`, etc.).
- Keep naming consistent with protocol terms already used (`peer_id`, `tunnel_id`, `cmd`, `seq`).

### Error Handling

- Use `CmdResult<T>` for fallible operations in this crate.
- Use `cmd_err!` to construct domain errors with `CmdErrorCode`.
- Use `into_cmd_err!` with `map_err(...)` at IO/codec boundaries.
- Prefer propagating errors with `?` over ad-hoc unwraps in library code.
- If behavior intentionally ignores an error, log it with context.
- Map external error categories to existing `CmdErrorCode` variants.

### Async and Concurrency

- Tokio is the runtime; use async IO (`AsyncReadExt`, `AsyncWriteExt`) consistently.
- Use `Arc` for shared ownership between spawned tasks.
- Existing code uses `Mutex` and lock guards; avoid holding locks across `.await` unless required.
- Preserve task-spawn patterns for connection loops and handlers.
- Ensure send/recv ordering invariants are preserved when refactoring.

### Logging and Diagnostics

- Use `log::trace!`/`debug!` for protocol flow details.
- Use `log::error!` for handler/runtime failures.
- Include key identifiers in logs where possible (`peer_id`, `tunnel_id`, command code, length).
- Avoid logging sensitive raw payloads unless already established by local pattern.

### Protocol / API Patterns

- `CmdHeader` length is encoded into one byte for header-size prefix; validate `<= 255`.
- Preserve existing response correlation (`gen_seq`, `gen_resp_id`, waiter futures).
- Preserve semantics of `send`, `send_with_resp`, `send2`, and `send_cmd` families.
- Register command handlers through provided handler maps instead of custom dispatch paths.

### Testing and Validation Expectations

- Minimum before submitting changes:
  - `cargo fmt --all -- --check`
  - `cargo test`
- If touching lint-sensitive areas, also run:
  - `cargo clippy --all-targets --all-features`
- If adding behavior in examples or network flow, run the affected example(s).

## Practical Agent Workflow

- Read nearby module files before editing (client/server/node patterns are similar).
- Make focused edits; avoid unrelated large-scale refactors.
- Do not change public trait signatures unless required by the task.
- Keep compatibility with existing `CmdErrorCode` and `CmdResult` usage.
- Update docs/comments only when behavior changes or clarity is missing.
- If introducing new tests, prefer narrow test names so single-test commands are useful.

## Known Current State (useful context)

- `cargo test` currently succeeds (0 tests present) with warnings in examples/doc lint names.
- `cargo fmt --all -- --check` currently reports formatting differences.
- Strict clippy (`-D warnings`) currently fails with many pre-existing lints.
- Treat those as baseline unless your task is specifically lint-cleanup.
