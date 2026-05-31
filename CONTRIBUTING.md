# Contributing to TraceDB

## Building and Testing

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --check
```

## Pull Request Requirements

- All tests pass: `cargo test --workspace`
- Clippy is clean: `cargo clippy --workspace -- -D warnings`
- Code is formatted: `cargo fmt --check`
- New functionality includes tests
- Documentation updated for API changes

## Commit Message Conventions

- `feat:` — New feature
- `fix:` — Bug fix
- `docs:` — Documentation changes
- `style:` — Code style changes
- `refactor:` — Code refactoring
- `perf:` — Performance improvements
- `test:` — Adding or updating tests
- `chore:` — Build process or auxiliary tool changes
