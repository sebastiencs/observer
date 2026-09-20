# observer

## Git hooks

Pre-commit runs `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`. Install once per clone:

```bash
./scripts/install-githooks.sh
```