# MQDB Release Guide

## Prerequisites

- All tests pass: `cargo make test`
- All lints pass: `cargo make clippy`
- Changelog updated (if applicable)

## mqtt-lib Dependency Patches

The `Cargo.toml` contains local path patches for rapid development iteration:

```toml
[patch.crates-io]
mqtt5 = { path = "../mqtt-lib/crates/mqtt5" }
mqtt5-protocol = { path = "../mqtt-lib/crates/mqtt5-protocol" }
```

These patches override the crates.io dependencies with local copies, which is useful during development but prevents reproducible builds.

### For Reproducible Release Builds

**Option 1: Comment out patches (recommended)**

```toml
# [patch.crates-io]
# mqtt5 = { path = "../mqtt-lib/crates/mqtt5" }
# mqtt5-protocol = { path = "../mqtt-lib/crates/mqtt5-protocol" }
```

**Option 2: Remove patches entirely**

Delete the `[patch.crates-io]` section from Cargo.toml.

After either option, run:
```bash
cargo update -p mqtt5 -p mqtt5-protocol
cargo make ci
```

### Version Pinning

For strict version control in releases, use exact versions:

```toml
mqtt5 = { version = "=0.22.0", optional = true }
```

## Release Checklist

1. [ ] Update version in `Cargo.toml`
2. [ ] Remove or comment out `[patch.crates-io]` section
3. [ ] Run `cargo update -p mqtt5 -p mqtt5-protocol`
4. [ ] Run `cargo make ci` (format-check + clippy + test)
5. [ ] Build release binary: `cargo make build-release`
6. [ ] Test release binary manually
7. [ ] Create git tag: `git tag -a v0.X.Y -m "Release 0.X.Y"`
8. [ ] Push tag: `git push origin v0.X.Y`
9. [ ] Restore patches for continued development (if desired)

## Published Crate Versions

The mqtt-lib crates are published to crates.io:

| Crate | Version | Published |
|-------|---------|-----------|
| mqtt5 | 0.22.0 | March 2025 |
| mqtt5-protocol | 0.9.4 | March 2025 |

These are the versions that will be used when patches are removed.
