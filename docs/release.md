# MQDB Release Guide

How maintainers cut and publish an MQDB release.

## Prerequisites

- All tests pass: `cargo make test`
- All lints pass: `cargo make clippy`

## Process

Releases are triggered by pushing a `v*` tag. The tag **must** equal the
`mqdb-cli` version — `.github/workflows/release.yml` verifies that
`TAG == mqdb-cli version` and fails the run otherwise. Bump `mqdb-cli` first,
even if only another crate changed.

1. [ ] Bump the version in `crates/mqdb-cli/Cargo.toml` **first**. Bump any other
       crates being released (`mqdb-core`, `mqdb-agent`, `mqdb-wasm`) as needed.
2. [ ] Add a dated `CHANGELOG.md` heading listing the released crate versions:
       `## YYYY-MM-DD — mqdb-cli <ver>[, mqdb-core <ver>, ...]`. The heading must
       list `mqdb-cli <tag>` — CI anchors the release notes on it.
3. [ ] Run `cargo make ci` (format-check + clippy + test).
4. [ ] Commit, then create and push the tag: `git tag v<mqdb-cli-version>` and
       `git push origin v<mqdb-cli-version>`.

## What CI does

On the pushed `v*` tag, `release.yml` runs three jobs:

1. **Publish to crates.io** — verifies the tag matches the `mqdb-cli` version,
   then publishes `mqdb-core`, `mqdb-agent`, and `mqdb-wasm` (each
   `|| echo "already published"`, so re-runs are safe).
2. **Build binaries** — cross-builds four platform binaries with
   `cargo build --profile dist --package mqdb-cli --no-default-features
   --features http-api`: linux-amd64, macos-amd64, macos-arm64, windows-amd64.
3. **Create GitHub release** — extracts the release body from `CHANGELOG.md` by
   matching the dated heading that lists `mqdb-cli <tag>`, then attaches the four
   binaries to the release.
