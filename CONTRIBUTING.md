# Contributing to MQDB

Thank you for your interest in contributing to MQDB. This document explains how to get involved.

## How to Contribute

1. Check the [issue tracker](https://github.com/LabOverWire/MQDB/issues) for open issues or create a new one to discuss your idea
2. Fork the repository and create a feature branch
3. Make your changes following the code standards below
4. Submit a pull request

## Contributor License Agreement

All contributors must sign our [Contributor License Agreement](CLA.md) before any pull request can be merged. This is required even for small changes.

**Why?** The CLA grants LabOverWire the rights needed to distribute MQDB under both the AGPL-3.0 open source license and commercial licenses. Without it, we cannot accept your contribution.

**How to sign:**
- First-time contributors: add your name to the signature table in [CLA.md](CLA.md), or
- Comment on your pull request: *"I have read and agree to the CLA"*

## Development Setup

```bash
git clone https://github.com/LabOverWire/MQDB.git
cd mqdb
cargo make dev
```

`cargo make dev` runs formatting, clippy (pedantic), and the full test suite.

## Code Standards

- Run `cargo make clippy` before submitting — pedantic warnings are enabled and must be zero
- Run `cargo make test` to verify all tests pass
- Run `cargo make format` to auto-format code
- All new files must include the AGPL-3.0 copyright header:

```rust
// Copyright (C) 2025-2026 LabOverWire
// SPDX-License-Identifier: AGPL-3.0-only
```

## Pull Request Process

1. Create a branch from `main` (e.g., `fix-partition-rebalance` or `add-websocket-tls`)
2. Keep commits focused — one logical change per commit
3. Write a clear PR description explaining what changed and why
4. Ensure CI passes (formatting, clippy, tests)
5. Sign the CLA if you haven't already
6. A maintainer will review and merge your PR

## Reporting Issues

- **Bug reports**: include steps to reproduce, expected behavior, actual behavior, and MQDB version
- **Feature requests**: describe the use case and proposed solution
- **Security issues**: email contact@laboverwire.ca directly — do not open a public issue
