# Contributing to h3-toolkit

Thank you for your interest in contributing to h3-toolkit! This document outlines the process for contributing and helps you get started quickly.

---

## Table of Contents

- [Contributing to h3-toolkit](#contributing-to-h3-toolkit)
  - [Table of Contents](#table-of-contents)
  - [Ways to Contribute](#ways-to-contribute)
  - [Design Philosophy](#design-philosophy)
  - [Before You Start: Open an Issue First](#before-you-start-open-an-issue-first)
  - [Development Setup](#development-setup)
    - [1. Fork and clone](#1-fork-and-clone)
    - [2. Install dependencies](#2-install-dependencies)
    - [3. Set up pre-commit hooks](#3-set-up-pre-commit-hooks)
    - [4. Verify your setup](#4-verify-your-setup)
  - [Branching and Workflow](#branching-and-workflow)
    - [Branch naming](#branch-naming)
    - [Commit messages](#commit-messages)
  - [Code Style](#code-style)
  - [Testing](#testing)
    - [Guidelines](#guidelines)
  - [Submitting a Pull Request](#submitting-a-pull-request)
    - [PR merge criteria](#pr-merge-criteria)
  - [What We Are and Are Not Looking For](#what-we-are-and-are-not-looking-for)
    - [In scope](#in-scope)
    - [Out of scope (for now)](#out-of-scope-for-now)

---

## Ways to Contribute

Contributions are not limited to code. All of the following are welcome:

- **Bug reports** — open a GitHub Issue with a reproducible example
- **Feature proposals** — open a GitHub Issue to discuss the idea before implementing
- **Documentation improvements** — fix typos, add examples, improve clarity
- **New examples** — notebooks or scripts demonstrating real-world use cases
- **Performance improvements** — especially Polars-native optimizations
- **New aggregation strategies** — must include tests and documentation

---

## Design Philosophy

Understanding the design philosophy helps you write contributions that fit naturally into the codebase.

- **Polars-first**: All data processing is built on Polars. Avoid pandas or numpy unless absolutely necessary.
- **Chainable API**: Operations are designed to be chained on an `H3Toolkit` instance, keeping pipelines readable.
- **Explicit over implicit**: Aggregation strategies are explicit classes, not magic strings. New strategies should follow the same pattern.
- **Minimal dependencies**: We keep the core dependency list small. Visualization tools are isolated behind an optional `vis` extra.

---

## Before You Start: Open an Issue First

For anything beyond small bug fixes or documentation typos, **please open a GitHub Issue before writing code**. This allows us to:

- Confirm the feature aligns with the project direction
- Discuss the API design before you invest significant time
- Avoid duplicate work if something similar is already in progress

This is especially important for new aggregation strategies, performance refactors, or features that change the public API.

---

## Development Setup

### 1. Fork and clone

Fork the repository on GitHub, then clone your fork locally:

```bash
git clone https://github.com/<your-username>/H3-ToolKits.git
cd H3-ToolKits
```

### 2. Install dependencies

This project uses [Poetry](https://python-poetry.org/) for dependency management.

```bash
# Install Poetry if you don't have it
pip install poetry

# Install all development dependencies
poetry install --with dev

# Activate the virtual environment
poetry shell
```

### 3. Set up pre-commit hooks

We use [pre-commit](https://pre-commit.com/) to enforce code style automatically before each commit.

```bash
pre-commit install
```

### 4. Verify your setup

```bash
pytest tests/
```

All tests should pass before you make any changes.

---

## Branching and Workflow

### Branch naming

Create a new branch from `main` for your work. Use the following naming convention:

| Type | Format | Example |
|------|--------|---------|
| New feature | `feat/<short-description>` | `feat/weighted-mean-aggregation` |
| Bug fix | `fix/<short-description>` | `fix/split-rounding-error` |
| Documentation | `docs/<short-description>` | `docs/contributing-guide` |
| Performance | `perf/<short-description>` | `perf/single-polars-expression` |
| Refactor | `refactor/<short-description>` | `refactor/aggregation-base-class` |

```bash
git checkout -b feat/weighted-mean-aggregation
```

### Commit messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>: <short description>

[optional body]
```

Types: `feat`, `fix`, `docs`, `perf`, `refactor`, `test`, `chore`

Examples:
```
feat: add WeightedMean aggregation strategy
fix: correct rounding in equal-split aggregation
docs: add weighted aggregation example to README
```

---

## Code Style

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.

Run it manually before committing:

```bash
ruff check .
ruff format .
```

Pre-commit will also run this automatically on each commit. If a commit is blocked by a pre-commit hook, fix the reported issues and re-stage your changes.

---

## Testing

All new features and bug fixes must include tests. We use [pytest](https://docs.pytest.org/).

```bash
# Run all tests
pytest tests/

# Run with coverage report
pytest tests/ --cov=h3_toolkit
```

### Guidelines

- Place tests in the `tests/` directory following the existing file structure.
- Each new aggregation strategy should have its own test cases covering: basic usage, edge cases (empty input, single row), and expected output values.
- Tests should be deterministic and self-contained — no external API calls or databases unless the test is explicitly an integration test.

---

## Submitting a Pull Request

1. Push your branch to your fork:
   ```bash
   git push origin feat/weighted-mean-aggregation
   ```

2. Open a Pull Request against the `main` branch of the main repository.

3. Fill out the PR description with:
   - **What** this PR does
   - **Why** it is needed (link to the related Issue)
   - **How** you tested it (paste relevant test output)

4. A maintainer will review your PR. Please be patient — we will respond as soon as we can.

### PR merge criteria

- All existing tests pass
- New functionality is covered by tests
- Code passes `ruff check` and `ruff format`
- Documentation is updated if the public API changes
- At least one maintainer has approved the PR

---

## What We Are and Are Not Looking For

### In scope

- New aggregation strategies with clear use cases
- Performance improvements to the aggregation pipeline using Polars expressions
- Improved documentation and examples
- Bug fixes with regression tests

### Out of scope (for now)

- Multi-resolution geometry support (e.g., census block geometries) — this is better maintained as a separate interoperable package
- Breaking changes to the public `H3Toolkit` API without prior discussion

If you are unsure whether your idea is in scope, open an Issue first and we will let you know.

---

We appreciate every contribution, large or small. Thank you for helping make h3-toolkit better.
