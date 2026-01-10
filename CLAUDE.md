# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Patni is a Python library providing pipe-like syntax. Currently at version 0.1.4.

## Development Commands

This project uses `uv` as the package manager.

```bash
# Install dependencies
uv sync

# Run the CLI
uv run patni

# Run main directly
uv run python main.py

# Build the package
uv build

# Publish to TestPyPI
uv publish --index testpypi
```

## Project Structure

- `main.py` - Entry point containing the `main()` function, exposed as the `patni` CLI command
- `pyproject.toml` - Project configuration with TestPyPI publishing setup

## Requirements

- Python >= 3.12 (using 3.13)
