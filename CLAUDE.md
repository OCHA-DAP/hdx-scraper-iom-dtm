# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-iom-dtm** retrieves displacement data from the IOM Displacement Tracking Matrix (DTM) API and publishes it to HDX. It generates both country-level and global datasets, as well as a HAPI-compatible dataset, covering displacement tracking operations and population movements.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.iom_dtm
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_iom_dtm.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `__main__.py`:

1. **`main`** — Calls `facade()` to set up HDX configuration, creates a `Pipeline` instance, fetches country and operation data, then generates and publishes datasets.

Key modules:
- **`pipeline.py`** — Core scraper logic: fetches countries and operation status from the DTM API, generates per-country and global datasets, and produces a HAPI-compatible dataset with admin-level displacement data.

### Key design points

- Uses `DTM_API_KEY` (via `Ocp-Apim-Subscription-Key` header) for authenticated API access.
- Generates one dataset per country plus one global dataset.
- The HAPI dataset is generated only for the global (all-countries) pass.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-iom-dtm` entry.

Additional env vars used at runtime: `DTM_API_KEY`, `ERR_TO_HDX`.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
