# Collector for IOM DTM Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-iom-dtm/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-iom-dtm/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-iom-dtm/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-iom-dtm?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline retrieves internally displaced persons (IDP) data from the
[IOM Displacement Tracking Matrix (DTM) API](https://dtmapi.iom.int/) and
publishes it to HDX first as per-country and global displacement datasets,
and then as a HAPI dataset derived from the global data. It makes reads to the DTM API (one call each
for the country list and operation status, plus up to three admin-level calls
per country across admin levels 0–2) and HDX writes (one per
country dataset plus a global dataset and a HAPI dataset). Temporary
per-country CSV files of tens to hundreds of KB each are created during
processing. For each country, displacement data is downloaded per admin level,
merged with operation status, and normalised into a standard column set; the
per-country and global standard datasets are uploaded to HDX first; the HAPI
dataset is then generated from the global data, adding admin P-codes and date
fields from the most recent reporting round.

## Data Pipeline

### API reads

- **Country list** (1 read): fetches the list of countries with active DTM
  operations.
- **Operation status** (1 read): fetches the current operation status for all
  countries.
- **Admin-level displacement data** (up to 3 reads per country across admin levels
  0–2): displacement figures downloaded per admin level for each country.

### API writes

- **Per-country displacement datasets** (~one write per country): each dataset
  contains displacement data across admin levels 0–2.
- **Global displacement dataset** (1 write): aggregates data across all countries.
- **HAPI dataset** (1 write): derived from the global data, enriched with admin
  P-codes and date fields from the most recent reporting round.

### Temporary files

- Per-country CSV files of tens to hundreds of KB each, created during processing.

### Uploaded files

- Per-country displacement datasets with admin-level breakdown.
- Global displacement dataset.
- HAPI dataset derived from the global output.

### Transformations

1. **Admin-level download**: displacement data is fetched separately for admin
   levels 0, 1, and 2 per country.
2. **Operation status merge**: admin-level data is joined with the operation status
   response.
3. **Column normalisation**: source columns are renamed and aligned to a standard
   output schema.
4. **P-code and date enrichment**: the HAPI dataset adds admin P-codes and date
   fields derived from the most recent reporting round per country.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-iom-dtm** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.iom_dtm
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
