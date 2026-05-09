# Preparing the data for {geobr}

R pipeline that downloads, processes and standardizes Brazilian geospatial
datasets for the [`geobr`](https://github.com/ipeaGIT/geobr) package.

**Output:** zstd-compressed [GeoParquet](https://geoparquet.org/) files (with
spatial metadata via geoarrow) published to GitHub Releases of `ipeaGIT/geobr`
via [`piggyback`](https://github.com/ropensci/piggyback).

## Tech stack

R 4.5 · [targets](https://docs.ropensci.org/targets/) ·
[sf](https://r-spatial.github.io/sf/) ·
[arrow](https://arrow.apache.org/docs/r/) + [geoarrow](https://github.com/geoarrow/geoarrow-r) ·
[lwgeom](https://r-spatial.github.io/lwgeom/) ·
[crew](https://wlandau.github.io/crew/) ·
[renv](https://rstudio.github.io/renv/) ·
[piggyback](https://docs.ropensci.org/piggyback/) ·
[geocodebr](https://github.com/ipeaGIT/geocodebr) ·
[sfarrow](https://github.com/wcjochem/sfarrow) ·
[testthat](https://testthat.r-lib.org/)

## Getting started

```r
# 1. Install locked dependencies
renv::restore()

# 2. Run the full pipeline
library(targets)
tar_make()

# 3. Visualize the DAG
tar_visnetwork()

# 4. Check for warnings/errors
tar_meta(fields = warnings, complete_only = TRUE)
```

**Requirements:** R >= 4.5, internet connection (downloads from IBGE, DATASUS,
MMA, FUNAI FTP servers).

## Implemented datasets

To check what data sets have been implemented already, check [here](https://github.com/ipeaGIT/geobr#available-datasets)


**Total: 675 Parquet files (~8.6 GB)**

## Project structure

```
geobr_prep_data/
├── _targets.R                        # Pipeline definition (DAG)
├── R/
│   ├── support_harmonize_geobr.R     # Core: harmonization, projection, topology
│   ├── support_fun.R                 # Helpers: download, unzip, read/merge
│   ├── upload.R                      # Upload to GitHub Releases via piggyback
│   └── [dataset].R                   # download_X() + clean_X() per dataset
├── tests/testthat/                   # Unit tests (testthat, 22 tests)
├── ainda_sem_targets/                # Legacy scripts (reference only)
├── data/                             # Output GeoParquets (git-ignored, ~8.6 GB)
├── renv.lock                         # Locked R dependencies
├── CLAUDE.md                         # Claude Code project instructions
└── .claude/                          # Rules, plans, backlog, known issues
    ├── rules/                        # Column conventions, harmonization guide
    ├── plans/                        # Implementation plans
    ├── BACKLOG.md                    # Dataset status tracker
    └── PROBLEMS.md                   # Known bugs and fixes
```

## Data standards

All output Parquets follow these conventions:

- **CRS:** SIRGAS 2000 (EPSG:4674)
- **Geometry:** `MULTIPOLYGON` (except `POINT` for health_facilities, schools, schools_bi, capitals)
- **Format:** [GeoParquet](https://geoparquet.org/) with spatial metadata
  (CRS, geometry type, bbox) via `geoarrow`
- **Compression:** zstd, level 7
- **Column order:** `code_X`, `name_X`, `code_state`, `abbrev_state`,
  `name_state`, `code_region`, `name_region`, `year`, `geometry`
- **Types:** `code_*` = numeric, `name_*` = character (Title Case),
  `abbrev_state` = 2-letter uppercase

### Output layout

```
data/
└── [dataset]/
    └── [year]/
        ├── [dataset]_[year].parquet              # Full resolution
        └── [dataset]_[year]_simplified.parquet    # Simplified (100m tolerance)
```

## Adding a new dataset

1. Create `R/[dataset].R` with `download_X(year)` and `clean_X(raw, year)`
2. Add 3 targets in `_targets.R` (years, raw, clean)
3. Add `[dataset]_clean` to the `all_files` target at the end of `_targets.R`
4. Run `tar_make()` and validate output

See [`.claude/rules/new-dataset.md`](.claude/rules/new-dataset.md) for the
full checklist.

## Running tests

```r
# From the project root:
source("tests/testthat.R")
```

22 tests covering core harmonization functions (`snake_case_names`,
`add_state_info`, `add_region_info`, `normalize_sf_geometry`, `validate_geobr`).

The pipeline also includes a `validation` target that checks all output
GeoParquets for correct CRS, geometry types, column types, and schema.

## Documentation

| File | Description |
|------|-------------|
| [`CLAUDE.md`](CLAUDE.md) | Project instructions and conventions |
| [`.claude/rules/column-conventions.md`](.claude/rules/column-conventions.md) | Column naming, ordering, types |
| [`.claude/rules/harmonization.md`](.claude/rules/harmonization.md) | How to use `harmonize_geobr()` |
| [`.claude/rules/new-dataset.md`](.claude/rules/new-dataset.md) | Checklist for new datasets |
| [`.claude/BACKLOG.md`](.claude/BACKLOG.md) | Status of all 36 datasets |
| [`.claude/PROBLEMS.md`](.claude/PROBLEMS.md) | 21 bugs resolved (historical log) |
