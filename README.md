# prep_roger

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

All 36 datasets are active in the pipeline. Each produces a full-resolution and
a simplified Parquet file per year (except POINT datasets).

| # | Dataset | Years | Parquets | Source |
|---|---------|-------|----------|--------|
| 01 | `semiarid` | 2005, 2017, 2021, 2022 | 8 | IBGE |
| 02 | `amazonia_legal` | 2019–2022, 2024 | 10 | IBGE FTP |
| 03 | `biomes` | 2004, 2019 | 4 | IBGE |
| 04 | `statistical_grid` | 2010, 2022 | 4 | IBGE |
| 05 | `health_facilities` | current | 2 | CNES/DATASUS |
| 06 | `indigenous_land` | 2024 | 2 | FUNAI |
| 07 | `intermediate_regions` | 2019–2024 | 12 | IBGE |
| 08 | `immediate_regions` | 2019–2024 | 12 | IBGE |
| 09 | `schools` | 2007–2024 | 18 | INEP microdados + geocodebr |
| 09b | `schools_bi` | snapshot | 2 | INEP Oracle BI (manual CSV) |
| 10 | `states` | 2000, 2001, 2010, 2013–2024 | 30 | IBGE FTP |
| 11 | `regions` | 2000, 2001, 2010, 2013–2024 | 30 | IBGE FTP |
| 12 | `country` | 2000, 2001, 2010, 2013–2024 | 30 | IBGE FTP |
| 13 | `mesoregions` | 2000, 2001, 2010, 2013–2018 | 18 | IBGE FTP |
| 14 | `microregions` | 2000, 2001, 2010, 2013–2018 | 18 | IBGE FTP |
| 15 | `municipality` | 2000, 2001, 2005, 2007, 2010, 2013–2024 | 34 | IBGE FTP |
| 16 | `municipal_seat` | 2010 | 2 | IBGE |
| 17 | `census_tract` | 2010, 2022 | 108 | IBGE |
| 18 | `weighting_area` | 2010 | 54 | IBGE |
| 19 | `metro_area` | 1970, 2001–2003, 2005, 2008–2010, 2013–2024 | 40 | IBGE FTP (Excel + join) |
| 20 | `urban_area` | 2005, 2015 | 4 | IBGE |
| 21 | `conservation_units` | 2024, 2025 | 4 | MMA |
| 22 | `disaster_risk_areas` | — | 2 | IBGE |
| 23 | `health_region` | 1991, 1994, 1997, 2001, 2005, 2013 | 12 | DATASUS FTP |
| 24 | `neighborhoods` | 2010, 2022 | 4 | IBGE |
| 25 | `urban_concentrations` | — | 2 | IBGE |
| 26 | `pop_arrangements` | — | 2 | IBGE |
| 27 | `favelas` | 2022 | 2 | IBGE Census 2022 |
| 28 | `localidades` | 2010 | 2 | IBGE |
| 29 | `polling_places` | 2022, 2024 | 4 | TSE |
| 30 | `electoral_zones` | 2022, 2024 | 4 | TSE |
| 31 | `river_basins` | 2021 | 6 | ANA |
| 32 | `historical_empire` | 1872–1991 (11 years) | 22 | IBGE FTP |
| 33 | `capitals` | 2010 | 1 | IBGE (derived from municipal_seat) |
| 34 | `quilombo_area` | 2024 | 2 | INCRA |
| 35 | `comparable_areas` | 1872–2020 (91 pairs) | 182 | Algorithmic crosswalk |

**Total: 675 Parquet files (~8.6 GB)**

## Project structure

```
prep_roger/
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
