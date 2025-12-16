---
editor_options: 
  markdown: 
    wrap: 72
---

# prep data used in geobr

Data sets currenly implemented in the targets pipeline:

-   [ ] country
-   [ ] region
-   [ ] state
-   [ ] meso_region
-   [ ] micro_region
-   [ ] intermediate_region
-   [ ] immediate_region
-   [ ] municipality
-   [ ] municipal_seat
-   [ ] weighting_area
-   [ ] census_tract
-   [ ] statistical_grid
-   [ ] metro_area
-   [ ] urban_area
-   [ ] amazon
-   [x] biomes
-   [ ] conservation_units
-   [ ] disaster_risk_area
-   [ ] indigenous_land
-   [ ] semiarid
-   [ ] health_facilities
-   [ ] health_region
-   [ ] neighborhood
-   [ ] schools
-   [ ] comparable_areas
-   [ ] urban_concentrations
-   [ ] pop_arrangements
-   [ ] favelas

Functions used: - [x] harmonize_geobr - [x] unzip_geobr - [x]
readmerge_geobr - [R] folder_geobr - [P] glimpse_geobr

P = parcial x = done R = in review

Data structure of project

-   data_raw
    -   indiginous_land
        -   202901
            -   file .rds original
        -   202005
            -   file .rds original
-   data
    -   indiginous_land
        -   202901
            -   file .parquet
        -   202005
            -   file .parquet
