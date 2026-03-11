---
editor_options: 
  markdown: 
    wrap: 72
---

# prep data used in geobr

Data sets currenly implemented in the targets pipeline:


-   [x] semiarid
-   [X] amazon
-   [x] biomes
-   [X] statistical_grid
-   [X] health_facilities
-   [ ] indigenous_land
-   [X] intermediate_region
-   [X] immediate_region
-   [X] schools
-   [X] state
-   [X] region
-   [X] country
-   [ ] meso_region
-   [ ] micro_region
-   [X] municipality
-   [ ] municipal_seat
-   [ ] census_tract
-   [ ] weighting_area
-   [ ] metro_area
-   [ ] urban_area
-   [X] conservation_units
-   [X] disaster_risk_area
-   [ ] health_region
-   [ ] neighborhood
-   [ ] urban_concentrations
-   [ ] pop_arrangements
-   [ ] favelas
-   [ ] comparable_areas
-   [ ] electoral zones
-   [ ] river basins
-   [ ] historic brazilian empire


Functions used: - [x] harmonize_geobr - [x] unzip_geobr - [x]
readmerge_geobr 

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
