# prep data used in geobr

Data sets currenly implemented in the targets pipeline:

- [ ] country
- [ ] region
- [P] state
- [x] meso_region
- [x] micro_region
- [x] intermediate_region
- [x] immediate_region
- [ ] municipality
- [ ] municipal_seat
- [ ] weighting_area
- [ ] census_tract
- [x] statistical_grid
- [ ] metro_area
- [ ] urban_area
- [x] amazon
- [x] biomes
- [ ] conservation_units
- [ ] disaster_risk_area
- [ ] indigenous_land
- [x] semiarid
- [x] health_facilities
- [ ] health_region
- [ ] neighborhood
- [x] schools
- [ ] comparable_areas
- [ ] urban_concentrations
- [ ] pop_arrangements


Functions used:
- [x] harmonize_geobr
- [x] unzip_geobr
- [x] readmerge_geobr
- [R] folder_geobr
- [P] glimpse_geobr

P = parcial
x = done
R = in review

Data structure of project

-	data_raw
    - indiginous_land
      - 202901
        - file .rds original
      - 202005
        - file .rds original
-	data
    - indiginous_land
      - 202901
        - file .parquet
      - 202005
        - file .parquet