# prep data used in geobr

Data sets currenly implemented in the targets pipeline:

- [ ] country
- [ ] region
- [ ] state
- [P] meso_region
- [P] micro_region
- [P] intermediate_region
- [X] immediate_region
- [ ] municipality
- [ ] municipal_seat
- [ ] weighting_area
- [ ] census_tract
- [X] statistical_grid
- [ ] metro_area
- [ ] urban_area
- [x] amazon
- [x] biomes
- [ ] conservation_units
- [ ] disaster_risk_area
- [ ] indigenous_land
- [x] semiarid
- [P] health_facilities
- [ ] health_region
- [ ] neighborhood
- [ ] schools
- [ ] comparable_areas
- [ ] urban_concentrations
- [ ] pop_arrangements


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