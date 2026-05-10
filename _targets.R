library(targets)
library(tarchetypes)
library(crew)

# cores available
#coress <- floor(.5 * parallelly::freeCores()[1])
 coress <- 2  # limitado temporariamente para evitar rate-limit do FTP IBGE
# Check collumn names, order, size and schema ----------------------------------

# colunas <- check_collumns_geobr(dir_data = "./data")
# so rodar isso depois de carregar as funcoes harmonize_geobr

# RENV -------------------------------------------------------------------------

# # See if the packages are updated:
# renv::status()
# # Update packages if needed:
# renv::snapshot()

# Set target options: ----------------------------------------------------------
tar_option_set(
  format = "rds",
  memory = "transient",
  garbage_collection = TRUE,
  controller = crew_controller_local(
    workers = coress,
    options_local = crew_options_local(log_directory = "./logs/crew_workers")
  ),
  storage = "worker" ,
  retrieval = "worker",
  trust_timestamps = TRUE,
  error = "null",
  
  # Packages essentials --------------------------------------------------------
  packages = c('arrow',
               'collapse',
               'crew',
               'data.table',
               'devtools',
               'dplyr',
               'duckdb',
               'duckspatial',
               'furrr',
               'future',
               'fs',
               'geobr',
               'geocodebr',
               'geos',
               'geoarrow',
               'httr2',
               'igraph',
               'janitor',
               'knitr',
               'lubridate',
               'lwgeom',
               'magrittr',
               'mirai',
               'nanonext',
               'openxlsx',
               'parallelly',
               'parallel',
               'pbapply',
               'piggyback',
               'purrr',
               'RCurl',
               'readr',
               'readxl',
               'rvest',
               'sfheaders',
               's2',
               'sf',
               'sp',
               'stringi',
               'stringr',
               'tarchetypes',
               'tibble',
               'tidyverse',
               'varhandle',
               'visNetwork'
               )
  )

# invisible(lapply(packages, library, character.only = TRUE))

# tar_make_clustermq() configuration (okay to leave alone):
# options(clustermq.scheduler = "multisession")

# Run the R scripts in the R/ folder with your custom functions:
targets::tar_source('./R')

############# The Targets List #########----------------------------------------

# In case of error, run:
# test_errors <- targets::tar_meta(fields = warnings, complete_only = TRUE)

list(
  
  # 01. Semiarido ---------------------------------------------------------------
  
  # year input
  tar_target(name = years_semiarid,
             command = c(2005, 2017, 2021, 2022)),
  
  # download
  tar_target(name = semiarid_raw,
             command = download_semiarid(years_semiarid),
             pattern = map(years_semiarid)),
  
  # clean
  tar_target(name = semiarid_clean,
             command = clean_semiarid(semiarid_raw, municipality_clean),
             pattern = map(semiarid_raw),
             format = 'file'),
  
  #02. Amazonia Legal ----------------------------------------------------------

  # year input
  tar_target(name = years_amazon,
             command = c(2019:2022, 2024)),

  # download
  tar_target(name = amazonialegal_raw,
             command = download_amazonialegal(years_amazon),
             pattern = map(years_amazon)),

  # clean
  tar_target(name = amazonialegal_clean,
             command = clean_amazonialegal(amazonialegal_raw),
             pattern = map(amazonialegal_raw),
             format = 'file'),

  #03. Biomas ------------------------------------------------------------------

  # year input
  tar_target(name = years_biomes,
             command = c(2006, 2019, 2025)),
  
  # download
  tar_target(name = biomes_raw,
             command = download_biomes(years_biomes),
             pattern = map(years_biomes)),
  
  # clean
  tar_target(name = biomes_clean,
             command = clean_biomes(biomes_raw),
             pattern = map(biomes_raw),
             format = 'file'),
  
  #04. Grade estatistica -------------------------------------------------------

  # year input
  tar_target(name = years_statsgrid,
             command = c(2010, 2022)),

  # download
  tar_target(name = statsgrid_raw,
             command = download_statsgrid(years_statsgrid),
             pattern = map(years_statsgrid),
             deployment = 'main'
             ),

  # clean
  tar_target(name = statsgrid_clean,
             command = clean_statsgrid(statsgrid_raw, municipality_clean),
             pattern = map(statsgrid_raw),
             deployment = 'main',
             format = 'file'),
  
  #05. Estabelecimentos de saude -----------------------------------------------

  #year input
  tar_target(name = years_healthfacilities,
             command = seq(
               from = as.Date("2017-04-01"), # Jan de 2017 corrompido
               to = as.Date("2026-10-01"), 
               by = "3 months"
               ) |> 
               format("%Y%m") |> 
               as.integer()
             ),

  # download
  tar_target(name = healthfacilities_raw,
             command = download_healthfacilities(years_healthfacilities),
             pattern = map(years_healthfacilities)),

  # clean
  tar_target(name = healthfacilities_clean,
             command = clean_healthfacilities(healthfacilities_raw, municipality_clean),
             pattern = map(healthfacilities_raw),
             deployment = 'main', # necessary to avoid conflict of geocodebr
             format = 'file'),
  
  #06. Terras Indigenas --------------------------------------------------------

  # year input
  tar_target(name = years_indigenousland,
             command = c(2016:2019, 2022, 2024:2025)
             ),

  # download
  tar_target(name = indigenousland_raw,
             command = download_indigenousland(years_indigenousland),
             pattern = map(years_indigenousland)),

  # clean
  tar_target(name = indigenousland_clean,
             command = clean_indigenousland(indigenousland_raw),
             pattern = map(indigenousland_raw),
             format = 'file'),
  
  #07. Regioes Intermediarias --------------------------------------------------

  # year input
  tar_target(name = years_intermediateregions,
             command = c(2019:2025)),

  # download
  tar_target(name = intermediateregions_raw,
             command = download_intermediateregions(years_intermediateregions),
             pattern = map(years_intermediateregions)),

  # clean
  tar_target(name = intermediateregions_clean,
             command = clean_intermediateregions(intermediateregions_raw),
             pattern = map(intermediateregions_raw),
             format = 'file'),
  
  #08. Regioes Imediatas -------------------------------------------------------

  # year input
  tar_target(name = years_immediateregions,
             command = c(2019:2025)),

  # download
  tar_target(name = immediateregions_raw,
             command = download_immediateregions(years_immediateregions),
             pattern = map(years_immediateregions)),

  # clean
  tar_target(name = immediateregions_clean,
             command = clean_immediateregions(immediateregions_raw),
             pattern = map(immediateregions_raw),
             format = 'file'),
  
  #09. Escolas -----------------------------------------------------------------
  
  # year input
  tar_target(name = years_schools,
             # command = c(2020, 2023, 2026) # legacy release no github
             command = c(2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, # censo escolar
                         2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
                         2023, 2024, 2025)
             ),

  # download
  tar_target(name = schools_raw,
             command = download_schools_microdados(years_schools),
             pattern = map(years_schools)),

  # clean
  tar_target(name = schools_clean,
             command = clean_schools(schools_raw),
             pattern = map(schools_raw),
             deployment = 'main', # necessary to avoid conflict of geocodebr
             format = 'file'),
  # 
  # #09b. Escolas — Oracle BI (CSV com lat/lon oficiais INEP) --------------------
  # # CSVs baixados manualmente do Oracle BI em data-raw/schools_bi/YYYYMM_schools_bi.csv
  # # Snapshot do ano mais recente do Censo Escolar (sem filtro temporal)
  # 
  # tar_target(name = schoolsbi_csv_files,
  #            command = {
  #              files <- list.files("./data-raw/schools_bi",
  #                                  pattern = "[.]csv$", full.names = TRUE)
  #              if (length(files) == 0) warning("Nenhum CSV em data-raw/schools_bi/")
  #              files
  #            }),
  # 
  # tar_target(name = schoolsbi_raw,
  #            command = download_schools_bi(schoolsbi_csv_files),
  #            pattern = map(schoolsbi_csv_files)),
  # 
  # tar_target(name = schoolsbi_clean,
  #            command = {
  #              # Extract year from filename: YYYYMM_schools_bi.csv -> YYYY
  #              # BI is a snapshot of the current moment (no historical data)
  #              fname <- basename(schoolsbi_csv_files)
  #              snapshot_year <- as.numeric(substr(fname, 1, 4))
  #              clean_schools_bi(schoolsbi_raw, snapshot_year)
  #            },
  #            pattern = map(schoolsbi_raw, schoolsbi_csv_files),
  #            format = 'file'),

  #10. Estados -----------------------------------------------------------------

  #year input
  tar_target(name = years_states,
             command = c(2000, 2001, 2010, 2013:2025)),

  # download
  tar_target(name = states_raw,
             command = download_states(years_states),
             pattern = map(years_states)),

  # clean
  tar_target(name = states_clean,
             command = clean_states(states_raw),
             pattern = map(states_raw),
             format = 'file'),

  #11. Regioes -----------------------------------------------------------------
  # esse target gera os poligos das regioes a partir da agregacao dos estados

  # unifica um vetor com file path to todos states_clean
  tar_target(name = all_states_clean,
             command = get_all_states_clean(states_clean, hist_states_clean)
             ),
  
  # clean
  tar_target(name = regions_clean,
             command = clean_regions(all_states_clean),
             pattern = map(all_states_clean),
             
             format = 'file'),
  
  #12. Pais --------------------------------------------------------------------
  
  # esse target gera os poligonos das regioes a partir da agregacao dos estados
  
  # clean
  tar_target(name = country_clean,
             command = clean_country(regions_clean),
             # pattern = map(regions_clean), # we perform the map internally within clean_country()
             format = 'file'),
  
  #13. Meso Regioes ------------------------------------------------------------

  # year input
  tar_target(name = years_mesoregions,
             command = c(2000, 2001, 2013:2022)),

  # download
  tar_target(name = mesoregions_raw,
             command = download_mesoregions(years_mesoregions),
             pattern = map(years_mesoregions)),

  # clean
  # NOTA: a correcao das mesorregioes 2104, 2105 (faltantes em 2000) foi
  # integrada dentro de clean_mesoregions(year=2000). O target mesoregions_fixed
  # foi removido em 2026-04-26 — sobrescrevia mesoregions_2000.parquet,
  # causando outdated perpetuo do branch mesoregions_clean[year=2000].
  tar_target(name = mesoregions_clean,
             command = clean_mesoregions(mesoregions_raw),
             pattern = map(mesoregions_raw),
             format = 'file'),


  #14. Micro Regioes -----------------------------------------------------------

  # year input
  tar_target(name = years_microregions,
             command = c(2000, 2001, 2013:2022)),
  # 2010 com problema sem code_micro 6666666666 falta resolver

  # download
  tar_target(name = microregions_raw,
             command = download_microregions(years_microregions),
             pattern = map(years_microregions)),

  # clean
  # NOTA: a correcao das microrregioes 210520, 210521 (faltantes em 2000) foi
  # integrada dentro de clean_microregions(year=2000). O target microregions_fixed
  # foi removido em 2026-04-26 — sobrescrevia microregions_2000.parquet,
  # causando outdated perpetuo do branch microregions_clean[year=2000].
  tar_target(name = microregions_clean,
             command = clean_microregions(microregions_raw),
             pattern = map(microregions_raw),
             format = 'file'),


  #15. Municipios  -------------------------------------------------------------
  
  # year input
  tar_target(name = years_municipality,
             command = c(2000, 2001, 2005, 2007, 2010, 2013:2025)),

  # download
  tar_target(name = municipality_raw,
             command = download_municipality(years_municipality),
             pattern = map(years_municipality)),

  # clean
  tar_target(name = municipality_clean,
             command = clean_municipality(municipality_raw),
             pattern = map(municipality_raw),
             format = 'file'),
  
  #16. Sede municipal -------------------------------------------------------

  # year input
  tar_target(name = years_muniseats,
             command = c(2010, 2022)),

  # download
  tar_target(name = muniseats_raw,
             command = download_muniseats(years_muniseats),
             pattern = map(years_muniseats)),

  # clean
  tar_target(name = muniseats_clean,
             command = clean_muniseats(muniseats_raw),
             pattern = map(muniseats_raw),
             format = 'file'),
  
  #17. Setor censitario ----------------------------------------------------------
  # Each year is an independent target (no map pattern). This avoids
  # re-processing 2010/2022 when 2000 code changes. Year 2000 is further
  # split into urbano/rural/unified sub-targets for incremental caching.

  # ---- 2010 (independent) ----
  tar_target(name = censustract_2010_raw,
             command = download_censustract_2010(2010)),
  tar_target(name = censustract_2010_clean,
             command = clean_censustract(censustract_2010_raw),
             format = 'file'),

  # ---- 2022 (independent) ----
  tar_target(name = censustract_2022_raw,
             command = download_censustract_2022(2022)),
  tar_target(name = censustract_2022_clean,
             command = clean_censustract(censustract_2022_raw),
             format = 'file'),

  # ---- 2000: 3 sub-products with explicit dependencies ----
  # urbano_clean + rural_clean = faithful to IBGE source (no CompatMalhas)
  # unified_clean = combines both + splits ranges using 2010 as reference (DESATIVADO)
  tar_target(name = censustract_2000_raw,
             command = download_censustract_2000(2000)),
  tar_target(name = censustract_2000_urbano_clean,
             command = clean_censustract_2000_urbano(censustract_2000_raw),
             format = 'file'),
  tar_target(name = censustract_2000_rural_clean,
             command = clean_censustract_2000_rural(censustract_2000_raw),
             format = 'file'),
  # tar_target(name = censustract_2000_unified_clean,
  #            command = clean_censustract_2000_unified(censustract_2000_urbano_clean,
  #                                                     censustract_2000_rural_clean,
  #                                                     censustract_2010_clean,
  #                                                     censustract_2000_raw),
  #            format = 'file'),

  #18. Area de ponderacao ------------------------------------------------------------

  # year input
  tar_target(name = years_weightarea,
             command = c(2010)),
  
  # download
  tar_target(name = weightarea_raw,
             command = download_weightarea(years_weightarea),
             pattern = map(years_weightarea)),

  # clean
  tar_target(name = weightarea_clean,
             command = clean_weightarea(weightarea_raw),
             #command = clean_weightarea(years_weightarea, censustract_2010_clean),
             pattern = map(weightarea_raw),
             format = 'file'),
  
  #19. Metro Area --------------------------------------------------------------

  # year input (todos os anos com dado no FTP IBGE; 2004, 2006, 2007, 2011, 2012 ausentes)
  tar_target(name = years_metro_area,
             command = c(1970,
                         2001, 2002, 2003, 2005,
                         2008, 2009,
                         2010, 2013:2020,
                         2021:2024)),

  # download
  tar_target(name = metro_area_raw,
             command = download_metro_area(years_metro_area),
             pattern = map(years_metro_area)),

  # clean (depende de municipality_clean + hist_muni_clean)
  tar_target(name = metro_area_clean,
             command = clean_metro_area(metro_area_raw, municipality_clean, hist_muni_clean),
             pattern = map(metro_area_raw),
             format = 'file'),

  #19b. Metro Area DePara ------------------------------------------------------
  # Tabela de lookup RMs antigas (SIDRA/BET) -> novas (cod_recmetropol 2022)
  # Uso: bridge temporal 2019-2020 -> 2021+. Cobertura ~86%; 2 RMs orfas.
  tar_target(name = metro_area_depara_raw,
             command = download_metro_area_depara()),

  tar_target(name = metro_area_depara_clean,
             command = clean_metro_area_depara(metro_area_depara_raw),
             format = 'file'),

  #20. Areas urbanas -----------------------------------------------------------
  
  # year input
  tar_target(name = years_urbanarea,
             command = c(2005, 2015, 2019)),

  # download
  tar_target(name = urbanarea_raw,
             command = download_urbanarea(years_urbanarea),
             pattern = map(years_urbanarea)),

  # clean
  tar_target(name = urbanarea_clean,
             command = clean_urbanarea(urbanarea_raw, municipality_clean),
             pattern = map(urbanarea_raw),
             format = 'file'),
  
  
  #21. Unidades de conservacao -------------------------------------------------
  
  # year input
  tar_target(name = years_conservationunits,
             command = c(2024, 2025)),

  # download
  tar_target(name = conservationunits_raw,
             command = download_conservationunits(years_conservationunits),
             pattern = map(years_conservationunits)),

  # clean
  tar_target(name = conservationunits_clean,
             command = clean_conservationunits(conservationunits_raw),
             pattern = map(conservationunits_raw),
             format = 'file'),
  
  #22. Areas de risco de desastre ----------------------------------------------
  
  # year input
  tar_target(name = years_riskdisasterareas,
             command = c(2010)),
  
  # download
   tar_target(name = riskdisasterareas_raw,
              command = download_riskdisasterareas(years_riskdisasterareas)),
  
  # clean
   tar_target(name = riskdisasterareas_clean,
              command = clean_riskdisasterareas(riskdisasterareas_raw),
              format = 'file'),
  
  # 23a. Regiao de Saude pre-2020  ---------------------------------------------------------
  # antes de 2023, usamos os poligonos
  # apos 2023, usamos a lista de municipios em cada regiao
  
  tar_target(name = years_healthregions_old,
             command = c(1991, 1994, 1997, 2001, 2005, 2013)
             ),

  # download
  tar_target(name = healthregions_raw_old,
             command = download_healthregions_old(years_healthregions_old),
             pattern = map(years_healthregions_old)
             ),

  # clean
  tar_target(name = healthregions_clean_old,
             command = clean_healthregions_old(healthregions_raw_old, municipality_clean, hist_muni_clean),
             pattern = map(healthregions_raw_old),
             format = 'file'
             ),


  
  # 23b. Regiao de Saude pos-2020  ---------------------------------------------------------
  # antes de 2023, usamos os poligonos
  # apos 2023, usamos a lista de municipios em cada regiao
  
  tar_target(name = years_healthregions_new,
             command = c(2023:2025)
             ),
  
  # download
  tar_target(name = healthregions_raw_new,
             command = download_healthregions_new(years_healthregions_new),
             pattern = map(years_healthregions_new)
             ),
  
  # clean
  tar_target(name = healthregions_clean_new,
             command = clean_healthregions_new(healthregions_raw_new, municipality_clean),
             pattern = map(healthregions_raw_new),
             format = 'file'
             ),
  
  #24. Bairros -----------------------------------------------------------------

  # esse targer gera os poligos das bairros a partir da agregacao dos setores censitarios
  # clean
  tar_target(name = neighborhoods_clean,
             command = clean_neighborhoods(c(censustract_2010_clean,
                                           censustract_2022_clean
                                           #,censustract_2000_unified_clean
                                           )),
             format = 'file'),

  
  # #25. Concentracoes urbanas ---------------------------------------------------
  # old 

    
  #26. Arranjos populacionais e Areas de concentracao urbana no mesmo dado --------------------------------------------------

  # year input
  tar_target(name = years_poparrangements,
             command = c(2010)
  ),

  # download
  tar_target(name = poparrangements_raw,
             command = download_poparrangements(years_poparrangements)),

  # clean
  tar_target(name = poparrangements_clean,
             command = clean_poparrangements(poparrangements_raw, municipality_clean),
             format = 'file'),
  
  
  #27. Favelas e aglomerados urbanos -------------------------------------------

  # year input
  # adicionar 2010 666666666666666666666666666666666666666666666666666666666666666
  # https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/malha_de_aglomerados_subnormais/
  
  tar_target(name = years_favela,
             command = c(2022)
             ),

  # download
  tar_target(name = favela_raw,
             command = download_favela(years_favela),
             pattern = map(years_favela)),

  # clean
  tar_target(name = favela_clean,
             command = clean_favela(favela_raw),
             pattern = map(favela_raw),
             format = 'file'),
  
  # #28. Localidades -------------------------------------------------------------
  # 
  # # year input
  # tar_target(name = years_locality,
  #            command = c(2010)),
  # 
  # # download
  # tar_target(name = locality_raw,
  #            command = download_locality(years_locality),
  #            pattern = map(years_locality)),
  # 
  # # clean
  # tar_target(name = locality_clean,
  #            command = clean_locality(locality_raw),
  #            pattern = map(locality_raw),
  #            format = 'file'),


  # 29. Locais de votacao -------------------------------------------------------

  # year input
  tar_target(name = years_pollingplaces,
             command = c(2010, 2012, 2014, 2016, 2018,
                         2020, 2022, 2024)
             ),

  # download
  tar_target(name = pollingplaces_raw,
             command = download_pollingplaces(years_pollingplaces),
             pattern = map(years_pollingplaces)
             ),

    # get cross walk of code_muni from IBGE and TSE
  tar_target(name = crosswalk_tse_ibge,
             command = get_crosswalk_tse_ibge()
             ),
  
  # clean polling places
  tar_target(name = pollingplaces_clean,
             command = clean_pollingplaces(pollingplaces_raw, crosswalk_tse_ibge),
             pattern = map(pollingplaces_raw),
             deployment = 'main', # necessary to avoid conflict of geocodebr
             format = 'file'
             ),


  
  # #31. Bacias Hidrograficas (DHN250) -------------------------------------------
  # 
  # # year input
  # tar_target(name = years_riverbasins,
  #            command = c(2021)),
  # 
  # # download
  # tar_target(name = riverbasins_raw,
  #            command = download_riverbasins(years_riverbasins),
  #            pattern = map(years_riverbasins)),
  # 
  # # clean
  # tar_target(name = riverbasins_clean,
  #            command = clean_riverbasins(riverbasins_raw),
  #            pattern = map(riverbasins_raw),
  #            format = 'file'),
  
  
  # #32. Capitais (State Capitals) -----------------------------------------------
  # 
  # tar_target(name = capitals_raw,
  #            command = download_capitals(muniseats_clean)),
  # 
  # tar_target(name = capitals_clean,
  #            command = clean_capitals(capitals_raw),
  #            format = 'file'),
  
  
  #33. Municipios historicos 1872-1991 ---------------------------------

  # year input
  tar_target(name = years_historical_data,
             command = c(1872, 1900, 1911, 1920, 1933, 1940, 1950, 1960,
                         1970, 1980, 1991)),

  # download
  tar_target(name = hist_muni_raw,
             command = download_hist_muni(years_historical_data),
             pattern = map(years_historical_data)),

  # clean
  tar_target(name = hist_muni_clean,
             command = clean_hist_muni(hist_muni_raw),
             pattern = map(hist_muni_raw),
             format = 'file'),

  # #32. Estados historicos 1872-1991 ---------------------------------
  
  # download
  tar_target(name = hist_state_raw,
             command = download_hist_states(years_historical_data),
             pattern = map(years_historical_data)),
  
  # clean
  tar_target(name = hist_states_clean,
             command = clean_hist_states(hist_state_raw),
             pattern = map(hist_state_raw),
             format = 'file'),
  
  
  #33. Sedes municipais historicas 1872-1991 ---------------------------------
  
  # download
  tar_target(name = hist_muniseats_raw,
             command = download_hist_muniseats(years_historical_data),
             pattern = map(years_historical_data)),
  
  # clean
  tar_target(name = hist_muniseats_clean,
             command = clean_hist_muniseats(hist_muniseats_raw),
             pattern = map(hist_muniseats_raw),
             format = 'file'),
  

  #34. Quilombolas (Quilombo Areas) --------------------------------------------

  #year input
  tar_target(name = years_quilombo,
             command = c(format(Sys.Date(), "%Y%m"))),
  
  tar_target(name = quilombo_raw,
             command = download_quilombo(years_quilombo)),

  tar_target(name = quilombo_clean,
             command = clean_quilombo(quilombo_raw),
             format = 'file'),

  # #35. AMC (Comparable Areas / Áreas Mínimas Comparáveis) ----------------------
  # # NOTE: AMC produces year-pair combinations (start_year, end_year).
  # # For now, using a representative subset. Full 91 pairs can be enabled later.
  # 
  # tar_target(name = amc_combinations,
  #            command = amc_year_combinations()),
  # 
  # tar_target(name = amc_raw,
  #            command = download_comparable_areas(
  #              amc_combinations$start_year,
  #              amc_combinations$end_year,
  #              municipality_clean,
  #              historicalempire_clean),
  #            pattern = map(amc_combinations)),
  # 
  # tar_target(name = amc_clean,
  #            command = clean_comparable_areas(
  #              amc_raw,
  #              amc_combinations$start_year,
  #              amc_combinations$end_year,
  #              municipality_clean,
  #              historicalempire_clean),
  #            pattern = map(amc_raw, amc_combinations),
  #            format = 'file'),

  #END. Upload files -----------------------------------------------------------

  # all files input
  tar_target(name = all_files,
             command = c(
               semiarid_clean, #01
               amazonialegal_clean, #02
               biomes_clean, #03
               statsgrid_clean, #04
               healthfacilities_clean, #05
               indigenousland_clean, #06
               intermediateregions_clean, #07
               immediateregions_clean, #08
               schools_clean, #09
               # schoolsbi_clean, #09b
               states_clean, #10
               regions_clean, #11
               country_clean, #12
               mesoregions_clean, #13
               microregions_clean, #14
               municipality_clean, #15
               muniseats_clean, #16
               censustract_2010_clean,          #17a
               censustract_2022_clean,          #17b
               censustract_2000_urbano_clean,   #17c
               censustract_2000_rural_clean,    #17d
               # censustract_2000_unified_clean,  #17e (declaracao comentada — desativado)
               weightarea_clean, #18 (declaracao comentada)
               metro_area_clean, #19
               metro_area_depara_clean, #19b
               urbanarea_clean, #20
               conservationunits_clean, #21
               riskdisasterareas_clean, #22
               healthregions_clean_old,  # 23a
               healthregions_clean_new,  # 23b
               neighborhoods_clean, #24
               poparrangements_clean, #26
               favela_clean, #27
               # locality_clean, #28
               pollingplaces_clean, #29
               # riverbasins_clean, #31
               hist_states_clean, #32
               hist_muni_clean,
               hist_muniseats_clean,
               # capitals_clean, #33 (declaracao comentada)
               quilombo_clean #34 (declaracao comentada)
               # amc_clean #35 (declaracao comentada)
             )),
  
  # # Validate all output parquets
  # # Pula metro_area_depara: tabela tabular (lookup SIDRA/BET → cod_recmetropol)
  # # sem coluna geometry. validate_geobr assume sf object e quebra com
  # # "no simple features geometry column present".
  # tar_target(name = validation,
  #            command = {
  #              files <- unlist(all_files)
  #              full_res <- files[!grepl("_simplified|metro_area_depara", files)]
  #              lapply(full_res, validate_geobr)
  #              "OK"
  #            }),

  tar_target(name = versao_dados,
             command = "v2.0.0"
  ) #add comma here

  # Upload to GitHub Releases (requires GITHUB_TOKEN in ~/.Renviron)
  # Sys.setenv(GITHUB_TOKEN = "ghp_...") or add to ~/.Renviron
  # tar_target(name = upload,
  #            command = upload_arquivos(files = all_files, versao_dados)
  # )
)

# check errors
# targets::tar_meta(fields = error, complete_only = TRUE) |> View()