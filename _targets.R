library(targets)
library(tarchetypes)
library(crew)


# Set target options: ----------------------------------------------------------
tar_option_set(
  format = "rds",
  memory = "transient",
  garbage_collection = TRUE,
  controller = crew_controller_local(workers = 8),
  
  
  # Packages -------------------------------------------------------------------
  packages = c('arrow',
               'collapse',
               'crew',
               'data.table',
               'devtools',
               'dplyr',
               'furrr',
               'future',
               'geobr',
               'geos',
               'geoarrow',
               'httr',
               'igraph',
               'janitor',
               'knitr',
               'lubridate',
               'lwgeom',
               'magrittr',
               # 'maptools', # removed from cran
               'mirai',
               'nanonext',
               'openxlsx',
               'parallel',
               'pbapply',
               'piggyback',
               'purrr',
               'RCurl',
               'readr',
               'readxl',
               #'rgeos',    # removed from cran
               #'rJava',
               #'rland', # not available for 4.5.1 R version
               #'rlang',
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
               'utils',
               'visNetwork'
               #'xlsx'
  )
)
# invisible(lapply(packages, library, character.only = TRUE))

# tar_make_clustermq() configuration (okay to leave alone):
# options(clustermq.scheduler = "multisession")

# tar_make_future() configuration (okay to leave alone):
# Install packages {{future}}, {{future.callr}}, and {{future.batchtools}} to allow use_targets() to configure tar_make_future() options.

# Run the R scripts in the R/ folder with your custom functions:
# tar_source()
targets::tar_source('./R')

############# The Targets List #########----------------------------------------

# In case of error, run:
# test_errors <- targets::tar_meta(fields = warnings, complete_only = TRUE)

list(
  #01. Semiárido ---------------------------------------------------------------
  
  # year input
  tar_target(name = years_semiarid,
             command = c(2005, 2017, 2021, 2022)),
  
  # download
  tar_target(name = semiarid_raw,
             command = download_semiarid(years_semiarid),
             pattern = map(years_semiarid)),
  
  # clean
  tar_target(name = semiarid_clean,
             command = clean_semiarid(semiarid_raw, years_semiarid),
             pattern = map(semiarid_raw, years_semiarid),
             format = 'file'),
  
  #02. Amazonia Legal ----------------------------------------------------------
  
  # year input
  tar_target(name = years_amazon,
             command = c(
               #2014,
               2019, 2020,
               2021, 2022, 
               2024)),
  
  # download
  tar_target(name = amazonialegal_raw,
             command = download_amazonialegal(years_amazon),
             pattern = map(years_amazon)),
  
  # clean
  tar_target(name = amazonialegal_clean,
             command = clean_amazonialegal(amazonialegal_raw, years_amazon),
             pattern = map(amazonialegal_raw, years_amazon),
             format = 'file'),
  
  #03. Biomas ------------------------------------------------------------------
  
  # year input
  tar_target(name = years_biomes,
             command = c(2004, 2019)),
  
  # download
  tar_target(name = biomes_raw,
             command = download_biomes(years_biomes),
             pattern = map(years_biomes)),
  
  # clean
  tar_target(name = biomes_clean,
             command = clean_biomes(biomes_raw, years_biomes),
             pattern = map(biomes_raw, years_biomes),
             format = 'file'),
  
  #04. Grade estatística -------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  # 
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #05. Estabelecimentos de saúde -----------------------------------------------
  
  #year input
  tar_target(name = years_healthfacilities,
             command = c(format(Sys.Date(), "%Y_%m"))),

  # download
  tar_target(name = healthfacilities_raw,
             command = download_healthfacilities(years_healthfacilities),
             pattern = map(years_healthfacilities)),

  # clean
  tar_target(name = healthfacilities_clean,
             command = clean_healthfacilities(healthfacilities_raw,years_healthfacilities),
             pattern = map(healthfacilities_raw, years_healthfacilities),
             format = 'file'),
  
  #06. Terras Indígenas --------------------------------------------------------
  
  # # year imput
  # tar_target(name = years_indigenousland,
  #            command = c(2024, 2025)),
  # 
  # # download
  # tar_target(name = indigenousland_raw,
  #            command = download_indigenousland(years_indigenousland),
  #            pattern = map(years_indigenousland)),
  # 
  # # clean
  # tar_target(name = indigenousland_clean,
  #            command = clean_indigenousland(indigenousland_raw, years_indigenousland),
  #            pattern = map(indigenousland_raw, years_indigenousland),
  #            format = 'file'),
  
  #07. Regioes Intermediarias --------------------------------------------------
  
  # year input
  tar_target(name = years_intermediateregions,
             command = c(2019:2024)),

  # download
  tar_target(name = intermediateregions_raw,
             command = download_intermediateregions(years_intermediateregions),
             pattern = map(years_intermediateregions)),

  # clean
  tar_target(name = intermediateregions_clean,
             command = clean_intermediateregions(intermediateregions_raw,
                                                 years_intermediateregions),
             pattern = map(intermediateregions_raw, years_intermediateregions),
             format = 'file'),
  
  #08. Regiões Imediatas -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_immediateregions,
  #            command = c(2019:2024)),
  # 
  # # download
  # tar_target(name = immediateregions_raw,
  #            command = download_immediateregions(years_immediateregions),
  #            pattern = map(years_immediateregions)),
  # 
  # # clean
  # tar_target(name = immediateregions_clean,
  #            command = clean_immediateregions(immediateregions_raw, years_immediateregions),
  #            pattern = map(immediateregions_raw, years_immediateregions),
  #            format = 'file'),
  
  #09. Escolas ----
  
  # # year input
  # tar_target(name = years_schools,
  #            command = c(Sys.Date())),
  # 
  # # download
  # tar_target(name = schools_raw,
  #            command = download_schools(years_schools),
  #            pattern = map(years_schools)),
  # 
  # # clean
  # tar_target(name = schools_clean,
  #            command = clean_schools(schools_raw, years_schools),
  #            pattern = map(schools_raw, years_schools),
  #            format = 'file'),
  
  #10. Estados -----------------------------------------------------------------
  # 
  #   #year input
  #   tar_target(name = years_states,
  #              command = c(2000, 2001, 2010,
  #                          2013:2024)),
  # 
  #   # download
  #   tar_target(name = states_raw,
  #              command = download_states(years_states),
  #              pattern = map(years_states)),
  # 
  #   # clean
  #   tar_target(name = states_clean,
  #              command = clean_states(states_raw, years_states),
  #              pattern = map(states_raw, years_states),
  #              format = 'file'),
  
  #11. Regiões -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_regions,
  #            command = c(2000, 2001, 2010,
  #                        2013:2024)),
  # 
  # # download
  # tar_target(name = regions_raw,
  #            command = download_regions(years_regions),
  #            pattern = map(years_regions)),
  # 
  # # clean
  # tar_target(name = regions_clean,
  #            command = clean_regions(regions_raw, years_regions),
  #            pattern = map(regions_raw, years_regions),
  #            format = 'file'),
  
  #12. País -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_country,
  #            command = c(2000, 2001, 2010,
  #                        2013:2024)),
  # 
  # # download
  # tar_target(name = country_raw,
  #            command = download_country(years_country),
  #            pattern = map(years_country)),
  # 
  # # clean
  # tar_target(name = country_clean,
  #            command = clean_country(country_raw, years_country),
  #            pattern = map(country_raw, years_country),
  #            format = 'file'),
  
  #13. Meso Regiões -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_mesoregions,
  #            command = c(2000, 2001, #error in number of collumns
  #                        #2005, 2007, # No mesoregions file
  #                        2010, 2013, 2014, # mesorregioes sem BR folder
  #                        2015:2018)),
  # 
  # # download
  # tar_target(name = mesoregions_raw,
  #            command = download_mesoregions(years_mesoregions),
  #            pattern = map(years_mesoregions)),
  # 
  # # clean
  # tar_target(name = mesoregions_clean,
  #            command = clean_mesoregions(mesoregions_raw, years_mesoregions),
  #            pattern = map(mesoregions_raw, years_mesoregions),
  #            format = 'file'),
  
  #14. Microrregiões -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_microregions,
  #            command = c(2000, 2001, #error in number of collumns
  #                        #2005, 2007, # No microregions file
  #                        2010, 2013, 2014, # microrregioes sem BR folder
  #                        2015:2018)),
  # 
  # # download
  # tar_target(name = microregions_raw,
  #            command = download_microregions(years_microregions),
  #            pattern = map(years_microregions)),
  # 
  # # clean
  # tar_target(name = microregions_clean,
  #            command = clean_microregions(microregions_raw, years_microregions),
  #            pattern = map(microregions_raw, years_microregions),
  #            format = 'file'),
  
  #15. Municipalidade -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #16. Assento municipal -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #17. Traço do Censo -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #18. Área de peso -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #19. Áreas metropolitanas -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  #
  #20. Áreas urbanas -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  #
  #21. Unidades de conservação -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #22. Áreas de risco de desastre -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #23. Região de Saúde -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #24. Vizinhança -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  #
  #25. Concentrações urbanas -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_statsgrid,
  #            command = c(2010, 2022)),
  #
  # # download
  # tar_target(name = statsgrid_raw,
  #            command = download_statsgrid(years_statsgrid),
  #            pattern = map(years_statsgrid)),
  #
  # # clean
  # tar_target(name = statsgrid_clean,
  #            command = clean_statsgrid(statsgrid_raw, years_statsgrid),
  #            pattern = map(statsgrid_raw, years_statsgrid),
  #            format = 'file'),
  
  #26. Arranjos populacionais -----------------------------------------------------------------
  
  # # year input
  # tar_target(name = years_poparrangements,
  #            command = c(2010, 2022)),
  
  # # download
  # tar_target(name = poparrangements_raw,
  #            command = download_poparrangements(years_poparrangements),
  #            pattern = map(years_poparrangements)),
  #
  # # clean
  # tar_target(name = poparrangements_clean,
  #            command = clean_poparrangements(poparrangements_raw, years_poparrangements),
  #            pattern = map(poparrangements_raw, years_poparrangements),
  #            format = 'file'),
  
  #27. Favelas e aglomerados urbanos ----
  
  # year input
  # tar_target(name = years_favela,
  #            command = c(2010, 2022)
  #            ),
  
  # # download
  # tar_target(name = poparrangements_raw,
  #            command = download_poparrangements(years_poparrangements),
  #            pattern = map(years_poparrangements)),
  #
  # # clean
  # tar_target(name = poparrangements_clean,
  #            command = clean_poparrangements(poparrangements_raw, years_poparrangements),
  #            pattern = map(poparrangements_raw, years_poparrangements),
  #            format = 'file')
  
  #END. Upload files -------------------------------------------------------
  
  # all files input
  tar_target(name = all_files,
             command = c(
               semiarid_clean,
               amazonialegal_clean,
               biomes_clean,
               #statsgrid_clean,
               healthfacilities_clean,
               intermediateregions_clean 
               # immediateregions_clean,
               # schools_clean, states_clean, 
               # regions_clean,
               # country_clean,
               # mesoregions_clean, 
               # microregions_clean
             )),
  
  tar_target(name = versao_dados,
             command = "v2.0.0"
  ) #add comma here
  
  # tar_target(name = upload,
  #            command = upload_arquivos(files = all_files, versao_dados)
  # )
)

##################### UNTIL HERE UPDATED ---------------------------------------
#
# # 3. Municipios --------------------------------------------------------------
# 
#   # year input
#   tar_target(years_muni, c(2000, 2001, 2005, 2007, 2010,
#                            2013, 2014,  2015, 2016, 2017,
#                            2018, 2019, 2020, 2021, 2022,
#                            2023, 2024)),
#   # download
#   tar_target(name = download_municipios,
#              command = download_muni(years_muni),
#              pattern = map(years_muni)),
# 
#   # clean (aprox 14870.86 sec)
#   tar_target(name = clean_municipios,
#              command = clean_muni(download_municipios)
#              , pattern = map(download_municipios)
#              )
#
# # 4. Estados -----------------------------------------------------------------------------------------------------------------------
#
# # year input
#   tar_target(years_states, c(2000, 2001, 2010, 2013, 2014,  2015,
#                            2016, 2017, 2018, 2019, 2020, 2021, 2022)),
#   # download
#   tar_target(name = download_states,
#              command = download_states(years_states),
#              pattern = map(years_states)),
#
#
# # 5. Pais -----------------------------------------------------------------
#
# # year input
# tar_target(years_country, c(1872, 1900, 1911, 1920, 1933, 1940, 1950, 1960, 1970,
#                             1980, 1991, 2000, 2001, 2010, 2013, 2014, 2015, 2016,
#                             2017, 2018, 2019, 2020)),
#
# # download
# tar_target(name = get_country,
#            command = get_country(years_country),
#            pattern = map(years_country))
#)
#