library(targets)

  # Set target options: ----
tar_option_set(
  format = "rds", # default storage format "rds" "feather"
  memory = "transient",
  garbage_collection = TRUE,
  
  # Packages ----
  packages = c('data.table',
               'dplyr',
               'furrr',
               'future',
               'geobr',
               'httr',
               'lwgeom',
               'magrittr',
               # 'maptools', # removed from cran
               'openxlsx',
               'pbapply',
               'RCurl',
               'readr',
               'readxl',
               # 'rgeos',    # removed from cran
               'rvest',
               'sfheaders',
               'sf',
               'sp',
               'stringi',
               'stringr',
               'utils'
               )
  )
# invisible(lapply(packages, library, character.only = TRUE))

# # tar_make_clustermq() configuration (okay to leave alone):
# options(clustermq.scheduler = "multisession")

# tar_make_future() configuration (okay to leave alone):
# Install packages {{future}}, {{future.callr}}, and {{future.batchtools}} to allow use_targets() to configure tar_make_future() options.

# Run the R scripts in the R/ folder with your custom functions:
# tar_source()
targets::tar_source('./R')


############# The Targets List #########

list(
  #1. Semiárido ----------------------------------------------------------

    # year input
    tar_target(name = years_semiarid,
               command = c(2005, 2017, 2021, 2022)),

    # download
    tar_target(name = semiarid_raw,
               command = download_semiarid(years_semiarid),
               pattern = map(years_semiarid)),

    # clean (aprox 14870.86 sec)
    tar_target(name = semiarid_clean,
               command = clean_semiarid(semiarid_raw, years_semiarid),
               pattern = map(semiarid_raw, years_semiarid),
               format = 'file'),
  

  
  #2. Amazônia Legal ----------------------------------------------------------
  
 
  # download
  tar_target(name = amazonialegal_raw,
             command = download_amazonialegal()),

  # clean (aprox 14870.86 sec)
  tar_target(name = amazonialegal_clean,
             command = clean_amazonialegal(amazonialegal_raw),
             format = 'file')
  )
  
  

  
  
  
  
# # 3. Municipios ----------------------------------------------------------
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

# # 4. Estados ----------------------------------------------------------
#
# # year input
#   tar_target(years_states, c(2000, 2001, 2010, 2013, 2014,  2015,
#                            2016, 2017, 2018, 2019, 2020, 2021, 2022)),
#   # download
#   tar_target(name = download_states,
#              command = download_states(years_states),
#              pattern = map(years_states)),
#

# # 5. Pais ----------------------------------------------------------
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
#
#)



# Metadata ----------------------------------------------------------

