library(targets)
setwd("d:/Dropbox/Artigos/geobr_prep_data")
tar_make(names = c(censustract_2000_rural_clean,
                   censustract_2000_urbano_clean,
                   censustract_2000_unified_clean))
