library(targets)
library(tictoc)

tictoc::tic()
targets::tar_make()
tictoc::toc()

#1  todos cores
#7795.01 sec

#2 todos cores
# 2762.06 sec

#3 2 core

# clean_schools(download_schools_microdados(2025))

# lapply(
#   X = c(2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, # censo escolar
#         2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
#         2023, 2024, 2025),
#   
#   FUN = function(x){
#     message(x)
#     clean_schools(download_schools_microdados(x))
#     }
#   )


# lapply(
#   X = c(2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, # censo escolar
#         2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
#         2023, 2024, 2025),
#   
#   FUN = function(x){
#     message(x)
#     clean_schools(download_schools_microdados(x))
#     }
#   )

# pc casa
# 41583.78  
# 2153.09