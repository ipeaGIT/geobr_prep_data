library(targets)
library(tictoc)

tictoc::tic()
targets::tar_make()
tictoc::toc()