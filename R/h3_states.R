# # Function to generate H3 hexagonal grid cells covering Brazilian states
# # Requires: geobr, h3o, sf, dplyr
# 
# get_states_h3 <- function(resolution) {
#   # resolution: integer, H3 resolution level (0 = coarsest, 15 = finest)
#   # Typical useful range for states: 3 (very coarse) to 7 (fine)
# 
#   stopifnot(is.numeric(resolution), resolution == as.integer(resolution),
#             resolution >= 0, resolution <= 15)
# 
#   resolution <- as.integer(resolution)
# 
#   ## 1. Download state geometries -----------------------------------------------
# 
#   states <- geobr::read_state(year = 2022, showProgress = FALSE)
# 
#   # H3 requires WGS 84 (EPSG:4326)
#   states <- sf::st_transform(states, crs = 4326)
# 
#   ## 2. Find H3 cells covering each state ---------------------------------------
# 
#   cells_list <- lapply(seq_len(nrow(states)), function(i) {
# 
#     state_geom <- states$geom[i]
#     h3_cells   <- h3o::polygon_to_cells(state_geom, res = resolution)
# 
#     data.frame(
#       code_state   = states$code_state[i],
#       abbrev_state = states$abbrev_state[i],
#       name_state   = states$name_state[i],
#       h3_address   = h3_cells,
#       stringsAsFactors = FALSE
#     )
#   })
# 
#   cells_df <- do.call(rbind, cells_list)
# 
#   ## 3. Add H3 cell geometry and return as sf -----------------------------------
# 
#   cells_sf <- cells_df |>
#     dplyr::mutate(geometry = h3o::cell_to_polygon(h3_address)) |>
#     sf::st_as_sf(crs = 4326)
# 
#   return(cells_sf)
# }
