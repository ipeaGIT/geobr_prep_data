test_that("add_state_info derives state from code_state", {
  poly <- sf::st_sfc(sf::st_polygon(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  )), crs = 4674)
  df <- sf::st_sf(code_muni = c(3550308, 2304400),
                   code_state = c(35, 23),
                   geometry = c(poly, poly))

  result <- add_state_info(df, "code_state")

  expect_equal(result$abbrev_state, c("SP", "CE"))
  expect_true("name_state" %in% names(result))
  expect_true(all(nchar(result$abbrev_state) == 2))
})

test_that("add_state_info handles all 27 states", {
  codes <- c(11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,
             31,32,33,35,41,42,43,50,51,52,53)
  poly <- sf::st_sfc(sf::st_polygon(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  )), crs = 4674)

  df <- sf::st_sf(code_state = codes,
                   geometry = rep(poly, length(codes)))

  result <- add_state_info(df, "code_state")

  expect_equal(nrow(result), 27)
  expect_true(all(!is.na(result$abbrev_state)))
  expect_true(all(!is.na(result$name_state)))
  expect_true(all(nchar(result$abbrev_state) == 2))
})
