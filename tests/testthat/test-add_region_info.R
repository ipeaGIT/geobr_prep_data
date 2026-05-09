test_that("add_region_info derives region from code_state", {
  poly <- sf::st_sfc(sf::st_polygon(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  )), crs = 4674)
  df <- sf::st_sf(code_state = c(35, 23, 43, 13, 53),
                   geometry = rep(poly, 5))

  result <- add_region_info(df, "code_state")

  expect_equal(result$code_region, c(3, 2, 4, 1, 5))
  expect_equal(result$name_region, c("Sudeste", "Nordeste", "Sul",
                                      "Norte", "Centro-Oeste"))
})
