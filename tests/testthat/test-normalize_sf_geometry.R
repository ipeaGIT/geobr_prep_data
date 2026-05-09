test_that("normalize_sf_geometry renames geom to geometry", {
  poly <- sf::st_sfc(sf::st_polygon(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  )), crs = 4674)
  df <- sf::st_sf(code_muni = 1, geom = poly)

  expect_equal(attr(df, "sf_column"), "geom")
  result <- normalize_sf_geometry(df)
  expect_equal(attr(result, "sf_column"), "geometry")
  expect_true("geometry" %in% names(result))
})

test_that("normalize_sf_geometry is no-op when already named geometry", {
  poly <- sf::st_sfc(sf::st_polygon(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  )), crs = 4674)
  df <- sf::st_sf(code_muni = 1, geometry = poly)

  result <- normalize_sf_geometry(df)
  expect_equal(names(result), names(df))
})
