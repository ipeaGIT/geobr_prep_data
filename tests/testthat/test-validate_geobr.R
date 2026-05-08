test_that("validate_geobr passes for valid sf", {
  poly <- sf::st_sfc(sf::st_multipolygon(list(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  ))), crs = 4674)
  df <- sf::st_sf(code_muni = 3550308,
                   name_muni = "Sao Paulo",
                   code_state = 35,
                   abbrev_state = "SP",
                   year = 2020,
                   geometry = poly)

  # Write temp parquet and validate
  tmp <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, tmp)
  expect_true(validate_geobr(tmp))
  unlink(tmp)
})

test_that("validate_geobr fails for wrong CRS", {
  poly <- sf::st_sfc(sf::st_multipolygon(list(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  ))), crs = 4326)
  df <- sf::st_sf(code_muni = 1, geometry = poly)

  tmp <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, tmp)
  expect_error(validate_geobr(tmp), "CRS")
  unlink(tmp)
})

test_that("validate_geobr fails for non-numeric code_*", {
  poly <- sf::st_sfc(sf::st_multipolygon(list(list(
    matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol = 2, byrow = TRUE)
  ))), crs = 4674)
  df <- sf::st_sf(code_muni = "3550308", geometry = poly)

  tmp <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, tmp)
  expect_error(validate_geobr(tmp), "not numeric")
  unlink(tmp)
})
