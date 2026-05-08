test_that("snake_case_names applies Title Case", {
  df <- data.frame(name_muni = c("SAO PAULO", "rio de janeiro"),
                   stringsAsFactors = FALSE)
  result <- snake_case_names(df, "name_muni")
  expect_equal(result$name_muni[1], "Sao Paulo")
  expect_equal(result$name_muni[2], "Rio de Janeiro")
})

test_that("snake_case_names lowercases prepositions", {
  df <- data.frame(name_muni = c("MATO GROSSO DO SUL",
                                  "RIO GRANDE DOS SANTOS",
                                  "SANTA MARIA DA VITORIA"),
                   stringsAsFactors = FALSE)
  result <- snake_case_names(df, "name_muni")
  expect_true(grepl(" do ", result$name_muni[1]))
  expect_true(grepl(" dos ", result$name_muni[2]))
  expect_true(grepl(" da ", result$name_muni[3]))
})

test_that("snake_case_names skips missing columns", {
  df <- data.frame(other_col = "test", stringsAsFactors = FALSE)
  result <- snake_case_names(df, "name_muni")
  expect_equal(names(result), "other_col")
})
