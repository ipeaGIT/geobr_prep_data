# Toy test: hardening defensivo - schema mismatch deve disparar stopifnot/stop
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))

source("R/metro_area.R")

# Test 1: 2008-2009 com menos de 4 colunas
raw_bad_8 <- data.frame(a = 1, b = 2, c = 3)
err1 <- tryCatch(normalize_metro_2008_2009(raw_bad_8, 2008),
                 error = function(e) e$message)
stopifnot(grepl("Schema mismatch", err1))

# Test 2: 2001-2003 sem coluna NOME RM
raw_bad_01 <- data.frame(`CÓDIGO_DO_MUNICÍPIO` = 1, check.names = FALSE)
err2 <- tryCatch(normalize_metro_2001_2003(raw_bad_01, 2001),
                 error = function(e) e$message)
stopifnot(grepl("Schema mismatch|NOME RM", err2))

# Test 3: 2010-2015 sem coluna Codigo Municipio
raw_bad_13 <- data.frame(`Região Metropolitana` = "X", check.names = FALSE)
err3 <- tryCatch(normalize_metro_2010_2015(raw_bad_13, 2014),
                 error = function(e) e$message)
stopifnot(grepl("Schema mismatch", err3))

# Test 4: 2016-2018 sem NOME_RM
raw_bad_17 <- data.frame(COD_UF = 1, COD_MUN = 1)
err4 <- tryCatch(normalize_metro_2016_2018(raw_bad_17),
                 error = function(e) e$message)
stopifnot(grepl("Schema mismatch", err4))

# Test 5: 2019-2020 sem NOME
raw_bad_19 <- data.frame(COD_UF = 1, COD_MUN = 1)
err5 <- tryCatch(normalize_metro_2019_2020(raw_bad_19, 2019),
                 error = function(e) e$message)
stopifnot(grepl("Schema mismatch", err5))

# Test 6: 2021-2024 sem NOME_RECMETROPOL
raw_bad_21 <- data.frame(COD_MUN = 1, LEG = "X", DATA = "X")
err6 <- tryCatch(normalize_metro_2021_2024(raw_bad_21, 2021),
                 error = function(e) e$message)
stopifnot(grepl("Schema mismatch", err6))

cat("PASS: toy_clean_metro_area_validators\n")
