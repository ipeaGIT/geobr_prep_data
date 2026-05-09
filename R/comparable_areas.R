#> DATASET: Comparable Areas / Áreas Mínimas Comparáveis (AMC)
#> Source: Algoritmo de crosswalk usando dados de município (IBGE)
#> Metadata:
# Titulo: Areas Minimas Comparaveis (AMC)
# Titulo alternativo: Comparable Areas
# Frequencia de atualizacao: Censal
# Forma de apresentacao: Polygons (dissolved municipalities)
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Areas minimas comparaveis para analise temporal de municipios.
# Agrega municipios que sofreram divisoes/fusoes entre dois anos censitarios
# em areas espacialmente comparaveis ao longo do tempo.
#
# Informacoes adicionais: Algoritmo de crosswalk que rastreia mudancas
# territoriais municipais de 1872 a 2020 usando dados historicos do IBGE.
# Codigo original em ainda_sem_targets/amc_algorithm/_Crosswalk_main.R
#
# Estado: Ativo
# Informacao do Sistema de Referencia: SIRGAS 2000
#
# Anos disponiveis: pares de anos censitarios
#   (1872, 1900, 1911, 1920, 1933, 1940, 1950, 1960, 1970, 1980, 1991, 2000, 2010, 2020)
#   Total: 91 combinacoes (start_year < end_year)

### Libraries (use any library as necessary) -----------------------------------

# library(sf)
# library(dplyr)
# library(data.table)
# library(arrow)
# library(tidyr)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Path to AMC algorithm files
AMC_ALGORITHM_DIR <- "./ainda_sem_targets/amc_algorithm"

# Census years for AMC
AMC_YEARS <- c(1872, 1900, 1911, 1920, 1933, 1940, 1950, 1960,
               1970, 1980, 1991, 2000, 2010, 2020)


#' Generate all AMC year pair combinations
#' @return data.frame with start_year and end_year columns
amc_year_combinations <- function() {
  combos <- expand.grid(start_year = AMC_YEARS, end_year = AMC_YEARS)
  combos <- combos[combos$start_year < combos$end_year, ]
  combos <- combos[order(combos$start_year, combos$end_year), ]
  rownames(combos) <- NULL
  combos
}


# ==============================================================================
# Support function: matching()
# Cluster matching algorithm for AMC crosswalk.
# Logic preserved EXACTLY from the legacy _Crosswalk_main.R (lines 33-167).
# ==============================================================================

matching <- function(data_mun = NULL, y0) {

  temp <- data_mun %>%
    select(c(paste0("clu", y0), clu_new)) %>%
    filter(!is.na(clu_new)) %>%
    arrange(get(paste0("clu", y0)), clu_new) %>%
    filter(!(get(paste0("clu", y0)) == clu_new))

  if (nrow(temp) > 1) {

    temp$diff <- 0

    for (i in 2:nrow(temp)) {
      if (is.na(temp[, c(paste0("clu", y0))][i - 1]) |
          is.na(temp[, c("clu_new")][i - 1])) next
      else if (temp[, c(paste0("clu", y0))][i] == temp[, c(paste0("clu", y0))][i - 1]
               & temp[, c("clu_new")][i] == temp[, c("clu_new")][i - 1])

        temp$diff[i] <- 1
    }

    temp <- temp %>%
      filter(diff != 1)

    temp <- temp %>% select(-diff)

  }

  temp <- temp %>% mutate(!!paste0("clu", quo_name(y0)) :=
    ifelse(is.na(get(paste0("clu", y0))), -999999999, get(paste0("clu", y0))))

  rep_c <- 0

  repeat {

    rep_c <- (rep_c + 1)

    while (sum(temp[, c(paste0("clu", y0))] == lag(temp[, c(paste0("clu", y0))], 1), na.rm = T) != 0) {

      for (i in 2:nrow(temp)) if (temp[, c(paste0("clu", y0))][i] == temp[, c(paste0("clu", y0))][i - 1]) temp[, c(paste0("clu", y0))][i] <- temp$clu_new[i]

      for (i in 2:nrow(temp)) if (temp$clu_new[i] == temp[, c(paste0("clu", y0))][i]) temp$clu_new[i] <- temp$clu_new[i - 1]

      temp <- temp[order(temp[, 1], temp[, 2]), ]

      temp <- temp %>% filter(!(get(paste0("clu", y0)) == clu_new))

      temp$diff <- 0

      for (i in 2:nrow(temp)) {
        if (is.na(temp[, c(paste0("clu", y0))][i - 1]) |
            is.na(temp[, c("clu_new")][i - 1])) next
        else if (temp[, c(paste0("clu", y0))][i] == temp[, c(paste0("clu", y0))][i - 1]
                 & temp[, c("clu_new")][i] == temp[, c("clu_new")][i - 1])

          temp$diff[i] <- 1
      }

      temp <- temp %>%
        filter(diff != 1)

      temp <- temp %>% select(-diff)

    }

    temp2 <- temp

    temp2 <- temp2 %>% rename(help = clu_new, clu_new = paste0("clu", y0))

    temp2 <- bind_rows(temp, temp2) %>% mutate_all(function(x) ifelse(is.na(x), -999999999, x))

    temp2 <- temp2[order(temp2[, 2], -xtfrm(temp2[, 3])), ]

    if (sum(temp2$clu_new == lead(temp2$clu_new, 1) & temp2$help != -999999999, na.rm = T) != 0) {

      temp3 <- temp

      temp3 <- temp3 %>% rename(clu_new2 = clu_new, clu_new = paste0("clu", y0))

      temp3 <- left_join(temp, temp3)

      temp3 <- temp3 %>% mutate(clu_new2 = ifelse(is.na(clu_new2), clu_new, clu_new2))

      temp3 <- temp3 %>% filter(get(paste0("clu", y0)) != -999999999)

      temp3 <- temp3 %>% filter(!is.na(get(paste0("clu", y0))))

      temp3 <- temp3 %>% select(-clu_new)

      temp3 <- temp3 %>% rename(clu_new = clu_new2)

      temp3 <- temp3[order(temp3[, 1], temp3[, 2]), ]

      temp3 <- temp3 %>% filter(!(get(paste0("clu", y0)) == clu_new))

      temp3$diff <- 0

      for (i in 2:nrow(temp3)) {
        if (is.na(temp3[, c(paste0("clu", y0))][i - 1]) |
            is.na(temp3[, c("clu_new")][i - 1])) next
        else if (temp3[, c(paste0("clu", y0))][i] == temp3[, c(paste0("clu", y0))][i - 1]
                 & temp3[, c("clu_new")][i] == temp3[, c("clu_new")][i - 1])

          temp3$diff[i] <- 1
      }

      temp3 <- temp3 %>%
        filter(diff != 1)

      temp <- temp3 %>% select(-diff)

      rm(temp3)

    }

    if (rep_c == 3) {
      break
    }

  }

  temp <- as.data.table(temp) %>%
    mutate(!!paste0("clu", quo_name(y0)) := as.numeric(as.character(get(paste0("clu", y0)))))

  data_mun <- data_mun %>% select(-clu_new) %>% left_join(temp) %>%
    mutate(!!paste0("clu", quo_name(y0)) := ifelse(!(is.na(clu_new)), clu_new, get(paste0("clu", y0)))) %>%
    select(-clu_new)

  rm(temp, temp2)

  return(data_mun)
}


# ==============================================================================
# compute_amc(): Core AMC crosswalk algorithm
# Extracted from legacy table_amc() in _Crosswalk_main.R (lines 171-928).
# Returns the crosswalk data.frame (code_muni, name_muni, code_amc).
# Does NOT read spatial data or write files.
# ==============================================================================

compute_amc <- function(startyear = NULL, endyear = NULL) {

  # Validate inputs: must be census years
  valid_years <- c(1872, 1900, 1911, 1920, 1933, 1940,
                   1950, 1960, 1970, 1980, 1991, 2000, 2010, 2020)

  if (!(startyear %in% valid_years) || !(endyear %in% valid_years)) {
    stop("Error: Invalid Value to argument. Must be census years.")
  }
  if (startyear >= endyear) {
    stop("Error: startyear must be less than endyear.")
  }

  message(paste0("Computing AMC crosswalk for ", startyear, " to ", endyear, "\n"))

  y0 <- startyear

  # the loop stops when startyear == endyear
  while (y0 != endyear) {

    # First year reads from RDS; subsequent years use previous iteration's result
    if (y0 == startyear) {

      # Load input table from correct path
      crosswalk_path <- file.path(AMC_ALGORITHM_DIR, "_Crosswalk_pre.rds")
      if (!file.exists(crosswalk_path)) {
        stop("Arquivo _Crosswalk_pre.rds nao encontrado em ", AMC_ALGORITHM_DIR)
      }
      data_mun <- readr::read_rds(crosswalk_path)

    } else {

      data_mun <- get(paste0("_Crosswalk_", y_1))

    }

    # Define the following Census year
    if (y0 == 1872) {
      y1 <- 1900
    } else if (y0 == 1900) {
      y1 <- 1911
    } else if (y0 == 1911) {
      y1 <- 1920
    } else if (y0 == 1920) {
      y1 <- 1933
    } else if (y0 == 1933) {
      y1 <- 1940
    } else if (y0 == 1940) {
      y1 <- 1950
    } else if (y0 == 1950) {
      y1 <- 1960
    } else if (y0 == 1960) {
      y1 <- 1970
    } else if (y0 == 1970) {
      y1 <- 1980
    } else if (y0 == 1980) {
      y1 <- 1991
    } else if (y0 == 1991) {
      y1 <- 2000
    } else if (y0 == 2000) {
      y1 <- 2010
    } else if (y0 == 2010) {
      y1 <- 2020
    } else if (y0 == 2020) {
      y1 <- 2030
    } else {
      y1 <- 2040
    }

    # prepare inputs
    ano_dest <- paste0("n_dest", y0)
    ano_dest1 <- paste0("dest1", y0)
    exist_dummy1 <- paste0("exist_d", y1)
    exist_dummy0 <- paste0("exist_d", y0)

    cluster0 <- paste0("clu", y0)
    cluster1 <- paste0("clu", quo_name(y0))
    cluster_original <- paste0("clu", quo_name(y0), "_orig")

    # Convert character to numeric
    data_mun <- data_mun %>%
      mutate(ch_match = as.numeric(get(ano_dest)))

    data_mun[, c(exist_dummy1)] <- as.numeric(data_mun[, c(exist_dummy1)])
    data_mun[, c(exist_dummy0)] <- as.numeric(data_mun[, c(exist_dummy0)])

    # generate the new cluster-var
    if (y0 == startyear) {

      data_mun <- data_mun %>%
        mutate(uf_amc = as.numeric(uf_amc)) %>%
        arrange(uf_amc, get(ano_dest1),
                desc(get(exist_dummy0)), final_name)

      # Selecting only municipalities that exist in y0
      a <- data_mun %>%
        select(c(uf_amc, ano_dest1)) %>%
        unique() %>%
        filter(get(ano_dest1) != "")

      a <- a %>% mutate(!!cluster1 := rownames(a),
                        !!cluster1 := as.numeric(get(cluster0)))

      data_mun <- left_join(data_mun, a)
      rm(a)

      # Generate the clu_y0_orig variable
      setDT(data_mun)[, paste0(cluster_original) := get(cluster0)]
      data_mun <- as.data.frame(data_mun)

    } else {

      data_mun[, c(cluster0)] <- NA

      data_mun <- data_mun %>%
        mutate(!!cluster1 := get(paste0("clu", y_1, "_final"))) %>%
        arrange(get(cluster0), desc(get(ano_dest1)), code2020)

      for (i in 2:(nrow(data_mun))) {
        if (data_mun[, c(ano_dest1)][i] != "" & is.na(data_mun[, c(cluster0)][i]))
          data_mun[, c(cluster0)][i] <- data_mun[, c(cluster0)][i - 1] + 1
      }

    }

    # Mun with destiny/origin outside their own UF_amc
    data_mun <- data_mun %>%
      mutate(ch_match = ifelse(code2020 == 2205706 & y0 == 1872, ch_match - 1,
             ifelse(code2020 == 4204202 & y0 == 1911, ch_match - 1,
             ifelse(code2020 == 4209003 & y0 == 1911, ch_match - 1,
             ifelse(code2020 == 4213609 & y0 == 1911, ch_match - 1,
             ifelse(code2020 == 4208104 & y0 == 1911, ch_match - 1,
             ifelse(code2020 == 4210100 & y0 == 1911, ch_match - 1,
             ifelse(code2020 == 1100205 & y0 == 1911, ch_match - 1, ch_match))))))))

    # Assign new cluster number to 1. destinies
    data_mun$clu_new <- NA

    # sort
    data_mun <- data_mun %>%
      arrange(uf_amc, get(ano_dest1), desc(get(exist_dummy0)), final_name)

    # Compare consecutive rows; if equal, assign same cluster
    for (i in 2:nrow(data_mun)) {
      if (data_mun[, c(ano_dest1)][i] == data_mun[, c(ano_dest1)][i - 1]
          & !is.na(data_mun[, c(cluster0)][i - 1])
          & data_mun[, c(ano_dest1)][i] != "")

        data_mun$clu_new[i] <- data_mun[, c(cluster0)][i - 1]
    }

    # Replace the clu-number of the new mun (same origin -> same cluster)
    for (i in 2:nrow(data_mun)) {
      if (data_mun[, c(ano_dest1)][i] == data_mun[, c(ano_dest1)][i - 1]
          & !is.na(data_mun[, c("clu_new")][i - 1]))

        data_mun$clu_new[i] <- data_mun$clu_new[i - 1]
    }

    # Subtract 1 from the number of missing matches
    data_mun <- data_mun %>%
      mutate(ch_match = ifelse(!is.na(clu_new), ch_match - 1, ch_match))

    # Generate consistent clusters
    data_mun <- matching(data_mun = data_mun, y0 = y0)

    for (p in 2:5) {

      # Do this only for the cases that have more than 2 destinies
      if (paste0("dest", p, y0) %in% colnames(data_mun)) {

        data_mun <- data_mun %>%
          mutate(!!paste0("mis", quo_name(y0)) := ifelse(get(ano_dest) >= p, get(paste0("dest", p, y0)), NA))

        data_mun <- data_mun %>%
          mutate(target = ifelse(!is.na(get(paste0("mis", y0))), 1, 0))

        data_mun$clu_new <- NA

        # try mun-name from next period
        data_mun <- data_mun %>%
          mutate(!!paste0("mis", quo_name(y0)) := ifelse(target == 0 & get(exist_dummy1) == 1,
                                                          get(paste0("dest1", y1)), get(paste0("mis", y0))))

        data_mun <- data_mun %>%
          arrange(uf_amc, get(paste0("mis", y0)), desc(target), final_name)

        for (i in 1:(nrow(data_mun) - 1)) {
          if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i + 1]
              & !is.na(data_mun[, c(cluster0)][i + 1])
              & data_mun[, c("target")][i] == 1)

            data_mun$clu_new[i] <- data_mun[, c(cluster0)][i + 1]
        }

        # overwrite entry of mis but do NOT OVERWRITE clu_new in case there has been a matching already
        data_mun <- data_mun %>%
          mutate(!!paste0("mis", quo_name(y0)) := ifelse(target == 0 & get(exist_dummy0) == 1,
                                                          get(ano_dest1), get(paste0("mis", y0))))

        data_mun <- data_mun %>%
          arrange(uf_amc, get(paste0("mis", y0)), desc(target), final_name)

        for (i in 1:(nrow(data_mun) - 1)) {
          if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i + 1]
              & !is.na(data_mun[, c(cluster0)][i + 1])
              & data_mun[, c("target")][i] == 1
              & is.na(data_mun[, c("clu_new")][i]))

            data_mun$clu_new[i] <- data_mun[, c(cluster0)][i + 1]
        }

        # overwrite entry of mis but do NOT OVERWRITE clu_new
        data_mun <- data_mun %>%
          mutate(!!paste0("mis", quo_name(y0)) := ifelse(target == 0, final_name, get(paste0("mis", y0))))

        data_mun <- data_mun %>%
          arrange(uf_amc, get(paste0("mis", y0)), desc(target), final_name)

        for (i in 1:(nrow(data_mun) - 1)) {
          if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i + 1]
              & !is.na(data_mun[, c(cluster0)][i + 1])
              & data_mun[, c("target")][i] == 1
              & is.na(data_mun[, c("clu_new")][i]))

            data_mun$clu_new[i] <- data_mun[, c(cluster0)][i + 1]
        }

        # adjust the ch_match for matches
        data_mun <- data_mun %>%
          mutate(ch_match = ifelse(!is.na(clu_new), ch_match - 1, ch_match)) %>%
          select(-c("target", paste0("mis", y0)))

        # apply matching between old and new cluster numbers
        data_mun <- matching(data_mun = data_mun, y0 = y0)

      }

    }


    # procedure for dest1, bc not all groups may be matched so far
    data_mun <- data_mun %>%
      mutate(!!paste0("mis", quo_name(y0)) := ifelse(ch_match > 0, get(ano_dest1), NA),
             target = ifelse(!is.na(get(paste0("mis", y0))), 1, 0))

    data_mun$clu_new <- NA

    # try possible matching partners from next period
    data_mun <- data_mun %>%
      mutate(!!paste0("mis", quo_name(y0)) := ifelse(target == 0 & get(exist_dummy1) == 1,
                                                      get(paste0("dest1", y1)), get(paste0("mis", y0))))

    data_mun <- data_mun %>%
      arrange(uf_amc, get(paste0("mis", y0)), desc(target), final_name)

    for (i in 1:(nrow(data_mun) - 1)) {
      if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i + 1]
          & !is.na(data_mun[, c(cluster0)][i + 1])
          & data_mun[, c("target")][i] == 1)

        data_mun$clu_new[i] <- data_mun[, c(cluster0)][i + 1]
    }

    # overwrite entry of mis but do NOT OVERWRITE clu_new
    data_mun <- data_mun %>%
      mutate(!!paste0("mis", quo_name(y0)) := ifelse(target == 0 & get(exist_dummy0) == 1,
                                                      get(ano_dest1), get(paste0("mis", y0))))

    data_mun <- data_mun %>%
      arrange(uf_amc, get(paste0("mis", y0)), desc(target), final_name)

    for (i in 1:(nrow(data_mun) - 1)) {
      if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i + 1]
          & !is.na(data_mun[, c(cluster0)][i + 1])
          & data_mun[, c("target")][i] == 1)

        data_mun$clu_new[i] <- data_mun[, c(cluster0)][i + 1]
    }

    data_mun <- data_mun %>%
      mutate(ch_match = ifelse(!is.na(data_mun$clu_new), ch_match - 1, ch_match)) %>%
      select(-c("target", paste0("mis", y0)))

    data_mun <- matching(data_mun = data_mun, y0 = y0)


    ## Crossref problem: May occur in rare occasions
    if (any(data_mun$ch_match != 0 | !is.na(data_mun$ch_match))) {

      data_mun$clu_new <- NA

      data_mun <- data_mun %>%
        mutate(!!paste0("mis", quo_name(y0)) := ifelse(ch_match > 0, get(ano_dest1), NA))

      data_mun <- data_mun %>%
        arrange(uf_amc, get(paste0("mis", y0)))

      for (i in 2:nrow(data_mun)) {
        if (!is.na(data_mun[, c(paste0("mis", y0))][i])
            & !is.na(data_mun[, c(paste0("mis", y0))][i - 1])
            & data_mun[, c("uf_amc")][i] == data_mun[, c("uf_amc")][i - 1])

          data_mun[, c(paste0("mis", y0))][i] <- data_mun[, c(paste0("dest1", y1))][i]
      }

      for (i in 2:nrow(data_mun)) {
        if (is.na(data_mun[, c(paste0("mis", y0))][i - 1])) next
        else if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i - 1]
                 & !is.na(data_mun[, c(cluster0)][i - 1])
                 & !is.na(data_mun[, c(paste0("mis", y0))][i])
                 & data_mun[, c("ch_match")][i] != 0)

          data_mun$clu_new[i] <- data_mun[, c(cluster0)][i - 1]
      }

      for (i in 2:nrow(data_mun)) {
        if (is.na(data_mun[, c(paste0("mis", y0))][i - 1])) next
        else if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i - 1]
                 & !is.na(data_mun[, c(cluster0)][i - 1])
                 & !is.na(data_mun[, c(paste0("mis", y0))][i])
                 & data_mun[, c("ch_match")][i] != 0)

          data_mun$ch_match[i] <- data_mun$ch_match[i - 1]
      }

      for (i in 1:(nrow(data_mun) - 1)) {
        if (is.na(data_mun[, c(paste0("mis", y0))][i + 1])) next
        else if (data_mun[, c(paste0("mis", y0))][i] == data_mun[, c(paste0("mis", y0))][i + 1]
                 & !is.na(data_mun[, c(cluster0)][i + 1])
                 & !is.na(data_mun[, c(paste0("mis", y0))][i])
                 & data_mun[, c("ch_match")][i] != 0)

          data_mun$ch_match[i] <- data_mun$ch_match[i - 1]
      }

      data_mun <- matching(data_mun = data_mun, y0 = y0)

    }

    ## Adding aggregation to the final cluster
    data_mun <- data_mun %>%
      mutate(!!paste0("clu", quo_name(y0), "_final") := dense_rank(get(cluster0)))

    # Removing year target column
    data_mun <- data_mun[, !(names(data_mun) %in% c(ano_dest1,
                                        paste0("dest2", y0),
                                        paste0("dest3", y0),
                                        paste0("dest4", y0),
                                        paste0("dest5", y0),
                                        exist_dummy0,
                                        ano_dest,
                                        "ch_match"))] %>%
      dplyr::arrange(uf_amc, get(cluster0), code2020)

    assign(paste0("_Crosswalk_", y0), data_mun)

    # Define the new years and begin next loop
    y_1 <- y0
    y0 <- y1

  }

  ## Final changes in the procedure
  # Use the last generated data set
  data_mun <- get(paste0("_Crosswalk_", y_1))

  # Drop unnecessary information and generate auxiliary cluster variable
  data_mun <- data_mun %>%
    select_if(colnames(data_mun) %in% c("uf_amc", "code2020", "final_name", "clu1872_final",
             "clu1900_final", "clu1911_final", "clu1920_final", "clu1933_final",
             "clu1940_final", "clu1950_final", "clu1960_final", "clu1970_final",
             "clu1980_final", "clu1991_final", "clu2000_final", "clu2010_final")) %>%
    mutate(!!paste0("clu", quo_name(y_1), "_final") := as.numeric(get(paste0("clu", y_1, "_final"))),
           !!paste0("clu", quo_name(y_1), "_final2") := get(paste0("clu", y_1, "_final")))

  # Last changes (semi-manual)
  # See "_Crosswalk_pre.r" - destiny/origin outside their own UF_amc

  data_mun <- data_mun %>%
    mutate(!!paste0("clu", y_1, "_final2") := get(paste0("clu", y_1, "_final")))

  if (startyear <= 1872) {

    n0 <- data_mun %>%
      filter(final_name == "Granja") %>%
      select(paste0("clu", y_1, "_final"))

    n1 <- data_mun %>%
      filter(code2020 == 2205706) %>%
      select(paste0("clu", y_1, "_final"))

    data_mun <- data_mun %>%
      mutate(!!paste0("clu", y_1, "_final2") := ifelse(get(paste0("clu", y_1, "_final")) %in% n1, n0,
                                                         get(paste0("clu", y_1, "_final2"))))

  }

  if (startyear <= 1911 & endyear >= 1911) {

    n0 <- data_mun %>%
      filter(final_name == "Palmas" & uf_amc == 15) %>%
      select(paste0("clu", y_1, "_final"))

    n1 <- data_mun %>%
      filter(code2020 == 4204202) %>%
      select(paste0("clu", y_1, "_final"))

    n2 <- data_mun %>%
      filter(code2020 == 4209003) %>%
      select(paste0("clu", y_1, "_final"))

    n3 <- data_mun %>%
      filter(code2020 == 4213609) %>%
      select(paste0("clu", y_1, "_final"))

    data_mun <- data_mun %>%
      mutate(!!paste0("clu", y_1, "_final2") := ifelse(get(paste0("clu", y_1, "_final")) %in% n1, n0,
                                                  ifelse(get(paste0("clu", y_1, "_final")) %in% n2, n0,
                                                  ifelse(get(paste0("clu", y_1, "_final")) %in% n3, n0,
                                                         get(paste0("clu", y_1, "_final2"))))))

    n0 <- data_mun %>%
      filter(final_name == "Rio Negro" & uf_amc == 15) %>%
      select(paste0("clu", y_1, "_final"))

    n1 <- data_mun %>%
      filter(code2020 == 4208104) %>%
      select(paste0("clu", y_1, "_final"))

    n2 <- data_mun %>%
      filter(code2020 == 4210100) %>%
      select(paste0("clu", y_1, "_final"))

    data_mun <- data_mun %>%
      mutate(!!paste0("clu", y_1, "_final2") := ifelse(get(paste0("clu", y_1, "_final")) %in% n1, n0,
                                                  ifelse(get(paste0("clu", y_1, "_final")) %in% n2, n0,
                                                         get(paste0("clu", y_1, "_final2")))))

    n0 <- data_mun %>%
      filter(final_name == "Humaita" & uf_amc == 1) %>%
      select(paste0("clu", y_1, "_final"))

    n1 <- data_mun %>%
      filter(code2020 == 1100205) %>%
      select(paste0("clu", y_1, "_final"))

    data_mun <- data_mun %>%
      mutate(!!paste0("clu", y_1, "_final2") := ifelse(get(paste0("clu", y_1, "_final")) %in% n1, n0,
                                                         get(paste0("clu", y_1, "_final2"))))

  }

  if (startyear <= 1940 | endyear >= 1960) {

    n0 <- data_mun %>%
      filter(code2020 == 3104700) %>%
      select(paste0("clu", y_1, "_final"))

    n1 <- data_mun %>%
      filter(code2020 == 3203304) %>%
      select(paste0("clu", y_1, "_final"))

    n2 <- data_mun %>%
      filter(code2020 == 3200904) %>%
      select(paste0("clu", y_1, "_final"))

    data_mun <- data_mun %>%
      mutate(!!paste0("clu", y_1, "_final2") := ifelse(get(paste0("clu", y_1, "_final")) %in% n1, n0,
                                                  ifelse(get(paste0("clu", y_1, "_final")) %in% n2, n0,
                                                         get(paste0("clu", y_1, "_final2")))))

    rm(n0, n1, n2)

  }

  if (startyear <= 1950) {

    n0 <- data_mun %>%
      filter(code2020 == 4101705) %>%
      select(paste0("clu", y_1, "_final"))

    n1 <- data_mun %>%
      filter(code2020 == 4105508) %>%
      select(paste0("clu", y_1, "_final"))

    data_mun <- data_mun %>%
      mutate(!!paste0("clu", y_1, "_final2") := ifelse(get(paste0("clu", y_1, "_final")) %in% n1, n0,
                                                         get(paste0("clu", y_1, "_final2"))))

    rm(n0, n1)

  }

  ## Fixing code_muni to 1970 code Mato Grosso do Sul
  if (endyear <= 1970) {

    data_mun <- data_mun %>%
      mutate(code_state = as.numeric(substr(code2020, 1, 2)),
             code_state = ifelse(code_state == 50, 51, code_state),
             code_state = ifelse(code_state == 17, 52, code_state),
             code2020 = substr(code2020, 3, 7),
             code2020 = as.numeric(paste0(code_state, code2020))) %>%
      select(-c(code_state))

  }

  ## Fixing code_muni to 1980 code Tocantins
  if (endyear == 1980) {

    data_mun <- data_mun %>%
      mutate(code_state = as.numeric(substr(code2020, 1, 2)),
             code_state = ifelse(code_state == 17, 52, code_state),
             code2020 = substr(code2020, 3, 7),
             code2020 = as.numeric(paste0(code_state, code2020))) %>%
      select(-c(code_state))

  }

  # generate a new code for the final AMCs
  # generate common UF_AMCs first

  data_mun <- data_mun %>%
    arrange(uf_amc) %>%
    mutate(clu_final = dense_rank(unlist(get(paste0("clu", y_1, "_final2"))))) %>%
    select(-c(paste0("clu", y_1, "_final2")))

  # Generating new amc column and name
  data_mun <- data_mun %>%
    mutate(
      uf_amc = ifelse(uf_amc %in% c(1, 20), 1,
      ifelse(uf_amc %in% c(4, 5), 4,
      ifelse(uf_amc %in% c(6), 5,
      ifelse(uf_amc %in% c(7), 6,
      ifelse(uf_amc %in% c(8), 7,
      ifelse(uf_amc %in% c(9), 8,
      ifelse(uf_amc %in% c(10), 9,
      ifelse(uf_amc %in% c(11), 10,
      ifelse(uf_amc %in% c(12, 18), 11,
      ifelse(uf_amc %in% c(13), 12,
      ifelse(uf_amc %in% c(14), 13,
      ifelse(uf_amc %in% c(15, 16), 14,
      ifelse(uf_amc %in% c(17), 15,
      ifelse(uf_amc %in% c(19), 16, uf_amc)))))))))))))),
      uf_amc_lb = ifelse(uf_amc %in% c(1), "AM/MT/(RO/RR/MS)",
      ifelse(uf_amc %in% c(2), "PA/(AP)",
      ifelse(uf_amc %in% c(3), "MA",
      ifelse(uf_amc %in% c(4), "PI/CE",
      ifelse(uf_amc %in% c(5), "RN",
      ifelse(uf_amc %in% c(6), "PB",
      ifelse(uf_amc %in% c(7), "PE",
      ifelse(uf_amc %in% c(8), "AL",
      ifelse(uf_amc %in% c(9), "SE",
      ifelse(uf_amc %in% c(10), "BA",
      ifelse(uf_amc %in% c(11), "ES/MG",
      ifelse(uf_amc %in% c(12), "RJ",
      ifelse(uf_amc %in% c(13), "SP",
      ifelse(uf_amc %in% c(14), "PR/SC",
      ifelse(uf_amc %in% c(15), "RS",
      ifelse(uf_amc %in% c(16), "GO/(DF/TO)", uf_amc)))))))))))))))))

  # Assign a new cluster number, with UF in first 2 digits
  data_mun <- data_mun %>%
    dplyr::arrange(clu_final, uf_amc, code2020)

  data_mun <- data_mun %>%
    dplyr::group_by(clu_final, uf_amc) %>%
    mutate(help = ifelse(!is.na(clu_final) & row_number() == 1, 1, NA))

  data_mun <- data_mun %>%
    dplyr::arrange(help, uf_amc, code2020) %>% ungroup()

  data_mun <- data_mun %>%
    dplyr::group_by(help, uf_amc) %>%
    mutate(amc_n = ifelse(help == 1, row_number(), NA))

  data_mun <- data_mun %>% ungroup() %>%
    dplyr::arrange(uf_amc, clu_final, code2020) %>% as.data.frame()

  for (i in 2:nrow(data_mun)) {
    if (is.na(data_mun[, c("amc_n")][i]))
      data_mun$amc_n[i] <- data_mun$amc_n[i - 1]
  }

  # Including other AMC numbers
  data_mun <- data_mun %>%
    mutate(amc = ifelse(!is.na(clu_final), uf_amc * 1000, NA),
           amc = amc + amc_n) %>%
    select(-c(amc_n, help))

  # clean table
  data_mun <- data_mun %>%
    arrange(uf_amc, clu_final, final_name)

  # subset columns
  data_mun <- setDT(data_mun)[, .(final_name, code2020, amc)]

  # rename columns
  setnames(data_mun, c("name_muni", "code_muni", "code_amc"))

  data_mun$code_muni <- as.integer(data_mun$code_muni)

  return(as.data.frame(data_mun))
}


# ==============================================================================
# download_comparable_areas(): Compute crosswalk + join with municipality polygons
# ==============================================================================

download_comparable_areas <- function(start_year, end_year,
                                      municipality_files = NULL,
                                      historical_files = NULL) {

  message(sprintf("Computando AMC %d -> %d...", start_year, end_year))

  ## 1. Compute crosswalk -------------------------------------------------------
  crosswalk <- compute_amc(startyear = start_year, endyear = end_year)

  ## 2. Get municipality geometries from pipeline parquets ----------------------
  #    endyear determines which municipality boundaries to use.
  #    For 2000+: use data/municipality/{endyear}/ parquets
  #    For 1872-1991: use data/historical_empire/{endyear}/ parquets

  if (end_year >= 2000) {

    # Modern municipality data
    muni_pattern <- paste0("municipality/", end_year, "/municipalities_", end_year, "[.]parquet$")
    muni_file <- municipality_files[grepl(muni_pattern, municipality_files)]
    muni_file <- muni_file[!grepl("simplified", muni_file)]

    if (length(muni_file) == 0) {
      # Fallback: try list.files directly
      muni_dir <- paste0("./data/municipality/", end_year, "/")
      muni_file <- list.files(muni_dir, pattern = paste0("municipalities_", end_year, "[.]parquet$"),
                              full.names = TRUE)
      muni_file <- muni_file[!grepl("simplified", muni_file)]
    }

    if (length(muni_file) == 0) {
      stop("Municipality parquet for year ", end_year, " not found. Run municipality_clean first.")
    }

  } else {

    # Historical empire data (1872-1991)
    muni_pattern <- paste0("historical_empire/", end_year, "/historical_empire_", end_year, "[.]parquet$")
    muni_file <- historical_files[grepl(muni_pattern, historical_files)]
    muni_file <- muni_file[!grepl("simplified", muni_file)]

    if (length(muni_file) == 0) {
      # Fallback: try list.files directly
      muni_dir <- paste0("./data/historical_empire/", end_year, "/")
      muni_file <- list.files(muni_dir, pattern = paste0("historical_empire_", end_year, "[.]parquet$"),
                              full.names = TRUE)
      muni_file <- muni_file[!grepl("simplified", muni_file)]
    }

    if (length(muni_file) == 0) {
      stop("Historical empire parquet for year ", end_year, " not found. Run historicalempire_clean first.")
    }

  }

  map <- read_geoparquet(muni_file)
  map$code_muni <- as.integer(map$code_muni)

  ## 3. Join crosswalk with municipality polygons --------------------------------
  # Drop name_muni from crosswalk (use the name from the spatial data)
  data_mun_sf <- dplyr::left_join(map, crosswalk %>% dplyr::select(-c(name_muni)),
                                   by = "code_muni")

  # Remove municipalities without AMC assignment
  data_mun_sf <- data_mun_sf %>% dplyr::filter(!is.na(code_amc))

  ## 4. Dissolve borders by code_amc --------------------------------------------
  data_mun_sf <- dissolve_polygons_no_split(mysf = data_mun_sf, group_column = "code_amc")

  ## Return list with both sf and crosswalk (avoids recomputing in clean step)
  return(list(sf = data_mun_sf, crosswalk = crosswalk))
}


# ==============================================================================
# clean_comparable_areas(): Add list columns, convert to MULTIPOLYGON, save
# ==============================================================================

clean_comparable_areas <- function(amc_raw, start_year, end_year,
                                   municipality_files = NULL,
                                   historical_files = NULL) {

  dir_clean <- sprintf("./data/comparable_areas/%d_%d", start_year, end_year)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## amc_raw is a list with $sf and $crosswalk from download_comparable_areas()
  temp_sf <- amc_raw$sf
  crosswalk <- amc_raw$crosswalk

  ## 1. Build list columns from the crosswalk ------------------------------------

  code_list <- crosswalk %>%
    dplyr::group_by(code_amc) %>%
    dplyr::summarise(
      !!paste0("list_code_muni_", end_year) := paste(code_muni, collapse = ","),
      !!paste0("list_name_muni_", end_year) := paste(name_muni, collapse = ","),
      .groups = "drop"
    )

  ## 2. Join list columns --------------------------------------------------------
  temp_sf <- dplyr::left_join(temp_sf, code_list, by = "code_amc")

  ## 3. Ensure geometry column is named "geometry" and is last -------------------
  geo_col <- attr(temp_sf, "sf_column")
  if (!is.null(geo_col) && geo_col != "geometry") {
    names(temp_sf)[names(temp_sf) == geo_col] <- "geometry"
    attr(temp_sf, "sf_column") <- "geometry"
  }

  ## 4. Convert to MULTIPOLYGON via st_cast (NOT to_multipolygon — avoids BUG #30)
  temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")

  ## 5. Ensure CRS is EPSG:4674 -------------------------------------------------
  if (is.na(sf::st_crs(temp_sf)) || sf::st_crs(temp_sf)$epsg != 4674) {
    temp_sf <- sf::st_transform(temp_sf, 4674)
  }

  ## 6. Reorder columns (geometry last) ------------------------------------------
  geo_cols <- "geometry"
  other_cols <- setdiff(names(temp_sf), geo_cols)
  temp_sf <- temp_sf[, c(other_cols, geo_cols)]

  ## 7. Validation ---------------------------------------------------------------
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))

  ## 8. Save full version --------------------------------------------------------
  fname <- sprintf("comparable_areas_%d_%d", start_year, end_year)
  write_geobr_parquet(temp_sf,
    paste0(dir_clean, "/", fname, ".parquet"))

  ## 9. Simplified version -------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  write_geobr_parquet(temp_sf_simplified,
    paste0(dir_clean, "/", fname, "_simplified.parquet"))

  ## 10. Return file paths -------------------------------------------------------
  files <- list.files(path = dir_clean, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)
  return(files)
}
