# Harmonize Geobr --------------------------------------------------------------
harmonize_geobr <- function(temp_sf, 
                            year = parent.frame()$year,
                            add_state = TRUE, 
                            state_column = c('name_state', 'code_state'),
                            add_region = TRUE, 
                            region_column = c('code_state', 'code_muni'),
                            add_snake_case = TRUE,
                            snake_colname,
                            projection_fix = TRUE,
                            encoding_utf8 = TRUE,
                            topology_fix = TRUE,
                            remove_z_dimension = TRUE,
                            use_multipolygon = TRUE
                            ### dissolve
                            ){
  
  
  ## add state colum
  if (isTRUE(add_state)) {
    
    ### check if "state_column" is in data
    if (!state_column %in% names(temp_sf)) {
      stop(paste("The data temp_sf does not have a column named ", state_column))
    }
    temp_sf <- add_state_info(temp_sf, column = state_column)
  }
  
  
  ## add region colum
  if (isTRUE(add_region)) {
    
    # check if "state_column" is in data
    if(! region_column %in% names(temp_sf)){
      stop(paste("The data temp_sf does not have a column named ", state_column))
    }
    temp_sf <- add_region_info(temp_sf, column = region_column)
  }
  
  # snake case column name
  if (isTRUE(add_snake_case)) {
    temp_sf <- snake_case_names(temp_sf, colname = snake_colname)
  }
  
  # harmonize spatial projection
  if (isTRUE(projection_fix)) {
    temp_sf <- harmonize_projection(temp_sf)
  }
  
  # always use encoding UTF-8
  if (isTRUE(encoding_utf8)) {
    temp_sf <- use_encoding_utf8(temp_sf)
  }
  
  # fix eventual topoly errors
  if (isTRUE(topology_fix)) {
    temp_sf <- fix_topology(temp_sf)
  }
  
  # remove Z dimension of spatial data
  if (isTRUE(remove_z_dimension)) {
    remove_z_dimension <- function(temp_df){sf::st_zm(temp_df, drop = TRUE, what = "ZM")}
    temp_sf <- remove_z_dimension(temp_sf)
  }
  
  # make sure geometry column is named "geometry"
  temp_sf <- normalize_sf_geometry(temp_sf)

  # convert to multipolygon
  if (isTRUE(use_multipolygon)) {
    temp_sf <- to_multipolygon(temp_sf)
  }

  # add year column (before geometry to keep geometry last)
  temp_sf$year <- year
  temp_sf <- dplyr::relocate(temp_sf, geometry, .after = dplyr::last_col())

  # code columns to numeric
  temp_sf <- code_cols_to_numeric(temp_sf)
  
  # if there's at least one column named with "code_", sort by them
  
  if (any(grepl("code_", names(temp_sf)))) {
    temp_sf <- temp_sf |>
      dplyr::arrange(dplyr::across(dplyr::starts_with("code_")))
  }
  
  
  return(temp_sf)
}


# Normalize geometry column name ------------------------------------------------
normalize_sf_geometry <- function(temp_sf) {
  old_name <- attr(temp_sf, "sf_column")
  if (old_name != "geometry") {
    names(temp_sf)[names(temp_sf) == old_name] <- "geometry"
    attr(temp_sf, "sf_column") <- "geometry"
  }
  return(temp_sf)
}

# Add State abbreviation -------------------------------------------------------
add_state_info <- function(temp_sf, column){
  
  options(encoding = "UTF-8")
  
  col_names <- names(temp_sf)
  
  # IF only the "name_state" column is present
  # Add code_state
  if ("name_state" %in% col_names & !"code_state" %in% col_names) {
      
    temp_sf <- temp_sf |> 
      dplyr::mutate(code_state = ifelse(name_state %like% "Rondonia|Rondônia|Rond\u00f4nia|Guapoé|Guapor\u00e9",11,
      ifelse(name_state %like% "Acre", 12,
      ifelse(name_state== "Amazonas",13,
      ifelse(name_state %like% "Roraima|Rio Branco",14,
      ifelse(name_state %in% c("Par\u00e1", "Pará", "Para") ,15,
      ifelse(name_state %like% "Amap\u00e1|Amapá|Amapa", 16,
      ifelse(name_state== "Tocantins",17,
      ifelse((name_state %like% "(?i)territ") & (name_state %like% "(?i)fernando"),20,
      ifelse(name_state== "Maranh\u00e3o",21,
      ifelse(name_state %like% "Piaui|Piauhy|Piauí|Piau\\u00ed",22,
      ifelse(name_state== "Cear\u00e1",23,
      ifelse(name_state== "Rio Grande do Norte",24,
      ifelse(name_state %like% "Paraiba|Parahyba|Para\\u00edbaParaíba",25,
      ifelse(name_state== "Pernambuco" | name_state %like% "Fernando de Noronha",26,
      ifelse(name_state== "Alagoas" | name_state=="Alag\u00f4as",27,
      ifelse(name_state== "Sergipe",28,
      ifelse(name_state== "Bahia",29,
      ifelse(name_state== "Minas Gerais" | name_state== "Minas Geraes",31,
      ifelse(name_state  %like% "Espirito Santo|Esp\\u00edrito Santo|Espírito Santo" ,32,
      ifelse(name_state== "Rio de Janeiro",33,
      ifelse(name_state== "Guanabara" | name_state %like% "(?i)munic[ií]pio neutro",34,
      ifelse(name_state== "S\u00e3o Paulo" | name_state== "São Paulo",35,
      ifelse(name_state %like% "Paran\u00e1|Paraná|Parana",41,
      ifelse(name_state== "Santa Catarina" | name_state== "Santa Catharina",42,
      ifelse(name_state== "Rio Grande do Sul",43,
      ifelse(name_state== "Mato Grosso do Sul",50,
      ifelse(name_state== "Mato Grosso" | name_state== "Matto Grosso",51,
      ifelse(name_state== "Goi\u00e1s" | name_state== "Goyaz" | name_state== "Goiás" | name_state== "Goias",52,
      ifelse((name_state== "Distrito Federal" | name_state== "Districto Federal") & year<1960,34,
      ifelse((name_state== "Distrito Federal" | name_state=="Brasilia" | name_state=="Brasília" | name_state=="Bras\\u00edlia") & (year>1950),53,NA
                    )))))))))))))))))))))))))))))))
  }

  # Bloco aditivo: se chegamos aqui com code_state ja existente mas com NAs
  # (caso tipico: hist_states 1940-1991 — raw IBGE traz code_state=NA pra
  # quase todos estados) E temos name_state, fazemos um segundo lookup pra
  # preencher os NAs via coalesce. NAo altera valores nao-NA existentes.
  if ("name_state" %in% col_names &&
      "code_state" %in% names(temp_sf) &&
      any(is.na(as.numeric(temp_sf$code_state)))) {

    .fill_cs <- ifelse(temp_sf$name_state %like% "Rondonia|Rondônia|Rondônia|Guapoé|Guaporé",11,
      ifelse(temp_sf$name_state %like% "Acre", 12,
      ifelse(temp_sf$name_state== "Amazonas",13,
      ifelse(temp_sf$name_state %like% "Roraima|Rio Branco",14,
      ifelse(temp_sf$name_state %in% c("Pará", "Pará", "Para") ,15,
      ifelse(temp_sf$name_state %like% "Amapá|Amapá|Amapa", 16,
      ifelse(temp_sf$name_state== "Tocantins",17,
      ifelse((temp_sf$name_state %like% "(?i)territ") & (temp_sf$name_state %like% "(?i)fernando"),20,
      ifelse(temp_sf$name_state== "Maranhão",21,
      ifelse(temp_sf$name_state %like% "Piaui|Piauhy|Piauí|Piau\\u00ed",22,
      ifelse(temp_sf$name_state== "Ceará",23,
      ifelse(temp_sf$name_state== "Rio Grande do Norte",24,
      ifelse(temp_sf$name_state %like% "Paraiba|Parahyba|Para\\u00edba|Paraíba",25,
      ifelse(temp_sf$name_state== "Pernambuco" | temp_sf$name_state %like% "Fernando de Noronha",26,
      ifelse(temp_sf$name_state== "Alagoas" | temp_sf$name_state=="Alagôas",27,
      ifelse(temp_sf$name_state== "Sergipe",28,
      ifelse(temp_sf$name_state== "Bahia",29,
      ifelse(temp_sf$name_state== "Minas Gerais" | temp_sf$name_state== "Minas Geraes",31,
      ifelse(temp_sf$name_state %like% "Espirito Santo|Esp\\u00edrito Santo|Espírito Santo",32,
      ifelse(temp_sf$name_state== "Rio de Janeiro",33,
      ifelse(temp_sf$name_state== "Guanabara" | temp_sf$name_state %like% "(?i)munic[ií]pio neutro",34,
      ifelse(temp_sf$name_state== "São Paulo" | temp_sf$name_state== "São Paulo",35,
      ifelse(temp_sf$name_state %like% "Paraná|Paraná|Parana",41,
      ifelse(temp_sf$name_state== "Santa Catarina" | temp_sf$name_state== "Santa Catharina",42,
      ifelse(temp_sf$name_state== "Rio Grande do Sul",43,
      ifelse(temp_sf$name_state== "Mato Grosso do Sul",50,
      ifelse(temp_sf$name_state== "Mato Grosso" | temp_sf$name_state== "Matto Grosso",51,
      ifelse(temp_sf$name_state== "Goiás" | temp_sf$name_state== "Goyaz" | temp_sf$name_state== "Goiás" | temp_sf$name_state== "Goias",52,
      ifelse((temp_sf$name_state== "Distrito Federal" | temp_sf$name_state== "Districto Federal") & temp_sf$year<1960,34,
      ifelse((temp_sf$name_state== "Distrito Federal" | temp_sf$name_state=="Brasilia" | temp_sf$name_state=="Brasília" | temp_sf$name_state=="Bras\\u00edlia") & (temp_sf$year>1950),53,NA
                    ))))))))))))))))))))))))))))))

    # Original code_state, tratando 0 (marker de litigio) como NA
    .orig_cs <- as.numeric(temp_sf$code_state)
    .orig_cs <- ifelse(is.na(.orig_cs) | .orig_cs == 0, NA_real_, .orig_cs)
    temp_sf$code_state <- dplyr::coalesce(.orig_cs, as.numeric(.fill_cs))
    rm(.fill_cs, .orig_cs)
  }

  # IF there is no "name_state" column
  if (column != 'name_state') {
    
    # add code_state
    temp_sf$code_state <- substr( temp_sf[[ column ]] , 1,2) |> as.numeric()
    
    # # add name_state ENCODING ISSUES
    # stringi::stri_encode(from='latin1', to="utf8", str= "S\u00e3o Paulo")
    # stringi::stri_encode('S\u00e3o Paulo', to="UTF-8")
    # gtools::ASCIIfy('S\u00e3o Paulo')
    temp_sf <- temp_sf |> dplyr::mutate(name_state =
                                          data.table::fcase(code_state== 11, "Rond\u00f4nia",
                                                            code_state== 12, "Acre",
                                                            code_state== 13, "Amazonas",
                                                            code_state== 14, "Roraima",
                                                            code_state== 15, "Par\u00e1",
                                                            code_state== 16, "Amap\u00e1",
                                                            code_state== 17, "Tocantins",
                                                            code_state== 20, "Fernando de Noronha",
                                                            code_state== 21, "Maranh\u00e3o",
                                                            code_state== 22, "Piau\u00ed",
                                                            code_state== 23, "Cear\u00e1",
                                                            code_state== 24, "Rio Grande do Norte",
                                                            code_state== 25, "Para\u00edba",
                                                            code_state== 26, "Pernambuco",
                                                            code_state== 27, "Alagoas",
                                                            code_state== 28, "Sergipe",
                                                            code_state== 29, "Bahia",
                                                            code_state== 31, "Minas Gerais",
                                                            code_state== 32, "Esp\u00edrito Santo",
                                                            code_state== 33, "Rio de Janeiro",
                                                            code_state== 34, "Guanabara",
                                                            code_state== 35, "S\u00e3o Paulo",
                                                            code_state== 41, "Paran\u00e1",
                                                            code_state== 42, "Santa Catarina",
                                                            code_state== 43, "Rio Grande do Sul",
                                                            code_state== 50, "Mato Grosso do Sul",
                                                            code_state== 51, "Mato Grosso",
                                                            code_state== 52, "Goi\u00e1s",
                                                            code_state== 53, "Distrito Federal",
                                                            default = NA))
  }
  
  # add abbrev state
  temp_sf <- temp_sf |> dplyr::mutate(
    abbrev_state = data.table::fcase(
      code_state== 11, "RO",
      code_state== 12, "AC",
      code_state== 13, "AM",
      code_state== 14, "RR",
      code_state== 15, "PA",
      code_state== 16, "AP",
      code_state== 17, "TO",
      code_state== 20, "FN",
      code_state== 21, "MA",
      code_state== 22, "PI",
      code_state== 23, "CE",
      code_state== 24, "RN",
      code_state== 25, "PB",
      code_state== 26, "PE",
      code_state== 27, "AL",
      code_state== 28, "SE",
      code_state== 29, "BA",
      code_state== 31, "MG",
      code_state== 32, "ES",
      code_state== 33, "RJ",
      code_state== 34, "GB",
      code_state== 35, "SP",
      code_state== 41, "PR",
      code_state== 42, "SC",
      code_state== 43, "RS",
      code_state== 50, "MS",
      code_state== 51, "MT",
      code_state== 52, "GO",
      code_state== 53, "DF",
      default = NA))
  
  return(temp_sf)
}


# Add Region info --------------------------------------------------------------
add_region_info <- function(temp_sf, column){
  
  # add code_region
  temp_sf$code_region <- substr( temp_sf[[ column ]] , 1,1) |> as.numeric()
  
  # add name_region
  temp_sf <- temp_sf |> dplyr::mutate(name_region =
                                        data.table::fcase(code_region==1, 'Norte',
                                                          code_region==2, 'Nordeste',
                                                          code_region==3, 'Sudeste',
                                                          code_region==4, 'Sul',
                                                          code_region==5, 'Centro-Oeste',
                                                          default = NA))
  return(temp_sf)
}


# snake case names -------------------------------------------------------------
snake_case_names <- function(temp_sf, colname){
  
  for (i in colname) {
    
    if(i %in% names(temp_sf)){
      # Capitalize the first letter
      temp_sf[[ i ]] <- stringr::str_to_title( temp_sf[[ i ]] )

      # prepositions to lower
      temp_sf[[ i ]] <- gsub(" Do ",  " do ",   temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" Dos ", " dos ", temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" Da ",  " da ",   temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" Das ", " das ", temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" De ",  " de ",   temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" Del ", " del ",   temp_sf[[ i ]] )
      
      temp_sf[[ i ]] <- gsub(" D'o",   " d'O",   temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" D'ó",   " d'Ó",   temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" D'a",   " d'A",   temp_sf[[ i ]] )
      temp_sf[[ i ]] <- gsub(" D'á",   " d'Á",   temp_sf[[ i ]] )
    }
  }
  return(temp_sf)
}


# Harmonize spatial projection -------------------------------------------------

# Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674

harmonize_projection <- function(temp_sf){
  
  class(temp_sf) <- c("sf", "data.frame")
  
  temp_sf <- if (is.na(sf::st_crs(temp_sf))) {
    sf::st_set_crs(temp_sf, 4674)
  } else {
    sf::st_transform(temp_sf, 4674)
  }
  sf::st_crs(temp_sf) <- 4674
  
  return(temp_sf)
}




# Use UTF-8 encoding -----------------------------------------------------------
use_encoding_utf8 <- function(temp_sf){
  options(encoding = "UTF-8")
  
  
  temp_sf <- temp_sf |>
    dplyr::mutate_if(is.factor, function(x){
      x |> as.character() |> stringi::stri_encode(to="UTF-8") } )
  
  temp_sf <- temp_sf |>
    dplyr::mutate_if(is.character, function(x){
      x  |> stringi::stri_encode(to="UTF-8") } )
  
  # code columns remain numeric
  temp_sf <- temp_sf |> dplyr::mutate_at(dplyr::vars(starts_with("code_")), .funs = function(x){ as.numeric(x) })
  
  return(temp_sf)
}


# convert to MULTIPOLYGON ------------------------------------------------------

# to_multipolygon <- function(temp_sf){
# if(sf::st_geometry_type(temp_sf) |> unique() |> as.character() |> length() > 1 |
#     any(  !(sf::st_geometry_type(temp_sf) |> unique() |> as.character() %like% "MULTIPOLYGON|GEOMETRYCOLLECTION"))) {
#   # remove linstring
#   temp_sf <- subset(temp_sf,sf::st_geometry_type(temp_sf) |> as.character() != "LINESTRING")
#   temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
#   return(temp_sf)
# }else{ return(temp_sf)}}

to_multipolygon <- function(temp_sf){
  
  # get geometry types
  geom_types <- sf::st_geometry_type(temp_sf) |> unique() |> as.character()
  
  # se todos forem polygon ou MULTIPOLYGON, retorna MULTIPOLYGON
 if (all(data.table::like(unique(geom_types) ,"POLYGON|MULTIPOLYGON"))) {
   temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
   return(temp_sf)
  }
  
  # checks
  if (length(geom_types) > 1 | any(  !( data.table::like(geom_types,"POLYGON")))) {
    
    # remove linestrings and points
    temp_sf <- subset(temp_sf, sf::st_geometry_type(temp_sf) |> as.character() != "LINESTRING")
    # temp_sf <- subset(temp_sf, sf::st_geometry_type(temp_sf) |> as.character() != "POINT")
    
    # get polyons
    temp_sf <- sf::st_cast(temp_sf, "POLYGON")
    temp_sf <- sf::st_collection_extract(temp_sf, "POLYGON")
    temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
    
  }
  
  # convert everything to MULTIPOLYGON
  temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
  
  # merge polygons into single MULTIPOLYGON
  col_names <- names(temp_sf)
  col_names <- col_names[ !col_names %like% 'geometry|geom']
  
  temp_sf <- temp_sf |>
    dplyr::group_by(across(all_of(col_names))) |>
    dplyr::summarise() |>
    dplyr::ungroup()

  temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
  
  return(temp_sf)
}

### OBS: é possível que esse trecho da função esteja apagando certas colunas

# Fix topology -----------------------------------------------------------------

fix_topology <- function(temp_sf){
  
  sf::sf_use_s2(TRUE)
  
  # first check whether there are any offending feature(s)
  bad_ix <- which(!sf::st_is_valid(temp_sf))
  # bad_ix <- which(
  #   ! duckspatial::ddbs_is_valid(temp_sf) |> 
  #     pull(is_valid)
  #   )
    
  if (length(bad_ix) == 0) {
    return(temp_sf)
  }
  
  # ### Attempt 1 - fast duckdbspatial
  # # temp_sf <- sf::st_make_valid(temp_sf)
  # 
  # try(silent = T,
  #     temp_sf <- duckspatial::ddbs_make_valid(temp_sf) |>
  #       duckspatial::ddbs_collect()
  # )
  # 
  # # Find the offending feature(s)
  # bad_ix <- which(
  #   ! duckspatial::ddbs_is_valid(temp_sf) |> 
  #     pull(is_valid)
  # )
  # 
  # if (length(bad_ix) == 0) {
  #   return(temp_sf)
  # }

  ### Attempt 2 - not so fast sf
  temp_sf <- sf::st_make_valid(temp_sf)
  
  # Find the offending feature(s)
  bad_ix <- which(!sf::st_is_valid(temp_sf))
  # bad_ix <- which(
  #   ! duckspatial::ddbs_is_valid(temp_sf) |> 
  #     pull(is_valid)
  #   )
  
  if (length(bad_ix) == 0) {
    return(temp_sf)
  }
  
  # # detect problematic edges
  # poly_invalid <- temp_sf[bad_ix, ]
  # problem_edges <-sf::st_is_valid(poly_invalid, reason=TRUE)
  # problem_edges <- sub(".*:\\D*(\\d+).*", "\\1", problem_edges) |> as.numeric()
  # all_edges <- sf::st_cast(poly_invalid, "POINT") |> suppressWarnings()
  # problem_edges <- all_edges[problem_edges, ]
  # mapview::mapview(poly_invalid) + problem_edges
  
  ### Attempt 3 {lwgeom::lwgeom_make_valid}
  
  # Try lwgeom’s make_valid first (sometimes succeeds where sf:: does not)
  geom <- sf::st_geometry(temp_sf)
  geom[bad_ix] <- lwgeom::lwgeom_make_valid(geom[bad_ix])
  
  sf::st_geometry(temp_sf) <- geom
  
  # Find the offending feature(s)
  still_bad <- which(!st_is_valid(temp_sf))
  
  if (length(still_bad) == 0) {
    return(temp_sf)
  }
  
  ### Attempt 4 - rebuild polygon: if still invalid, rebuild via node + polygonize
  
  fix_one <- function(g) {
    b  <- sf::st_boundary(g)
    b  <- sf::st_cast(b, "MULTILINESTRING")
    b <- sf::st_transform(b, crs = 5880)
    bN <- sf::st_node(b)                   # split at crossings
    p  <- sf::st_polygonize(bN) |> sf::st_collection_extract("POLYGON")
    p <- sf::st_union(p)                                # merge back to a multipolygon
    p <- sf::st_transform(p, crs = sf::st_crs(g))
  } 
  
  if (length(still_bad)) {
    for (i in still_bad) {
     sf::st_geometry(temp_sf)[i] <- fix_one(st_geometry(temp_sf)[i])
    }
  }  
  
  # Find the offending feature(s)
  still_bad <- which(!st_is_valid(temp_sf))
  
  if (length(still_bad) == 0) {
    return(temp_sf)
  } else(stop("Topology errors could not be fixed."))
  
}



# Dissolve borders temp_sf -----------------------------------------------------

## Function to clean and dissolve the borders of polygons by groups

dissolvefun <- function(df, group_column = NULL){
  
  # temp_region <- df |>
  #   ungroup() |>
  #   summarise(.by = group_column)
  
  # temp_region <- df |>
  #   dplyr::group_by(dplyr::across(dplyr::all_of(group_column))) |>
  #   dplyr::summarise(.groups = "drop")
  
  # convert to UTM before dissolving borders. this helps avoiding 
  # small gap / slivers
  df <- df |> sf::st_transform(crs = 3857)
  
  temp_region <- duckspatial::ddbs_union_agg(
    x =  df,
    by = group_column
    ) |>
    duckspatial::ddbs_collect()
  
  temp_region <- fix_topology(temp_region)
  temp_region <- sfheaders::sf_remove_holes(temp_region)
  temp_region <- harmonize_projection(temp_region)
  
  return(temp_region)
}

dissolve_polygons_split <- function(mysf, group_column = NULL){
  
  # make sure it is projects
  mysf <- harmonize_projection(mysf)
  
  # a) make sure we have valid geometries
  mysf <- fix_topology(mysf)
  
  # b) make sure we have sf MULTIPOLYGON
  mysf <- to_multipolygon(mysf)
  
  # # c) dissolve function with duckspatial
  # temp_sf <- duckspatial::ddbs_union_agg(
  #   x =  mysf,
  #   by = group_column #, conn = conn
  #   ) |>
  #   duckspatial::ddbs_collect() |> 
  #   sfheaders::sf_remove_holes()
  #> ERROR: Not compatible with STRSXP: [type=NULL].
  
  # Apply sub-function
  all_areas <- split(mysf, f = mysf[[group_column]] ) |>
    pbapply::pblapply(
      FUN = function(x, grp_column = group_column) {
        temp <- dissolvefun(x, group_column = grp_column )
        temp <- sfheaders::sf_remove_holes(temp)
        return(temp)
      }
    )
  temp_sf <- dplyr::bind_rows(all_areas)
  # temp_sf <- do.call("rbind", all_areas)
  
  # unecessary because we remove hole in the dissolve fun already
  # # remove furos e rebarbas
  # temp_sf <- sfheaders::sf_remove_holes(temp_sf)
  
  
  return(temp_sf)
}



dissolve_polygons_no_split <- function(mysf, group_column = NULL){
  
  # make sure it is projects
  mysf <- harmonize_projection(mysf)
  
  # a) make sure we have valid geometries
  mysf <- fix_topology(mysf)
  
  # b) make sure we have sf MULTIPOLYGON
  mysf <- to_multipolygon(mysf)
  
  # remove furos e rebarbas
  mysf <- sfheaders::sf_remove_holes(mysf)

  temp_sf <- duckspatial::ddbs_union_agg(
    x =  mysf,
    by = group_column
  ) |>
    duckspatial::ddbs_collect()
  
  # b) make sure we have sf MULTIPOLYGON
  temp_sf <- to_multipolygon(temp_sf)
  
  temp_sf <- sfheaders::sf_remove_holes(temp_sf)
  temp_sf <- fix_topology(temp_sf)
  temp_sf <- harmonize_projection(temp_sf)
  temp_sf <- sfheaders::sf_remove_holes(temp_sf)
  
  # get attributes back
  temp_sf <- dplyr::left_join(
    x = temp_sf, 
    y = sf::st_drop_geometry(mysf) |> unique()
    )
  
  return(temp_sf)
}



# # test
# states <- geobr::read_state(year=2000)
# a <- dissolve_polygons(states, group_column='code_region')
# plot(a)

# Read DATASUS .MAP binary format into sf ---------------------------------------
# Pure-R reader — no maptools dependency. Reads the DATASUS TabWin binary format
# used for health regions, macroregions, etc.
# Source format: ftp://ftp.datasus.gov.br/territorio/mapas/
read_datasus_map <- function(filename) {

  zz <- file(filename, "rb")
  on.exit(close(zz))

  ## 1. Header
  versao <- readBin(zz, "integer", 1, size = 2)   # 100 = version 1.00
  leste  <- readBin(zz, "numeric", 1, size = 4)   # bounding box
  norte  <- readBin(zz, "numeric", 1, size = 4)
  oeste  <- readBin(zz, "numeric", 1, size = 4)
  sul    <- readBin(zz, "numeric", 1, size = 4)

  ## 2. Read objects
  geocodes <- character(0)
  names_vec <- character(0)
  geom_list <- list()
  idx <- 0

  repeat {
    tipoobj <- readBin(zz, "integer", 1, size = 1)
    if (length(tipoobj) == 0) break
    idx <- idx + 1

    # Geocode (Pascal string: length byte + 10 chars)
    len_geo <- readBin(zz, "integer", 1, size = 1)
    raw_geo <- readBin(zz, "raw", 10)
    geocodes[idx] <- rawToChar(raw_geo[seq_len(min(len_geo, 10))])

    # Name (Pascal string: length byte + 25 chars, WINDOWS-1252 encoding)
    len_name <- readBin(zz, "integer", 1, size = 1)
    raw_name <- readBin(zz, "raw", 25)
    name_raw <- raw_name[seq_len(min(len_name, 25))]
    names_vec[idx] <- tryCatch(
      iconv(rawToChar(name_raw), from = "WINDOWS-1252", to = "UTF-8"),
      error = function(e) {
        name_raw[name_raw > as.raw(127)] <- as.raw(0x3F)
        rawToChar(name_raw)
      }
    )

    # Legend coordinates (skip)
    readBin(zz, "numeric", 1, size = 4)
    readBin(zz, "numeric", 1, size = 4)

    # Number of points
    numpontos <- readBin(zz, "integer", 1, size = 2)

    # Read coordinates
    x <- numeric(numpontos)
    y <- numeric(numpontos)
    for (j in seq_len(numpontos)) {
      x[j] <- readBin(zz, "numeric", 1, size = 4)
      y[j] <- readBin(zz, "numeric", 1, size = 4)
    }

    ## 3. Split into polygon rings (detect ring closures)
    rings <- list()
    ring_start <- 1

    if (numpontos >= 3) {
      for (j in 2:numpontos) {
        dx <- abs(x[j] - x[ring_start])
        dy <- abs(y[j] - y[ring_start])
        if (dx < 1e-6 && dy < 1e-6 && (j - ring_start) >= 2) {
          rings <- c(rings, list(cbind(x[ring_start:j], y[ring_start:j])))
          ring_start <- j + 1
        }
      }
      # Leftover unclosed ring
      if (ring_start <= numpontos && (numpontos - ring_start) >= 2) {
        rc <- cbind(x[ring_start:numpontos], y[ring_start:numpontos])
        rc <- rbind(rc, rc[1, , drop = FALSE])
        rings <- c(rings, list(rc))
      }
    }

    ## 4. Build sf geometry
    if (length(rings) == 0) {
      geom_list[[idx]] <- sf::st_polygon()
    } else if (length(rings) == 1) {
      geom_list[[idx]] <- sf::st_polygon(rings)
    } else {
      poly_parts <- lapply(rings, function(r) list(r))
      geom_list[[idx]] <- sf::st_multipolygon(poly_parts)
    }
  }

  ## 5. Assemble sf, make valid, set CRS (SIRGAS 2000)
  sfc <- sf::st_sfc(geom_list, crs = 4674)
  result <- sf::st_sf(
    geocode = geocodes,
    name    = names_vec,
    geometry = sfc
  )
  result <- sf::st_make_valid(result)
  
  result <- fix_topology(result)
  
  return(result)
}

# Write GeoParquet with spatial metadata ----------------------------------------
# Requires geoarrow package loaded (via _targets.R packages list).
# Produces OGC GeoParquet with CRS, geometry type, and bbox metadata.
write_geobr_parquet <- function(sf_obj, path) {
  
  # Ensure geometry is always the last column (geobr convention)
  geo_col <- attr(sf_obj, "sf_column")
  
  if (!is.null(geo_col) && geo_col %in% names(sf_obj)) {
    other_cols <- setdiff(names(sf_obj), geo_col)
    sf_obj <- sf_obj[, c(other_cols, geo_col)]
  }
  
  arrow::write_parquet(x = sf_obj, 
                       sink = path,
                       compression = "zstd", 
                       compression_level = 7
                       )
}

# Read GeoParquet files into sf -------------------------------------------------
read_geoparquet <- function(files) {
  
  arrow::open_dataset(files) |> sf::st_as_sf()
}

# Validate geobr parquet output ------------------------------------------------
validate_geobr <- function(file_path) {
  sf_obj <- read_geoparquet(file_path)
  fname <- basename(file_path)
  is_point <- any(sf::st_geometry_type(sf_obj) %in% c("POINT", "MULTIPOINT"))

  errors <- character(0)

  # CRS = 4674
  if (is.na(sf::st_crs(sf_obj)) || sf::st_crs(sf_obj)$epsg != 4674)
    errors <- c(errors, paste(fname, "- CRS != 4674"))

  # Geometry is last column
  if (names(sf_obj)[ncol(sf_obj)] != "geometry")
    errors <- c(errors, paste(fname, "- geometry not last column"))

  # Geometry type
  if (!is_point) {
    geom_types <- unique(sf::st_geometry_type(sf_obj))
    if (!all(geom_types %in% c("MULTIPOLYGON", "POLYGON")))
      errors <- c(errors, paste(fname, "- unexpected geometry type:",
                                paste(geom_types, collapse = ",")))
  }

  # code_* columns are numeric
  code_cols <- grep("^code_", names(sf_obj), value = TRUE)
  for (col in code_cols) {
    if (!is.numeric(sf_obj[[col]]))
      errors <- c(errors, paste(fname, "-", col, "is not numeric"))
  }

  # name_* columns are character
  name_cols <- grep("^name_", names(sf_obj), value = TRUE)
  for (col in name_cols) {
    if (!is.character(sf_obj[[col]]))
      errors <- c(errors, paste(fname, "-", col, "is not character"))
  }

  # abbrev_state has 2 chars
  if ("abbrev_state" %in% names(sf_obj)) {
    bad <- sf_obj$abbrev_state[!is.na(sf_obj$abbrev_state) &
                                nchar(sf_obj$abbrev_state) != 2]
    if (length(bad) > 0)
      errors <- c(errors, paste(fname, "- abbrev_state has non-2-char values"))
  }

  # year column is numeric
  if ("year" %in% names(sf_obj) && !is.numeric(sf_obj$year))
    errors <- c(errors, paste(fname, "- year is not numeric"))

  # Empty geometries: warn for small amounts, error if >1% of total
  n_empty <- sum(sf::st_is_empty(sf_obj))
  pct_empty <- 100 * n_empty / nrow(sf_obj)
  if (n_empty > 0 && pct_empty > 1 && !is_point)
    errors <- c(errors, paste(fname, "-", n_empty, "empty geometries (", round(pct_empty, 1), "%)"))
  if (n_empty > 0 && pct_empty <= 1)
    message(fname, " - ", n_empty, "/", nrow(sf_obj), " empty geometries (", round(pct_empty, 2), "%, OK)")

  if (length(errors) > 0) stop(paste(errors, collapse = "\n"))
  return(TRUE)
}

# Folder creation geobr function -----------------------------------------------

folder_geobr <- function(folder_name = NULL, temp = FALSE) {
  
  if (temp == TRUE){ 
    
    # create a temp folder
    zip_dir <- paste0(tempdir(), "/", folder_name, "/", year)
    dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
    dir.exists(zip_dir)
    
  } else {
    
    # create a folder on the project
    zip_dir <- paste0("./data_raw/", folder_name, "/", year)
    dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
    dir.exists(zip_dir)
    
  }
  
  # unzip folder
  in_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  # unzip folder
  out_zip <- paste0(zip_dir, "/zipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ### Data folder
  dir_clean <- paste0("./data/", folder_name, "/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
}


# Columns names check geobr function -------------------------------------------

check_collumns_geobr <- function(dir_data = "./data") {
  
  arquivos <- list.files(
    dir_data,
    pattern = "\\.parquet$",
    recursive = TRUE,
    full.names = TRUE
  )
  
  purrr::map_dfr(arquivos, function(arquivo_path) {
    
    partes <- fs::path_split(
      fs::path_rel(arquivo_path, start = dir_data)
    )[[1]]
    
    # dataset lazy (Arrow)
    ds <- arrow::open_dataset(arquivo_path)
    
    tamanho_mb <- file.info(arquivo_path)$size / 1024^2
    
    nomes_colunas <- names(ds)
    
    tibble(
      tema      = partes[1],
      ano       = partes[2],
      arquivo   = fs::path_file(arquivo_path),
      tamanho_mb = round(tamanho_mb, 2),
      n_linhas  = nrow(ds),
      n_colunas = length(nomes_colunas),
      colunas = paste(nomes_colunas, collapse = ", ")
    )
  })
}


# Columns names geobr function ------------------------------------------------

# Essa função padroniza os nomes das colunas de qualquer dataset de acordo com
# um dic que deverá ser criado no script de cada dataset dentro das
# funções clean. Abaixo um exemplo para ser copiado para o scritp do dataset.

## Dicionário de equivalências do geobr ----------------------------------------
# para cada dataset é preciso fazer um dicionário de padronização para coluna
# destino. 
# Modelo de dicionário de equivalências (a ser utilizado em cada script de
# dataset): Copiar deste dataframe long e ajustar. Lista de padrões e variações 
# precisam formar uma tabela quadrada. Importante: não renomear coluna geometry.

# Este é um dicionário padrão com as denominações GEOBR:
# dicionario <- data.frame(
#   # Lista de nomes padronizados de colunas
#   padrao = c(
#     #CÓDIGO DE MUNICÍPIO e número de variações associadas
#     rep("code_muni", 4),
#     #NOME DO MUNICÍPIO e número de variações associadas
#     rep("name_muni", 4),
#     #CÓDIGO DO ESTADO e número de variações associadas
#     rep("code_state", 5),
#     #ABREVIAÇÃO DO ESTADO e número de variações associadas
#     rep("abbrev_state", 4),
#     #NOME DO ESTADO e número de variações associadas
#     rep("name_state", 3),
#     #CÓDIGO DA REGIÃO e número de variações associadas
#     rep("code_region", 2),
#     #NOME DA REGIÃO e número de variações associadas
#     rep("name_region", 2),
#     #ABREVIAÇÃO DA REGIÃO e número de variações associadas
#     rep("abbrev_region", 1)
#   ),
#   # Lista de variações
#   variacao = c(
#     #Variações que convergem para "code_muni"
#     "cod_muni", "cd_mun", "cod_mun", "cd_mun",
#     #Variações que convergem para "name_muni"
#     "nome_cidade", "cidade", "nm_muni", "nome_muni",
#     #Variações que convergem para "code_state"
#     "cod_uf", "cd_uf", "code_uf", "codigo_uf", "cod_state",
#     #Variações que convergem para "abbrev_state"
#     "sigla", "sigla_uf", "uf", "sg_uf",
#     #Variações que convergem para "name_state"
#     "nm_uf", "nm_state", "nm_estado",
#     #Variações que convergem para "code_region"
#     "cd_regia", "cd_regiao",
#     #Variações que convergem para "name_region"
#     "nm_regia", "nm_regiao",
#     #Variações que convergem para "abbrev_region"
#     "sigla_rg"
#     ), stringsAsFactors = FALSE)
  



# Robust sf read geobr function ------------------------------------------------
# Função para ler shp de multiplos arquivos e importá-los com
#  apenas as colunas que existem em todos

readmerge_geobr <-  function(folder_path,
                             encoding = "UTF-8"
                             ) {
  
  # lista todos os .shp na pasta (busca recursiva opcional)
  shp_files <- list.files(folder_path, pattern = "\\.shp$", full.names = TRUE)
  shp_files <- unique(shp_files)

  # read files
  shp_list <- purrr::map(shp_files, function(f) {
      message("Lendo: ", f)
      sf::st_read(f, quiet = TRUE, stringsAsFactors=F, options = encoding) |> 
        janitor::clean_names()
    })

    # une tudo
  merged <- dplyr::bind_rows(shp_list)
  
  return(merged)
}

# States table codes for correction geobr function -----------------------------

states_geobr <-  function() {
  states <- tibble::tibble(code_state = as.numeric(c(11, 12, 13, 14, 15, 16, 17, 21, 22,
                                               23, 24, 25, 26, 27, 28, 29, 31, 32,
                                               33, 35, 41, 42, 43, 50, 51, 52, 53)),
                   abbrev_state = c("RO", "AC", "AM", "RR", "PA", "AP", "TO",
                                    "MA", "PI", "CE", "RN", "PB", "PE", "AL",
                                    "SE", "BA", "MG", "ES", "RJ", "SP", "PR",
                                    "SC", "RS", "MS", "MT", "GO", "DF"),
                   name_state = c("Rondônia", "Acre", "Amazonas", "Roraima",
                                  "Pará", "Amapá", "Tocantins", "Maranhão",
                                  "Piauí", "Ceará", "Rio Grande do Norte",
                                  "Paraíba", "Pernambuco", "Alagoas", "Sergipe",
                                  "Bahia", "Minas Gerais", "Espírito Santo", 
                                  "Rio de Janeiro", "São Paulo", "Paraná",
                                  "Santa Catarina", "Rio Grande do Sul",
                                  "Mato Grosso do Sul", "Mato Grosso", "Goiás",
                                  "Distrito Federal"),
                   code_region = c(rep(1, 7), rep(2, 9), rep(3, 4), rep(4, 3),
                                   rep(5, 4)),
                   name_region = c(rep("Norte", 7), rep("Nordeste", 9),
                                   rep("Sudeste", 4), rep("Sul", 3), 
                                   rep("Centro-Oeste", 4)),
                   abbrevm_state = stringr::str_to_lower(abbrev_state))
  
  return(states)
}

# Glimpse geobr ----------------------------------------------------------------

# glimpse_geobr(dataset)

glimpse_geobr <- function(dataset) {
  
  table_collumns <- tibble(ordem = 1:ncol(dataset),
                           nome_coluna = names(dataset),
                           classe = sapply(dataset, class),
                           n_unicos = sapply(dataset,
                                             function(x) n_distinct(x, na.rm = TRUE)),
                           n_na = colSums(is.na(dataset)),
                           exemplos = sapply(dataset,
                                             function(x) paste(head(unique(na.omit(x)), 6),
                                                               collapse = "; "))) 
  
  return(table_collumns)
}




# dicionario de colunas geobr --------------------------------------------------------

dicionario_municipality <- list(
  code_muni = c("cod_mun", "codmunic", "mun", "municipio_codigo", "geocod", "cod",
                "cod_muni", "cd_mun", "geocodigo", "geocodig_m", "br91poly_i",
                "cd_geocodm", "cd_geocmu", "geo_mun", "codigo"),
  name_muni = c("nome_cidade", "cidade", "nm_muni", "nome_muni", 
                "nome_munic", "nm_municip", "nm_mun", "municipio", "nomemuni",
                "nomemunicp", "nome")
  )


dicionario_state <- list(
  code_state = c("cd_estado", "uf", "coduf", "estado_codigo", "cod_uf", "codigo",
                 "cd_uf", "code_uf", "codigo_uf", "cod_state", "cd_geocodu", 
                 "cd_geocuf", "geo_uf"),
  # abbrev_state = c("sigla", "sigla_uf", "sg_uf", "uf"),
  name_state = c("nm_uf", "nm_state", "nm_estado", "nome")
)



dicionario_micro <- list(
  code_micro = c("codigo", "cd_geocmi", "geocodigo", "cd_geocodu", "cd_micro"),
  name_micro = c("nome", "nm_micro")
)

dicionario_meso <- list(
  code_meso = c("codigo", "cd_geocme", "geocodigo", "cd_geocodu", "cd_meso"),
  name_meso = c("nome", "nm_meso")
)

dicionario_intermediate <- list(
  code_intermediate = c("cd_rgint", "rgint"),
  name_intermediate = c("nm_rgint", "nome_rgint")
)

dicionario_immediate <- list(
  code_immediate = c("cd_rgi", "rgi"),
  name_immediate = c("nm_rgi", "nome_rgi"),
  code_intermediate = c("cd_rgint", "rgint"),
  name_intermediate = c("nm_rgint", "nome_rgint")
)


dicionario_region <- list(
  code_region = c("cd_regia", "cd_regiao"),
  name_region = c("nm_regia", "nm_regiao"),
  abbrev_region = c("sigla_rg", "sg_rg")
)

dicionario_biomes <- list(
  code_biome = c("cd_bioma", "cod_bioma"),
  name_biome = c("nom_bioma", "bioma", "nm_bioma")
)

dicionario_health_region <- list(
  code_health_region = c("co_regsau", "cd_regsau", "co_regsaud", "cd_regsaud",
                         "co_regiao_saude"),
  name_health_region = c("nm_regsau", "nm_regsau", "no_regsaud", "nm_regsaud",
                         "no_regiao_saude"),
  code_state = c("cod_uf", "cd_uf", "codigo_uf", "co_uf")
)

# Original FUNAI columns:
# gid, terrai_cod, terrai_nom, etnia_nome, municipio_, uf_sigla,
# superficie, fase_ti, modalidade, reestudo_t, cr, faixa_fron,
# undadm_cod, undadm_nom, undadm_sig, dominio_un, geometry
dicionario_indigenousland <- list(
  code_indigenous_land = c("terrai_cod"),
  name_indigenous_land = c("terrai_nom"),
  name_ethnic_group    = c("etnia_nome"),
  name_muni            = c("municipio_"),
  abbrev_state         = c("uf_sigla"),
  area_ha              = c("superficie"),
  coordination_region  = c("cr"),
  code_adm_unit        = c("undadm_cod"),
  name_adm_unit        = c("undadm_nom"),
  abbrev_adm_unit      = c("undadm_sig")
)

# microregions_raw <- tar_read(microregions_raw, branches = 2)
# rename_cols_geobr(microregions_raw, dicionario_micro) |> 
#   head()

# Função para aplicar a padronização de colnames ------------------------------------------

rename_cols_geobr <- function(df, dict) {
  
  # original names
  original_names <- names(df)
  
  # normalized version for flexible matching
  normalize <- function(x) gsub("[^a-z0-9]", "", tolower(x))
  
  normalized_names <- normalize(original_names)
  
  # iterate over dictionary
  for (new_name in names(dict)) {
    candidates <- dict[[new_name]]
    normalized_candidates <- normalize(candidates)
    
    # find match
    idx <- match(normalized_candidates, normalized_names)
    idx <- idx[!is.na(idx)]  # keep only matches
    
    if (length(idx) > 0) {
      # rename: only first match if several exist
      names(df)[idx[1]] <- new_name  
    }
  }
  
  return(df)
}







