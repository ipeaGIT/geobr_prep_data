# Harmonize Geobr -----------------
harmonize_geobr <- function(temp_sf, 
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
    if(! state_column %in% names(temp_sf)){
      stop(paste("The data temp_sf does not have a columna named ", state_column))
    }
    temp_sf <- add_state_info(temp_sf)
  }
  
  
  ## add region colum
  if (isTRUE(add_region)) {
    
    # check if "state_column" is in data
    if(! region_column %in% names(temp_sf)){
      stop(paste("The data temp_sf does not have a columna named ", state_column))
    }
    temp_sf <- add_region_info(temp_sf)
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
  
  # convert to multipolygon
  if (isTRUE(use_multipolygon)) {
    temp_sf <- to_multipolygon(temp_sf)
  }
  
  return(temp_sf)
}



# Add State abbreviation -----------------
add_state_info <- function(temp_sf, column){
  options(encoding = "UTF-8")
  
  # IF only the "name_state" column is present
  # Add code_state
  if (!is.null(temp_sf$code_muni) & "name_state" %in% names(temp_sf) ) {
    temp_sf <- dplyr::mutate(code_state = ifelse(name_state== "Rondonia" | name_state== "Territ\u00f3rio de Rondonia"  | name_state== "Territorio de Rondonia",11,
                                                 ifelse(name_state== "Acre" | name_state== "Territ\u00f3rio do Acre",12,
                                                        ifelse(name_state== "Amazonas",13,
                                                               ifelse(name_state== "Roraima" | name_state=="Territ\u00f3rio de Roraima",14,
                                                                      ifelse(name_state== "Par\u00e1",15,
                                                                             ifelse(name_state== "Amap\u00e1" | name_state=="Territorio do Amapa",16,
                                                                                    ifelse(name_state== "Tocantins",17,
                                                                                           ifelse(name_state== "Maranh\u00e3o",21,
                                                                                                  ifelse(name_state== "Piaui" | name_state== "Piauhy",22,
                                                                                                         ifelse(name_state== "Cear\u00e1",23,
                                                                                                                ifelse(name_state== "Rio Grande do Norte",24,
                                                                                                                       ifelse(name_state== "Paraiba" | name_state== "Parahyba",25,
                                                                                                                              ifelse(name_state== "Pernambuco",26,
                                                                                                                                     ifelse(name_state== "Alagoas" | name_state=="Alag\u00f4as",27,
                                                                                                                                            ifelse(name_state== "Sergipe",28,
                                                                                                                                                   ifelse(name_state== "Bahia",29,
                                                                                                                                                          ifelse(name_state== "Minas Gerais" | name_state== "Minas Geraes",31,
                                                                                                                                                                 ifelse(name_state== "Espirito Santo" | name_state== "Esp\\u00edrito Santo",32,
                                                                                                                                                                        ifelse(name_state== "Rio de Janeiro",33,
                                                                                                                                                                               ifelse(name_state== "S\u00e3o Paulo",35,
                                                                                                                                                                                      ifelse(name_state== "Paran\u00e1",41,
                                                                                                                                                                                             ifelse(name_state== "Santa Catarina" | name_state== "Santa Catharina",42,
                                                                                                                                                                                                    ifelse(name_state== "Rio Grande do Sul",43,
                                                                                                                                                                                                           ifelse(name_state== "Mato Grosso do Sul",50,
                                                                                                                                                                                                                  ifelse(name_state== "Mato Grosso" | name_state== "Matto Grosso",51,
                                                                                                                                                                                                                         ifelse(name_state== "Goi\u00e1s" | name_state== "Goyaz",52,
                                                                                                                                                                                                                                ifelse((name_state== "Distrito Federal" | name_state=="Brasilia") & (year>1950),53,NA
                                                                                                                                                                                                                                ))))))))))))))))))))))))))))
  }
  
  # IF there is no "name_state" column
  if (column != 'name_state'){
    
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


# Add Region info -----------------
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


# snake case names ---------------------------------------------------
snake_case_names <- function(temp_sf, colname){
  
  # Capitalize the first letter
  temp_sf[[ colname ]] <- stringr::str_to_title( temp_sf[[ colname ]] )
  
  # prepositions to lower
  temp_sf[[ colname ]] <- gsub(" Do ",  " do ",   temp_sf[[ colname ]] )
  temp_sf[[ colname ]] <- gsub(" Dos ", " dos ", temp_sf[[ colname ]] )
  temp_sf[[ colname ]] <- gsub(" Da ",  " da ",   temp_sf[[ colname ]] )
  temp_sf[[ colname ]] <- gsub(" Das ", " das ", temp_sf[[ colname ]] )
  temp_sf[[ colname ]] <- gsub(" De ",  " de ",   temp_sf[[ colname ]] )
  temp_sf[[ colname ]] <- gsub(" Del ", " del ",   temp_sf[[ colname ]] )
  temp_sf[[ colname ]] <- gsub(" D'",   " d'",   temp_sf[[ colname ]] )
  
  return(temp_sf)
}


# Harmonize spatial projection -----------------

# Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674

harmonize_projection <- function(temp_sf){
  
  temp_sf <- if (is.na(sf::st_crs(temp_sf))) {
    sf::st_set_crs(temp_sf, 4674)
  } else {
    sf::st_transform(temp_sf, 4674)
  }
  sf::st_crs(temp_sf) <- 4674
  
  return(temp_sf)
}




# Use UTF-8 encoding -----------------
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


# convert to MULTIPOLYGON -----------------

# to_multipolygon <- function(temp_sf){
# if( st_geometry_type(temp_sf) |> unique() |> as.character() |> length() > 1 |
#     any(  !( st_geometry_type(temp_sf) |> unique() |> as.character() %like% "MULTIPOLYGON|GEOMETRYCOLLECTION"))) {
#   # remove linstring
#   temp_sf <- subset(temp_sf, st_geometry_type(temp_sf) |> as.character() != "LINESTRING")
#   temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
#   return(temp_sf)
# }else{ return(temp_sf)}}

to_multipolygon <- function(temp_sf){
  
  # get geometry types
  geom_types <- sf::st_geometry_type(temp_sf) |> unique() |> as.character()
  
  # checks
  if (length(geom_types) > 1 | any(  !( data.table::like(geom_types,"MULTIPOLYGON")))) {
    
    # remove linestring
    temp_sf <- subset(temp_sf, sf::st_geometry_type(temp_sf) |> as.character() != "LINESTRING")
    
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
    dplyr::summarise()
  
  
  temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
  
  return(temp_sf)
}




# Fix topology -----------------

fix_topology <- function(temp_sf){
  
  ### Attempt 1
  sf::sf_use_s2(TRUE)
  temp_sf <- sf::st_make_valid(temp_sf)
  
  # 1) Find the offending feature(s)
  bad_ix <- which(!st_is_valid(temp_sf))
  
  if (length(bad_ix) == 0) {
    return(temp_sf)
  }
  
  # # detect problematic edges
  # poly_invalid <- temp_sf[bad_ix, ]
  # problem_edges <- st_is_valid(poly_invalid, reason=TRUE)
  # problem_edges <- sub(".*:\\D*(\\d+).*", "\\1", problem_edges) |> as.numeric()
  # all_edges <- sf::st_cast(poly_invalid, "POINT") |> suppressWarnings()
  # problem_edges <- all_edges[problem_edges, ]
  # mapview::mapview(poly_invalid) + problem_edges
  
  ### Attempt 2
  
  # 2) Try lwgeom’s make_valid first (sometimes succeeds where sf:: does not)
  geom <- st_geometry(temp_sf)
  geom[bad_ix] <- lwgeom::lwgeom_make_valid(geom[bad_ix])
  
  st_geometry(temp_sf) <- geom
  
  # 3) Find the offending feature(s)
  still_bad <- which(!st_is_valid(temp_sf))
  
  if (length(still_bad) == 0) {
    return(temp_sf)
  }
  
  ### Attempt 3 - rebuild polygon: if still invalid, rebuild via node + polygonize
  
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
      st_geometry(temp_sf)[i] <- fix_one(st_geometry(temp_sf)[i])
    }
  }  
  
  # 3) Find the offending feature(s)
  still_bad <- which(!st_is_valid(temp_sf))
  
  if (length(still_bad) == 0) {
    return(temp_sf)
  } else(stop("Topology errors could not be fixed."))
  
}



# Dissolve borders temp_sf -----------------

## Function to clean and dissolve the borders of polygons by groups
dissolve_polygons <- function(mysf, group_column){
  
  
  # a) make sure we have valid geometries
  mysf <- fix_topology(mysf) #change to fix_topology? #666
  # Before fix_topoly
  
  # b) make sure we have sf MULTIPOLYGON
  #temp_sf1 <- temp_sf |> st_cast("MULTIPOLYGON")
  temp_sf1 <- to_multipolygon(mysf)
  
  # c) long but complete dissolve function
  dissolvefun <- function(grp){
    
    # c.1) subset region
    temp_region <- subset(mysf, get(group_column, mysf)== grp ) |> 
      ungroup() ### 666 added ungroup because of error
    
    
    temp_region <- summarise(temp_region, .by = group_column)
    # plot(temp_region)
    
    # temp_sf3 <- temp_sf |>
    #   mutate(geometry = s2::as_s2_geography(geometry)) |>
    #   group_by(Bioma, CD_Bioma) |>
    #   summarise(geometry = s2::s2_union_agg(geometry)) |>
    #   mutate(geometry = st_as_sfc(geometry))
    
    temp_region <- sfheaders::sf_remove_holes(temp_region)
    temp_region <- fix_topology(temp_region) #666 fix_topoly?
    
    return(temp_region)
  }
  
  
  # Apply sub-function
  groups_sf <- pbapply::pblapply(X = unique(get(group_column, mysf)), FUN = dissolvefun )
  
  # rbind results
  temp_sf <- do.call('rbind', groups_sf)
  return(temp_sf)
}

# # test
# states <- geobr::read_state(year=2000)
# a <- dissolve_polygons(states, group_column='code_region')
# plot(a)

# Folder creation geobr function -----------------

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

# Unzip geobr function -----------------

unzip_geobr <- function(zip_dir, in_zip, out_zip = NULL, is_shp = FALSE) {
  
  if (is.null(in_zip)) {
    # unzip folder
    in_zip <- paste0(zip_dir, "/unzipped/")
    dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
    dir.exists(in_zip)
  }
  
  if (is.null(out_zip)) {
    # unzip folder
    out_zip <- paste0(zip_dir, "/zipped/")
    dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
    dir.exists(out_zip)
  }
  
  # directory of zips
  zip_names <- list.files(in_zip, pattern = "\\.zip", full.names = TRUE)
  
  # Delimit a number of files
  
  if (is_shp == TRUE){ 
    files_inzip <- map(
      zip_names, 
      function(x) {
        unzip(x, list = TRUE)
      }
    )
    
    shp_delimit <- "shp|cpg|dbf|prj|shx|xml|sbn|sbx"
    
    files_delimit <- map(
      files_inzip, function(x) {
        str_subset(x$Name, pattern = shp_delimit)
      }
    )
    
  } else {NULL}
  
  # unzip part 
  
  if (is_shp == TRUE){ 
    imap(
      zip_names, 
      function(x, idx) {
        unzip(x,
              exdir = out_zip,
              files = files_delimit[[idx]])
      },
      .progress = TRUE)
  } else {
    map(
      zip_names, 
      function(x) {
        unzip(x,
              exdir = out_zip)
      },
      .progress = TRUE)
  }
}

# Collumns names geobr function -----------------

glimpse_geobr <- function(data = NULL, filenames = NULL,
                          multiple_files = FALSE) {
  
  # if (multiple_files == TRUE){ 
  #   
  # } else {
    
    table_collumns <- tibble(name_collum = colnames(data),
                             type_collum = sapply(data, class)) |> 
      rownames_to_column(var = "num_collumn")
  #}
  
  return(table_collumns)
  
}
  
# Robust sf read geobr function -----------------

readmerge_geobr <-  function(folder_path
                             #, encoding
                             ) {
  
  # lista todos os .shp na pasta (busca recursiva opcional)
  shp_files <- list.files(folder_path, pattern = "\\.shp$", full.names = TRUE)
  
  # choose the encoding
  
  #if (encoding == NULL){
    shp_list <- map(shp_files, function(f) {
      message("Lendo: ", f)
      st_read(f, quiet = TRUE)
    })
  #}
  
  # #UTF8
  # if (encoding == "UTF-8"){ 
  #   shp_list <- map(shp_files, function(f) {
  #     message("Lendo: ", f)
  #     st_read(f, quiet = TRUE, options = "ENCODING=UTF8")
  #   })
  # }
  # 
  # #Latin1
  # if (encoding == "Latin-1"){ 
  #   shp_list <- map(shp_files, function(f) {
  #     message("Lendo: ", f)
  #     st_read(f, quiet = TRUE, options = "ENCODING=LATIN1")
  #   })
  # }
  
  # encontra colunas que estão em todos os shapefiles
  common_cols <- reduce(map(shp_list, names), intersect)
  
  # mantém só as colunas comuns
  shp_list <- map(shp_list, ~ .x[, common_cols])
  
  # une tudo
  merged <- bind_rows(shp_list)
  
  return(merged)
}

# States table codes for correction geobr function -----------------

states_geobr <-  function() {
  states <- tibble(cod_states = c(11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24,
                                  25, 26, 27, 28, 29, 31, 32, 33, 35, 41, 42,
                                  43, 50, 51, 52, 53),
                   sg_state = c("RO", "AC", "AM", "RR", "PA", "AP", "TO",
                                "MA", "PI", "CE", "RN", "PB", "PE", "AL",
                                "SE", "BA", "MG", "ES", "RJ", "SP", "PR",
                                "SC", "RS", "MS", "MT", "GO", "DF"),
                   nm_state = c("Rondônia", "Acre", "Amazonas", "Roraima",
                                "Pará", "Amapá", "Tocantins", "Maranhão",
                                "Piauí", "Ceará", "Rio Grande do Norte",
                                "Paraíba", "Pernambuco", "Alagoas", "Sergipe",
                                "Bahia", "Minas Gerais", "Espírito Santo", 
                                "Rio de Janeiro", "São Paulo", "Paraná",
                                "Santa Catarina", "Rio Grande do Sul",
                                "Mato Grosso do Sul", "Mato Grosso", "Goiás",
                                "Distrito Federal"),
                   nm_region = c("Norte", "Norte", "Norte", "Norte", "Norte",
                                 "Norte", "Norte", "Nordeste", "Nordeste",
                                 "Nordeste", "Nordeste", "Nordeste", "Nordeste",
                                 "Nordeste", "Nordeste", "Nordeste", "Sudeste",
                                 "Sudeste", "Sudeste", "Sudeste", "Sul", "Sul",
                                 "Sul", "Centro-Oeste", "Centro-Oeste",
                                 "Centro-Oeste", "Centro-Oeste"),
                   sgm_state = str_to_lower(sg_state))
  
  return(states)
  }