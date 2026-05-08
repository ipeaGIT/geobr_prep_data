#> DATASET: Immediate Geographic Regions - 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata:
# Título: Regiões Geográficas Imediatas
# Título alternativo:
# Frequência de atualização: decenal
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Regiões Geográficas Imediatas foram criadas pelo IBGE em 2017 para substituir as micro-regiões
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000

# Observações: 
# Anos disponíveis: ****************

## Libraries (use any library as necessary) ------------------------------------


# Download the data  -----------------------------------------------------------
download_immediateregions <- function(year){ # year = 2024
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  ###2019
  if(year == 2019) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2019/Brasil/BR/br_regioes_geograficas_imediatas.zip"
  }
  
  ###  2020:2022
  if(year %in% c(2020:2022)) {
    ftp_link <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_",year,"/Brasil/BR/BR_RG_Imediatas_", year,".zip")
  }
  
  if(year >= 2023) {
    ftp_link <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_",year,"/Brasil/BR_RG_Imediatas_", year,".zip")
  }
  
  # ###2022
  # if(year == 2022) {
  #   ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_RG_Imediatas_2022.zip"
  # }
  # 
  # ###2023
  # if(year == 2023) {
  #   ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2023/Brasil/BR_RG_Imediatas_2023.zip"
  # }
  # 
  # ###2024
  # if(year == 2024) {
  #   ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_RG_Imediatas_2024.zip"
  # }
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/immediate_regions/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  
  ## 2. Download Raw data ------------------------------------------------------
  file_raw <- download_file_geobr(
    file_url = ftp_link, 
    dest_dir = zip_dir
  )
  
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  ### unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  unzip_geobr(zip_dir = zip_dir, out_zip = out_zip)
  

  ## 5. Set correct encoding ---------------------------------------------------
  
  if (year >= 2019) {
    encode =  "ENCODING=UTF8"
  }
  
  ## 6. Bind Raw data together -------------------------------------------------
  
  immediateregions_raw <- readmerge_geobr(
    folder_path = out_zip,
    encoding = encode) |> 
    janitor::clean_names()
  
  immediateregions_raw$year <- year
  
  return(immediateregions_raw)
}

# Clean the data  --------------------------------------------------------------
clean_immediateregions <- function(immediateregions_raw){ # year = 2024

  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- immediateregions_raw$year[1]
  dir_clean <- paste0("./data/immediate_regions/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  

  


  ## 2. Rename columns names ---------------------------------------------------
  statesgeobr <- states_geobr()
  
  # standardize colnames
  immediateregions <- rename_cols_geobr(immediateregions_raw, dicionario_immediate) |> 
    dplyr::select(
      dplyr::any_of(c("code_immediate", "name_immediate", 
                      "code_intermediate", "name_intermediate"))
      )
  
  
  immediateregions <- immediateregions |> 
    mutate(code_state = substr(code_immediate, 1, 2)) |> 
    filter(code_immediate != 0)
    
  
  # names_2019 <- c("cd_rgi", "nm_rgi", "sigla_uf", "geometry")
  # names_2020 <- c("cd_rgi", "nm_rgi", "sigla_uf", "geometry")
  # names_2021 <- c("cd_rgi", "nm_rgi", "sigla", "geometry")
  # names_2022 <- c("cd_rgi", "nm_rgi", "sigla_uf", "area_km2", "geometry")
  # names_2023 <- c("cd_rgi", "nm_rgi", "cd_uf", "nm_uf", "cd_regiao", "nm_regiao", "area_km2", "geometry")
  # names_2024 <- c("cd_rgi", "nm_rgi", "cd_rgint", "nm_rgint", "cd_uf", "nm_uf", "sigla_uf", "cd_regia", "nm_regia", "sigla_rg", "area_km2", "geometry")
  
  
  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = immediateregions,
    year = yyyy,
    add_state = T,
    state_column = "code_state",
    add_region = T,
    region_column = "code_state",
    add_snake_case = T,
    snake_colname = "name_immediate",
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )

  ## 3b. Enforce column order (geometry always last)
  temp_sf <- temp_sf |>
    dplyr::select(code_immediate, name_immediate,
                  dplyr::any_of(c("code_intermediate", "name_intermediate")),
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)


  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_immediate)
  

  ## 4. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 5. Save datasets  ---------------------------------------------------------

  ### Save in parquet
  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/immediateregions_", yyyy, ".parquet"))

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean,"/immediateregions_", yyyy, "_simplified", ".parquet"))
  
  ## 6. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}


########################## OLD FILE BELOW HERE ##########

# # if(update == 2019){
# #   temp_sf <- st_read("BR_RG_Imediatas_2019.shp", quiet = F, stringsAsFactors=F, options = "ENCODING=UTF8")
# #
# #   temp_sf <- dplyr::rename(temp_sf, code_immediate = CD_RGI, name_immediate = NM_RGI)
# # }
# #
# # if(update == 2017){
# #
# #   temp_sf <- dplyr::rename(temp_sf, code_immediate = rgi, name_immediate = nome_rgi)
# 
#   # for each file
#   for (i in sf_files){ #  i <- sf_files[1]
# 
#     # read sf file
#     temp_sf <- st_read(i)
#     names(temp_sf) <- names(temp_sf) %>% tolower()
#     head(temp_sf)
# 
# 
#     if (year %like% "2020"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_immediate'=cd_rgi,
#                                           'name_immediate'=nm_rgi,
#                                           'abbrev_state'=sigla_uf,
#                                           'geom'))
#     }
# 

