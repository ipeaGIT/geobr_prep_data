#> DATASET: Brazilian semi-arid
#> Source: IBGE - https://www.ibge.gov.br/geociencias/cartas-e-mapas/mapas-regionais/15974-semiarido-brasileiro.html?=&t=downloads
#> Metadata:
# Titulo: Semiárido brasileiro
# Titulo alternativo: Semiarido brasileiro
# Frequencia de atualizacao: Ocasional
#
# Forma de apresentacao: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígonos e Pontos do semiárido brasileiro.
# Informacoes adicionais: Dados produzidos pelo IBGE com base em decretos administrativos do Ministério da Integração Nacional.
# -"Resolução nº 115 do Ministério da Integração Nacional, de 23 de novembro de 2017"
# -"Portaria N°89 de 16 de março de 2005, do Ministério da Integração Nacional"
# Proposito: Identificação do clima semiárido brasileiro.

# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000



##### Download the data  -----------------
download_semiarid <- function(year){ # year = 2022

  # #### 0. Create file directory  -----------------
  # # Directory to keep raw zipped files and cleaned files
  # dir.create("./data_raw/semiarid", showWarnings = FALSE)
  # dir.create("./data/semiarid", showWarnings = FALSE)
  # dir_raw <- paste0("./data_raw/semiarid/", year)
  # dir.create(dir_raw, recursive = T, , showWarnings = FALSE)
  

  #### 1. Download original data sets from source website -----------------

  # get correct ftp url link

  if(year == 2005) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_2005a2017/lista_municipios_semiarido.xls'
  }

  if (year == 2017) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_23nov2017/lista_municipios_Semiarido_2017_11_23.xlsx'
  }

  if (year == 2021) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_2021/lista_municipios_Semiarido_2021.xls'
  }

  if (year == 2022) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_2022/lista_municipios_Semiarido_2022.xlsx'
  }


  # directions to download file
  # file_raw <- paste0(dir_raw,"/", year, "_", basename(ftp))
  file_raw <-fs::file_temp(ext = fs::path_ext(ftp))

  #### 2. Download  -----------------
  httr::GET(url = ftp,
            httr::progress(),
            httr::write_disk(path = file_raw,
                             overwrite = T))

  
  if (year == 2005){
    # read IBGE data frame
    munis_semiarid <- readxl::read_xls(path = file_raw,
                                       skip = 1, n_max = 1133)
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = `Código do Município`,
                                    name_muni = `Nome do Município`)
  }
  
  if (year == 2017){
    # read IBGE data frame
    munis_semiarid <- readxl::read_xlsx(path = file_raw,
                                        skip = 1, n_max = 1262)
    
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = `Código do Município`,
                                    name_muni = `Nome do Município`)
  }
  
  if (year == 2021) {
    # read IBGE data frame
    munis_semiarid <- readxl::read_xls(path = file_raw,
                                       n_max = 1263)
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = CD_MUN,
                                    name_muni = NM_MUN)
  }
  
  if (year == 2022) {
    # read IBGE data frame
    munis_semiarid <- readxl::read_xlsx(path = file_raw,
                                        n_max = 1477)
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = CD_MUN,
                                    name_muni = NM_MUN)
  }
  
  munis_semiarid <- munis_semiarid |>
    mutate( year = year)
  
  return(munis_semiarid)
}


# tar_load(names = munis_semiarid, branches = 1)
# tar_load(names = years_semiarid, branches = 1)



##### Clean the data   -----------------
clean_semiarid <- function(munis_semiarid, year) { 
  
  2+2
  
  #### 0. Create folders to save clean sf.rds files  -----------------
  dir_clean <- paste0("./data/semiarid/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  
  
  #### 2. Clean data set -----------------
  
  # load all munis sf
  all_munis <- geobr::read_municipality(code_muni = 'all',
                                        year = year,
                                        simplified = FALSE)
  
  # subset municipalities
  temp_sf <- subset(all_munis, code_muni %in% munis_semiarid$code_muni)
  
  # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
  temp_sf <- harmonize_projection(temp_sf)
  
  # Make any invalid geometry valid # st_is_valid( sf)
  temp_sf <- fix_topoly(temp_sf)
  
  # 4 lighter version
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  #### 3. Save data set -----------------
  sf::st_write(temp_sf, dsn= paste0(dir_clean,"/semiarid_", year, ".gpkg"), delete_dsn=TRUE)
  sf::st_write(temp_sf_simplified, dsn= paste0(dir_clean,"/semiarid_", year, "_simplified.gpkg"), delete_dsn=TRUE )
  return(dir_clean)
}

# Clean every semiarid database  -----------------
# clean_semiarid( download_semiarid(2005), 2005 )
#  clean_semiarid( download_semiarid(2017) )
