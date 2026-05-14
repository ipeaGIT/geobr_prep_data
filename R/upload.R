# files <- targets::tar_read(all_files, branch=1)
# files <- list.files(path = "./data/", recursive = T, full.names = T)
#
# versao_dados <- targets::tar_read(versao_dados)
upload_arquivos <- function(files, versao_dados) {
  
  # remover repeticoes
  files <- unique(files)
  
  # only parquet files
  files <- files[grepl(".parquet",files)]
  
  piggyback::pb_upload(
    files,
    repo = "ipea/geobr_prep_data",
    tag = versao_dados,
    overwrite = "use_timestamps"
  )
  
  # Error in rawConnection(raw(1000), open = "wb") :
  #   all 128 connections are in use
  
  endereco_release <- paste0(
    "https://github.com/ipeaGIT/geobr/releases/",
    versao_dados
  )
  
  return(endereco_release)
}




# # 66666666666666666666666666666666666
# # delete all files
# all_file_names <- piggyback::pb_list(
#   repo = "ipeaGIT/geobr", 
#   tag = versao_dados
# )
# 
# piggyback::pb_delete(
#   repo = "ipeaGIT/geobr", 
#   tag = versao_dados
# )
