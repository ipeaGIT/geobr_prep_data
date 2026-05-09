# files <- tar_read(all_files, branch=1)
# files <- list.files(path = "./data/", recursive = T, full.names = T)
#
# versao_dados <- tar_read(versao_dados)
upload_arquivos <- function(files, versao_dados) {
  
  # # tenta criar um release pra fazer upload do cnefe padronizado. se release já
  # # existe a função retorna um warning. no caso, fazemos um "upgrade" de warning
  # # pra erro
  # 
  # tryCatch(
  #   piggyback::pb_release_create(
  #     "ipeaGIT/geobr",
  #     tag = versao_dados,
  #     body = paste("Dados geobr", versao_dados), prerelease = TRUE
  #   )
  # )
  # 
  # o github tem um pequeno lagzinho pra identificar que o release foi criado,
  # então nós damos um sleep de 2 segundos aqui só pra garantir que a chamada
  # abaixo da pb_upload() não dá ruim.
  # issue relacionado: https://github.com/ropensci/piggyback/issues/101
  # Sys.sleep(10)
  
  # remover repeticoes
  files <- unique(files)
  
  # only parquet files
  files <- files[files %like% ".parquet"]
  
  piggyback::pb_upload(
    files,
    repo = "ipeaGIT/geobr",
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

erro_release_existente <- function(versao_dados) {
  cli::cli_abort(
    c(
      "O release {.val {versao_dados}} j\u00e1 existe.",
      "i" = "Por favor, use uma nova tag ou apague o release existente."
    ),
    call = rlang::caller_env(n = 5)
  )
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
