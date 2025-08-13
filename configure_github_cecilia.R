library(usethis)

usethis::use_git_config(# Seu nome
  user.name = "Cecília do Lago", 
  # Seu email
  user.email = "ceciliadolago@gmail.com")

usethis::create_github_token()

usethis::edit_r_environ()

usethis::git_sitrep()
