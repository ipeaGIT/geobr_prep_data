# reports/compile_report.R
#
# Compiles the Markdown report (census_tract_2000_implementation.md) into
# self-contained HTML (embedded images) and PDF (xelatex with UTF-8 support).
#
# Usage:
#   "/c/Program Files/R/R-4.5.0/bin/Rscript.exe" reports/compile_report.R
#
# Outputs:
#   reports/census_tract_2000_implementation.html   (1.3 MB, fully self-contained)
#   reports/census_tract_2000_implementation.pdf    (0.3 MB, via xelatex)
#
# Requirements:
#   - rmarkdown, knitr
#   - pandoc (bundled with RStudio or installed separately)
#   - tinytex with xelatex (for PDF)
#
# The script builds a temporary .Rmd wrapper with YAML frontmatter, then
# renders to both formats and cleans up.

suppressPackageStartupMessages({
  library(rmarkdown)
  library(knitr)
})

# Ensure we run from project root
if (!dir.exists("reports") || !file.exists("reports/census_tract_2000_implementation.md")) {
  stop("Run this script from the project root (geobr_prep_data/). ",
       "Current wd: ", getwd())
}

input_md <- "reports/census_tract_2000_implementation.md"

# --- 1. Prepare a temporary .Rmd wrapper with YAML header ---
tmp_rmd <- tempfile(fileext = ".Rmd", tmpdir = "reports")
yaml_header <- c(
  "---",
  "title: \"Implementação do Censo 2000 no pipeline census_tract\"",
  "subtitle: \"geobr_prep_data (IPEA/geobr)\"",
  "author: \"Pipeline documentation\"",
  "date: \"2026-04-11\"",
  "output:",
  "  html_document:",
  "    self_contained: true",
  "    toc: true",
  "    toc_depth: 3",
  "    toc_float: true",
  "    number_sections: false",
  "    theme: cosmo",
  "    highlight: tango",
  "    df_print: paged",
  "    fig_caption: true",
  "  pdf_document:",
  "    toc: true",
  "    toc_depth: 3",
  "    number_sections: false",
  "    latex_engine: xelatex",
  "    fig_caption: true",
  "    extra_dependencies:",
  "      - \"hyperref\"",
  "      - \"xcolor\"",
  "      - \"longtable\"",
  "      - \"booktabs\"",
  "lang: pt-BR",
  "---",
  "",
  "```{r setup, include=FALSE}",
  "knitr::opts_chunk$set(echo = FALSE, eval = FALSE,",
  "                      warning = FALSE, message = FALSE)",
  "```",
  ""
)

# Read the original MD and strip the duplicate title (first # heading)
md_content <- readLines(input_md, encoding = "UTF-8")
first_h1 <- which(grepl("^# Implementação", md_content))[1]
if (!is.na(first_h1)) {
  md_content <- md_content[-first_h1]
}

writeLines(c(yaml_header, md_content), tmp_rmd, useBytes = TRUE)
cat("Wrote wrapper:", tmp_rmd, "\n")

# --- 2. Render HTML (self-contained, all images as base64) ---
cat("\n=== Rendering HTML ===\n")
html_out <- tryCatch({
  rmarkdown::render(
    tmp_rmd,
    output_format = "html_document",
    output_file = "census_tract_2000_implementation.html",
    output_dir = "reports",
    quiet = TRUE,
    envir = new.env()
  )
}, error = function(e) {
  cat("HTML render ERROR:", conditionMessage(e), "\n"); NULL
})
if (!is.null(html_out)) {
  cat("  output:", html_out, "\n")
  cat("  size:", round(file.size(html_out) / 1e6, 2), "MB\n")
}

# --- 3. Render PDF (xelatex for UTF-8 / Portuguese support) ---
cat("\n=== Rendering PDF ===\n")
pdf_out <- tryCatch({
  rmarkdown::render(
    tmp_rmd,
    output_format = "pdf_document",
    output_file = "census_tract_2000_implementation.pdf",
    output_dir = "reports",
    quiet = TRUE,
    envir = new.env()
  )
}, error = function(e) {
  cat("PDF render ERROR:", conditionMessage(e), "\n"); NULL
})
if (!is.null(pdf_out)) {
  cat("  output:", pdf_out, "\n")
  cat("  size:", round(file.size(pdf_out) / 1e6, 2), "MB\n")
}

# --- 4. Cleanup temp .Rmd and LaTeX artifacts ---
file.remove(tmp_rmd)
# Remove LaTeX log files if any
for (ext in c(".log", ".aux", ".out", ".toc")) {
  stray <- file.path("reports",
                     paste0("census_tract_2000_implementation", ext))
  if (file.exists(stray)) file.remove(stray)
}

# --- 5. Summary ---
cat("\n=== Summary ===\n")
for (f in c("reports/census_tract_2000_implementation.md",
            "reports/census_tract_2000_implementation.html",
            "reports/census_tract_2000_implementation.pdf")) {
  if (file.exists(f)) {
    cat(sprintf("  %-55s %7.2f MB\n", f, file.size(f) / 1e6))
  } else {
    cat(sprintf("  %-55s NOT FOUND\n", f))
  }
}
