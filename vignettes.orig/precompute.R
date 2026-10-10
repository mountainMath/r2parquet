# Precompute the vignettes.
#
# The vignettes query files on R2 and download DuckDB extensions, and vignettes
# that show uploads need R2 credentials, none of which a package build, a CRAN
# check or the pkgdown action should depend on. So each vignette is *written*
# here, in `vignettes.orig/`, and knitted into the `vignettes/` directory that
# ships, where the `.Rmd` that R CMD build sees holds the results already.
#
# Both directories hold plain `.Rmd` files, so an editor treats the source as
# R Markdown; `vignettes.orig/` is .Rbuildignore'd, so none of it is shipped.
#
#   Rscript vignettes.orig/precompute.R                            # all of them
#   Rscript vignettes.orig/precompute.R accessing_public_buckets   # just this one
#
# Commit the resulting `vignettes/*.Rmd` together with any figures it wrote.
# Mind what ends up in the output: presigned URLs carry the account id and
# access key id and stay valid until they expire, so don't print them.

# Where this script lives, so it works from the package root or from anywhere
# else. `--file=` is absent when the script is sourced rather than Rscript'ed,
# in which case fall back to the package root's own layout.
this <- grep("^--file=", commandArgs(), value = TRUE)
src_dir <- if (length(this)) dirname(sub("^--file=", "", this[1])) else
  "vignettes.orig"
src_dir <- normalizePath(src_dir, mustWork = TRUE)
out_dir <- normalizePath(file.path(src_dir, "..", "vignettes"), mustWork = TRUE)

devtools::load_all(file.path(src_dir, ".."), quiet = TRUE)

# duckdb announces where it keeps its extensions on each connection of a
# non-interactive session, naming this machine's home directory, and a chunk
# that connects would carry that into the vignette. Stating the choice it would
# make anyway, once, is what it documents as the way to be told no more.
if ("shared_home" %in% names(formals(duckdb::duckdb))) {
  duckdb::duckdb_shutdown(duckdb::duckdb(
    shared_home = dir.exists(file.path("~", ".duckdb"))))
}

available <- sub("\\.Rmd$", "", list.files(src_dir, pattern = "\\.Rmd$"))
wanted <- sub("\\.Rmd$", "", commandArgs(trailingOnly = TRUE))
if (!length(wanted)) wanted <- available

unknown <- setdiff(wanted, available)
if (length(unknown)) {
  stop("No such vignette source: ", paste(unknown, collapse = ", "),
       ". Available: ", paste(available, collapse = ", "))
}

# knit from within `vignettes/`, because `fig.path` is relative to the working
# directory and the figures have to land beside the `.Rmd` that ships.
old <- setwd(out_dir)
on.exit(setwd(old), add = TRUE)

for (v in wanted) {
  message("knitting ", v)
  # figures of an earlier run that the vignette no longer produces would ship otherwise
  unlink(list.files(".", pattern = paste0("^", v, "-.*\\.png$")))
  knitr::knit(file.path(src_dir, paste0(v, ".Rmd")), paste0(v, ".Rmd"))

  # Say so in the file itself, so an edit to the wrong copy is obvious.
  lines <- readLines(paste0(v, ".Rmd"), warn = FALSE)
  end <- if (identical(trimws(lines[1]), "---")) {
    which(trimws(lines[-1]) == "---")[1] + 1L
  } else 0L
  writeLines(append(lines, c(
    paste0("<!-- Generated from vignettes.orig/", v, ".Rmd -- do not edit. -->"),
    "<!-- Rebuild with: Rscript vignettes.orig/precompute.R -->"),
    after = end), paste0(v, ".Rmd"))
}

# the session-cached connections of parquet_tbl() and r2_parquet_tbl()
for (cache in list(parquet_cache, r2_cache)) {
  con <- cache$duckdb_connection
  if (!is.null(con) && DBI::dbIsValid(con)) DBI::dbDisconnect(con, shutdown = TRUE)
}
