# Lazy DuckDB tables for parquet files at public URLs or local paths, no credentials needed.
# See r2.R for access to private files on R2.

# session cache for the duckdb connection
parquet_cache <- new.env(parent=emptyenv())

# cached in-memory DuckDB connection for parquet_tbl
parquet_duckdb_connection <- function(){
  con <- parquet_cache$duckdb_connection
  if (is.null(con) || !DBI::dbIsValid(con)) {
    con <- DBI::dbConnect(duckdb::duckdb())
    # avoids downloading the parquet file footers again for every query
    DBI::dbExecute(con,"SET parquet_metadata_cache = true")
    parquet_cache$duckdb_connection <- con
  }
  con
}

#' lazy table to query parquet files at URLs or local paths
#'
#' @description
#' Lazy `dplyr` table backed by DuckDB, for parquet files at public URLs, like files in public R2 buckets,
#' or at local paths. No credentials are needed, see `r2_parquet_tbl` for private files on R2.
#' No data is downloaded until the query is executed, e.g. via `dplyr::collect()`, and DuckDB only requests
#' the parts of the files it needs to answer the query.
#'
#' The `httpfs` DuckDB extension gets installed and loaded for URLs. If the table has GEOMETRY columns, like
#' GeoParquet files written by `data_to_parquet`, the `spatial` extension gets installed and loaded too, so that
#' spatial functions like `ST_Area` can be used in queries. Use `filter_spatial` to filter by location and
#' `collect_sf` to collect the result as an sf object.
#'
#' Local directories get read as all parquet files within them, URLs have to point to files since directories
#' can't be listed over https.
#'
#' @param path URL or local path of the parquet file or local directory with (partitioned) parquet files,
#' or a vector of these
#' @param hive_partitioning if `TRUE`, the default, partition columns get derived from `key=value` components of the file paths
#' @param con DuckDB connection, defaults to a connection that is cached for the session so that tables from
#' several calls can be joined. Pass `r2_duckdb_connection()` to join with tables from `r2_parquet_tbl`.
#' @return a lazy `dplyr` table
#' @export
parquet_tbl <- function(path,hive_partitioning=TRUE,con=parquet_duckdb_connection()) {
  is_url <- grepl("^[a-z0-9]+://",path)
  is_directory <- !is_url & dir.exists(path)
  path[is_directory] <- file.path(sub("/+$","",path[is_directory]),"**","*.parquet")
  if (any(is_url)) {
    DBI::dbExecute(con,"INSTALL httpfs")
    DBI::dbExecute(con,"LOAD httpfs")
  }
  tbl <- duckdb_parquet_tbl(con,path,hive_partitioning=hive_partitioning)
  if (length(duckdb_geometry_columns(tbl))>0) {
    DBI::dbExecute(con,"INSTALL spatial")
    DBI::dbExecute(con,"LOAD spatial")
  }
  tbl
}
