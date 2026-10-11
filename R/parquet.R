# Lazy DuckDB tables for parquet files at public URLs, on R2 (r2://bucket/path) or at local paths.

# session cache for the duckdb connection
parquet_cache <- new.env(parent=emptyenv())

#' DuckDB connection shared by the parquet functions
#'
#' @description
#' The in-memory DuckDB connection that `parquet_tbl`, `parquet_metadata` and
#' `data_to_parquet` use. It gets created on first use and is cached for the session, so that tables from
#' different `parquet_tbl` calls can be joined with each other. Use it to bring other data onto the same
#' connection, e.g. `dplyr::copy_to(parquet_connection(), df)` to join a data frame with a parquet table, or to
#' change DuckDB settings via `DBI::dbExecute`, like `SET memory_limit = '4GB'` or `SET threads = 2`.
#' Disconnecting it, e.g. to free memory, is fine, the next call creates a new connection, but tables from
#' `parquet_tbl` created on the old connection stop working. `parquet_tbl` can also create its tables on a
#' connection of your own via its `con` argument.
#'
#' @return a DuckDB connection
#' @export
parquet_connection <- function(){
  con <- parquet_cache$duckdb_connection
  if (is.null(con) || !DBI::dbIsValid(con)) {
    con <- DBI::dbConnect(duckdb::duckdb())
    # avoids downloading the parquet file footers again for every query
    DBI::dbExecute(con,"SET parquet_metadata_cache = true")
    parquet_cache$duckdb_connection <- con
  }
  con
}

# register the R2 credentials with the DuckDB connection, replaced on every call so changed credentials get picked up
duckdb_r2_secret <- function(con){
  credentials <- r2_credentials(c("R2_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
  DBI::dbExecute(con,paste0("CREATE OR REPLACE SECRET r2 (TYPE r2",
                            ", KEY_ID ",DBI::dbQuoteString(con,credentials[["R2_ACCESS_KEY_ID"]]),
                            ", SECRET ",DBI::dbQuoteString(con,credentials[["R2_SECRET_ACCESS_KEY"]]),
                            ", ACCOUNT_ID ",DBI::dbQuoteString(con,credentials[["R2_ACCOUNT_ID"]]),
                            # without this the region gets picked up from AWS_DEFAULT_REGION if set, which R2 rejects
                            ", REGION 'auto')"))
}

# lazy table for parquet files readable by duckdb, filters and column selections get pushed down to the parquet scan
duckdb_parquet_tbl <- function(con,urls,hive_partitioning=TRUE){
  urls <- paste0(DBI::dbQuoteString(con,urls),collapse=", ")
  if (grepl(", ",urls,fixed=TRUE)) urls <- paste0("[",urls,"]")
  dplyr::tbl(con,dplyr::sql(paste0("SELECT * FROM read_parquet(",urls,
                                   ", hive_partitioning = ",if (hive_partitioning) "true" else "false",")")))
}

# glob pattern for the parquet files in local directories, other paths unchanged
local_parquet_glob <- function(path){
  is_directory <- dir.exists(path)
  path[is_directory] <- file.path(sub("/+$","",path[is_directory]),"**","*.parquet")
  path
}

# r2://bucket/path with the path resolved to the file or a glob pattern for the files in the directory
r2_parquet_url <- function(path){
  vapply(path,function(p){
    p <- r2_split_path(p)
    paste0("r2://",p$bucket,"/",r2_parquet_glob(p$bucket,p$path))
  },character(1),USE.NAMES=FALSE)
}

# paths resolved to files or glob patterns DuckDB can read, with the httpfs extension and R2 credentials in place
duckdb_parquet_paths <- function(con,path){
  is_r2 <- startsWith(path,"r2://")
  is_url <- !is_r2 & grepl("^[a-z0-9]+://",path)
  path[is_r2] <- r2_parquet_url(path[is_r2])
  path[!is_r2 & !is_url] <- local_parquet_glob(path[!is_r2 & !is_url])
  if (any(is_r2 | is_url)) {
    DBI::dbExecute(con,"INSTALL httpfs")
    DBI::dbExecute(con,"LOAD httpfs")
  }
  if (any(is_r2)) duckdb_r2_secret(con)
  path
}

#' lazy table to query parquet files at URLs, on R2 or at local paths
#'
#' @description
#' Lazy table for parquet files at public URLs, like files in public R2 buckets or presigned URLs, in private
#' R2 buckets, given as `r2://bucket/path`, or at local paths. No data is downloaded until the query is
#' executed, e.g. via `dplyr::collect()`, and only the parts of the files needed to answer the query get read.
#' Filters on partition columns restrict which files are read, other filters and the selection of columns
#' restrict which parts of the files are downloaded. When filtering on integer columns use integer values like
#' `2021L`, comparing to a plain number like `2021` is a floating point comparison that downloads considerably
#' more data.
#'
#' Public URLs and local paths need no credentials. Paths on R2 expect the R2 account id and the access key id
#' and secret access key of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and
#' `R2_SECRET_ACCESS_KEY` environment variables. Directories on R2 are often named like a parquet file,
#' e.g. `r2://bucket/data.parquet`, these get resolved to all parquet files within them.
#' Local directories get read as all parquet files within them, URLs have to point to files since directories
#' can't be listed over https.
#'
#' The table is a lazy `dplyr` table on a DuckDB connection that is cached for the session (`parquet_connection`),
#' so that tables from several calls can be joined, or on the connection given as `con`. `dplyr` verbs get
#' translated to SQL. If the table has GEOMETRY columns, like GeoParquet files written by `data_to_parquet`,
#' the `spatial` extension gets loaded so that spatial functions like `ST_Area` can be used in queries. Use
#' `filter_spatial` to filter by location and `collect_parquet` to collect the result with its metadata
#' attached, as an sf object if it has geometry. `parquet_metadata` reads the metadata of the table's files
#' and `list_parquet` lists the files.
#'
#' @param path URL, `r2://bucket/path` or local path of the parquet file or the directory with (partitioned)
#' parquet files, or a vector of these
#' @param hive_partitioning if `TRUE`, the default, partition columns get derived from `key=value` components of the
#' file paths. This includes the directories above a single file, reading one file out of a partitioned
#' directory, like `data/year=2021/data_0.parquet`, adds a `year` column.
#' @param con DuckDB connection to create the table on. Defaults to `parquet_connection`, the connection shared
#' by all calls. Tables can only be joined when they are on the same connection, so pass your own connection to
#' join with data that lives there. The `httpfs` and `spatial` extensions get installed and loaded on it as
#' needed, and the R2 credentials get registered as a secret.
#' @return a lazy `dplyr` table on DuckDB
#' @export
parquet_tbl <- function(path,hive_partitioning=TRUE,con=NULL) {
  if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
  if (is.null(con)) {
    con <- parquet_connection()
  } else if (!inherits(con,"duckdb_connection")) {
    stop("con has to be a DuckDB connection.")
  }
  path <- duckdb_parquet_paths(con,path)
  tbl <- duckdb_parquet_tbl(con,path,hive_partitioning=hive_partitioning)
  if (length(duckdb_geometry_columns(tbl))>0) {
    DBI::dbExecute(con,"INSTALL spatial")
    DBI::dbExecute(con,"LOAD spatial")
  }
  # for parquet_metadata, list_parquet and collect_parquet, dbplyr keeps attributes through its verbs
  attr(tbl,"r2parquet_paths") <- path
  tbl
}

# one row per parquet file with its size in bytes and modification time in UTC
parquet_files <- function(path,size=NA_real_,modified=NA_real_){
  dplyr::tibble(path=as.character(path),size=as.numeric(size),modified=.POSIXct(as.numeric(modified),tz="UTC"))
}

# the local parquet file at a path, or the parquet files below the directory, nothing if there is neither
local_parquet_files <- function(path){
  files <- if (dir.exists(path)) dir(path,"\\.parquet$",recursive=TRUE,full.names=TRUE) else if (file.exists(path)) path else character()
  info <- file.info(files,extra_cols=FALSE)
  parquet_files(files,info$size,info$mtime)
}

#' list parquet files
#'
#' @description
#' Lists the parquet files at paths on R2 or at local paths with their size and modification time, or the
#' files a lazy table from `parquet_tbl` reads. Directories get listed recursively, so a directory of
#' (partitioned) parquet files lists all the parquet files in it, and a path that does not exist lists nothing.
#' Paths on R2 need the same credentials as `upload_parquet`. URLs can't be listed, so
#' for a table on URLs size and modification time are `NA`.
#'
#' @param x `r2://bucket/path` or local path of a parquet file or of a directory with (partitioned) parquet
#' files, a vector of these, or a lazy DuckDB table from `parquet_tbl`
#' @return a tibble with one row per parquet file and the columns `path` (`r2://bucket/path`, local path or
#' URL), `size` in bytes and `modified`, the modification time in UTC
#' @export
list_parquet <- function(x) UseMethod("list_parquet")

#' @rdname list_parquet
#' @export
list_parquet.character <- function(x) {
  is_r2 <- startsWith(x,"r2://")
  is_url <- !is_r2 & grepl("^[a-z0-9]+://",x)
  files <- lapply(seq_along(x),function(i){
    if (is_r2[i]) r2_parquet_files(x[i]) else if (is_url[i]) parquet_files(x[i]) else local_parquet_files(x[i])
  })
  dplyr::distinct(dplyr::bind_rows(parquet_files(character()),files))
}

#' @rdname list_parquet
#' @export
list_parquet.tbl_lazy <- function(x) {
  path <- attr(x,"r2parquet_paths")
  if (is.null(path)) stop("The table was not created by parquet_tbl, pass the path of the parquet files instead.")
  # parquet_tbl resolves directories to globs for all parquet files in them
  list_parquet(sub("/\\*\\*/\\*\\.parquet$","",path))
}

#' @rdname list_parquet
#' @export
list_parquet.default <- function(x) {
  stop("x has to be a path, a vector of paths or a lazy DuckDB table from parquet_tbl.")
}
