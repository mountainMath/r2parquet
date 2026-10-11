# Lazy tables for parquet files at public URLs, on R2 (r2://bucket/path) or at local paths,
# read with DuckDB, arrow or polars.

# session cache for the duckdb connection
parquet_cache <- new.env(parent=emptyenv())

# cached in-memory DuckDB connection for parquet_tbl, shared by all tables so they can be joined
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
#' The `engine` determines what gets returned:
#' * `"duckdb"`, the default, returns a lazy `dplyr` table on a DuckDB connection that is cached for the session,
#'   so that tables from several calls can be joined. `dplyr` verbs get translated to SQL. If the table has
#'   GEOMETRY columns, like GeoParquet files written by `data_to_parquet`, the `spatial` extension gets loaded
#'   so that spatial functions like `ST_Area` can be used in queries. Use `filter_spatial` to filter by location
#'   and `collect_parquet` to collect the result with its metadata attached, as an sf object if it has geometry,
#'   both only work with this engine. `parquet_metadata` reads the metadata of the table's files.
#' * `"arrow"` returns an arrow `Dataset` that can be queried with `dplyr` verbs. Needs the arrow package, and
#'   can't read URLs, only paths on R2 and local paths. As of arrow 25 the GEOMETRY columns of GeoParquet files
#'   written by `data_to_parquet` come back as plain binary (WKB) columns without their crs.
#' * `"polars"` returns a polars `LazyFrame`, executed via `$collect()`, or with the `tidypolars` package loaded
#'   queried with `dplyr` verbs ending in `dplyr::collect()`. Needs the polars package. As of polars 1.16
#'   GeoParquet files written by `data_to_parquet` can't be read, polars rejects the native GEOMETRY type.
#'
#' @param path URL, `r2://bucket/path` or local path of the parquet file or the directory with (partitioned)
#' parquet files, or a vector of these. The arrow and polars engines can't mix paths on R2 with other paths.
#' @param engine `"duckdb"` (the default), `"arrow"` or `"polars"`, see description
#' @param hive_partitioning if `TRUE`, the default, partition columns get derived from `key=value` components of the
#' file paths. This includes the directories above a single file, reading one file out of a partitioned
#' directory, like `data/year=2021/data_0.parquet`, adds a `year` column.
#' @return a lazy `dplyr` table (DuckDB), an arrow `Dataset` or a polars `LazyFrame`, depending on `engine`
#' @export
parquet_tbl <- function(path,engine=c("duckdb","arrow","polars"),hive_partitioning=TRUE) {
  engine <- match.arg(engine)
  is_r2 <- startsWith(path,"r2://")
  is_url <- !is_r2 & grepl("^[a-z0-9]+://",path)
  if (engine!="duckdb" && any(is_r2) && !all(is_r2)) {
    stop("The ",engine," engine can't mix paths on R2 with other paths.")
  }
  if (engine=="duckdb") {
    con <- parquet_duckdb_connection()
    path <- duckdb_parquet_paths(con,path)
    tbl <- duckdb_parquet_tbl(con,path,hive_partitioning=hive_partitioning)
    if (length(duckdb_geometry_columns(tbl))>0) {
      DBI::dbExecute(con,"INSTALL spatial")
      DBI::dbExecute(con,"LOAD spatial")
    }
    # for parquet_metadata and collect_parquet, dbplyr keeps attributes through its verbs
    attr(tbl,"r2parquet_paths") <- path
    tbl
  } else if (engine=="arrow") {
    if (!requireNamespace("arrow",quietly=TRUE)) stop("The arrow package is required for the arrow engine.")
    if (any(is_url)) stop("The arrow engine can't read URLs, use the duckdb or polars engine.")
    partitioning <- if (hive_partitioning) arrow::hive_partition() else NULL
    if (any(is_r2)) {
      # the S3 filesystem addresses files as bucket/path
      path <- sub("/+$","",sub("^r2://","",path))
      arrow::open_dataset(path,filesystem=r2_filesystem(),format="parquet",partitioning=partitioning)
    } else {
      arrow::open_dataset(path,format="parquet",partitioning=partitioning)
    }
  } else {
    if (!requireNamespace("polars",quietly=TRUE)) stop("The polars package is required for the polars engine.")
    storage_options <- NULL
    if (any(is_r2)) {
      path <- sub("^r2://","s3://",r2_parquet_url(path))
      credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
      storage_options <- c(aws_access_key_id=credentials[["R2_ACCESS_KEY_ID"]],
                           aws_secret_access_key=credentials[["R2_SECRET_ACCESS_KEY"]],
                           aws_endpoint_url=r2_endpoint(),
                           aws_region="auto")
    } else {
      path[!is_url] <- local_parquet_glob(path[!is_url])
    }
    polars::pl$scan_parquet(path,hive_partitioning=hive_partitioning,storage_options=storage_options)
  }
}
