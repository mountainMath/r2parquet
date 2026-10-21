# Lazy DuckDB tables for parquet files at public URLs, on R2 (r2://bucket/path) or at local paths.

# session cache for the duckdb connection
parquet_cache <- new.env(parent=emptyenv())

# The package's connection class: duckdb's connection, inheriting all its DBI and dbplyr methods, with a
# dbplyr translation of its own (sql_translation.r2parquet_connection) that renders whole numbers in
# comparisons as integers. dbplyr renders every R double with a decimal point, `x == 1` as `x = 1.0`, and
# DuckDB (1.5.6) resolves that against an INTEGER column by casting the column, inside the scan filter, where
# the row group statistics no longer apply, so such a filter downloads every row group. Against a DOUBLE column
# an integer literal is cast on the literal side, so the rewrite is harmless there. Only comparisons are
# rewritten: in arithmetic `x * 1000.0` keeps integer columns from overflowing, which DuckDB reports as an
# error instead of promoting.
methods::setClass("r2parquet_connection",contains="duckdb_connection")

# a connection as the package's connection class, the same underlying connection. Only the session connection
# gets wrapped: dbplyr tells tables apart by the identity of their connection object, so a wrapped copy of a
# connection passed as parquet_tbl(con=) could not be joined with tables the user made on that connection
r2parquet_connection <- function(con){
  if (methods::is(con,"r2parquet_connection")) con else methods::new("r2parquet_connection",con)
}

# whole number doubles within the integer range, as plain vectors (dates are doubles too, they keep their
# class), become integers, with a message once per session; everything else is returned as is
integer_literal <- function(x){
  if (!is.double(x) || is.object(x) || length(x)==0 || !all(is.finite(x) & x==trunc(x) & abs(x)<2^31)) return(x)
  if (!isTRUE(parquet_cache$integer_literal_noted)) {
    parquet_cache$integer_literal_noted <- TRUE
    message("Comparing with the whole number ",format(x[1]),", rendered as the integer ",format(x[1]),"L so that DuckDB can ",
            "skip row groups by the statistics of integer columns. Write integers as ",format(x[1]),"L to avoid this message, ",
            "it is shown once per session.")
  }
  as.integer(x)
}

#' @exportS3Method dbplyr::sql_translation
#' @importClassesFrom duckdb duckdb_connection
sql_translation.r2parquet_connection <- function(con){
  translation <- NextMethod()
  scalar <- translation$scalar
  comparison <- function(operator){
    infix <- dbplyr::sql_infix(operator)
    function(x,y)infix(integer_literal(x),integer_literal(y))
  }
  parent_in <- scalar$`%in%`
  parent_between <- scalar$between
  translation$scalar <- dbplyr::sql_translator(.parent=scalar,
    `==`=comparison("="),`!=`=comparison("<>"),`<`=comparison("<"),`<=`=comparison("<="),`>`=comparison(">"),`>=`=comparison(">="),
    `%in%`=function(x,table)parent_in(x,integer_literal(table)),
    between=function(x,left,right)parent_between(x,integer_literal(left),integer_literal(right)))
  translation
}

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
#' The connection is DuckDB's connection class extended with the package's SQL translation, which renders
#' whole numbers in comparisons as integers, see `parquet_tbl`; everything else works as on any DuckDB
#' connection.
#'
#' @return a DuckDB connection
#' @export
parquet_connection <- function(){
  con <- parquet_cache$duckdb_connection
  if (is.null(con) || !DBI::dbIsValid(con)) {
    con <- r2parquet_connection(DBI::dbConnect(duckdb::duckdb()))
    # avoids downloading the parquet file footers again for every query
    DBI::dbExecute(con,"SET parquet_metadata_cache = true")
    parquet_cache$duckdb_connection <- con
  }
  con
}

# a DuckDB connection for the package's internal use, kept out of RStudio's Connections pane: duckdb registers
# every in-memory connection there as "DuckDB" at ":memory:" (when duckdb.enable_rstudio_connection_pane is on)
# and closing one removes the first entry with that host, not necessarily its own, so the short-lived
# connections of parquet_footers() would leave the pane calling closed connections, including the user's
# own. Only the session connection of parquet_connection() shows up in the pane. The returned function
# disconnects, with the registration suppressed the same way
duckdb_connect_quietly <- function(){
  pane <- options(duckdb.enable_rstudio_connection_pane=FALSE,duckdb.force_rstudio_connection_pane=FALSE)
  on.exit(options(pane))
  con <- DBI::dbConnect(duckdb::duckdb())
  attr(con,"disconnect") <- function(){
    pane <- options(duckdb.enable_rstudio_connection_pane=FALSE,duckdb.force_rstudio_connection_pane=FALSE)
    on.exit(options(pane))
    DBI::dbDisconnect(con,shutdown=TRUE)
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

# install and load a DuckDB extension, the R package turns autoinstall off, so extensions get installed
# explicitly, and spatial functions do not autoload
load_extension <- function(con,name){
  DBI::dbExecute(con,paste0("INSTALL ",name))
  DBI::dbExecute(con,paste0("LOAD ",name))
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
    paste0("r2://",p$bucket,"/",r2_parquet_glob(p$bucket,p$path,p$directory))
  },character(1),USE.NAMES=FALSE)
}

# the URLs of the parquet files a directory at a URL holds, from the manifest next to them since directories
# can't be listed over https, NULL without a manifest if that is tolerated
url_directory_files <- function(url,missing_ok=FALSE){
  base <- paste0(sub("/+$","",sub("[?#].*$","",url)),"/")
  if (!missing_ok) return(url_cache_source(list(url=base))$files$key)
  tryCatch(url_cache_source(list(url=base))$files$key,error=function(e)NULL)
}

# paths resolved to files or glob patterns DuckDB can read, with the httpfs extension and R2 credentials in place;
# directory URLs, ending in a slash, resolve to the files their manifest lists
duckdb_parquet_paths <- function(con,path){
  is_r2 <- startsWith(path,"r2://")
  is_url <- !is_r2 & grepl("^[a-z0-9]+://",path)
  is_url_dir <- is_url & grepl("^https?://",path) & grepl("/$",sub("[?#].*$","",path))
  path[is_r2] <- r2_parquet_url(path[is_r2])
  path[!is_r2 & !is_url] <- local_parquet_glob(path[!is_r2 & !is_url])
  if (any(is_r2 | is_url)) load_extension(con,"httpfs")
  if (any(is_r2)) duckdb_r2_secret(con)
  if (!any(is_url_dir)) return(path)
  as.character(unlist(lapply(seq_along(path),function(i)if (is_url_dir[i]) url_directory_files(path[i]) else path[i])))
}

#' lazy table to query parquet files at URLs, on R2 or at local paths
#'
#' @description
#' Lazy table for parquet files at public URLs, like files in public R2 buckets or presigned URLs, in private
#' R2 buckets, given as `r2://bucket/path`, or at local paths. No data is downloaded until the query is
#' executed, e.g. via `dplyr::collect()`, and only the parts of the files needed to answer the query get read.
#' Filters on partition columns restrict which files are read, other filters and the selection of columns
#' restrict which parts of the files are downloaded.
#'
#' Whole numbers in comparisons (`==`, `!=`, `<`, `<=`, `>`, `>=`, `%in%`, `between`) are rendered as
#' integers: a plain number like `2021` is a double in R, which `dplyr` renders as `2021.0`, and DuckDB
#' resolves that against an integer column by casting the column, which defeats the statistics it uses to
#' skip row groups, so the filter downloads the whole column instead of the matching row groups. The package
#' renders `2021` as the integer `2021`, with a message the first time in a session, and comparisons with
#' double columns are unaffected. Write integers as `2021L` to avoid the message. Arithmetic is not
#' rewritten, `x * 1000` stays a double multiplication. Tables on a connection of your own passed as `con`
#' keep DuckDB's own translation, so write integers as `2021L` there.
#'
#' Directories on R2 are often named like a parquet file,
#' e.g. `r2://bucket/data.parquet`, these get resolved to all parquet files within them.
#' Local directories get read as all parquet files within them. Directories can't be listed over https, so a
#' directory at a URL gets read through the manifest next to its files (`_manifest.json`, which
#' `upload_parquet`, `data_to_parquet` and `write_manifest` write), which lists the files to read. A URL
#' ending in a slash is read as a directory; a URL without one is read as a file first and, if there is no
#' file at it, as a directory, so a directory named like a parquet file works either way.
#'
#' The table is a lazy `dplyr` table on a DuckDB connection that is cached for the session (`parquet_connection`),
#' so that tables from several calls can be joined, or on the connection given as `con`. `dplyr` verbs get
#' translated to SQL. If the table has GEOMETRY columns, like GeoParquet files written by `data_to_parquet`,
#' the `spatial` extension gets loaded so that spatial functions like `ST_Area` can be used in queries. Use
#' `sf::st_filter` to filter by location and `collect_parquet` to collect the result with its metadata
#' attached, as an sf object if it has geometry. `parquet_metadata` reads the metadata of the table's files
#' and `list_parquet` lists the files.
#'
#' Remote files can be kept in a local cache directory via `cache_parquet`, for data that gets queried regularly,
#' and the table then reads the local copies. By default (`cache = NULL`) a cached copy gets used if it is
#' current, checked against the etags of the remote files, and the remote files get read otherwise, with a
#' warning when the copy is out of date. Paths without a copy in the cache are read remotely without any
#' request to check them, so the default costs nothing for them. `cache = TRUE` caches the files first, downloading what is
#' new or changed, and `cache = FALSE` always reads the remote files. In both the default and `cache = TRUE`
#' the cached copy gets used with a warning when the remote can't be reached, e.g. offline.
#' `cache = "offline"` reads the cached copy as it is, without contacting the remote, for working without a
#' network connection, or to compare the copy with the current remote files (`cache = FALSE`) before updating
#' it. Directories at public URLs can only be cached if they have a manifest, see `upload_parquet`.
#'
#' @inheritSection upload_parquet Credentials
#' @param path URL, `r2://bucket/path` or local path of the parquet file or the directory with (partitioned)
#' parquet files, or a vector of these
#' @param hive_partitioning if `TRUE`, the default, partition columns get derived from `key=value` components of the
#' file paths. This includes the directories above a single file, reading one file out of a partitioned
#' directory, like `data/year=2021/data_0.parquet`, adds a `year` column.
#' @param con DuckDB connection to create the table on. Defaults to `parquet_connection`, the connection shared
#' by all calls. Tables can only be joined when they are on the same connection, so pass your own connection to
#' join with data that lives there. The `httpfs` and `spatial` extensions get installed and loaded on it as
#' needed, and the R2 credentials get registered as a secret.
#' @param cache `NULL`, the default, reads the copies in the local cache directory where they exist and are
#' current, with a warning for copies that are out of date, and the remote files otherwise. A copy only gets
#' checked against the remote files the first time its path is read in the session, later calls reuse the
#' result (see `clear_cache_checks`). `TRUE` updates the
#' copies first via `cache_parquet`, downloading the files on first use, and reads them. `FALSE` reads the remote
#' files directly. `"offline"` reads the copies without checking them against the remote files, whether they are
#' current or not, and fails for paths that have no copy. The cache directory is the one in the
#' `R2PARQUET_CACHE_PATH` environment variable, or the user cache directory if that is not set.
#' @return a lazy `dplyr` table on DuckDB
#' @export
parquet_tbl <- function(path,hive_partitioning=TRUE,con=NULL,cache=NULL) {
  if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
  if (is.null(con)) {
    con <- parquet_connection()
  } else if (!methods::is(con,"duckdb_connection")) {
    stop("con has to be a DuckDB connection.")
  }
  if (is.null(cache)) {
    path <- cache_lookup(path)
  } else if (isTRUE(cache)) {
    path <- cache_parquet(path)
  } else if (identical(cache,"offline")) {
    path <- cache_offline(path)
  } else if (!isFALSE(cache)) {
    stop('cache has to be NULL, TRUE, FALSE or "offline".')
  }
  files <- duckdb_parquet_paths(con,path)
  tbl <- tryCatch(duckdb_parquet_tbl(con,files,hive_partitioning=hive_partitioning),error=function(e)e)
  if (inherits(tbl,"error")) {
    # a directory at a URL given without the trailing slash can't be told from a file without a request, so
    # the manifests get tried once reading fails
    is_file_url <- grepl("^https?://",path) & !grepl("/$",sub("[?#].*$","",path))
    is_directory <- is_file_url & vapply(path,function(p)is_file_url[match(p,path)] && !is.null(url_directory_files(p,missing_ok=TRUE)),logical(1))
    if (!any(is_directory)) stop(tbl)
    path[is_directory] <- paste0(sub("[?#].*$","",path[is_directory]),"/")
    files <- duckdb_parquet_paths(con,path)
    tbl <- duckdb_parquet_tbl(con,files,hive_partitioning=hive_partitioning)
  }
  if (length(duckdb_geometry_columns(tbl))>0) load_extension(con,"spatial")
  # for parquet_metadata, list_parquet and collect_parquet, dbplyr keeps attributes through its verbs: the
  # files the table reads, and the paths they were resolved from
  attr(tbl,"r2parquet_paths") <- files
  attr(tbl,"r2parquet_source") <- path
  tbl
}

# sizes in bytes formatted for reading, like 2.5 kB
format_size <- function(size){
  vapply(size,function(s)if (is.na(s)) NA_character_ else format(structure(s,class="object_size"),units="auto",standard="SI"),"")
}

# one row per parquet file with its name, size in bytes, modification time in UTC, cache status, etag and path
parquet_files <- function(path,size=NA_real_,modified=NA_real_,etag=NA_character_,cache=NA_character_){
  path <- as.character(path)
  bytes <- as.numeric(size)
  # tibble() lets later columns refer to earlier ones, so the bytes are not called size here
  dplyr::tibble(file=basename(sub("[?#].*$","",path)),size=format_size(bytes),sizeRaw=bytes,
                modified=.POSIXct(as.numeric(modified),tz="UTC"),cache=as.character(cache),etag=as.character(etag),path=path)
}

# the file at a URL with size and etag from its headers, or the files the manifest of a directory at the URL
# lists, just the URL if there is neither or it can't be reached
url_parquet_files <- function(url){
  source <- if (grepl("^https?://",url)) tryCatch(url_cache_source(list(url=url)),error=function(e)NULL)
  if (is.null(source)) return(parquet_files(url))
  parquet_files(source$files$key,source$files$size,source$files$modified,source$files$etag)
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
#' Lists the parquet files at paths on R2, at URLs or at local paths with their size, modification time and
#' etag, and whether the local cache holds a current copy, or the files a lazy table from `parquet_tbl` reads.
#' Directories get listed recursively, so a directory of (partitioned) parquet files lists all the parquet
#' files in it, and a path that does not exist lists nothing.
#' Files at URLs, including presigned URLs, get their size and etag from a request for their
#' first byte, directories at URLs from the manifest next to the files (see `upload_parquet`). A URL with
#' neither, or one that can't be reached, is listed as it is with `NA`s.
#'
#' @inheritSection upload_parquet Credentials
#' @param x `r2://bucket/path`, URL or local path of a parquet file or of a directory with (partitioned)
#' parquet files, a vector of these, or a lazy DuckDB table from `parquet_tbl`
#' @return a tibble with one row per parquet file and the columns `file`, the file name, `size`, formatted for
#' reading like `2.5 kB`, `sizeRaw`, the size in bytes, `modified`, the modification time in UTC, `cache`, the
#' status of the file in the local cache directory of `cache_parquet`: `"not cached"`, `"current"` when the
#' cached copy has the etag of the file, `"stale"` when it does not, and `NA` for local files and where the
#' etag of the file is unknown, `etag`, the etag of the file, which is the MD5 hash of its content for files
#' uploaded by this package, `NA` for local files, and `path`, the `r2://bucket/path`, URL or local path
#' @export
list_parquet <- function(x) UseMethod("list_parquet")

#' @rdname list_parquet
#' @export
list_parquet.character <- function(x) {
  is_r2 <- startsWith(x,"r2://")
  is_url <- !is_r2 & grepl("^[a-z0-9]+://",x)
  files <- lapply(seq_along(x),function(i){
    if (is_r2[i]) r2_parquet_files(x[i]) else if (is_url[i]) url_parquet_files(x[i]) else local_parquet_files(x[i])
  })
  files <- dplyr::distinct(dplyr::bind_rows(parquet_files(character()),files))
  files$cache <- cache_status(files$path,files$etag)
  files
}

#' @rdname list_parquet
#' @export
list_parquet.tbl_lazy <- function(x) {
  path <- attr(x,"r2parquet_paths")
  if (is.null(path)) stop("The table was not created by parquet_tbl, pass the path of the parquet files instead.")
  # the paths parquet_tbl resolved to the files, local and R2 directories as globs for all parquet files in them
  list_parquet(attr(x,"r2parquet_source") %||% sub("/\\*\\*/\\*\\.parquet$","",path))
}

#' @rdname list_parquet
#' @export
list_parquet.default <- function(x) {
  stop("x has to be a path, a vector of paths or a lazy DuckDB table from parquet_tbl.")
}
