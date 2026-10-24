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
# error instead of promoting. Hive partition columns are the other case: their values are read as strings
# (hive_types_autocast = false in duckdb_parquet_tbl, so `PRUID=01` and `PRUID=10` give the same type), and
# a number against a VARCHAR column gets the column cast too, above the scan, so `PRUID == 59` would read every
# partition. Numbers compared for equality with a partition column (`==`, `!=`, `%in%`) are therefore rendered
# as string literals, which DuckDB prunes partitions on. Order comparisons are left alone: strings compare
# lexicographically, and DuckDB refuses `PRUID > 10` on a VARCHAR column with a binder error asking for an
# explicit cast, which is better than a silently wrong `PRUID > '10'`. The translation only sees column names,
# not the table, so parquet_tbl() records the
# partition columns of every table it creates in parquet_partitions; a numeric column of another table with
# the same name is unaffected, DuckDB casts a string literal against a numeric column on the literal side.
methods::setClass("r2parquet_connection",contains="duckdb_connection")

# the names of the hive partition columns of the tables parquet_tbl created in the session
parquet_partitions <- new.env(parent=emptyenv())

# whether a translated expression is a reference to a partition column, by its name: dbplyr passes column
# references to the translation as ident objects (as sql once rendered), other values as themselves
is_partition_column <- function(x){
  if (!(inherits(x,"ident") || inherits(x,"sql")) || length(x)!=1) return(FALSE)
  name <- gsub('""','"',sub('^"(.*)"$',"\\1",as.character(x)))
  exists(name,envir=parquet_partitions,inherits=FALSE)
}

# plain numbers compared with partition column x, as the strings partition values are read as, with a message
# once per session; everything else is returned as is
partition_literal <- function(x,column){
  if (!is.numeric(x) || is.object(x) || length(x)==0 || !all(is.finite(x))) return(x)
  literal <- ifelse(x==trunc(x),formatC(x,format="f",digits=0),as.character(x))
  if (!isTRUE(parquet_cache$partition_literal_noted)) {
    parquet_cache$partition_literal_noted <- TRUE
    message("Comparing the partition column ",column," with the number ",format(x[1]),", rendered as the string \"",literal[1],
            "\": hive partition values are read as strings, and a string lets DuckDB skip the other partitions. ",
            "Write it as \"",literal[1],"\" to avoid this message, it is shown once per session.")
  }
  literal
}

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
  comparison <- function(operator,equality=FALSE){
    infix <- dbplyr::sql_infix(operator)
    function(x,y){
      if (equality && is_partition_column(x)) return(infix(x,partition_literal(y,x)))
      if (equality && is_partition_column(y)) return(infix(partition_literal(x,y),y))
      infix(integer_literal(x),integer_literal(y))
    }
  }
  parent_in <- scalar$`%in%`
  parent_between <- scalar$between
  translation$scalar <- dbplyr::sql_translator(.parent=scalar,
    `==`=comparison("=",equality=TRUE),`!=`=comparison("<>",equality=TRUE),
    `<`=comparison("<"),`<=`=comparison("<="),`>`=comparison(">"),`>=`=comparison(">="),
    `%in%`=function(x,table)parent_in(x,if (is_partition_column(x)) partition_literal(table,x) else integer_literal(table)),
    between=function(x,left,right)parent_between(x,integer_literal(left),integer_literal(right)))
  translation
}

#' DuckDB connection shared by the parquet functions
#'
#' @description
#' The in-memory DuckDB connection that [parquet_tbl()], [parquet_metadata()] and [data_to_parquet()] use. It gets
#' created on first use and is cached for the session, so that tables from different [parquet_tbl()] calls can
#' be joined with each other.
#'
#' @details
#' Use it to bring other data onto the same connection, e.g. `dplyr::copy_to(parquet_connection(), df)` to
#' join a data frame with a parquet table, or to change DuckDB settings via [DBI::dbExecute()], like
#' `SET memory_limit = '4GB'` or `SET threads = 2`. Disconnecting it, e.g. to free memory, is fine, the next
#' call creates a new connection, but tables created on the old connection stop working. [parquet_tbl()] can also
#' create its tables on a connection of your own via its `con` argument.
#'
#' The connection is DuckDB's connection class extended with the package's SQL translation, which renders
#' whole numbers in comparisons as integers, see [parquet_tbl()]; everything else works as on any DuckDB
#' connection.
#'
#' @return a DuckDB connection.
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

# the same for s3:// paths, from the settings of the S3 service; a custom endpoint is given as host and port,
# without the scheme, which decides about SSL
duckdb_s3_secret <- function(con){
  store <- s3_settings()
  endpoint <- if (store$custom) paste0(", ENDPOINT ",DBI::dbQuoteString(con,sub("^https?://","",store$endpoint)),
                                       ", USE_SSL ",if (startsWith(store$endpoint,"http://")) "false" else "true")
  DBI::dbExecute(con,paste0("CREATE OR REPLACE SECRET s3 (TYPE s3",
                            ", KEY_ID ",DBI::dbQuoteString(con,store$access_key),
                            ", SECRET ",DBI::dbQuoteString(con,store$secret),
                            if (store$token!="") paste0(", SESSION_TOKEN ",DBI::dbQuoteString(con,store$token)),
                            ", REGION ",DBI::dbQuoteString(con,store$region),
                            ", URL_STYLE ",DBI::dbQuoteString(con,store$url_style),
                            endpoint,")"))
}

# the secrets for the buckets that paths are in
duckdb_bucket_secrets <- function(con,path){
  if (any(startsWith(path,"r2://"))) duckdb_r2_secret(con)
  if (any(startsWith(path,"s3://"))) duckdb_s3_secret(con)
}

# install and load a DuckDB extension, the R package turns autoinstall off, so extensions get installed
# explicitly, and spatial functions do not autoload
load_extension <- function(con,name){
  # INSTALL downloads an extension the first time only, which fails without a connection
  tryCatch(DBI::dbExecute(con,paste0("INSTALL ",name)),error=function(e){
    url <- regmatches(conditionMessage(e),regexec("Failed to download extension \"[^\"]+\" at URL \"([^\"]+)\"",conditionMessage(e)))[[1]]
    if (length(url)!=2 || !grepl("establish connection|resolve|[Tt]imeout|connect to server",conditionMessage(e))) stop(e)
    stop(network_error(url[2],paste0("The DuckDB extension ",name," gets downloaded the first time it is needed.")))
  })
  DBI::dbExecute(con,paste0("LOAD ",name))
}

# the scan of parquet files readable by duckdb, as SQL; partition values stay strings, DuckDB would otherwise
# read PRUID=10 as an integer and PRUID=01 as a string (see the translation above for the filters on them)
duckdb_parquet_sql <- function(con,urls,hive_partitioning=TRUE){
  urls <- paste0(DBI::dbQuoteString(con,urls),collapse=", ")
  if (grepl(", ",urls,fixed=TRUE)) urls <- paste0("[",urls,"]")
  paste0("SELECT * FROM read_parquet(",urls,", hive_partitioning = ",if (hive_partitioning) "true" else "false",
         ", hive_types_autocast = false)")
}

# lazy table for parquet files readable by duckdb, filters and column selections get pushed down to the parquet scan
duckdb_parquet_tbl <- function(con,urls,hive_partitioning=TRUE){
  dplyr::tbl(con,dplyr::sql(duckdb_parquet_sql(con,urls,hive_partitioning)))
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
    bucket_url(p$bucket,r2_parquet_glob(p$bucket,p$path,p$directory))
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
  is_r2 <- is_bucket_path(path)
  at_url <- is_url(path)
  is_url_dir <- at_url & grepl("^https?://",path) & grepl("/$",sub("[?#].*$","",path))
  path[is_r2] <- r2_parquet_url(path[is_r2])
  path[!is_r2 & !at_url] <- local_parquet_glob(path[!is_r2 & !at_url])
  if (any(is_r2 | at_url)) load_extension(con,"httpfs")
  duckdb_bucket_secrets(con,path[is_r2])
  if (!any(is_url_dir)) return(path)
  as.character(unlist(lapply(seq_along(path),function(i)if (is_url_dir[i]) url_directory_files(path[i]) else path[i])))
}

# the error for a failed DuckDB read of paths: missing paths and denied access as r2parquet_error conditions that
# name the path and explain the failure, unreachable servers as r2parquet_network_error, other errors unchanged
read_error <- function(e,path,call=NULL){
  messages <- character(0)
  for (cause in list(e,e$parent,e$parent$parent)) if (!is.null(cause)) messages <- c(messages,conditionMessage(cause))
  message <- paste0(messages,collapse="\n")
  status <- as.numeric(regmatches(message,regexpr("(?<=\\(HTTP )[0-9]{3}",message,perl=TRUE)))
  not_found <- grepl("No files found that match the pattern",message,fixed=TRUE) || identical(status,404)
  # connection failures, DuckDB names the request it could not send
  connection <- regmatches(message,regexec("IO Error: (.+?) error for HTTP [A-Z]+ to '([^']+)'",message))[[1]]
  if (length(connection)==3) return(network_error(connection[3],paste0(connection[2],"."),call))
  if (!not_found && !(length(status)==1 && status %in% c(401,403))) return(e)
  # the path the failed read was for, the longest one the file or pattern in the message starts with
  # captured rather than in lookbehinds, PCRE2 before 10.43 (Ubuntu) refuses lookbehinds of varying length
  target <- regmatches(message,regexec("pattern \"([^\"]+)|error (on|reading) '([^']+)",message))[[1]]
  target <- if (length(target)==4) paste0(target[2],target[4])
  base <- sub("/+$","",sub("[?#].*$","",path))
  matches <- which(startsWith(if (length(target)==1) sub("[?#].*$","",target) else "",base))
  if (length(matches)==0 && length(path)!=1) return(e)
  p <- if (length(matches)==0) path else path[matches[which.max(nchar(base[matches]))]]
  # the error code and explanation S3 compatible services send along, like SignatureDoesNotMatch: ...
  server <- regmatches(message,regexpr("(?m)^[A-Z][A-Za-z]+: [^\n]+",message,perl=TRUE))
  is_local <- !is_bucket_path(p) && !is_url(p)
  text <- if (!not_found) {
    paste0("Reading ",p," failed with status ",status,".",access_hint(status,p),if (length(server)==1) paste0("\n",server))
  } else if (is_local && dir.exists(p)) {
    paste0("No parquet files in the directory ",p,".")
  } else if (is_local) {
    paste0("No file or directory at ",p,if (!grepl("^(/|~|[A-Za-z]:)",p)) paste0(" (relative to the working directory ",getwd(),")"),".")
  } else if (grepl("NoSuchBucket",message,fixed=TRUE)) {
    paste0("The ",bucket_label(r2_split_path(p)$bucket)," does not exist.")
  } else if (is_bucket_path(p)) {
    paste0("No parquet file or directory of parquet files at ",p,".")
  } else {
    paste0("No file at ",p,if (!endsWith(sub("[?#].*$","",p),"/")) paste0(" and no manifest of a directory at ",sub("[?#].*$","",p),"/",manifest_name),".")
  }
  r2parquet_error(if (not_found) "not_found" else "access_denied",text,call)
}

#' lazy table to query parquet files at URLs, on R2 or S3, or at local paths
#'
#' @description
#' Lazy `dplyr` table on DuckDB for parquet files at public URLs (like public buckets or presigned URLs), in
#' private buckets on R2 as `r2://bucket/path` or on S3 as `s3://bucket/path`, or at local paths. Nothing gets
#' downloaded until the query is executed, e.g. via [dplyr::collect()], and then only what the query needs:
#' filters on partition columns restrict which files get read, other filters and the selected columns which
#' parts of the files.
#'
#' @details
#' `dplyr` verbs get translated to SQL. Use [`sf::st_filter()`][st_filter.tbl_duckdb_connection] to filter by
#' location, [collect_parquet()] to collect the result with its metadata attached (as an sf object if it has
#' geometry), [parquet_metadata()] for the metadata of the files and [list_parquet()] to list them. Tables
#' with GEOMETRY columns, like GeoParquet files written by [data_to_parquet()], get the `spatial` extension
#' loaded, so that spatial functions like `ST_Area` work in queries.
#'
#' # Directories
#' * Local directories and directories on R2 and S3 get read as all parquet files within them. Directories in
#'   buckets are often named like a parquet file, e.g. `r2://bucket/data.parquet`, which works too.
#' * Directories at URLs can't be listed, they get read through the manifest next to their files
#'   (`_manifest.json`, written by [upload_parquet()], [data_to_parquet()] and [write_manifest()]). A URL
#'   ending in a slash is read as a directory; a URL without one is read as a file first and, if there is no
#'   file at it, as a directory.
#'
#' # Integer literals
#' Whole numbers in comparisons (`==`, `!=`, `<`, `<=`, `>`, `>=`, `%in%`, `between`) get rendered as
#' integers, with a message the first time in a session. A plain number like `2021` is a double in R, which
#' `dplyr` renders as `2021.0`, and DuckDB resolves that against an integer column by casting the column,
#' which defeats the statistics it uses to skip row groups, so the filter downloads the whole column.
#' Comparisons with double columns are unaffected, and arithmetic is not rewritten (`x * 1000` stays a double
#' multiplication). Write integers as `2021L` to avoid the message, and on a connection of your own passed as
#' `con`, which keeps DuckDB's own translation.
#'
#' # Languages
#' Datasets can carry labels and columns in several languages, see the Languages section of
#' [collect_parquet()]. With a language the table gets its columns selected in that language through
#' [select_language()]: a column in the language asked for takes the place and name of its plain column,
#' `name_fr` becomes `name`, or stands in for a plain column the data doesn't have, and the columns in the
#' other languages get left out before anything is read, so a bilingual dataset read with
#' `options(r2parquet.language = "fr")` has `name` and no `name_en`. Which suffixes are languages is declared
#' by the metadata of the files (see the Languages section of [data_to_parquet()]), which gets read once
#' per table for this. The column names of the table
#' therefore depend on the language. `language = NULL` gives the columns as stored, for selecting them by
#' hand or with [select_language()] later, which also works on joins of such tables.
#'
#' # Errors
#' Paths without parquet files and denied requests fail with an error naming the path, of class
#' `r2parquet_not_found` or `r2parquet_access_denied` (both also `r2parquet_error`) for use with
#' [tryCatch()]. Denied requests say what access depends on: the credentials for buckets, the expiry of
#' presigned URLs, public access for other URLs. Servers that can't be reached, e.g. offline, fail with an
#' `r2parquet_network_error`, as does installing a DuckDB extension the first time it is needed. The
#' files get read when the query is executed, [collect_parquet()] raises the same errors then, e.g. when the
#' connection dropped after the table was created.
#'
#' # Cache
#' Remote files that get queried regularly can be kept in a local cache directory via [cache_parquet()], and
#' the table then reads the local copies, see the `cache` argument. The default costs nothing for paths
#' without a copy, they are read remotely without any request to check them. With the default and with
#' `cache = TRUE` a cached copy gets used, with a warning, when the remote can't be reached, e.g. offline.
#' `cache = "offline"` also allows comparing the copy with the current remote files (`cache = FALSE`) before
#' updating it. Directories at public URLs can only be cached if they have a manifest. The cache directory is
#' the one in the `R2PARQUET_CACHE_PATH` environment variable, or the user cache directory if that is not set.
#'
#' @inheritSection upload_parquet Credentials
#' @param path URL, `r2://bucket/path`, `s3://bucket/path` or local path of a parquet file or a directory of
#'   (partitioned) parquet files, or a vector of these.
#' @param hive_partitioning `TRUE` (default) or `FALSE`: derive partition columns from `key=value` components
#'   of the file paths. This includes the directories above a single file, so reading
#'   `data/year=2021/data_0.parquet` adds a `year` column. Partition values are read as strings, whatever
#'   they look like, so `PRUID=01` and `PRUID=10` give the same type; on [parquet_connection()], numbers
#'   compared with a partition column for equality (`==`, `!=`, `%in%`) are rendered as strings, with a
#'   message once per session, so that `filter(year == 2021)` only reads that partition. Order comparisons
#'   need the cast spelled out, `as.integer(year) > 2020`, since strings compare lexicographically.
#' @param con DuckDB connection to create the table on. Tables can only be joined when they are on the same
#'   connection.
#'   * `NULL` (default): [parquet_connection()], shared by all calls.
#'   * a connection of your own, to join with data that lives there. The `httpfs` and `spatial` extensions
#'     get installed and loaded on it as needed, and the R2 or S3 credentials get registered as
#'     secrets.
#' @param cache how to use the local cache (see the Cache section):
#'   * `NULL` (default): read the cached copy if it is current, else the remote files, with a warning for
#'     copies that are out of date. A copy only gets checked the first time its path is read in the session,
#'     later calls reuse the result (see [clear_cache_checks()]).
#'   * `TRUE`: update the copy first via [cache_parquet()], downloading the files on first use, and read it.
#'   * `FALSE`: read the remote files directly.
#'   * `"offline"`: read the copy without checking it against the remote files, current or not; fails for
#'     paths without a copy.
#' @param language language code like `"en"` or `"fr"`, or `NULL`: with a language, the columns the data
#'   holds in several languages get selected in that language under their plain names, see the Languages
#'   section. Defaults to the `r2parquet.language` option.
#' @return a lazy `dplyr` table on DuckDB.
#' @export
parquet_tbl <- function(path,hive_partitioning=TRUE,con=NULL,cache=NULL,language=getOption("r2parquet.language")) {
  call <- sys.call()
  if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
  language <- check_language(language)
  given <- path
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
    is_directory <- vapply(seq_along(path),function(i)is_file_url[i] && !is.null(url_directory_files(path[i],missing_ok=TRUE)),logical(1))
    if (!any(is_directory)) stop(read_error(tbl,path,call))
    path[is_directory] <- paste0(sub("[?#].*$","",path[is_directory]),"/")
    files <- duckdb_parquet_paths(con,path)
    tbl <- tryCatch(duckdb_parquet_tbl(con,files,hive_partitioning=hive_partitioning),error=function(e)stop(read_error(e,path,call)))
  }
  # the columns in the files themselves, described without hive partitioning (one bind, as the geometry check
  # took before): GEOMETRY columns need the spatial extension, and the columns the table has beyond them are
  # the partition columns, whose filters the translation renders as strings
  file_columns <- DBI::dbGetQuery(con,paste0("DESCRIBE ",duckdb_parquet_sql(con,files,hive_partitioning=FALSE)))
  if (any(grepl("^GEOMETRY",file_columns$column_type))) load_extension(con,"spatial")
  partitions <- setdiff(colnames(tbl),file_columns$column_name)
  for (column in partitions) assign(column,TRUE,envir=parquet_partitions)
  # for parquet_metadata, list_parquet, remove_parquet and collect_parquet, dbplyr keeps attributes through its
  # verbs: the files the table reads (local copies for cached paths), the paths as given and the partition columns
  attr(tbl,"r2parquet_paths") <- files
  attr(tbl,"r2parquet_source") <- given
  attr(tbl,"r2parquet_partitions") <- partitions
  # tables joined on the right lose their attributes, parquet_sources() finds them by the SQL of their scan
  key <- as.character(tbl$lazy_query$x)
  assign(key,list(paths=files,source=given,key=key),envir=parquet_scans)
  # in a language, the columns in that language stand in for the plain ones and the others are left out
  tbl <- select_language(tbl,language)
  # the columns the table starts out with, for collect_parquet() to tell whether some were selected away;
  # select_language() may have recorded the metadata in the entry in the meantime
  entry <- get(key,envir=parquet_scans)
  entry$columns <- colnames(tbl)
  assign(key,entry,envir=parquet_scans)
  tbl
}

# the tables parquet_tbl created in the session, by the SQL of their scan: the files they read (local copies
# for cached paths), the paths as given, the key itself, the columns the table started out with and, once
# read, their metadata (scan_footers())
parquet_scans <- new.env(parent=emptyenv())

# the files a scan written by duckdb_parquet_tbl reads, from its SQL, nothing for other SQL
scan_sql_paths <- function(sql){
  pattern <- "^SELECT \\* FROM read_parquet\\((.*), hive_partitioning = (true|false), hive_types_autocast = false\\)$"
  if (!grepl(pattern,sql)) return(character())
  urls <- sub(pattern,"\\1",sql)
  quoted <- regmatches(urls,gregexpr("'([^']|'')*'",urls))[[1]]
  gsub("''","'",substr(quoted,2,nchar(quoted)-1),fixed=TRUE)
}

# the parquet_scans entries of the tables a lazy table reads, left-hand table first and each once: dbplyr nests
# the lazy queries of joined, semi joined and combined tables, with the scans as lazy_base_remote_query leaves;
# tables created before parquet_scans was reset, e.g. by reloading the package, get recovered from the SQL of
# their scan, with the paths the left-hand table was given where it is one of them
table_scans <- function(x){
  found <- list()
  walk <- function(query){
    if (inherits(query,"lazy_base_remote_query")) {
      key <- as.character(query$x)
      if (length(key)!=1 || !nzchar(key)) return()
      if (exists(key,envir=parquet_scans,inherits=FALSE)) {
        found[[key]] <<- get(key,envir=parquet_scans)
      } else if (length(paths <- scan_sql_paths(key))>0) {
        source <- if (identical(paths,attr(x,"r2parquet_paths"))) attr(x,"r2parquet_source")
        # local and bucket directories are read as globs for the parquet files in them
        source <- source %||% sub("/\\*\\*/\\*\\.parquet$","",paths)
        found[[key]] <<- list(paths=paths,source=source,key=key)
      }
    } else if (is.list(query)) {
      for (element in query) walk(element)
    }
  }
  walk(x$lazy_query)
  unname(found)
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
# lists, just the URL if there is neither, failing only if the server can't be reached
url_parquet_files <- function(url){
  source <- if (grepl("^https?://",url)) {
    tryCatch(url_cache_source(list(url=url)),error=function(e)if (inherits(e,"r2parquet_network_error")) stop(e))
  }
  if (is.null(source)) return(parquet_files(url))
  parquet_files(source$files$key,source$files$size,source$files$modified,source$files$etag)
}

# the local parquet file at a path, or the parquet files below the directory, nothing if there is neither
local_parquet_files <- function(path){
  files <- if (dir.exists(path)) dir(path,"\\.parquet$",recursive=TRUE,full.names=TRUE) else if (file.exists(path)) path else character()
  info <- file.info(files,extra_cols=FALSE)
  parquet_files(files,info$size,info$mtime)
}

# the paths a lazy table from parquet_tbl was created from, for list_parquet and remove_parquet: the paths as
# given, the remote ones for a table reading copies from the cache
table_source <- function(x){
  path <- attr(x,"r2parquet_paths")
  if (is.null(path)) stop("The table was not created by parquet_tbl, pass the path of the parquet files instead.")
  # local and bucket directories are read as globs for all parquet files in them
  attr(x,"r2parquet_source") %||% sub("/\\*\\*/\\*\\.parquet$","",path)
}

#' list parquet files
#'
#' @description
#' Lists the parquet files at paths on R2 or S3, at URLs or at local paths, or at the paths a lazy table from
#' [parquet_tbl()] was created from (the remote paths for a table reading copies from the cache), with their
#' size, modification time and etag, and whether the local cache holds a current copy. Directories get listed recursively, a path that does not exist lists nothing.
#'
#' @details
#' Files at URLs, including presigned URLs, get their size and etag from a request for their first byte,
#' directories at URLs from the manifest next to the files (see [upload_parquet()]). A URL with neither is listed
#' as it is with `NA`s, a server that can't be reached fails with an `r2parquet_network_error`.
#'
#' @inheritSection upload_parquet Credentials
#' @param x what to list:
#'   * `r2://bucket/path`, `s3://bucket/path`, URL or local path of a parquet file or of a directory of
#'     (partitioned) parquet files, or a vector of these;
#'   * a lazy DuckDB table from [parquet_tbl()].
#' @return a tibble with one row per parquet file and the columns
#'   * `file`: the file name;
#'   * `size`: the size formatted for reading, like `2.5 kB`;
#'   * `sizeRaw`: the size in bytes;
#'   * `modified`: the modification time in UTC;
#'   * `cache`: the status of the copy in the local cache directory of [cache_parquet()]: `"not cached"`,
#'     `"current"` when the copy has the etag of the file, `"stale"` when it does not, and `NA` for local
#'     files and where the etag is unknown;
#'   * `etag`: the etag of the file, the MD5 hash of its content for files uploaded by this package, `NA` for
#'     local files;
#'   * `path`: the `r2://bucket/path`, `s3://bucket/path`, URL or local path.
#' @export
list_parquet <- function(x) UseMethod("list_parquet")

#' @rdname list_parquet
#' @export
list_parquet.character <- function(x) {
  is_r2 <- is_bucket_path(x)
  at_url <- is_url(x)
  files <- lapply(seq_along(x),function(i){
    if (is_r2[i]) r2_parquet_files(x[i]) else if (at_url[i]) url_parquet_files(x[i]) else local_parquet_files(x[i])
  })
  files <- dplyr::distinct(dplyr::bind_rows(parquet_files(character()),files))
  files$cache <- cache_status(files$path,files$etag)
  files
}

#' @rdname list_parquet
#' @export
list_parquet.tbl_lazy <- function(x) {
  list_parquet(table_source(x))
}

#' @rdname list_parquet
#' @export
list_parquet.default <- function(x) {
  stop("x has to be a path, a vector of paths or a lazy DuckDB table from parquet_tbl.")
}
