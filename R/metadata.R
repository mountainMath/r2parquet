# Key-value metadata in the parquet footer: source, license, notes, column labels and descriptions.
# data_to_parquet writes it with DuckDB's KV_METADATA copy option, parquet_metadata reads it back with
# parquet_kv_metadata(), and collect_parquet attaches it to the collected data as attributes. Entries are
# strings in the footer, anything that is not a single value gets stored as JSON and parsed again when read.

# metadata entries, with column labels of the data added, as strings for the parquet footer
metadata_strings <- function(metadata,columns,labels=NULL){
  if (!is.null(metadata) && (!is.list(metadata) || (length(metadata)>0 && (is.null(names(metadata)) || any(names(metadata)==""))))) {
    stop("metadata has to be a named list.")
  }
  if ("geo" %in% names(metadata)) stop("The metadata entry geo is reserved for the GeoParquet metadata.")
  # NULL and NA entries have nothing to write, a NA would end up as the string "NULL" in the footer
  empty <- vapply(metadata,function(value)is.null(value) || (is.atomic(value) && length(value)==1 && is.na(value)),logical(1))
  if (any(empty)) {
    warning(paste0("Dropping the metadata entr",if (sum(empty)>1) "ies " else "y ",paste0(names(metadata)[empty],collapse=", "),
                   " since ",if (sum(empty)>1) "they are" else "it is"," NULL or NA."),call.=FALSE)
    metadata <- metadata[!empty]
  }
  if (!is.null(metadata$columns)) {
    if (!is.list(metadata$columns) || is.null(names(metadata$columns)) || any(names(metadata$columns)=="")) {
      stop("metadata$columns has to be a named list with one entry per column.")
    }
    missing_columns <- setdiff(names(metadata$columns),columns)
    if (length(missing_columns)>0) {
      stop(paste0("Columns ",paste0(missing_columns,collapse=", ")," in metadata$columns not found in data."))
    }
    # a string is short for the label
    metadata$columns <- lapply(metadata$columns,function(entry)if (is.character(entry) && length(entry)==1) list(label=entry) else entry)
  }
  labels <- labels[names(labels) %in% columns]
  labels <- labels[!vapply(labels,is.null,logical(1))]
  for (column in names(labels)) {
    if (is.null(metadata$columns[[column]]$label)) metadata$columns[[column]]$label <- as.character(labels[[column]])
  }
  # always an array, also for a single column
  if (!is.null(metadata$primary_key)) metadata$primary_key <- I(metadata$primary_key)
  if (length(metadata)==0) return(NULL)
  vapply(metadata,function(value){
    # a string that would read back as JSON gets written as a JSON string, so it reads back as it is
    if (is.atomic(value) && length(value)==1 && is.null(names(value)) && !inherits(value,"AsIs") && !grepl("^\\s*[\\[{\"]",value)) as.character(value)
    else as.character(jsonlite::toJSON(value,auto_unbox=TRUE,null="null",na="null",digits=NA))
  },character(1))
}

# the columns of the primary_key metadata entry, checked to exist and to identify the rows of source (a table or
# subquery on con) without missing values, NULL if there is no entry
check_primary_key <- function(con,source,metadata,columns){
  key <- if (is.list(metadata)) metadata$primary_key
  if (is.null(key)) return(NULL)
  if (!is.character(key) || length(key)==0 || anyNA(key) || anyDuplicated(key)) {
    stop("metadata$primary_key has to be a vector of column names.")
  }
  missing_columns <- setdiff(key,columns)
  if (length(missing_columns)>0) stop(paste0("Columns ",paste0(missing_columns,collapse=", ")," in metadata$primary_key not found in data."))
  quoted <- DBI::dbQuoteIdentifier(con,key)
  check <- DBI::dbGetQuery(con,paste0("SELECT count(*) AS n, count(DISTINCT row(",paste0(quoted,collapse=", "),")) AS distinct_keys, ",
                                      "count(*) FILTER (WHERE ",paste0(quoted," IS NULL",collapse=" OR "),") AS missing FROM ",source))
  if (check$missing>0) stop(paste0("The primary key ",paste0(key,collapse=", ")," has missing values in ",check$missing," rows."))
  if (check$distinct_keys<check$n) {
    stop(paste0("The primary key ",paste0(key,collapse=", ")," does not identify rows, ",check$n-check$distinct_keys," rows repeat a key."))
  }
  key
}

# metadata values parsed back from the footer strings, JSON objects, arrays and strings get parsed, the rest
# stays a string. Arrays stay lists, unsimplified, so that manifests write them back as they were, a one element
# array like ["DBUID"] would turn into a string otherwise; simplify_metadata() simplifies for the user
parse_metadata_value <- function(value){
  if (grepl("^\\s*[\\[{\"]",value)) {
    parsed <- tryCatch(jsonlite::fromJSON(value,simplifyVector=FALSE),error=function(e)NULL)
    if (!is.null(parsed)) return(parsed)
  }
  value
}

# the footer metadata of parquet files, paths or globs, on a connection that can read them: a list per file,
# named by the file and in file order, without the GeoParquet entry
read_parquet_footers <- function(con,path){
  files <- paste0(DBI::dbQuoteString(con,path),collapse=", ")
  if (length(path)>1) files <- paste0("[",files,"]")
  kv <- DBI::dbGetQuery(con,paste0("SELECT file_name, decode(key) AS k, decode(value) AS v FROM parquet_kv_metadata(",files,
                                   ") ORDER BY file_name"))
  kv <- kv[kv$k!="geo",]
  lapply(split(kv,factor(kv$file_name,levels=unique(kv$file_name))),function(entries){
    lapply(stats::setNames(entries$v,entries$k),parse_metadata_value)
  })
}

# the footer metadata of parquet files, local paths or r2://bucket/key, as a list per file named by the path,
# empty for files without. Reads on a connection of its own so that DuckDB's parquet metadata cache can't hand
# back the footer of an earlier version of a file; a remote footer costs a request or two per file. The
# connection stays out of RStudio's Connections pane, see duckdb_connect_quietly()
parquet_footers <- function(path){
  metadata <- stats::setNames(rep(list(list()),length(path)),path)
  if (length(path)==0) return(metadata)
  con <- duckdb_connect_quietly()
  on.exit(attr(con,"disconnect")())
  if (any(is_bucket_path(path))) {
    load_extension(con,"httpfs")
    duckdb_bucket_secrets(con,path)
  }
  footers <- read_parquet_footers(con,path)
  metadata[names(footers)] <- footers
  metadata
}

# metadata as returned to the user, arrays of single values simplified to vectors, as jsonlite simplifies them
simplify_metadata <- function(metadata){
  lapply(metadata,function(value){
    if (!is.list(value)) return(value)
    jsonlite::fromJSON(jsonlite::toJSON(value,auto_unbox=TRUE,null="null",na="null",digits=NA),simplifyDataFrame=FALSE,simplifyMatrix=FALSE)
  })
}

# the metadata of a set of parquet files from their footers, a list per file named by the file: the values of
# the first file where the files disagree, with a warning; simplified for the user
merge_footers <- function(footers){
  if (length(footers)==0) return(list())
  metadata <- footers[[1]]
  conflicts <- character()
  for (footer in footers[-1]) {
    conflicts <- c(conflicts,names(footer)[vapply(names(footer),function(key)!is.null(metadata[[key]]) && !identical(metadata[[key]],footer[[key]]),logical(1))])
    metadata <- c(metadata,footer[setdiff(names(footer),names(metadata))])
  }
  conflicts <- unique(conflicts)
  if (length(conflicts)>0) {
    warning("The files disagree on the metadata entr",if (length(conflicts)>1) "ies " else "y ",paste0(conflicts,collapse=", "),
            ", using the values of ",names(footers)[1],".",call.=FALSE)
  }
  simplify_metadata(metadata)
}

# the metadata of parquet files, paths or globs, read from their footers on a connection that can read them
duckdb_parquet_metadata <- function(con,path){
  merge_footers(read_parquet_footers(con,path))
}

# the footers of the parquet files at a path on R2 or at a URL as the nearest manifest at or above the path
# records them, a list per file named by the file's path like read_parquet_footers returns it. NULL when no
# manifest covers the path, the nearest one predates the metadata, or the path is a presigned URL, so the
# footers have to be read
manifest_footers <- function(path){
  if (is_bucket_path(path)) {
    location <- r2_split_path(path)
    key <- location$path
    prefix <- bucket_url(location$bucket)
    encode <- identity
    read <- function(directory)r2_get_manifest(location$bucket,paste0(directory,if (directory!="") "/",manifest_name))
  } else {
    if (grepl("[?#]",path)) return(NULL)
    url <- httr::parse_url(path)
    prefix <- paste0(url$scheme,"://",url$hostname,if (!is.null(url$port)) paste0(":",url$port),"/")
    key <- utils::URLdecode(sub("/+$","",url$path %||% ""))
    encode <- url_path_encode
    read <- function(directory){
      response <- http_get(paste0(prefix,encode(directory),if (directory!="") "/",manifest_name))
      if (httr::status_code(response)>=400) return(NULL)
      tryCatch(parse_manifest(httr::content(response,"text",encoding="UTF-8")),
               error=function(e)structure(manifest_files(),has_metadata=FALSE))
    }
  }
  for (directory in c(key,r2_ancestors(key))) {
    manifest <- read(directory)
    if (is.null(manifest)) next
    if (!isTRUE(attr(manifest,"has_metadata"))) return(NULL)
    relative <- substring(key,nchar(directory)+if (directory=="") 1 else 2)
    files <- manifest[relative=="" | manifest$path==relative | startsWith(manifest$path,paste0(relative,"/")),]
    if (nrow(files)==0) return(NULL)
    return(stats::setNames(files$metadata,paste0(prefix,encode(paste0(directory,if (directory!="") "/",files$path)))))
  }
  NULL
}

# attach metadata to collected data, the whole list as the metadata attribute and column labels as label attributes
attach_parquet_metadata <- function(data,metadata){
  if (length(metadata)==0) return(data)
  for (column in intersect(names(metadata$columns),names(data))) {
    label <- if (is.list(metadata$columns[[column]])) metadata$columns[[column]]$label
    if (is.character(label) && length(label)==1) attr(data[[column]],"label") <- label
  }
  attr(data,"metadata") <- metadata
  data
}

# the metadata of the parquet files data was read from, for data_to_parquet to point out when writing drops it:
# the footers (or manifests) a lazy table from parquet_tbl reads, the metadata attribute collect_parquet attaches.
# NULL without metadata, or when it can't be read, since this only decides about a warning
source_metadata <- function(data){
  if (inherits(data,"tbl_lazy")) {
    if (is.null(attr(data,"r2parquet_paths"))) return(NULL)
    metadata <- tryCatch(unclass(parquet_metadata(data)),error=function(e)NULL)
  } else {
    metadata <- attr(data,"metadata")
    # only metadata in the form parquet_metadata returns it
    if (!is.list(metadata) || is.null(names(metadata)) || any(names(metadata)=="") || "geo" %in% names(metadata)) return(NULL)
  }
  if (length(metadata)==0) return(NULL)
  metadata
}

# warn that the metadata of the files data was read from doesn't get written, metadata = list() writes none on purpose
warn_dropped_metadata <- function(data){
  metadata <- source_metadata(data)
  if (is.null(metadata)) return(invisible())
  warning(paste0("The data was read from parquet files with metadata (",paste0(names(metadata),collapse=", "),
                 "), which doesn't get written without the metadata argument. Pass metadata = parquet_metadata(data) ",
                 "to write it again, modifyList(parquet_metadata(data), list(...)) to change entries, or ",
                 "metadata = list() to write none."),call.=FALSE)
}

#' metadata of parquet files
#'
#' @description
#' Reads the key-value metadata from the footer of parquet files, like the `source`, `license`, `notes` and
#' `columns` entries written by `data_to_parquet` with its `metadata` argument. Only metadata gets read, no
#' data. Entries are strings in the footer, entries holding JSON objects or arrays, like `columns`, get parsed
#' into lists. A directory of partitioned files has to have the same metadata in all its files, since they get
#' written together, if the files disagree the values of the first file are used with a warning.
#' The `geo` entry with the GeoParquet metadata is left out, the crs of geometry columns is available via
#' the GEOMETRY column type and comes with the geometry when collecting via `collect_parquet`.
#'
#' For paths on R2 and at URLs the metadata comes from the manifest (`_manifest.json`, see `write_manifest`)
#' of the directory, or of the nearest directory above the path that has one, which records the footer
#' metadata of every parquet file below it: one request for a whole dataset instead of a footer read per file.
#' `upload_parquet` and `data_to_parquet` keep the manifests current, files changed with other tools are not
#' reflected until `write_manifest` rebuilds the manifest. `from_files = TRUE` reads the footers of the files
#' themselves instead, which is also what happens for paths without a manifest (or with one written before
#' manifests recorded metadata), for presigned URLs, for local paths and for tables that read local copies from
#' the cache.
#'
#' @inheritSection upload_parquet Credentials
#' @param x URL, `r2://bucket/path` or local path of the parquet file or the directory with (partitioned)
#' parquet files, a vector of these, a lazy DuckDB table from `parquet_tbl`, or data collected with
#' `collect_parquet`, which carries the metadata in its `metadata` attribute
#' @param from_files if `TRUE`, read the footers of the parquet files on R2 and at URLs instead of the
#' manifest, which may be out of date when the files were changed with other tools. The default `FALSE` reads
#' the manifest where there is one
#' @param ... not used
#' @return a named list of the metadata entries, empty if the files have none (or the data was collected
#' without attaching it), of class `parquet_metadata`, which only changes how it prints: one line per entry,
#' lists as compact JSON, and the `columns` entry as a table of the columns with their labels and descriptions
#' @export
parquet_metadata <- function(x,...) UseMethod("parquet_metadata")

#' @rdname parquet_metadata
#' @export
parquet_metadata.character <- function(x,from_files=FALSE,...) {
  footers <- list()
  if (!isTRUE(from_files)) {
    for (path in unique(x[is_remote(x)])) {
      recorded <- manifest_footers(path)
      if (!is.null(recorded)) {
        footers <- c(footers,recorded)
        x <- x[x!=path]
      }
    }
  }
  if (length(x)>0) {
    con <- parquet_connection()
    footers <- c(footers,read_parquet_footers(con,duckdb_parquet_paths(con,x)))
  }
  new_parquet_metadata(merge_footers(footers))
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.tbl_lazy <- function(x,from_files=FALSE,...) {
  path <- attr(x,"r2parquet_paths")
  if (is.null(path)) stop("The table was not created by parquet_tbl, pass the path of the parquet files instead.")
  # the manifests of the paths the table was resolved from serve when they cover all of them, a table reading
  # local copies from the cache reads their footers
  source <- attr(x,"r2parquet_source")
  if (!isTRUE(from_files) && length(source)>0 && all(is_remote(source))) {
    recorded <- lapply(unique(source),manifest_footers)
    if (!any(vapply(recorded,is.null,logical(1)))) return(new_parquet_metadata(merge_footers(do.call(c,recorded))))
  }
  new_parquet_metadata(duckdb_parquet_metadata(dbplyr::remote_con(x),path))
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.data.frame <- function(x,...) {
  new_parquet_metadata(attr(x,"metadata"))
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.default <- function(x,...) {
  stop("x has to be a path, a vector of paths, a lazy DuckDB table from parquet_tbl or data collected with collect_parquet.")
}

# the metadata as returned by parquet_metadata, a named list with a class for printing
new_parquet_metadata <- function(metadata){
  if (is.null(metadata)) metadata <- list()
  structure(unclass(metadata),class="parquet_metadata")
}

#' @export
`[.parquet_metadata` <- function(x,i){
  new_parquet_metadata(unclass(x)[i])
}

# a string cut to a display width, with an ellipsis where it was cut
truncate_width <- function(x,width){
  ellipsis <- if (l10n_info()[["UTF-8"]]) "\u2026" else "..."
  vapply(x,function(s){
    if (is.na(s) || nchar(s,type="width")<=width) return(s)
    while (nchar(s,type="width")>width-nchar(ellipsis,type="width")) s <- substr(s,1,nchar(s)-max(1,nchar(s,type="width")-width))
    paste0(s,ellipsis)
  },"",USE.NAMES=FALSE)
}

# a metadata value on one line: single values as they are, vectors comma separated, lists as JSON
format_metadata_value <- function(value){
  if (is.null(value)) return("NULL")
  if (is.atomic(value) && is.null(names(value))) return(gsub("\\s*\n\\s*"," ",paste0(as.character(value),collapse=", ")))
  as.character(jsonlite::toJSON(value,auto_unbox=TRUE,null="null",na="null",digits=NA))
}

#' @export
format.parquet_metadata <- function(x,width=getOption("width"),max_columns=20,...){
  x <- unclass(x)
  if (length(x)==0) return("<parquet metadata: no entries>")
  columns <- if (is.list(x$columns) && !is.null(names(x$columns))) x$columns
  values <- vapply(names(x),function(name){
    if (name=="columns" && !is.null(columns)) {
      return(paste0("<",length(columns)," column",if (length(columns)!=1) "s",", see below>"))
    }
    format_metadata_value(x[[name]])
  },"")
  key_width <- max(nchar(names(x),type="width"))
  lines <- c(paste0("<parquet metadata: ",length(x)," entr",if (length(x)==1) "y" else "ies",">"),
             truncate_width(paste0(formatC(names(x),width=-key_width),"  ",values),width))
  if (!is.null(columns)) {
    shown <- utils::head(columns,max_columns)
    descriptions <- vapply(shown,function(entry){
      if (!is.list(entry)) return(format_metadata_value(entry))
      label <- if (!is.null(entry$label)) format_metadata_value(entry$label) else ""
      description <- if (!is.null(entry$description)) format_metadata_value(entry$description) else ""
      rest <- entry[setdiff(names(entry),c("label","description"))]
      paste0(label,if (label!="" && description!="") ": ",description,
             if (length(rest)>0) paste0(if (label!="" || description!="") " ",format_metadata_value(rest)))
    },"")
    column_width <- max(nchar(names(shown),type="width"))
    lines <- c(lines,"","columns",
               truncate_width(paste0("  ",formatC(names(shown),width=-column_width),"  ",descriptions),width),
               if (length(columns)>max_columns) paste0("  ... and ",length(columns)-max_columns," more"))
  }
  lines
}

#' @export
print.parquet_metadata <- function(x,...){
  cat(format(x,...),sep="\n")
  invisible(x)
}
