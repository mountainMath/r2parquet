# Key-value metadata in the parquet footer: source, license, notes, column labels and descriptions.
# data_to_parquet writes it with DuckDB's KV_METADATA copy option, parquet_metadata reads it back with
# parquet_kv_metadata(), and collect_parquet attaches it to the collected data as attributes. Entries are
# strings in the footer, anything that is not a single value gets stored as JSON and parsed again when read.

# metadata entries, with column labels of the data added, as strings for the parquet footer
metadata_strings <- function(metadata,columns,labels=NULL){
  if (!is.null(metadata) && (!is.list(metadata) || is.null(names(metadata)) || any(names(metadata)==""))) {
    stop("metadata has to be a named list.")
  }
  if ("geo" %in% names(metadata)) stop("The metadata entry geo is reserved for the GeoParquet metadata.")
  metadata <- metadata[!vapply(metadata,is.null,logical(1))]
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
  if (length(metadata)==0) return(NULL)
  vapply(metadata,function(value){
    if (is.atomic(value) && length(value)==1 && is.null(names(value))) as.character(value)
    else as.character(jsonlite::toJSON(value,auto_unbox=TRUE,null="null",na="null",digits=NA))
  },character(1))
}

# metadata values parsed back from the footer strings, JSON objects and arrays get parsed, the rest stays a string
parse_metadata_value <- function(value){
  if (grepl("^\\s*[\\[{]",value)) {
    parsed <- tryCatch(jsonlite::fromJSON(value,simplifyDataFrame=FALSE,simplifyMatrix=FALSE),error=function(e)NULL)
    if (!is.null(parsed)) return(parsed)
  }
  value
}

# metadata from the footers of parquet files, resolved paths or globs on a connection that can read them
duckdb_parquet_metadata <- function(con,path){
  files <- paste0(DBI::dbQuoteString(con,path),collapse=", ")
  if (length(path)>1) files <- paste0("[",files,"]")
  kv <- DBI::dbGetQuery(con,paste0("SELECT file_name, decode(key) AS k, decode(value) AS v FROM parquet_kv_metadata(",files,") ORDER BY file_name"))
  kv <- kv[kv$k!="geo",]
  if (nrow(kv)==0) return(list())
  values <- unique(kv[c("k","v")])
  conflicts <- unique(values$k[duplicated(values$k)])
  first <- kv[!duplicated(kv$k),]
  if (length(conflicts)>0) {
    warning("The files disagree on the metadata entr",if (length(conflicts)>1) "ies " else "y ",paste0(conflicts,collapse=", "),
            ", using the values of ",first$file_name[1],".",call.=FALSE)
  }
  lapply(stats::setNames(first$v,first$k),parse_metadata_value)
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

#' metadata of parquet files
#'
#' @description
#' Reads the key-value metadata from the footer of parquet files, like the `source`, `license`, `notes` and
#' `columns` entries written by `data_to_parquet` with its `metadata` argument. Only the file footers get read.
#' Entries are strings in the footer, entries holding JSON objects or arrays, like `columns`, get parsed into
#' lists. A directory of partitioned files has to have the same metadata in all its files, since they get
#' written together, if the files disagree the values of the first file are used with a warning.
#' The `geo` entry with the GeoParquet metadata is left out, the crs of geometry columns is available via
#' the GEOMETRY column type and comes with the geometry when collecting via `collect_parquet`.
#'
#' @param x URL, `r2://bucket/path` or local path of the parquet file or the directory with (partitioned)
#' parquet files, a vector of these, a lazy DuckDB table from `parquet_tbl`, or data collected with
#' `collect_parquet`, which carries the metadata in its `metadata` attribute. Paths need the same
#' credentials as `parquet_tbl`.
#' @return a named list of the metadata entries, empty if the files have none (or the data was collected
#' without attaching it)
#' @export
parquet_metadata <- function(x) UseMethod("parquet_metadata")

#' @rdname parquet_metadata
#' @export
parquet_metadata.character <- function(x) {
  con <- parquet_connection()
  duckdb_parquet_metadata(con,duckdb_parquet_paths(con,x))
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.tbl_lazy <- function(x) {
  path <- attr(x,"r2parquet_paths")
  if (is.null(path)) stop("The table was not created by parquet_tbl, pass the path of the parquet files instead.")
  duckdb_parquet_metadata(dbplyr::remote_con(x),path)
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.data.frame <- function(x) {
  metadata <- attr(x,"metadata")
  if (is.null(metadata)) list() else metadata
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.default <- function(x) {
  stop("x has to be a path, a vector of paths, a lazy DuckDB table from parquet_tbl or data collected with collect_parquet.")
}
