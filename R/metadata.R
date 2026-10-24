# Key-value metadata in the parquet footer: source, license, notes, column labels and descriptions.
# data_to_parquet writes it with DuckDB's KV_METADATA copy option, parquet_metadata reads it back with
# parquet_kv_metadata(), and collect_parquet attaches it to the collected data as attributes. Entries are
# strings in the footer, anything that is not a single value gets stored as JSON and parsed again when read.
#
# Languages: column entries hold labels and descriptions per language under suffixed keys, label_en and
# label_fr, with the plain label as the version for readers that ask for no language, and the optional
# language entry names the language of the plain text (label, title, notes) and of the plain columns, as a
# language code like "en" or "fr". No language is privileged: a bilingual dataset can carry label_en and
# label_fr and nothing else. A reader asking for a language gets the suffixed entry where there is one, else
# the plain one, else the one in the dataset's language; a reader asking for none gets the plain one, else
# the dataset language's, else no label. Top-level text (title, attribution, notes) follows the same rule
# through resolve_language(), which collect_parquet(language=) applies to the metadata it attaches, so the
# attached metadata describes the monolingual data it comes with; the metadata as stored stays available
# from the lazy table or the path.

# the suffix of a language key like label_fr, "" for the plain key, NA for other keys
language_suffix <- function(key,base){
  ifelse(key==base,"",ifelse(startsWith(key,paste0(base,"_")),substring(key,nchar(base)+2),NA_character_))
}

# a column's label or description in a language: the suffixed entry for the language if the column has one,
# else the plain entry, else the entry in the dataset's language (the metadata's language entry), NULL without
# any. A language of NULL asks for the plain entry, with the dataset language's as the fallback
column_text <- function(entry,language=NULL,base="label",dataset_language=NULL){
  if (!is.list(entry)) return(NULL)
  single <- function(value)if (is.character(value) && length(value)==1 && !is.na(value)) value
  keys <- c(if (!is.null(language)) paste0(base,"_",language),base,
            if (is.character(dataset_language) && length(dataset_language)==1) paste0(base,"_",tolower(dataset_language)))
  for (key in unique(keys)) {
    text <- single(entry[[key]])
    if (!is.null(text)) return(text)
  }
  NULL
}

# the languages a dataset holds text in, from its metadata: the language entry and the suffixes of the
# label_xx and description_xx entries of its columns
metadata_languages <- function(metadata){
  languages <- if (is.character(metadata$language)) metadata$language
  for (entry in metadata$columns) {
    if (!is.list(entry)) next
    keys <- names(entry)
    languages <- c(languages,language_suffix(keys,"label"),language_suffix(keys,"description"))
  }
  languages <- unique(languages[!is.na(languages) & languages!=""])
  tolower(languages)
}

# the columns to collect in a language: a named character vector of the columns to keep by their names in
# the result, NULL when nothing changes. Columns with the suffix of one of the dataset's languages hold a
# column's text in that language, name_fr next to name, or name_en and name_fr without a plain name: the one in
# the language asked for takes the place (and name) of the plain column, the others get left out, and the
# plain column stays when there is none in that language, or the peers as stored when there is no plain one
# either. A lone suffixed column without a plain one or a peer in another language is left alone, as are
# columns with other suffixes like rate_men
language_columns <- function(columns,language,languages){
  # language codes are letters, digits and hyphens, safe in a pattern
  languages <- unique(tolower(c(languages,language)))
  languages <- languages[grepl("^[a-z]{2,3}(-[a-z0-9]+)*$",languages)]
  if (length(languages)==0) return(NULL)
  pattern <- paste0("^(.+)_(",paste0(languages,collapse="|"),")$")
  suffixed <- grepl(pattern,columns)
  stem <- sub(pattern,"\\1",columns)
  peers <- stats::ave(suffixed,stem,FUN=sum)
  translated <- suffixed & (stem %in% columns | peers>1)
  if (!any(translated)) return(NULL)
  of <- tolower(sub(pattern,"\\2",columns))
  selection <- stats::setNames(columns,columns)
  keep <- rep(TRUE,length(columns))
  for (i in seq_along(columns)) {
    own <- columns[translated & stem==stem[i] & of==language]
    if (translated[i]) {
      if (stem[i] %in% columns) {
        # the plain column stays, or gets replaced below
        keep[i] <- FALSE
      } else if (length(own)==1) {
        # the one in the language stands in for the missing plain column, under its name and in its place
        if (own==columns[i]) names(selection)[i] <- stem[i] else keep[i] <- FALSE
      }
    } else if (columns[i] %in% stem[translated] && length(own)==1) {
      selection[i] <- own
    }
  }
  selection <- selection[keep]
  if (identical(unname(selection),columns) && identical(names(selection),columns)) return(NULL)
  selection
}

# the language to describe a lazy table in: the language passed, checked, or, when none was passed, the
# language select_language() selected its columns in, else the default (the r2parquet.language option)
table_language <- function(tbl,language,missing){
  if (missing) attr(tbl,"r2parquet_language") %||% check_language(language) else check_language(language)
}

# the language to resolve labels in, the language argument or the r2parquet.language option, checked
check_language <- function(language){
  if (is.null(language)) return(NULL)
  if (!is.character(language) || length(language)!=1 || is.na(language) || !grepl("^[A-Za-z]{2,3}(-[A-Za-z0-9]+)*$",language)) {
    stop('language has to be a language code like "en" or "fr".')
  }
  tolower(language)
}

# a column entry in one language: label and description resolved through column_text(), the other
# languages' entries dropped, everything else kept
resolve_column_entry <- function(entry,language,dataset_language=NULL){
  if (!is.list(entry)) return(entry)
  keys <- names(entry)
  text <- list(label=column_text(entry,language,"label",dataset_language),
               description=column_text(entry,language,"description",dataset_language))
  c(text[!vapply(text,is.null,logical(1))],entry[is.na(language_suffix(keys,"label")) & is.na(language_suffix(keys,"description"))])
}

# a selection of columns (names in the result, values the columns selected) on top of the one recorded for
# the table it selects from (its columns by the stored columns they come from): the result's columns by the
# stored columns, columns the recorded selection doesn't know stand for themselves
compose_selection <- function(recorded,selection){
  if (is.null(recorded)) return(selection)
  if (is.null(selection)) return(recorded)
  origin <- unname(recorded[selection])
  origin[is.na(origin)] <- selection[is.na(origin)]
  stats::setNames(origin,names(selection))
}

# the metadata in one language, as collect_parquet(language=) delivers the data: entries in that language
# (title_fr, label_fr) take the place of the plain ones, falling back to the plain entry and then to the
# dataset language's, the other languages' entries get dropped, the columns entries follow the columns
# language_columns() selects for the data's columns, or the selection given (keyed by their names in the
# result, entries for columns the data doesn't have stay), on top of the selection select_language() recorded
# for the table, and the language entry names the language delivered. Metadata of a dataset with nothing in
# the language stays as stored, like the data
resolve_language <- function(metadata,language,columns=names(metadata$columns),selection=NULL,recorded=NULL){
  language <- check_language(language)
  metadata <- unclass(metadata)
  if (is.null(language) || length(metadata)==0) return(metadata)
  columns <- as.character(columns)
  # the stored columns the data's columns were selected from count as the data's columns too
  recorded <- recorded[names(recorded) %in% columns]
  stored <- unique(c(columns,unname(recorded)))
  if (!(language %in% print_languages(metadata) || any(endsWith(stored,paste0("_",language))))) return(metadata)
  languages <- unique(c(print_languages(metadata),language))
  selection <- selection %||% language_columns(columns,language,languages) %||% stats::setNames(columns,columns)
  selection <- compose_selection(recorded,selection)
  columns <- unique(c(stored,unname(selection)))
  dataset_language <- if (is.character(metadata$language) && length(metadata$language)==1) tolower(metadata$language)
  pattern <- paste0("^(.+)_(",paste0(languages,collapse="|"),")$")
  keys <- names(metadata)
  base <- ifelse(grepl(pattern,keys) & keys!="columns",sub(pattern,"\\1",keys),keys)
  resolved <- list()
  for (b in unique(base)) {
    variants <- keys[base==b]
    if (b=="columns" && is.list(metadata$columns) && !is.null(names(metadata$columns))) {
      entries <- metadata$columns
      # the result's columns by the columns they come from, then the entries of columns the data doesn't
      # have, except the other languages' versions of columns the data has, which stored columns like
      # name_fr beside a selected name_en are
      others <- setdiff(names(entries),columns)
      if (length(others)>0) {
        kept <- language_columns(c(columns,others),language,languages) %||% stats::setNames(nm=c(columns,others))
        others <- others[others %in% kept[names(kept)==unname(kept)]]
      }
      origins <- c(selection,stats::setNames(nm=others))
      resolved$columns <- list()
      for (name in names(origins)) {
        entry <- entries[[origins[[name]]]] %||% entries[[name]]
        if (!is.null(entry)) resolved$columns[[name]] <- resolve_column_entry(entry,language,dataset_language)
      }
    } else if (identical(variants,b)) {
      resolved[b] <- metadata[b]
    } else {
      text <- column_text(metadata[variants],language,b,dataset_language)
      if (!is.null(text)) resolved[[b]] <- text else if (b %in% variants) resolved[b] <- metadata[b]
    }
  }
  resolved$language <- language
  resolved
}

# metadata entries, with column labels of the data added, as strings for the parquet footer
metadata_strings <- function(metadata,columns,labels=NULL){
  if (!is.null(metadata) && (!is.list(metadata) || (length(metadata)>0 && (is.null(names(metadata)) || any(names(metadata)==""))))) {
    stop("metadata has to be a named list.")
  }
  if ("geo" %in% names(metadata)) stop("The metadata entry geo is reserved for the GeoParquet metadata.")
  if (!is.null(metadata$language)) {
    metadata$language <- tryCatch(check_language(metadata$language),error=function(e)stop("metadata$",conditionMessage(e),call.=FALSE))
  }
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
    metadata$columns <- check_column_languages(metadata$columns)
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

# column entries checked: labels and descriptions, plain or with a language suffix, have to be single strings.
# A plain label is not required, label_en and label_fr alone are fine, see the languages note at the top
check_column_languages <- function(columns){
  for (column in names(columns)) {
    entry <- columns[[column]]
    if (!is.list(entry)) next
    keys <- names(entry)
    for (base in c("label","description")) {
      texts <- keys[!is.na(language_suffix(keys,base))]
      bad <- texts[!vapply(entry[texts],function(value)is.character(value) && length(value)==1 && !is.na(value),logical(1))]
      if (length(bad)>0) stop(paste0("metadata$columns$",column,"$",bad[1]," has to be a single string."))
    }
  }
  columns
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
    read <- function(directory)url_get_manifest(paste0(prefix,encode(directory),if (directory!="") "/",manifest_name))
  }
  for (directory in c(key,r2_ancestors(key))) {
    # a denied request counts as no manifest: S3 denies requests for missing objects without listing rights, and
    # the footer read that follows fails on its own when access to the data is denied
    manifest <- tryCatch(read(directory),r2parquet_access_denied=function(e)NULL)
    if (is.null(manifest)) next
    if (!isTRUE(attr(manifest,"has_metadata"))) return(NULL)
    relative <- r2_relative(key,directory)
    files <- manifest[relative=="" | manifest$path==relative | startsWith(manifest$path,paste0(relative,"/")),]
    if (nrow(files)==0) return(NULL)
    return(stats::setNames(files$metadata,paste0(prefix,encode(paste0(directory,if (directory!="") "/",files$path)))))
  }
  NULL
}

# attach metadata to collected data, the whole list as the metadata attribute and column labels as label
# attributes, in the language asked for where the columns have it (see column_text for the fallbacks), also
# from the metadata of the other sources (joined tables) for columns that have none yet. A column that took
# the place of another, like name for name_fr (see language_columns), gets the label of the column it came
# from, else that of the plain one
attach_parquet_metadata <- function(data,metadata,sources=list(),language=NULL,selection=NULL){
  for (source in c(list(metadata),sources)) {
    for (column in names(data)) {
      if (!is.null(attr(data[[column]],"label"))) next
      origins <- unique(c(selection[column],column))
      for (origin in origins[!is.na(origins)]) {
        label <- column_text(source$columns[[origin]],language,dataset_language=source$language)
        if (!is.null(label)) {
          attr(data[[column]],"label") <- label
          break
        }
      }
    }
  }
  if (length(metadata)>0) attr(data,"metadata") <- metadata
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

# the paths of the tables other than the left-hand one that data was read from and that have metadata, for
# data_to_parquet to point out that it doesn't get written either. Empty when they can't be read
joined_source_paths <- function(data){
  sources <- tryCatch(parquet_sources(data),error=function(e)NULL)
  if (is.null(sources) || nrow(sources)==0) return(character(0))
  # the left-hand table comes first when it is from parquet_tbl, for collected data it has no row without metadata
  left <- if (inherits(data,"tbl_lazy")) !is.null(attr(data,"r2parquet_paths")) else !is.null(attr(data,"metadata"))
  if (left) sources <- sources[-1,]
  sources$path[vapply(sources$metadata,length,integer(1))>0]
}

# warn that the metadata of the files data was read from doesn't get written, metadata = list() writes none on purpose
warn_dropped_metadata <- function(data){
  metadata <- source_metadata(data)
  joined <- joined_source_paths(data)
  if (is.null(metadata) && length(joined)==0) return(invisible())
  joined_message <- if (length(joined)>0) {
    paste0("Joined parquet tables have metadata too (",paste0(joined,collapse=", "),"), see parquet_sources(data) ",
           "and parquet_sources_attribution(data) for an attribution of all of them.")
  }
  if (is.null(metadata)) {
    message <- paste0("The data was read from parquet files with metadata, which doesn't get written without the ",
                      "metadata argument. ",joined_message," Pass metadata = list() to write none.")
  } else {
    message <- paste0("The data was read from parquet files with metadata (",paste0(names(metadata),collapse=", "),
                      "), which doesn't get written without the metadata argument. Pass metadata = parquet_metadata(data) ",
                      "to write it again, modifyList(parquet_metadata(data), list(...)) to change entries, or ",
                      "metadata = list() to write none.",if (!is.null(joined_message)) paste0(" ",joined_message))
  }
  warning(message,call.=FALSE)
}

#' metadata of parquet files
#'
#' @description
#' Reads the key-value metadata from the footer of parquet files, like the `source`, `license`, `notes` and
#' `columns` entries written by [data_to_parquet()] with its `metadata` argument. Only metadata gets read, no
#' data.
#'
#' @details
#' Entries are strings in the footer, entries holding JSON objects or arrays, like `columns`, get parsed into
#' lists. The files of a partitioned directory get written together and should have the same metadata; where
#' they disagree the values of the first file are used, with a warning. The `geo` entry with the GeoParquet
#' metadata is left out, the crs of geometry columns is part of the GEOMETRY column type and comes with the
#' geometry when collecting via [collect_parquet()].
#'
#' For paths on R2 or S3 and at URLs the metadata comes from the manifest (`_manifest.json`, see
#' [write_manifest()]) of the directory, or of the nearest directory above the path that has one, which
#' records the footer metadata of every parquet file below it: one request for a whole dataset instead of a
#' footer read per file. [upload_parquet()] and [data_to_parquet()] keep the manifests current, files changed
#' with other tools are not reflected until [write_manifest()] rebuilds the manifest. The footers of the files
#' get read instead with `from_files = TRUE`, and for paths without a manifest (or with one written before
#' manifests recorded metadata), presigned URLs, local paths and tables that read local copies from the cache.
#'
#' @inheritSection upload_parquet Credentials
#' @param x what to read the metadata of:
#'   * URL, `r2://bucket/path`, `s3://bucket/path` or local path of a parquet file or of a directory of
#'     (partitioned) parquet files, or a vector of these;
#'   * a lazy DuckDB table from [parquet_tbl()];
#'   * data collected with [collect_parquet()], which carries the metadata in its `metadata` attribute.
#' @param from_files where to read the metadata of files on R2, S3 and at URLs from:
#'   * `FALSE` (default): the manifest, where there is one.
#'   * `TRUE`: the footers of the parquet files, current even when the files were changed with other tools.
#' @param language `NULL` (default) for the metadata as stored, with the entries of all languages, or a
#'   language code like `"fr"` for the metadata as [collect_parquet()] attaches it when collecting in that
#'   language, see its Languages section: `title_fr` and `label_fr` take the place of `title` and `label`, the
#'   other languages' entries are left out, `columns` follows the columns collected and the `language` entry
#'   names the language. Unlike [collect_parquet()], the `r2parquet.language` option does not apply, so that
#'   `parquet_metadata(tbl)` always returns everything the files hold.
#' @param ... not used.
#' @return a named list of the metadata entries, empty if the files have none (or the data was collected
#'   without attaching it), of class `parquet_metadata`, which only changes how it prints: one line per entry,
#'   lists as compact JSON, and the `columns` entry as a table of the columns with their labels and
#'   descriptions. Entries in several languages (`title` and `title_fr`, `label_en` and `label_fr`) are
#'   consolidated into one row with a column per language, the dataset's language (its `language` entry,
#'   or plain text without one) first.
#' @export
parquet_metadata <- function(x,...) UseMethod("parquet_metadata")

#' @rdname parquet_metadata
#' @export
parquet_metadata.character <- function(x,from_files=FALSE,language=NULL,...) {
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
  new_parquet_metadata(resolve_language(merge_footers(footers),language))
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.tbl_lazy <- function(x,from_files=FALSE,language=NULL,...) {
  if (is.null(attr(x,"r2parquet_paths"))) stop("The table was not created by parquet_tbl, pass the path of the parquet files instead.")
  metadata <- scan_footers(dbplyr::remote_con(x),left_scan(x),from_files)
  new_parquet_metadata(resolve_language(metadata,language,colnames(x),recorded=attr(x,"r2parquet_selection")))
}

# the parquet_scans entry of the table a lazy table from parquet_tbl was derived from (the left-hand table of
# joins), found among its scans by the files it reads, or made up from its attributes for tables created
# before parquet_scans was reset; NULL for tables not from parquet_tbl
left_scan <- function(x,scans=table_scans(x)){
  paths <- attr(x,"r2parquet_paths")
  if (is.null(paths)) return(NULL)
  for (scan in scans) if (identical(scan$paths,paths)) return(scan)
  list(paths=paths,source=attr(x,"r2parquet_source"))
}

# the metadata of a parquet_scans entry, read once per table: the entry keeps what was read, since the
# metadata gets looked up when the columns are selected in a language, when collecting and for the sources,
# and manifests of large directories are not small; a new parquet_tbl() call on the paths replaces the entry.
# from_files reads the footers afresh
scan_footers <- function(con,scan,from_files=FALSE){
  if (!isTRUE(from_files) && !is.null(scan$metadata)) return(scan$metadata)
  metadata <- scan_metadata(con,scan$paths,scan$source,from_files)
  key <- scan$key
  if (!isTRUE(from_files) && !is.null(key) && exists(key,envir=parquet_scans,inherits=FALSE)) {
    entry <- get(key,envir=parquet_scans)
    if (identical(entry$paths,scan$paths)) {
      entry$metadata <- metadata
      assign(key,entry,envir=parquet_scans)
    }
  }
  metadata
}

# the metadata of the files a table from parquet_tbl reads: the manifests of the paths the table was given serve
# when the files it reads are remote and the manifests cover all paths, a table reading local copies from the
# cache reads their footers
scan_metadata <- function(con,path,source,from_files=FALSE){
  if (!isTRUE(from_files) && length(source)>0 && all(is_remote(c(path,source)))) {
    recorded <- lapply(unique(source),manifest_footers)
    if (!any(vapply(recorded,is.null,logical(1)))) return(merge_footers(do.call(c,recorded)))
  }
  duckdb_parquet_metadata(con,path)
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.data.frame <- function(x,language=NULL,...) {
  new_parquet_metadata(resolve_language(attr(x,"metadata"),language,names(x)))
}

#' @rdname parquet_metadata
#' @export
parquet_metadata.default <- function(x,...) {
  stop("x has to be a path, a vector of paths, a lazy DuckDB table from parquet_tbl or data collected with collect_parquet.")
}

#' select the columns of a lazy table in a language
#'
#' @description
#' Selects, on a lazy table from [parquet_tbl()], the columns in one language under their plain names and
#' leaves the columns in the other languages out, before the query runs, so they never get read: `name_fr`
#' takes the place and name of `name`, or stands in for a `name` the data doesn't have, and `name_en` gets
#' left out. [parquet_tbl()] does this itself when a language is set, this is the verb to do it later, e.g.
#' after reading with `language = NULL`, or on a join of tables read without a language.
#'
#' @details
#' Which suffixes are languages is known from the metadata of the tables the lazy table was read from (the
#' `language` entry and the suffixes of the labels, see the Languages section of [collect_parquet()]), plus
#' the language asked for, so a column like `rate_men` next to `rate` is left alone; the metadata of files in
#' buckets and at URLs gets read once per table. A table without any column in the language is returned as
#' it is. The table remembers the selection and the language, so [collect_parquet()], [parquet_sources()]
#' and [parquet_metadata()] on it label `name` with the label of the column it came from, and the first two
#' describe the result in that language when no language is passed to them, whatever the option says;
#' [parquet_metadata()] returns the metadata as stored by default, as always.
#'
#' @param tbl lazy DuckDB table from [parquet_tbl()], or derived from one with `dplyr` verbs.
#' @param language language code like `"en"` or `"fr"`, or `NULL` to return the table unchanged. Defaults to
#'   the `r2parquet.language` option.
#' @return the lazy table with the columns of the language selected.
#' @examples
#' \donttest{
#' if (requireNamespace("curl", quietly = TRUE) && curl::has_internet()) {
#'   characteristics <- parquet_tbl("https://data.mountainmath.ca/census/2021/profile_characteristics.parquet",
#'                                  language = NULL)
#'   colnames(characteristics)
#'   colnames(select_language(characteristics, "fr"))
#' }
#' }
#' @export
select_language <- function(tbl,language=getOption("r2parquet.language")){
  language <- check_language(language)
  if (is.null(language)) return(tbl)
  if (!inherits(tbl,"tbl_lazy")) stop("tbl has to be a lazy table from parquet_tbl.")
  con <- dbplyr::remote_con(tbl)
  scans <- table_scans(tbl)
  left <- left_scan(tbl,scans)
  if (!is.null(left) && !any(vapply(scans,function(scan)identical(scan$paths,left$paths),logical(1)))) scans <- c(list(left),scans)
  languages <- unlist(lapply(scans,function(scan)metadata_languages(scan_footers(con,scan))))
  columns <- colnames(tbl)
  selection <- language_columns(columns,language,languages)
  recorded <- attr(tbl,"r2parquet_selection") %||% stats::setNames(columns,columns)
  if (!is.null(selection)) tbl <- dplyr::select(tbl,dplyr::all_of(selection))
  attr(tbl,"r2parquet_selection") <- compose_selection(recorded,selection %||% stats::setNames(columns,columns))
  attr(tbl,"r2parquet_language") <- language
  tbl
}

#' metadata of all parquet sources of a table
#'
#' @description
#' The metadata of every table from [parquet_tbl()] that a lazy table or collected data was read from, one row
#' per table: for joins the left-hand table and the joined ones, for example to credit all of them in a caption.
#' [parquet_metadata()] only has the left-hand table's.
#'
#' @details
#' The tables get found through the query of the lazy table, so tables joined with [dplyr::left_join()] and the
#' other joins, [dplyr::semi_join()], [dplyr::union_all()] and the like count, also when they were never assigned
#' to a variable. [collect_parquet()] attaches the result to the collected data as the `parquet_sources`
#' attribute. A table read from several paths in one [parquet_tbl()] call is one row, with its metadata merged
#' as [parquet_metadata()] merges it. Tables not created by [parquet_tbl()] in the current session, like data
#' copied to the connection, have no row.
#'
#' @inheritSection upload_parquet Credentials
#' @param x a lazy DuckDB table, e.g. from [parquet_tbl()] and joins of such tables, or data collected with
#'   [collect_parquet()].
#' @param from_files where lazy tables read the metadata of files on R2, S3 and at URLs from, see
#'   [parquet_metadata()].
#' @param language language code like `"fr"` for the metadata of each source in that language, as
#'   [collect_parquet()] attaches it (`attribution_fr` becomes `attribution`, see its Languages section), or
#'   `NULL` for the metadata as stored. Defaults, like [collect_parquet()], to the language the columns of a
#'   lazy table were selected in by [parquet_tbl()] or [select_language()], else to the `r2parquet.language`
#'   option. Without a language, a source with text in several languages and no `language` entry has no plain
#'   `attribution` to return, which a message points out once per session.
#' @param ... not used.
#' @return a tibble with one row per source table, left-hand table first: `path`, the path(s) passed to
#'   [parquet_tbl()], separated by `", "`; a column for each metadata entry that holds a single value in any of
#'   the sources, like `title`, `attribution` or `licence`, `NA` for sources without it; and `metadata`, the
#'   whole metadata of each source as returned by [parquet_metadata()].
#' @examples
#' \donttest{
#' if (requireNamespace("curl", quietly = TRUE) && curl::has_internet()) {
#'   census <- "https://data.mountainmath.ca/census/2021/"
#'   # the dissemination blocks of one dissemination area, with their population
#'   blocks <- parquet_tbl(paste0(census,"ldb_000b21f_e.parquet")) |>
#'     dplyr::filter(DAUID == "59150307") |>
#'     dplyr::select(DBUID,geometry)
#'   population <- parquet_tbl(paste0(census,"2021_92-151_X.parquet")) |>
#'     dplyr::select(DBUID,DBPOP2021)
#'   data <- collect_parquet(dplyr::inner_join(blocks,population,by="DBUID"))
#'   paste(unique(parquet_sources(data)$attribution),collapse=" | ")
#' }
#' }
#' @export
parquet_sources <- function(x,...) UseMethod("parquet_sources")

#' @rdname parquet_sources
#' @export
parquet_sources.tbl_lazy <- function(x,from_files=FALSE,language=getOption("r2parquet.language"),...) {
  scans <- table_scans(x)
  con <- dbplyr::remote_con(x)
  metadata <- lapply(scans,function(scan)scan_footers(con,scan,from_files))
  language <- table_language(x,language,missing(language))
  if (is.null(language)) note_missing_language(metadata)
  parquet_sources_table(scan_paths(scans),lapply(metadata,resolve_language,language))
}

#' @rdname parquet_sources
#' @export
parquet_sources.data.frame <- function(x,language=getOption("r2parquet.language"),...) {
  sources <- attr(x,"parquet_sources")
  if (is.null(sources)) return(parquet_sources_table(character(0),list()))
  if (is.null(check_language(language))) note_missing_language(sources$metadata)
  parquet_sources_table(sources$path,lapply(sources$metadata,resolve_language,language))
}

#' @rdname parquet_sources
#' @export
parquet_sources.default <- function(x,...) {
  stop("x has to be a lazy DuckDB table or data collected with collect_parquet.")
}

#' attribution of all parquet sources of a table
#'
#' @description
#' The `attribution` metadata entries of all tables from [parquet_tbl()] that a lazy table or collected data was
#' read from (see [parquet_sources()]) as one string, e.g. for the caption of a plot of joined data.
#'
#' @details
#' Repeated attributions appear once. Attributions that differ only in their last word get combined: "Statistics
#' Canada Product ldb_000b21f_e" and "Statistics Canada Product 2021_92-151_X" become "Statistics Canada Product
#' ldb_000b21f_e, 2021_92-151_X", other attributions get separated by `sep`, in the order of the sources.
#'
#' @param x a lazy DuckDB table, e.g. from [parquet_tbl()] and joins of such tables, or data collected with
#'   [collect_parquet()].
#' @param sep separator between attributions that don't get combined.
#' @param language language code like `"fr"` for the attributions in that language (`attribution_fr`, falling
#'   back to `attribution`), or `NULL` for the plain ones, see [parquet_sources()]. Defaults, like
#'   [parquet_sources()], to the language a lazy table's columns were selected in, else to the
#'   `r2parquet.language` option.
#' @return a string, or `NULL` if none of the sources has an attribution, which leaves out a caption passed to
#'   [ggplot2::labs()].
#' @examples
#' \donttest{
#' if (requireNamespace("curl", quietly = TRUE) && curl::has_internet()) {
#'   census <- "https://data.mountainmath.ca/census/2021/"
#'   # the dissemination blocks of one dissemination area, with their population
#'   blocks <- parquet_tbl(paste0(census,"ldb_000b21f_e.parquet")) |>
#'     dplyr::filter(DAUID == "59150307") |>
#'     dplyr::select(DBUID,geometry)
#'   population <- parquet_tbl(paste0(census,"2021_92-151_X.parquet")) |>
#'     dplyr::select(DBUID,DBPOP2021)
#'   data <- collect_parquet(dplyr::inner_join(blocks,population,by="DBUID"))
#'   parquet_sources_attribution(data)
#' }
#' }
#' @export
parquet_sources_attribution <- function(x,sep=" | ",language=getOption("r2parquet.language")){
  sources <- if (missing(language)) parquet_sources(x) else parquet_sources(x,language=language)
  combine_attributions(sources[["attribution"]],sep)
}

# attributions as one string, the ones that differ only in their last word combined, NULL without any
combine_attributions <- function(attribution,sep=" | "){
  attribution <- unique(trimws(attribution[!is.na(attribution)]))
  attribution <- attribution[nzchar(attribution)]
  if (length(attribution)==0) return(NULL)
  words <- strsplit(attribution,"\\s+")
  last <- vapply(words,function(w)w[length(w)],"")
  # single words have nothing to share, their key can't match a prefix
  prefix <- vapply(words,function(w)if (length(w)>1) paste0(w[-length(w)],collapse=" ") else NA_character_,"")
  key <- ifelse(is.na(prefix),paste0("\001",attribution),prefix)
  groups <- split(seq_along(attribution),factor(key,levels=unique(key)))
  parts <- vapply(groups,function(i)if (length(i)==1) attribution[i] else paste0(prefix[i[1]]," ",paste0(last[i],collapse=", ")),"")
  paste0(parts,collapse=sep)
}

# the paths of parquet_scans entries as parquet_sources shows them, the paths given to parquet_tbl joined
scan_paths <- function(scans) vapply(scans,function(scan)paste0(scan$source,collapse=", "),"")

# the parquet_sources tibble for source paths and their metadata, single valued entries as columns
parquet_sources_table <- function(paths,metadata){
  metadata <- lapply(metadata,new_parquet_metadata)
  single <- function(value)is.atomic(value) && length(value)==1 && is.null(names(value))
  entries <- unique(unlist(lapply(metadata,function(m)names(m)[vapply(unclass(m),single,logical(1))])))
  sources <- dplyr::tibble(path=as.character(paths))
  for (entry in setdiff(entries,c("path","metadata"))) {
    sources[[entry]] <- vapply(metadata,function(m)if (single(m[[entry]])) as.character(m[[entry]]) else NA_character_,"")
  }
  sources$metadata <- metadata
  sources
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

# the languages shown when printing: the dataset's languages plus the suffixes of top-level entries that come
# with a plain entry or another suffixed one of the same base (title and title_fr, or title_en and title_fr).
# Suffixes the dataset doesn't name as languages count only as two letters: licence_url next to licence is no
# variant of licence, and three letter suffixes like url are common, while a three letter language code (fra)
# has to come from the language entry or the column labels
print_languages <- function(metadata){
  keys <- names(metadata)
  known <- metadata_languages(metadata)
  match <- regmatches(keys,regexec("^(.+)_([a-z]{2,3})$",keys))
  base <- vapply(match,function(m)if (length(m)==3) m[2] else NA_character_,"")
  suffix <- vapply(match,function(m)if (length(m)==3) m[3] else NA_character_,"")
  paired <- !is.na(base) & (base %in% keys | base %in% base[duplicated(base)]) & (nchar(suffix)==2 | suffix %in% known)
  unique(c(known,suffix[paired]))
}

# a message, once per session, when metadata is resolved without a language and a dataset has text in several
# languages but no language entry: nothing plain to fall back on, so no labels or attributions get resolved
# until a language is asked for
note_missing_language <- function(metadata){
  if (isTRUE(parquet_cache$language_noted)) return(invisible(FALSE))
  peers <- Filter(function(m)is.list(m) && is.null(m$language) && length(print_languages(m))>0,metadata)
  if (length(peers)==0) return(invisible(FALSE))
  languages <- unique(unlist(lapply(peers,print_languages)))
  parquet_cache$language_noted <- TRUE
  message("The data carries its text in ",paste0(languages,collapse=" and ")," with no plain text to fall back on, so no ",
          "labels or attributions are resolved without a language. Set options(r2parquet.language = \"",languages[1],
          "\") for the session, or pass language = \"",languages[1],"\" to collect_parquet(), parquet_sources() or ",
          "parquet_sources_attribution(); parquet_metadata() returns the metadata in all languages. This message is ",
          "shown once per session.")
  invisible(TRUE)
}

# the text of a consolidated row in a language, "" without one
text_in <- function(texts,language) if (language %in% names(texts)) texts[[language]] else ""

# one row per base key of a named list, for printing: the key, whether it comes in several languages and the
# text per language, plain text under the dataset's language or under "plain" without one. The suffixed entry
# in the dataset's language wins over a plain one. value(entry) formats one entry
language_rows <- function(entries,languages,dataset_language,value=format_metadata_value){
  keys <- names(entries)
  pattern <- if (length(languages)>0) paste0("^(.+)_(",paste0(languages,collapse="|"),")$") else "^$"
  suffixed <- grepl(pattern,keys)
  base <- ifelse(suffixed,sub(pattern,"\\1",keys),keys)
  of <- ifelse(suffixed,sub(pattern,"\\2",keys),dataset_language %||% "plain")
  lapply(unique(base),function(b){
    texts <- stats::setNames(vapply(entries[base==b],value,""),of[base==b])
    list(key=b,variants=any(suffixed[base==b]),texts=texts[!duplicated(names(texts),fromLast=TRUE)])
  })
}

# the languages the rows with variants use: the dataset's language, or plain text without one, first
display_languages <- function(rows,dataset_language){
  used <- unique(unlist(lapply(rows,function(row)if (row$variants) names(row$texts))))
  if (length(used)==0) return(character(0))
  unique(c(dataset_language %||% intersect(used,"plain"),setdiff(used,"plain")))
}

# lines of indented keys followed by one value spanning the width, or by one cell per language for rows with
# variants, under a title at the margin that is followed by the language names when there are any
language_lines <- function(rows,languages,width,indent="",title=NULL){
  key_width <- max(nchar(vapply(rows,function(row)row$key,""),type="width"),
                   if (length(languages)>0) nchar(title,type="width")-nchar(indent))
  cell <- max(1,(width-nchar(indent)-key_width-2-2*(length(languages)-1))%/%max(1,length(languages)))
  line <- function(key,cells,at_margin=FALSE) {
    key <- if (at_margin) formatC(key,width=-(nchar(indent)+key_width)) else paste0(indent,formatC(key,width=-key_width))
    sub("\\s+$","",paste0(key,"  ",paste0(formatC(truncate_width(cells,cell),width=-cell),collapse="  ")))
  }
  c(if (length(languages)>0) line(title %||% "",languages,at_margin=TRUE) else title,
    vapply(rows,function(row){
      if (!row$variants) return(truncate_width(paste0(indent,formatC(row$key,width=-key_width),"  ",row$texts[[1]]),width))
      line(row$key,vapply(languages,function(language)text_in(row$texts,language),""))
    },"",USE.NAMES=FALSE))
}

# a column's entry as rows of text per language: "label: description" from the label and description in each
# language, other entries as JSON after the text in the dataset's language
column_row <- function(column,entry,languages,dataset_language){
  if (!is.list(entry)) return(list(key=column,variants=FALSE,texts=format_metadata_value(entry)))
  keys <- names(entry)
  is_label <- !is.na(language_suffix(keys,"label"))
  is_description <- !is.na(language_suffix(keys,"description"))
  labels <- language_rows(entry[is_label],languages,dataset_language)
  descriptions <- language_rows(entry[is_description],languages,dataset_language)
  texts <- if (length(labels)>0) labels[[1]]$texts else character(0)
  for (language in names(if (length(descriptions)>0) descriptions[[1]]$texts)) {
    label <- text_in(texts,language)
    texts[language] <- paste0(label,if (label!="") ": ",descriptions[[1]]$texts[[language]])
  }
  variants <- any(c(labels,descriptions) |> vapply(function(row)row$variants,logical(1)))
  if (any(!is_label & !is_description)) {
    at <- if (length(texts)>0) names(texts)[1] else dataset_language %||% "plain"
    text <- text_in(texts,at)
    texts[at] <- paste0(text,if (text!="") " ",format_metadata_value(entry[!is_label & !is_description]))
  }
  if (length(texts)==0) texts <- stats::setNames("",dataset_language %||% "plain")
  list(key=column,variants=variants,texts=texts)
}

#' @export
format.parquet_metadata <- function(x,width=getOption("width"),max_columns=20,...){
  x <- unclass(x)
  if (length(x)==0) return("<parquet metadata: no entries>")
  columns <- if (is.list(x$columns) && !is.null(names(x$columns))) x$columns
  languages <- print_languages(x)
  dataset_language <- if (is.character(x$language) && length(x$language)==1) tolower(x$language)
  # the entries, with text in several languages (title and title_fr) consolidated into one row of cells
  rows <- language_rows(x,languages,dataset_language,function(entry){
    if (!is.null(columns) && identical(entry,columns)) {
      paste0("<",length(columns)," column",if (length(columns)!=1) "s",", see below>")
    } else format_metadata_value(entry)
  })
  lines <- c(paste0("<parquet metadata: ",length(x)," entr",if (length(x)==1) "y" else "ies",">"),
             language_lines(rows,display_languages(rows,dataset_language),width))
  if (!is.null(columns)) {
    shown <- utils::head(columns,max_columns)
    column_rows <- Map(column_row,names(shown),shown,MoreArgs=list(languages=languages,dataset_language=dataset_language))
    lines <- c(lines,"",
               language_lines(column_rows,display_languages(column_rows,dataset_language),width,"  ","columns"),
               if (length(columns)>max_columns) paste0("  ... and ",length(columns)-max_columns," more"))
  }
  lines
}

#' @export
print.parquet_metadata <- function(x,...){
  cat(format(x,...),sep="\n")
  invisible(x)
}
