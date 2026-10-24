# Writing data frames, sf objects and lazy DuckDB tables to parquet, locally or on R2.
# Everything gets written by a DuckDB COPY, sf objects and tables with GEOMETRY columns as GeoParquet 2.0
# with the native parquet GEOMETRY type, sorted along a Hilbert curve so that the per row group bounding
# box statistics are selective.

#' write data to parquet
#'
#' @description
#' Writes a data frame, an sf object or a lazy DuckDB table to a parquet file, or to a directory of hive
#' partitioned parquet files, locally, on R2 as `r2://bucket/path` or on S3 as `s3://bucket/path`. Rows get
#' sorted so that filters on the `sort_by` columns, and spatial filters on sf data, only read the row groups
#' they need. Lazy tables get written by DuckDB directly, without collecting the data into R.
#'
#' @details
#' # Spatial data
#' sf objects get written as GeoParquet 2.0 with the native parquet GEOMETRY type, which stores the crs with
#' the column and a bounding box per row group. After the `sort_by` columns, rows get sorted along a Hilbert
#' curve so that each row group covers a compact area and [`sf::st_filter()`][st_filter.tbl_duckdb_connection]
#' on lazy tables can skip row groups. The geometry column is always named `geometry`, following the
#' GeoParquet convention.
#'
#' Invalid geometries get fixed via [sf::st_make_valid()], keeping multi geometries multi, with a message, and
#' the repairs get recorded in the metadata:
#' * `processing`: how many geometries were repaired, and with which version of GEOS (or s2 for geographic
#'   coordinates).
#' * `geometry_repairs`: the repaired rows, up to 1000, as an object with `columns`, the key columns, and
#'   `rows`, the key values per row. The key is the declared `primary_key`, or else the first column or a
#'   column ending in `ID` if it identifies the rows; otherwise only the count gets recorded, with a warning.
#'
#' Entries given in `metadata` are kept; repair the geometries before writing to avoid both.
#'
#' Lazy tables with GEOMETRY columns get written as GeoParquet too, sorted by the column named `geometry` or
#' else the first GEOMETRY column, without repairing geometries. The extent of the Hilbert curve gets computed
#' in a separate query, so the source gets scanned twice, and a remote source downloads its geometry twice.
#' Empty geometries sort last.
#'
#' # Row groups
#' Row groups are what readers can skip, so their size balances the selectivity of filters against the size
#' of the file metadata. DuckDB writes row groups in multiples of 2048 rows, so data with fewer rows ends up in
#' a single row group and filters can't skip any part of it.
#'
#' # Partitioning
#' With `partition_by`, `path` is a directory with one subdirectory per value, like
#' `year=2021/data_0.parquet`. The partition values are only stored in the directory names, readers recover
#' them with hive partitioning, DuckDB as strings (`"001"` stays `"001"`). Partitioned files get written single
#' threaded, since multi threaded writes don't keep the sort order within partitions. They get written to a new
#' directory next to `path` first and moved into place once complete, so a dataset can be rewritten from a lazy
#' table reading it, and a failed write leaves the existing files as they were. A file at `path` gets replaced
#' by the directory.
#'
#' # Writing to R2 and S3
#' Destinations in buckets get written to a temporary directory and uploaded via [upload_parquet()], into a bucket
#' that has to exist (see [create_r2_bucket()]). Each file goes up in a single request from memory, so files have
#' to fit into memory and stay below 5 GB; partition large data with `partition_by`. A partitioned directory may
#' be named like a parquet file, [parquet_tbl()] tells files and directories apart. Stale files get removed before
#' the upload, so readers may see a partitioned dataset incomplete while it runs. Changed files get purged from
#' the Cloudflare cache (R2 only) unless `purge_cache = FALSE`. Partitioned writes write a manifest of the
#' directory, and all writes update the existing manifests of the directories above the destination, see
#' [upload_parquet()].
#'
#' # Metadata
#' `metadata` gets written into the parquet footer, where other tools can read it too (DuckDB's
#' `parquet_kv_metadata`, arrow's schema metadata). [parquet_metadata()] reads it back and [collect_parquet()]
#' attaches it to collected data. Conventional entries:
#' * `source` (URL of the original data), `license`, `notes` and `description`.
#' * `columns`: a `label` and `description` per column, e.g.
#'   `columns = list(pop = list(label = "Population", description = "..."))`; a string is short for the label.
#'   `label` attributes of data frame columns, as set by the haven or labelled packages, get added
#'   automatically.
#' * `language` and `languages`: the language of the plain text, and every language the dataset holds text
#'   in, see the Languages section.
#' * `primary_key`: the columns that together identify the rows, one like `"DBUID"` or several for long
#'   tables, like `c("DAUID", "characteristic")`. They have to exist, have no missing values and identify the
#'   rows, which gets checked before writing, at the cost of an extra pass over a lazy table. It gets written
#'   as a JSON array, and is how repaired geometries get listed.
#' * `geo` is reserved for the GeoParquet metadata.
#'
#' Single values get written as strings, anything else as JSON; `NULL` and `NA` entries get dropped with a
#' warning. Changing the metadata means writing the data again.
#'
#' # Languages
#' Text can be stored in several languages, under keys with the language as suffix: `title_fr` next to
#' `title`, or `title_en` and `title_fr` without a plain `title`; `label_fr` and `description_fr` in the
#' column entries, `columns = list(pop = list(label_en = "Population", label_fr = "Population",
#' description_fr = "..."))`, with or without a plain `label`; and a column holding another column's text in
#' a language is named the same way, `name_fr` next to `name`, or `name_en` and `name_fr` without a plain
#' `name`. Which suffixes mean a language is declared, nothing is guessed from the keys. The languages of a
#' dataset are
#' * the `language` entry: the language of its plain text (`title`, `label`) and plain columns, as a
#'   language code like `"en"`;
#' * the `languages` entry: every language it holds text in, like `c("en", "fr")`, written as a JSON array;
#' * the suffixes of the `label_<language>` and `description_<language>` keys of its column entries.
#'
#' Only a suffix in that set marks a variant: `source_url` next to `source` and `rate_men` next to `rate`
#' are entries and columns of their own, and `title_fr` is the French `title` only when `fr` is a language
#' of the dataset. The column labels usually declare the languages already, `languages` is for datasets
#' whose column entries don't name them all. Language codes are lowercase, two or three letters with
#' optional subtags after a hyphen (`"en"`, `"fra"`, `"pt-br"`), and a suffix has to match exactly,
#' `name_FR` is no translation. No language is privileged: a bilingual dataset can carry only suffixed
#' entries, labels and columns, and `language` is only needed where there is plain text, to say which
#' language it is in.
#'
#' Readers get one language at a time, where the language asked for counts as a language of the dataset
#' too: [parquet_tbl()] and [select_language()] select the columns of that language under their plain names,
#' [collect_parquet()] attaches its labels and [parquet_metadata()] resolves the entries, see the Languages
#' section of [collect_parquet()] for the fallbacks.
#'
#' Metadata doesn't get carried over by itself, since data read from parquet files describes something else
#' once it got changed. With `metadata = NULL`, writing a lazy table from [parquet_tbl()] (the files of the
#' left-hand table for joins) or data collected with [collect_parquet()] warns when the files it was read from
#' have metadata:
#' * `metadata = parquet_metadata(data)` writes it again;
#' * `metadata = modifyList(parquet_metadata(data), list(...))` changes entries (a `NULL` drops one);
#' * `metadata = list()` writes none.
#'
#' Metadata read with [parquet_metadata()] describes the source, so its `columns` entries for columns the data
#' no longer has get dropped, and repaired geometries replace its `processing` and `geometry_repairs` entries.
#' A `primary_key` gets checked like any other.
#'
#' @inheritSection upload_parquet Credentials
#' @param data data frame, sf object or lazy DuckDB table.
#' @param path local path, `r2://bucket/path` or `s3://bucket/path` of the parquet file, or of the directory
#'   with `partition_by`. A file in a bucket has to end in `.parquet`. Existing files get overwritten.
#' @param sort_by column names to sort by, optional. Spatial data gets sorted spatially after these.
#' @param partition_by column names to partition by, optional; `path` is then a directory.
#' @param row_group_size rows per row group, default `10000`, rounded up to a multiple of 2048 by DuckDB.
#' @param remove_stale what happens to the files of an existing directory at `path`:
#'   * `TRUE` (default): parquet files in hive partition directories (`key=value`) get removed first, so no
#'     partitions of earlier writes remain, also if the partition columns changed. Other files are kept.
#'   * `FALSE`: existing files are kept and only the files at the paths being written get replaced, e.g. to
#'     add partitions.
#' @param metadata named list of metadata to write into the parquet footer, see the Metadata section.
#'   * `NULL` (default): none of the source's metadata, with a warning if the source has any.
#'   * `list()`: none, without the warning.
#' @param compression parquet compression codec: `"zstd"` (default), `"snappy"`, `"gzip"`, `"brotli"`,
#'   `"lz4"`, `"lz4_raw"` or `"uncompressed"`. zstd compresses well and decompresses fast, which suits files
#'   that get read remotely.
#' @param purge_cache `TRUE` (default) or `FALSE`: purge the changed files from the Cloudflare cache of the
#'   bucket's custom domains when writing to R2, like [upload_parquet()]. Ignored for S3 and local paths.
#' @return (invisibly) `path`, normalized to `r2://bucket/path` or `s3://bucket/path` for buckets, ready for
#'   [parquet_tbl()], with the written files (local paths or bucket paths) in the `files` attribute.
#' @export
data_to_parquet <- function(data,path,sort_by=NULL,partition_by=NULL,row_group_size=10000L,remove_stale=TRUE,metadata=NULL,
                            compression=c("zstd","snappy","gzip","brotli","lz4","lz4_raw","uncompressed"),purge_cache=TRUE) {
  if (!is.character(path) || length(path)!=1) stop("Expected a single path, got ",length(path),".")
  compression <- match.arg(compression)
  if (is_bucket_path(path)) {
    r2_data_to_parquet(data,path,sort_by=sort_by,partition_by=partition_by,row_group_size=row_group_size,
                       remove_stale=remove_stale,metadata=metadata,
                       compression=compression,purge_cache=purge_cache)
  } else {
    local_data_to_parquet(data,path,sort_by=sort_by,partition_by=partition_by,row_group_size=row_group_size,
                          remove_stale=remove_stale,metadata=metadata,compression=compression)
  }
}

# write data to a local parquet file or directory of partitioned files
local_data_to_parquet <- function(data,path,sort_by=NULL,partition_by=NULL,row_group_size=10000L,remove_stale=TRUE,metadata=NULL,
                                  compression="zstd") {
  # metadata read with parquet_metadata() describes the source, which the data may no longer match
  from_source <- inherits(metadata,"parquet_metadata")
  if (from_source) metadata <- unclass(metadata)
  if (row_group_size<2048) warning("DuckDB writes row groups of at least 2048 rows, row_group_size has no effect below that.",call.=FALSE)
  partitioned <- length(partition_by)>0
  if (partitioned && !remove_stale && file.exists(path) && !dir.exists(path)) {
    stop("There is a file at ",path,", which remove_stale = FALSE keeps, so the partitions can't be written there.")
  }
  # before writing, the write may replace the files a lazy table reads
  if (is.null(metadata)) warn_dropped_metadata(data)
  repaired <- FALSE
  if (inherits(data,"tbl_lazy")) {
    if (!methods::is(dbplyr::remote_con(data),"duckdb_connection")) stop("Lazy tables have to be DuckDB tables.")
    con <- dbplyr::remote_con(data)
    geometry_columns <- duckdb_geometry_columns(data)
    geometry_column <- if (length(geometry_columns)>0) duckdb_geometry_column(data,geometry_columns=geometry_columns)$name
    source <- paste0("(",dbplyr::sql_render(data),")")
    columns <- colnames(data)
  } else if (inherits(data,"sf")) {
    if ("geometry" %in% setdiff(names(data),attr(data,"sf_column"))) stop("Data has a non-geometry column named geometry.")
    con <- parquet_connection()
    load_extension(con,"spatial")
    geometry_column <- "geometry"
    if (attr(data,"sf_column")!=geometry_column) {
      message("Renaming geometry column ",attr(data,"sf_column")," to the GeoParquet standard name geometry.")
    }
    df <- sf::st_drop_geometry(data)
    fixed <- make_valid_geometries(sf::st_geometry(data))
    repaired <- attr(fixed,"repaired")
    df[[geometry_column]] <- unclass(sf::st_as_binary(fixed))
    duckdb::duckdb_register(con,"r2parquet_sf_data",df)
    on.exit(duckdb::duckdb_unregister(con,"r2parquet_sf_data"))
    geometry <- DBI::dbQuoteIdentifier(con,geometry_column)
    crs <- duckdb_crs(sf::st_crs(data))
    geometry_sql <- paste0("ST_GeomFromWKB(",geometry,")")
    if (!is.null(crs)) geometry_sql <- paste0("ST_SetCRS(",geometry_sql,", ",DBI::dbQuoteString(con,crs),")")
    # a table rather than a view, so the geometries get parsed once for the extent query and once for the copy
    DBI::dbExecute(con,paste0("CREATE OR REPLACE TABLE r2parquet_geo_data AS SELECT * EXCLUDE (",geometry,"), ",
                              geometry_sql," AS ",geometry," FROM r2parquet_sf_data"))
    on.exit(DBI::dbExecute(con,"DROP TABLE IF EXISTS r2parquet_geo_data"),add=TRUE)
    source <- "r2parquet_geo_data"
    columns <- names(df)
  } else if (is.data.frame(data)) {
    sfc_columns <- names(data)[vapply(data,inherits,logical(1),"sfc")]
    if (length(sfc_columns)>0) {
      stop(paste0("Column ",paste0(sfc_columns,collapse=", ")," holds geometries, convert data to sf via sf::st_as_sf first."))
    }
    con <- parquet_connection()
    duckdb::duckdb_register(con,"r2parquet_df_data",data)
    on.exit(duckdb::duckdb_unregister(con,"r2parquet_df_data"))
    source <- "r2parquet_df_data"
    columns <- names(data)
    geometry_column <- NULL
  } else {
    stop("data has to be a data frame, an sf object or a lazy DuckDB table.")
  }
  missing_columns <- setdiff(c(sort_by,partition_by),columns)
  if (length(missing_columns)>0) stop(paste0("Columns ",paste0(missing_columns,collapse=", ")," not found in data."))
  labels <- if (is.data.frame(data)) lapply(data,attr,"label")
  if (from_source && is.list(metadata$columns)) {
    kept <- metadata$columns[names(metadata$columns) %in% columns]
    metadata["columns"] <- if (length(kept)>0) list(kept)
  }
  primary_key <- check_primary_key(con,source,metadata,columns)
  # repairs change the data, so they get recorded in the metadata, replacing what was recorded for the source
  if (any(repaired)) {
    if (from_source) metadata[c("processing","geometry_repairs")] <- NULL
    metadata <- record_geometry_repairs(metadata,df,repaired,sf::st_geometry(data),primary_key)
  }
  metadata <- metadata_strings(metadata,columns,labels)

  order <- if (length(sort_by)>0) DBI::dbQuoteIdentifier(con,sort_by)
  options <- c("FORMAT parquet",paste0("COMPRESSION ",compression),paste0("ROW_GROUP_SIZE ",as.integer(row_group_size)))
  if (length(metadata)>0) {
    options <- c(options,paste0("KV_METADATA {",paste0(DBI::dbQuoteString(con,names(metadata)),": ",
                                                       DBI::dbQuoteString(con,unname(metadata)),collapse=", "),"}"))
  }
  if (!is.null(geometry_column)) {
    load_extension(con,"spatial")
    geometry <- DBI::dbQuoteIdentifier(con,geometry_column)
    # extent of the Hilbert curve
    stats <- DBI::dbGetQuery(con,paste0("SELECT ST_XMin(extent) AS xmin, ST_YMin(extent) AS ymin, ",
                                        "ST_XMax(extent) AS xmax, ST_YMax(extent) AS ymax FROM (",
                                        "SELECT ST_Extent_Agg(",geometry,") AS extent FROM ",source,")"))
    if (!is.na(stats$xmin)) {
      # ST_Hilbert errors on empty geometries, these sort last
      order <- c(order,paste0("CASE WHEN ST_IsEmpty(",geometry,") THEN NULL ELSE ST_Hilbert(",geometry,
                              ", {min_x: ",sprintf("%.17g",stats$xmin),", min_y: ",sprintf("%.17g",stats$ymin),
                              ", max_x: ",sprintf("%.17g",stats$xmax),", max_y: ",sprintf("%.17g",stats$ymax),
                              "}::BOX_2D) END NULLS LAST"))
    }
    options <- c(options,"GEOPARQUET_VERSION 'V2'")
  }
  # DuckDB does not create missing directories
  dir.create(dirname(path),recursive=TRUE,showWarnings=FALSE)
  target <- path
  if (partitioned) {
    # partitions get written to a new directory and moved into place afterwards: the data may be read from path,
    # where stale partitions get removed, and a failed write leaves the existing files alone
    target <- tempfile(paste0(".",basename(path),"-"),tmpdir=dirname(path))
    dir.create(target)
    on.exit(unlink(target,recursive=TRUE),add=TRUE)
    options <- c(options,paste0("PARTITION_BY (",paste0(DBI::dbQuoteIdentifier(con,partition_by),collapse=", "),")"))
    # multi threaded partitioned writes interleave chunks of rows within each partition file
    threads <- DBI::dbGetQuery(con,"SELECT current_setting('threads') AS threads")$threads
    DBI::dbExecute(con,"SET threads = 1")
    on.exit(if (DBI::dbIsValid(con)) DBI::dbExecute(con,paste0("SET threads = ",threads)),add=TRUE,after=FALSE)
  }
  DBI::dbExecute(con,paste0("COPY (SELECT * FROM ",source,
                            if (length(order)>0) paste0(" ORDER BY ",paste0(order,collapse=", ")),
                            ") TO ",DBI::dbQuoteString(con,target)," (",paste0(options,collapse=", "),")"))
  if (!partitioned) return(invisible(structure(path,files=path)))
  move_partitions(target,path,remove_stale)
  invisible(structure(path,files=dir(path,"\\.parquet$",recursive=TRUE,full.names=TRUE)))
}

# move the partition files written to directory from into the dataset directory to, replacing files with the
# same path, after removing the parquet files of earlier writes in hive partition directories if remove_stale
move_partitions <- function(from,to,remove_stale){
  # a file at to would shadow the directory
  if (file.exists(to) && !dir.exists(to)) unlink(to)
  if (remove_stale && dir.exists(to)) {
    files <- dir(to,"\\.parquet$",recursive=TRUE)
    unlink(file.path(to,files[is_partition_file(files)]))
    directories <- rev(list.dirs(to,recursive=TRUE)[-1])
    for (directory in directories) if (length(dir(directory,all.files=TRUE,no..=TRUE))==0) unlink(directory,recursive=TRUE)
  }
  for (file in dir(from,recursive=TRUE)) {
    destination <- file.path(to,file)
    dir.create(dirname(destination),recursive=TRUE,showWarnings=FALSE)
    if (!file.rename(file.path(from,file),destination)) stop("Could not move the written file ",file," into ",to,".")
  }
}

# write data to parquet in a temporary directory and upload it to R2
r2_data_to_parquet <- function(data,path,sort_by=NULL,partition_by=NULL,row_group_size=10000L,remove_stale=TRUE,metadata=NULL,
                               compression="zstd",purge_cache=TRUE) {
  p <- r2_split_path(path)
  r2_bucket <- p$bucket
  r2_path <- p$path
  r2_url <- bucket_url(r2_bucket,r2_path)
  partitioned <- length(partition_by)>0
  if (r2_path=="") stop("Please specify the path in the bucket.")
  if (!partitioned && !grepl("\\.parquet$",r2_path)) stop("The path has to be the path of a parquet file ending in .parquet.")
  tmp_dir <- tempfile()
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir,recursive=TRUE))
  local_path <- local_data_to_parquet(data,file.path(tmp_dir,basename(r2_path)),sort_by=sort_by,
                                      partition_by=partition_by,row_group_size=row_group_size,metadata=metadata,
                                      compression=compression)
  # what r2_upload removes first, given the listing of the path and the keys of the upload
  stale <- if (remove_stale) function(objects,r2_paths){
    # a file at r2_path would shadow the directory of partitions, a file is replaced by the upload anyway
    if (r2_path %in% objects$key) return(if (partitioned) r2_path)
    # partitions of earlier writes, other files in the directory are kept
    files <- objects$key[grepl("\\.parquet$",objects$key)]
    files[is_partition_file(r2_relative(files,r2_path))]
  }
  files <- attr(local_path,"files")
  message("Uploading ",format_size(sum(file.size(files)))," in ",plural(length(files),"parquet file")," to ",r2_url,".")
  uploaded <- r2_upload(local_path,r2_bucket,r2_path,stale=stale)
  if (purge_cache) r2_purge_cache_after_change(r2_bucket,union(uploaded$stale,c(uploaded$files,uploaded$manifests)))
  invisible(structure(r2_url,files=bucket_url(r2_bucket,uploaded$files)))
}

# whether paths relative to a dataset directory are in hive partition directories (key=value), as partitioned writes produce them
is_partition_file <- function(files){
  grepl("(^|/)[^/]+=[^/]*/",files)
}
