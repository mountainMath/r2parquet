# Writing data frames, sf objects and lazy DuckDB tables to parquet, locally or on R2.
# Everything gets written by a DuckDB COPY, sf objects and tables with GEOMETRY columns as GeoParquet 2.0
# with the native parquet GEOMETRY type, sorted along a Hilbert curve so that the per row group bounding
# box statistics are selective.

#' write data to parquet
#'
#' @description
#' Writes a data frame, an sf object or a lazy DuckDB table to a parquet file, or to a directory of
#' hive partitioned parquet files if `partition_by` is given, locally or on R2 as `r2://bucket/path`. Rows get sorted by the `sort_by` columns,
#' so that the row group statistics of these columns are selective and filters on them only read the
#' row groups they need when reading remotely.
#'
#' sf objects get written as GeoParquet 2.0 using the native parquet GEOMETRY type, which stores the crs with
#' the column and a bounding box per row group. After the `sort_by` columns, rows get sorted along a Hilbert curve
#' so that each row group covers a compact area and spatial filters can skip row groups based on their bounding
#' box, see `filter_spatial`. Invalid geometries get fixed via `sf::st_make_valid`, keeping multi geometries multi,
#' and the geometry column is always named `geometry`, following the GeoParquet convention, independent of its
#' name in `data`. Both emit a message when they change the data.
#'
#' Lazy DuckDB tables, e.g. from `parquet_tbl` after filtering, get written by DuckDB directly
#' without collecting the data into R. Tables with GEOMETRY columns get written as GeoParquet in the same way,
#' sorted by their first GEOMETRY column (or the one named `geometry`) and without repairing invalid geometries.
#'
#' Row groups are what readers can skip, so their size balances selectivity of filters against the size of the
#' file metadata. DuckDB writes row groups in multiples of 2048 rows, smaller values of `row_group_size` result in
#' row groups of 2048 rows, so data with fewer rows ends up in a single row group and filters can't skip any part of it.
#'
#' With `partition_by`, `path` is a directory that gets one subdirectory per value, like `year=2021/data_0.parquet`.
#' The partition values are only stored in the directory names, readers recover them from there with hive partitioning.
#' DuckDB and polars keep string values like `"001"` as strings, arrow infers the type from the values and
#' reads them as integers. Partitioned files get written single threaded, as multi threaded writes don't
#' keep the sort order within partitions.
#'
#' Destinations on R2 get written to a temporary directory and uploaded via `upload_parquet`. The bucket has to
#' exist, use `create_r2_bucket` to create it. This expects the R2 account id and the access key id and secret
#' access key of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables, and needs the arrow package. A directory of partitioned files may be named like a
#' parquet file, `parquet_tbl` tells files and directories apart.
#'
#' @param data data frame, sf object or lazy DuckDB table
#' @param path local path or `r2://bucket/path` of the parquet file to write, or of the directory if `partition_by`
#' is given. A parquet file on R2 has to end in `.parquet`. Existing files at that path get overwritten.
#' @param sort_by optional vector of column names to sort by, for spatial data before sorting spatially
#' @param partition_by optional vector of column names to partition by
#' @param row_group_size number of rows per row group, rounded up to a multiple of 2048 by DuckDB, default is 10000
#' @param overwrite if `TRUE`, the default, parquet files in an existing directory at `path` get removed first,
#' so that no partitions from earlier writes remain. With `FALSE` written files replace files with the same path
#' and other files are kept, e.g. to add partitions.
#' @return (invisibly) `path` for local paths, the uploaded files as `r2://bucket/path` for R2
#' @export
data_to_parquet <- function(data,path,sort_by=NULL,partition_by=NULL,row_group_size=10000L,overwrite=TRUE) {
  write <- if (startsWith(path,"r2://")) r2_data_to_parquet else local_data_to_parquet
  write(data,path,sort_by=sort_by,partition_by=partition_by,row_group_size=row_group_size,overwrite=overwrite)
}

# write data to a local parquet file or directory of partitioned files
local_data_to_parquet <- function(data,path,sort_by=NULL,partition_by=NULL,row_group_size=10000L,overwrite=TRUE) {
  if (row_group_size<2048) warning("DuckDB writes row groups of at least 2048 rows, row_group_size has no effect below that.",call.=FALSE)
  if (inherits(data,"tbl_lazy")) {
    if (!inherits(data,"tbl_duckdb_connection")) stop("Lazy tables have to be DuckDB tables.")
    con <- dbplyr::remote_con(data)
    geometry_columns <- names(duckdb_geometry_columns(data))
    geometry_column <- if ("geometry" %in% geometry_columns) "geometry" else if (length(geometry_columns)>0) geometry_columns[1]
    source <- paste0("(",dbplyr::sql_render(data),")")
    columns <- colnames(data)
  } else if (inherits(data,"sf")) {
    if ("geometry" %in% setdiff(names(data),attr(data,"sf_column"))) stop("Data has a non-geometry column named geometry.")
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con,shutdown=TRUE))
    DBI::dbExecute(con,"INSTALL spatial")
    DBI::dbExecute(con,"LOAD spatial")
    geometry_column <- "geometry"
    if (attr(data,"sf_column")!=geometry_column) {
      message("Renaming geometry column ",attr(data,"sf_column")," to the GeoParquet standard name geometry.")
    }
    df <- sf::st_drop_geometry(data)
    df[[geometry_column]] <- unclass(sf::st_as_binary(make_valid_geometries(sf::st_geometry(data))))
    duckdb::duckdb_register(con,"sf_data",df)
    geometry <- DBI::dbQuoteIdentifier(con,geometry_column)
    crs <- duckdb_crs(sf::st_crs(data))
    geometry_sql <- paste0("ST_GeomFromWKB(",geometry,")")
    if (!is.null(crs)) geometry_sql <- paste0("ST_SetCRS(",geometry_sql,", ",DBI::dbQuoteString(con,crs),")")
    DBI::dbExecute(con,paste0("CREATE TABLE geo_data AS SELECT * EXCLUDE (",geometry,"), ",
                              geometry_sql," AS ",geometry," FROM sf_data"))
    source <- "geo_data"
    columns <- names(df)
  } else if (is.data.frame(data)) {
    sfc_columns <- names(data)[vapply(data,inherits,logical(1),"sfc")]
    if (length(sfc_columns)>0) {
      stop(paste0("Column ",paste0(sfc_columns,collapse=", ")," holds geometries, convert data to sf via sf::st_as_sf first."))
    }
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con,shutdown=TRUE))
    duckdb::duckdb_register(con,"df_data",data)
    source <- "df_data"
    columns <- names(data)
    geometry_column <- NULL
  } else {
    stop("data has to be a data frame, an sf object or a lazy DuckDB table.")
  }
  missing_columns <- setdiff(c(sort_by,partition_by),columns)
  if (length(missing_columns)>0) stop(paste0("Columns ",paste0(missing_columns,collapse=", ")," not found in data."))

  order <- if (length(sort_by)>0) DBI::dbQuoteIdentifier(con,sort_by)
  options <- c("FORMAT parquet","COMPRESSION zstd",paste0("ROW_GROUP_SIZE ",as.integer(row_group_size)))
  if (!is.null(geometry_column)) {
    DBI::dbExecute(con,"INSTALL spatial")
    DBI::dbExecute(con,"LOAD spatial")
    geometry <- DBI::dbQuoteIdentifier(con,geometry_column)
    # extent of the Hilbert curve
    stats <- DBI::dbGetQuery(con,paste0("SELECT ST_XMin(extent) AS xmin, ST_YMin(extent) AS ymin, ",
                                        "ST_XMax(extent) AS xmax, ST_YMax(extent) AS ymax FROM (",
                                        "SELECT ST_Extent_Agg(",geometry,") AS extent FROM ",source,")"))
    if (!is.na(stats$xmin)) {
      order <- c(order,paste0("ST_Hilbert(",geometry,", {min_x: ",sprintf("%.17g",stats$xmin),", min_y: ",sprintf("%.17g",stats$ymin),
                              ", max_x: ",sprintf("%.17g",stats$xmax),", max_y: ",sprintf("%.17g",stats$ymax),"}::BOX_2D)"))
    }
    options <- c(options,"GEOPARQUET_VERSION 'V2'")
  }
  if (length(partition_by)>0) {
    # DuckDB's OVERWRITE keeps partitions that are not in the new data
    if (overwrite && dir.exists(path)) {
      unlink(dir(path,"\\.parquet$",recursive=TRUE,full.names=TRUE))
      directories <- rev(list.dirs(path,recursive=TRUE)[-1])
      for (directory in directories) if (length(dir(directory,all.files=TRUE,no..=TRUE))==0) unlink(directory,recursive=TRUE)
    }
    options <- c(options,paste0("PARTITION_BY (",paste0(DBI::dbQuoteIdentifier(con,partition_by),collapse=", "),")"),"OVERWRITE_OR_IGNORE")
    # multi threaded partitioned writes interleave chunks of rows within each partition file
    threads <- DBI::dbGetQuery(con,"SELECT current_setting('threads') AS threads")$threads
    DBI::dbExecute(con,"SET threads = 1")
    on.exit(if (DBI::dbIsValid(con)) DBI::dbExecute(con,paste0("SET threads = ",threads)),add=TRUE,after=FALSE)
  }
  DBI::dbExecute(con,paste0("COPY (SELECT * FROM ",source,
                            if (length(order)>0) paste0(" ORDER BY ",paste0(order,collapse=", ")),
                            ") TO ",DBI::dbQuoteString(con,path)," (",paste0(options,collapse=", "),")"))
  invisible(path)
}

# write data to parquet in a temporary directory and upload it to R2
r2_data_to_parquet <- function(data,path,sort_by=NULL,partition_by=NULL,row_group_size=10000L,overwrite=TRUE) {
  if (!requireNamespace("arrow",quietly=TRUE)) {
    stop("The arrow package is required to upload to R2.")
  }
  r2_bucket <- r2_split_path(path)$bucket
  r2_path <- sub("/+$","",r2_split_path(path)$path)
  r2_url <- paste0("r2://",r2_bucket,"/",r2_path)
  partitioned <- length(partition_by)>0
  if (r2_path=="") stop("Please specify the path in the bucket.")
  if (!partitioned && !grepl("\\.parquet$",r2_path)) stop("The path has to be the path of a parquet file ending in .parquet.")
  tmp_dir <- tempfile()
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir,recursive=TRUE))
  local_path <- local_data_to_parquet(data,file.path(tmp_dir,basename(r2_path)),sort_by=sort_by,
                                      partition_by=partition_by,row_group_size=row_group_size)
  if (overwrite) {
    # a file at r2_path gets replaced by the upload anyway, unless the upload is a directory
    file_type <- r2_filesystem()$GetFileInfo(paste0(r2_bucket,"/",r2_path))[[1]]$type
    if (file_type==arrow::FileType$Directory || (partitioned && file_type==arrow::FileType$File)) {
      remove_parquet(r2_url)
    }
  }
  files <- if (partitioned) dir(local_path,"\\.parquet$",recursive=TRUE,full.names=TRUE) else local_path
  message("Uploading ",format(structure(sum(file.size(files)),class="object_size"),units="auto",standard="SI"),
          " in ",length(files)," parquet file",if (length(files)>1) "s"," to ",r2_url,".")
  upload_parquet(local_path,r2_url)
}
