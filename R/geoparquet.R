# GeoParquet helpers
# Writing uses DuckDB to produce GeoParquet 2.0 files with the native parquet GEOMETRY type,
# sorted along a Hilbert curve so that the per row group bounding box statistics are selective.
# Reading works on lazy DuckDB tables, like the ones returned by r2_parquet_tbl.

# crs specification DuckDB understands, NULL if the crs is missing
duckdb_crs <- function(crs){
  if (is.na(crs)) return(NULL)
  if (!is.na(crs$epsg)) paste0("EPSG:",crs$epsg) else crs$wkt
}

# geometry columns of a lazy DuckDB table with their crs, only reads metadata
duckdb_geometry_columns <- function(tbl){
  con <- dbplyr::remote_con(tbl)
  columns <- DBI::dbGetQuery(con,paste0("DESCRIBE ",dbplyr::sql_render(tbl)))
  columns <- columns[grepl("^GEOMETRY",columns$column_type),]
  crs <- lapply(columns$column_type,function(type){
    crs <- sub("^GEOMETRY\\('(.*)'\\)$","\\1",type)
    if (crs==type) sf::NA_crs_ else sf::st_crs(gsub("''","'",crs))
  })
  stats::setNames(crs,columns$column_name)
}

# pick the geometry column of a lazy DuckDB table, the first one if not specified
duckdb_geometry_column <- function(tbl,geometry_column=NULL){
  geometry_columns <- duckdb_geometry_columns(tbl)
  if (length(geometry_columns)==0) stop("The table has no GEOMETRY column.")
  if (is.null(geometry_column)) geometry_column <- names(geometry_columns)[1]
  if (!(geometry_column %in% names(geometry_columns))) {
    stop(paste0("Column ",geometry_column," is not a GEOMETRY column, geometry columns are ",
                paste0(names(geometry_columns),collapse=", "),"."))
  }
  list(name=geometry_column,crs=geometry_columns[[geometry_column]])
}

# fix invalid geometries, leaving valid ones untouched, and keep multi geometries multi so that
# geometry types stay consistent, st_make_valid turns single part multi polygons into polygons
make_valid_geometries <- function(geometry){
  invalid <- !sf::st_is_valid(geometry)
  invalid[is.na(invalid)] <- TRUE
  if (!any(invalid)) return(geometry)
  message("Repairing ",sum(invalid)," invalid geometr",ifelse(sum(invalid)==1,"y","ies")," via sf::st_make_valid.")
  original_types <- as.character(sf::st_geometry_type(geometry[invalid]))
  fixed <- sf::st_make_valid(geometry[invalid])
  fixed_types <- as.character(sf::st_geometry_type(fixed))
  multi <- which(grepl("^MULTI",original_types) & fixed_types==sub("^MULTI","",original_types))
  fixed[multi] <- lapply(multi,function(i)sf::st_cast(fixed[[i]],original_types[i]))
  geometry[invalid] <- fixed
  geometry
}

#' write sf object to GeoParquet
#'
#' @description
#' Writes a GeoParquet 2.0 file using the native parquet GEOMETRY type, which stores the crs with the column
#' and a bounding box per row group. Rows get sorted along a Hilbert curve, so that each row group covers a
#' compact area and spatial filters can skip row groups based on their bounding box when reading remotely,
#' see `filter_spatial`. Use `sort_by` to sort by attribute columns first, so that filters on these columns
#' can skip row groups as well. Within each value of the `sort_by` columns the rows are still sorted spatially.
#' Smaller row groups allow for finer grained skipping, at the expense of larger metadata. DuckDB writes row groups
#' in multiples of 2048 rows, smaller values of `row_group_size` result in row groups of 2048 rows, so data with fewer
#' rows ends up in a single row group and filters can't skip any part of the geometry.
#'
#' Invalid geometries get fixed via `sf::st_make_valid`, keeping multi geometries multi, and the geometry column is always named `geometry`,
#' following the GeoParquet convention, independent of its name in `data`. Both emit a message when they change the data.
#'
#' Readers without support for the native GEOMETRY type, like arrow or polars, can't use the bounding box
#' statistics. For these `bbox_column=TRUE` adds a `bbox` struct column with the bounding box of each geometry,
#' whose statistics they can filter on. `filter_spatial` uses this column if present.
#'
#' @param data sf object
#' @param path path of the parquet file to write
#' @param sort_by optional vector of column names to sort by before sorting spatially
#' @param row_group_size number of rows per row group, rounded up to a multiple of 2048 by DuckDB, default is 10000
#' @param bbox_column if `TRUE`, add a `bbox` struct column with the bounding box of each geometry, default is `FALSE`
#' @return (invisibly) the path of the parquet file
#' @export
sf_to_geoparquet <- function(data,path,sort_by=NULL,row_group_size=10000L,bbox_column=FALSE) {
  if ("geometry" %in% setdiff(names(data),attr(data,"sf_column"))) stop("Data has a non-geometry column named geometry.")
  missing_columns <- setdiff(sort_by,names(data))
  if (length(missing_columns)>0) stop(paste0("Columns ",paste0(missing_columns,collapse=", ")," not found in data."))
  if (bbox_column && "bbox" %in% names(data)) stop("Data already has a bbox column.")
  if (row_group_size<2048) warning("DuckDB writes row groups of at least 2048 rows, row_group_size has no effect below that.",call.=FALSE)
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
  columns <- "*"
  if (bbox_column) {
    columns <- paste0("*, struct_pack(xmin := ST_XMin(",geometry,"), ymin := ST_YMin(",geometry,
                      "), xmax := ST_XMax(",geometry,"), ymax := ST_YMax(",geometry,")) AS bbox")
  }
  order <- c(if (length(sort_by)>0) DBI::dbQuoteIdentifier(con,sort_by),
             paste0("ST_Hilbert(",geometry,", (SELECT ST_Extent(ST_Extent_Agg(",geometry,")) FROM geo_data))"))
  DBI::dbExecute(con,paste0("COPY (SELECT ",columns," FROM geo_data ORDER BY ",paste0(order,collapse=", "),") TO ",
                            DBI::dbQuoteString(con,path),
                            " (FORMAT parquet, GEOPARQUET_VERSION 'V2', COMPRESSION zstd, ROW_GROUP_SIZE ",
                            as.integer(row_group_size),")"))
  invisible(path)
}

#' write sf object to GeoParquet on Cloudflare R2
#'
#' @description
#' Writes the sf object to a GeoParquet file via `sf_to_geoparquet` and uploads it to R2 via `parquet_to_r2`.
#' The bucket has to exist, use `create_r2_bucket` to create it.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' @param data sf object
#' @param r2_bucket R2 bucket name
#' @param r2_path path of the parquet file in the bucket, has to end in `.parquet`
#' @param ... further arguments passed on to `sf_to_geoparquet`, like `sort_by`, `row_group_size` or `bbox_column`
#' @return (invisibly) the path in the bucket of the uploaded file
#' @export
sf_to_r2_geoparquet <- function(data,r2_bucket,r2_path,...) {
  r2_path <- sub("^/+","",r2_path)
  if (!grepl("\\.parquet$",r2_path)) stop("The r2_path has to be the path of a parquet file ending in .parquet.")
  tmp_dir <- tempfile()
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir,recursive=TRUE))
  path <- sf_to_geoparquet(data,file.path(tmp_dir,basename(r2_path)),...)
  message("Uploading ",format(structure(file.size(path),class="object_size"),units="auto",standard="SI"),
          " parquet file to ",r2_bucket,"/",r2_path,".")
  parquet_to_r2(path,r2_bucket,r2_path)
}

#' spatially filter a lazy DuckDB table of GeoParquet data
#'
#' @description
#' Keeps the rows whose geometry intersects `y`. Remote GeoParquet files only get read where needed:
#' DuckDB skips row groups whose bounding box does not intersect the bounding box of `y`, using the
#' statistics of the native GEOMETRY type and of the `bbox` column if there is one. This only happens
#' for the bounding box comparison, a plain `ST_Intersects` filter downloads the geometry of all rows,
#' so the filter first compares bounding boxes and then, if `exact` is `TRUE`, checks for actual intersection
#' on the remaining rows. How much gets skipped depends on the file being sorted spatially, like the files
#' written by `sf_to_geoparquet`.
#' `y` gets transformed to the crs of the geometry column if both have a crs.
#'
#' @param tbl lazy DuckDB table, e.g. from `r2_parquet_tbl`
#' @param y sf, sfc or bbox object to filter by
#' @param geometry_column name of the geometry column, defaults to the first GEOMETRY column of `tbl`
#' @param exact if `TRUE`, the default, keep rows whose geometry intersects `y`, otherwise keep rows whose
#' bounding box intersects the bounding box of `y`
#' @return a lazy `dplyr` table
#' @export
filter_spatial <- function(tbl,y,geometry_column=NULL,exact=TRUE) {
  con <- dbplyr::remote_con(tbl)
  DBI::dbExecute(con,"INSTALL spatial")
  DBI::dbExecute(con,"LOAD spatial")
  geometry_column <- duckdb_geometry_column(tbl,geometry_column)
  if (inherits(y,"bbox")) y <- sf::st_as_sfc(y)
  y <- sf::st_geometry(y)
  if (!is.na(geometry_column$crs) && !is.na(sf::st_crs(y)) && sf::st_crs(y)!=geometry_column$crs) {
    y <- sf::st_transform(y,geometry_column$crs)
  }
  y <- sf::st_union(y)
  bbox <- sf::st_bbox(y)
  crs <- duckdb_crs(geometry_column$crs)
  with_crs <- function(sql)if (is.null(crs)) sql else paste0("ST_SetCRS(",sql,", ",DBI::dbQuoteString(con,crs),")")
  geometry <- DBI::dbQuoteIdentifier(con,geometry_column$name)
  envelope <- with_crs(paste0("ST_MakeEnvelope(",paste0(sprintf("%.17g",bbox[c("xmin","ymin","xmax","ymax")]),collapse=", "),")"))
  conditions <- paste0(geometry," && ",envelope)
  if ("bbox" %in% colnames(tbl)) {
    conditions <- c(conditions,sprintf("bbox.xmax >= %.17g AND bbox.xmin <= %.17g AND bbox.ymax >= %.17g AND bbox.ymin <= %.17g",
                                       bbox[["xmin"]],bbox[["xmax"]],bbox[["ymin"]],bbox[["ymax"]]))
  }
  if (exact) {
    conditions <- c(conditions,paste0("ST_Intersects(",geometry,", ",
                                      with_crs(paste0("ST_GeomFromHEXWKB('",sf::st_as_binary(y,hex=TRUE)[[1]],"')")),")"))
  }
  dplyr::filter(tbl,dplyr::sql(paste0(conditions,collapse=" AND ")))
}

#' collect a lazy DuckDB table of GeoParquet data into an sf object
#'
#' @description
#' Executes the query and converts the GEOMETRY columns to sf geometry columns, keeping their crs.
#'
#' @param tbl lazy DuckDB table, e.g. from `r2_parquet_tbl` or `filter_spatial`
#' @param geometry_column name of the active geometry column, defaults to the first GEOMETRY column of `tbl`
#' @return an sf object
#' @export
collect_sf <- function(tbl,geometry_column=NULL) {
  geometry_columns <- duckdb_geometry_columns(tbl)
  geometry_column <- duckdb_geometry_column(tbl,geometry_column)$name
  data <- dplyr::collect(tbl)
  for (column in names(geometry_columns)) {
    data[[column]] <- sf::st_as_sfc(structure(data[[column]],class="WKB"),crs=geometry_columns[[column]])
  }
  sf::st_as_sf(data,sf_column_name=geometry_column)
}
