# GeoParquet helpers
# Reading works on lazy DuckDB tables, like the ones returned by parquet_tbl. GeoParquet files get
# written by data_to_parquet in write.R.

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

# pick the geometry column of a lazy DuckDB table, if not specified the one named geometry as in files
# written by data_to_parquet, otherwise the first one
duckdb_geometry_column <- function(tbl,geometry_column=NULL,geometry_columns=duckdb_geometry_columns(tbl)){
  if (length(geometry_columns)==0) stop("The table has no GEOMETRY column.")
  if (is.null(geometry_column)) {
    geometry_column <- if ("geometry" %in% names(geometry_columns)) "geometry" else names(geometry_columns)[1]
  }
  if (!(geometry_column %in% names(geometry_columns))) {
    stop(paste0("Column ",geometry_column," is not a GEOMETRY column, geometry columns are ",
                paste0(names(geometry_columns),collapse=", "),"."))
  }
  list(name=geometry_column,crs=geometry_columns[[geometry_column]])
}

# sf geometry column from the WKB that DuckDB returns for a GEOMETRY column, a list of raw vectors with NULL
# for missing geometries, which become empty geometries of the column's type (sf has no NULL geometry)
wkb_to_sfc <- function(wkb,crs){
  missing <- vapply(wkb,is.null,logical(1))
  geometry <- sf::st_as_sfc(structure(wkb[!missing],class="WKB"),crs=crs)
  if (!any(missing)) return(geometry)
  types <- unique(as.character(sf::st_geometry_type(geometry)))
  empty <- sf::st_as_sfc(paste0(if (length(types)==1) types else "GEOMETRYCOLLECTION"," EMPTY"),crs=crs)
  filled <- rep(empty,length(wkb))
  filled[!missing] <- geometry
  filled
}

# fix invalid geometries, leaving valid ones untouched, and keep multi geometries multi so that
# geometry types stay consistent, st_make_valid turns single part multi polygons into polygons; which
# geometries got repaired is in the repaired attribute, a logical vector
make_valid_geometries <- function(geometry){
  invalid <- !sf::st_is_valid(geometry)
  invalid[is.na(invalid)] <- TRUE
  if (!any(invalid)) return(structure(geometry,repaired=invalid))
  message("Repairing ",sum(invalid)," invalid geometr",ifelse(sum(invalid)==1,"y","ies")," via sf::st_make_valid.")
  original_types <- as.character(sf::st_geometry_type(geometry[invalid]))
  fixed <- sf::st_make_valid(geometry[invalid])
  fixed_types <- as.character(sf::st_geometry_type(fixed))
  multi <- which(grepl("^MULTI",original_types) & fixed_types==sub("^MULTI","",original_types))
  fixed[multi] <- lapply(multi,function(i)sf::st_cast(fixed[[i]],original_types[i]))
  geometry[invalid] <- fixed
  structure(geometry,repaired=invalid)
}

# at most this many repaired rows get listed in the metadata, which every reader downloads with the footer
max_listed_repairs <- 1000

# a column that identifies the rows of data, for listing repaired rows when no primary key was declared: the
# first column or a column named like an id (ending in ID), whichever is unique first, NULL if neither is
id_column <- function(data){
  candidates <- unique(c(names(data)[1],grep("id$",names(data),ignore.case=TRUE,value=TRUE)))
  for (column in candidates) {
    values <- data[[column]]
    if ((is.character(values) || is.factor(values) || is.integer(values)) && !anyNA(values) && !anyDuplicated(values)) return(column)
  }
  NULL
}

# metadata with geometry repairs recorded, unless it has the entries already: processing says how many geometries
# got repaired and by which library, since results can differ between versions (s2 for geographic coordinates
# with sf_use_s2(), GEOS otherwise), and geometry_repairs lists the repaired rows by the primary key, or by a
# column that identifies rows when none was declared, as {"columns": [...], "rows": [[...], ...]}
record_geometry_repairs <- function(metadata,data,repaired,geometry,primary_key=NULL){
  # metadata that isn't a list gets rejected by metadata_strings()
  if ((!is.null(metadata) && !is.list(metadata)) || (!is.null(metadata$processing) && !is.null(metadata$geometry_repairs))) return(metadata)
  key <- if (length(primary_key)>0) primary_key else id_column(data)
  engine <- if (sf::sf_use_s2() && isTRUE(sf::st_is_longlat(geometry))) {
    paste0("s2 ",utils::packageVersion("s2"))
  } else {
    paste0("GEOS ",sf::sf_extSoftVersion()[["GEOS"]])
  }
  note <- paste0("Repaired ",format(sum(repaired),big.mark=",")," of ",format(length(repaired),big.mark=","),
                 " geometries that were invalid, via sf::st_make_valid (",engine,"), keeping multi geometry types.")
  if (is.null(key)) {
    warning("Repaired geometries can't be listed since the data has no primary key, declare the columns that identify ",
            "rows via metadata = list(primary_key = ...).",call.=FALSE)
  } else if (sum(repaired)>max_listed_repairs) {
    note <- paste0(note," Too many to list the rows.")
  } else {
    note <- paste0(note," The repaired rows are listed in geometry_repairs by ",paste0(key,collapse=", "),".")
    rows <- lapply(data[repaired,key,drop=FALSE],function(values)if (is.factor(values)) as.character(values) else values)
    if (is.null(metadata$geometry_repairs)) {
      metadata$geometry_repairs <- list(columns=I(key),rows=lapply(seq_len(sum(repaired)),function(i)I(unname(lapply(rows,`[[`,i)))))
    }
  }
  if (is.null(metadata$processing)) metadata$processing <- note
  metadata
}

#' spatially filter a lazy DuckDB table of GeoParquet data
#'
#' @description
#' Keeps the rows whose geometry intersects `y`. Remote GeoParquet files only get read where needed:
#' DuckDB skips row groups whose bounding box does not intersect the bounding box of `y`, using the
#' statistics of the native GEOMETRY type. This only happens
#' for the bounding box comparison, a plain `ST_Intersects` filter downloads the geometry of all rows,
#' so the filter first compares bounding boxes and then, if `exact` is `TRUE`, checks for actual intersection
#' on the remaining rows. How much gets skipped depends on the file being sorted spatially, like the files
#' written by `data_to_parquet`.
#' `y` gets transformed to the crs of the geometry column if both have a crs.
#'
#' @param tbl lazy DuckDB table, e.g. from `parquet_tbl`
#' @param y sf, sfc or bbox object to filter by
#' @param geometry_column name of the geometry column, defaults to the column named `geometry`, as in files
#' written by `data_to_parquet`, or else the first GEOMETRY column of `tbl`
#' @param exact if `TRUE`, the default, keep rows whose geometry intersects `y`, otherwise keep rows whose
#' bounding box intersects the bounding box of `y`
#' @return a lazy `dplyr` table
#' @export
filter_spatial <- function(tbl,y,geometry_column=NULL,exact=TRUE) {
  con <- dbplyr::remote_con(tbl)
  load_extension(con,"spatial")
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
  if (exact) {
    conditions <- c(conditions,paste0("ST_Intersects(",geometry,", ",
                                      with_crs(paste0("ST_GeomFromHEXWKB('",sf::st_as_binary(y,hex=TRUE)[[1]],"')")),")"))
  }
  dplyr::filter(tbl,dplyr::sql(paste0(conditions,collapse=" AND ")))
}

#' collect a lazy DuckDB table of parquet data with its metadata
#'
#' @description
#' Executes the query and returns the result as a tibble, or as an sf object if the table has GEOMETRY columns,
#' like GeoParquet files written by `data_to_parquet`, converting them to sf geometry columns with their crs.
#' The active geometry column is the one named `geometry`, as `data_to_parquet` writes it, or else the first
#' GEOMETRY column, use `sf::st_set_geometry` to switch to another one. Missing (`NULL`) geometries, e.g.
#' from a left join, become empty geometries, since sf has no missing geometries.
#'
#' For tables from `parquet_tbl` the metadata of the parquet files (see `parquet_metadata`) gets attached
#' to the result: the whole list as the `metadata` attribute, and the `label` entries of `columns` as `label`
#' attributes of the columns, as the haven and labelled packages use them. The files are looked up through
#' the table, so the metadata follows the table through `dplyr` verbs and is the metadata of the left-hand
#' table of joins.
#'
#' @param tbl lazy DuckDB table, e.g. from `parquet_tbl` or `filter_spatial`
#' @param attach_metadata if `TRUE`, the default, attach the metadata of the parquet files as attributes
#' @return a tibble, or an sf object if the table has GEOMETRY columns
#' @export
collect_parquet <- function(tbl,attach_metadata=TRUE) {
  geometry_columns <- duckdb_geometry_columns(tbl)
  data <- dplyr::collect(tbl)
  for (column in names(geometry_columns)) {
    data[[column]] <- wkb_to_sfc(data[[column]],geometry_columns[[column]])
  }
  if (length(geometry_columns)>0) {
    data <- sf::st_as_sf(data,sf_column_name=duckdb_geometry_column(tbl,geometry_columns=geometry_columns)$name)
  }
  if (isTRUE(attach_metadata) && !is.null(attr(tbl,"r2parquet_paths"))) {
    data <- attach_parquet_metadata(data,duckdb_parquet_metadata(dbplyr::remote_con(tbl),attr(tbl,"r2parquet_paths")))
  }
  data
}
