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

# sf binary predicates and the DuckDB functions they translate to
duckdb_predicates <- c(st_intersects="ST_Intersects",st_within="ST_Within",st_contains="ST_Contains",
                       st_contains_properly="ST_ContainsProperly",st_covers="ST_Covers",st_covered_by="ST_CoveredBy",
                       st_touches="ST_Touches",st_overlaps="ST_Overlaps",st_crosses="ST_Crosses",st_equals="ST_Equals",
                       st_disjoint="ST_Disjoint",st_is_within_distance="ST_DWithin")

duckdb_predicate <- function(predicate){
  for (name in names(duckdb_predicates)) {
    if (identical(predicate,getExportedValue("sf",name))) return(duckdb_predicates[[name]])
  }
  stop(paste0(".predicate has to be one of the sf predicates ",paste0("sf::",names(duckdb_predicates),collapse=", "),"."))
}

#' spatially filter a lazy DuckDB table of GeoParquet data
#'
#' @description
#' The [sf::st_filter()] method for lazy DuckDB tables, like the ones from [parquet_tbl()]: keeps the rows whose
#' geometry relates to `y` by `.predicate`, translated to the DuckDB spatial function of the same name. Remote
#' GeoParquet files only get read where needed, by comparing bounding boxes first.
#'
#' @details
#' DuckDB skips row groups whose bounding box does not intersect the bounding box of `y`, using the statistics
#' of the native GEOMETRY type. This only happens for the bounding box comparison, a plain `ST_Intersects`
#' filter downloads the geometry of all rows, so the filter first compares bounding boxes and then, if `exact`
#' is `TRUE`, applies the predicate to the remaining rows. How much gets skipped depends on the file being
#' sorted spatially, like the files written by [data_to_parquet()]. [sf::st_disjoint()] can't skip anything.
#'
#' Unlike [sf::st_filter()] on sf objects, `y` gets transformed to the crs of the geometry column if both have a
#' crs. DuckDB treats coordinates as planar, also for longitude/latitude data, where sf uses spherical geometry
#' ([sf::sf_use_s2()]), so results can differ near the edges of `y`, and `dist` is in degrees. The filter warns
#' about this for longitude/latitude data while [sf::sf_use_s2()] is on.
#'
#' @param x lazy DuckDB table, e.g. from [parquet_tbl()].
#' @param y sf, sfc or bbox object to filter by.
#' @param ... `dist` for [sf::st_is_within_distance()], in the units of the crs or as a units object.
#' @param .predicate sf predicate relating the geometry of `x` to `y`: [sf::st_intersects()] (default),
#'   [sf::st_within()], [sf::st_contains()], [sf::st_contains_properly()], [sf::st_covers()],
#'   [sf::st_covered_by()], [sf::st_touches()], [sf::st_overlaps()], [sf::st_crosses()], [sf::st_equals()],
#'   [sf::st_disjoint()] or [sf::st_is_within_distance()]. As for sf objects, rows are kept if the predicate
#'   holds for any feature of `y`.
#' @param geometry_column name of the geometry column. `NULL` (default) takes the column named `geometry`, as
#'   in files written by [data_to_parquet()], or else the first GEOMETRY column of `x`.
#' @param exact which rows to keep:
#'   * `TRUE` (default): the rows for which the predicate holds.
#'   * `FALSE`: the rows whose bounding box intersects the bounding box of `y` (grown by `dist`), a superset
#'     of these.
#' @return a lazy `dplyr` table.
#' @examples
#' \donttest{
#' # DuckDB downloads its spatial extension the first time it is needed
#' if (requireNamespace("curl", quietly = TRUE) && curl::has_internet()) {
#'   library(sf)
#'   sf_use_s2(FALSE) # DuckDB is planar
#'   nc <- read_sf(system.file("shape/nc.shp", package = "sf"))
#'   path <- tempfile(fileext = ".parquet")
#'   data_to_parquet(nc, path, metadata = list())
#'   parquet_tbl(path) |>
#'     st_filter(nc[nc$NAME == "Wake", ]) |>
#'     collect_parquet()
#' }
#' }
#' @exportS3Method sf::st_filter
st_filter.tbl_duckdb_connection <- function(x,y,...,.predicate=sf::st_intersects,geometry_column=NULL,exact=TRUE) {
  predicate <- duckdb_predicate(.predicate)
  args <- list(...)
  dist <- NULL
  if (predicate=="ST_DWithin") {
    if (is.null(args$dist)) stop("sf::st_is_within_distance needs dist.")
    dist <- args$dist
    args$dist <- NULL
  }
  if (length(args)>0) stop(paste0("Unsupported arguments for st_filter on a DuckDB table: ",paste0(names(args),collapse=", "),"."))
  if (predicate=="ST_Disjoint" && !exact) stop("exact = FALSE does not work with sf::st_disjoint.")
  con <- dbplyr::remote_con(x)
  load_extension(con,"spatial")
  geometry_column <- duckdb_geometry_column(x,geometry_column)
  if (inherits(y,"bbox")) y <- sf::st_as_sfc(y)
  y <- sf::st_geometry(y)
  if (!is.na(geometry_column$crs) && !is.na(sf::st_crs(y)) && sf::st_crs(y)!=geometry_column$crs) {
    y <- sf::st_transform(y,geometry_column$crs)
  }
  crs <- if (is.na(geometry_column$crs)) sf::st_crs(y) else geometry_column$crs
  if (isTRUE(sf::st_is_longlat(crs)) && sf::sf_use_s2()) {
    warning("DuckDB treats the longitude/latitude coordinates as planar, unlike sf with sf_use_s2() on, ",
            "results can differ",if (!is.null(dist)) " and dist is in degrees",
            ". Turn s2 off via sf::sf_use_s2(FALSE) to silence this warning.",call.=FALSE)
  }
  if (inherits(dist,"units")) {
    if (is.na(crs) || is.null(crs$ud_unit)) stop("dist has units but the crs of the data has none.")
    units(dist) <- units(crs$ud_unit)
    dist <- as.numeric(dist)
  }
  if (length(y)==0 || (predicate!="ST_Disjoint" && all(sf::st_is_empty(y)))) return(dplyr::filter(x,dplyr::sql("FALSE")))
  # intersecting or being within distance of any feature is the same as for their union, which makes for one test
  if (predicate %in% c("ST_Intersects","ST_DWithin")) y <- sf::st_union(y)
  crs <- duckdb_crs(geometry_column$crs)
  with_crs <- function(sql)if (is.null(crs)) sql else paste0("ST_SetCRS(",sql,", ",DBI::dbQuoteString(con,crs),")")
  geometry <- DBI::dbQuoteIdentifier(con,geometry_column$name)
  conditions <- character(0)
  if (predicate!="ST_Disjoint") {
    bbox <- as.numeric(sf::st_bbox(y)[c("xmin","ymin","xmax","ymax")])+c(-1,-1,1,1)*(if (is.null(dist)) 0 else dist)
    envelope <- with_crs(paste0("ST_MakeEnvelope(",paste0(sprintf("%.17g",bbox),collapse=", "),")"))
    conditions <- paste0(geometry," && ",envelope)
  }
  if (exact) {
    tests <- paste0(predicate,"(",geometry,", ",
                    with_crs(paste0("ST_GeomFromHEXWKB('",sf::st_as_binary(y,hex=TRUE),"')")),
                    if (!is.null(dist)) paste0(", ",sprintf("%.17g",dist)),")")
    conditions <- c(conditions,paste0("(",paste0(tests,collapse=" OR "),")"))
  }
  dplyr::filter(x,dplyr::sql(paste0(conditions,collapse=" AND ")))
}

# tables on parquet_connection() have their own class, see r2parquet_connection
#' @rdname st_filter.tbl_duckdb_connection
#' @exportS3Method sf::st_filter
st_filter.tbl_r2parquet_connection <- st_filter.tbl_duckdb_connection

#' collect a lazy DuckDB table of parquet data with its metadata
#'
#' @description
#' Executes the query and returns the result as a tibble, or as an sf object if the table has GEOMETRY
#' columns, with the metadata of the parquet files attached.
#'
#' @details
#' GEOMETRY columns, like those of GeoParquet files written by [data_to_parquet()], become sf geometry columns
#' with their crs. The active geometry column is the one named `geometry`, as [data_to_parquet()] writes it, or
#' else the first GEOMETRY column, use [sf::st_set_geometry()] to switch to another one. Missing (`NULL`)
#' geometries, e.g. from a left join, become empty geometries, since sf has no missing geometries.
#'
#' Reading the files fails with the errors described in the Errors section of [parquet_tbl()], e.g. an
#' `r2parquet_network_error` when the server can't be reached.
#'
#' Parquet is stored by column, so a query that keeps only the columns it needs downloads only those.
#' Collecting a table on R2, S3 or at a URL that still has every column it was read with (nothing
#' selected away, no summary) gives a message, once per session, with a `select()` of the columns it is
#' about to fetch, to paste into the query and prune.
#'
#' For tables from [parquet_tbl()] the metadata of the parquet files (see [parquet_metadata()]) gets attached to
#' the result: the whole list as the `metadata` attribute, and the `label` entries of `columns` as `label`
#' attributes of the columns, as the haven and labelled packages use them. The files are looked up through the
#' table, so the metadata follows the table through `dplyr` verbs and is the metadata of the left-hand table of
#' joins. The metadata of every table from [parquet_tbl()] the result was read from, joined tables included, gets
#' attached as the `parquet_sources` attribute, see [parquet_sources()], and columns the left-hand table has no
#' label for get the labels of the other tables.
#'
#' # Languages
#' Datasets can carry their text in several languages, see the Languages section of [data_to_parquet()] for
#' the convention: labels and columns per language are named with the language as suffix, `label_fr` and
#' `name_fr`, next to a plain `label` and `name` or without them, the `language` entry names the language of
#' the plain ones, and which suffixes are languages is declared by the metadata (`language`, `languages` and
#' the suffixes of the labels) plus the language asked for here, so a column like `rate_men` next to `rate`
#' is left alone. With `language`, the result is in that language where the data has it:
#' * column labels come from the `label_<language>` entries, else from `label`, else from the label in the
#'   dataset's language;
#' * a column in that language takes the place and name of its plain column, `name_fr` gets collected as
#'   `name`, and the columns in the other languages get left out. The plain column stays where the data has
#'   none in the language asked for, and a suffixed column without a plain column or a peer in another
#'   language is left alone. The selection happens before the query runs, so the columns left out don't get
#'   read.
#'
#' The metadata attached (the `metadata` and `parquet_sources` attributes) describes the result in that
#' language too: `title_fr`, `attribution_fr` and `label_fr` take the place of `title`, `attribution` and
#' `label` (falling back to the plain entry and then to the dataset language's), the other languages' entries
#' and `languages` are left out, `columns` is keyed by the columns as collected and the `language` entry
#' names the language delivered. Writing the result with that metadata therefore gives a correct dataset in one language. The
#' metadata as stored, with all languages, is `parquet_metadata(tbl)` on the lazy table or the path. A
#' dataset without anything in the language asked for is collected as stored, metadata included.
#'
#' Without `language` the result is the dataset as stored, with all its columns, labelled with the plain
#' labels, else those in the dataset's language; columns with labels only in other languages get none. A
#' dataset that carries its text in several languages without a `language` entry, as Statistics Canada's
#' English and French data does, therefore gets no labels and no attribution until a language is asked for,
#' which a message points out once per session. The default is the `r2parquet.language` option, so
#' `options(r2parquet.language = "fr")` sets the language once for a session.
#'
#' With the option set, [parquet_tbl()] already selects the columns in that language, through
#' [select_language()], so the lazy table has `name` rather than `name_en` and `name_fr` and queries can
#' use it; the labels then come from the stored column it stands in for, and a table whose columns were
#' selected in a language is described in that language when none is passed here, whatever the option says.
#'
#' @param tbl lazy DuckDB table, e.g. from [parquet_tbl()] or [`sf::st_filter()`][st_filter.tbl_duckdb_connection].
#' @param attach_metadata `TRUE` (default) or `FALSE`: attach the metadata of the parquet files as attributes.
#' @param language language code like `"en"` or `"fr"` to collect the labels and columns in, see the Languages
#'   section, or `NULL` for the dataset as stored. Defaults to the language the table's columns were selected
#'   in by [parquet_tbl()] or [select_language()], else to the `r2parquet.language` option.
#' @return a tibble, or an sf object if the table has GEOMETRY columns.
#' @export
collect_parquet <- function(tbl,attach_metadata=TRUE,language=getOption("r2parquet.language")) {
  call <- sys.call()
  # a table whose columns select_language() picked in a language is described in that language, unless
  # another one (or none) is asked for
  language <- table_language(tbl,language,missing(language))
  con <- dbplyr::remote_con(tbl)
  scans <- table_scans(tbl)
  read_metadata <- isTRUE(attach_metadata) || !is.null(language)
  metadata <- if (read_metadata) lapply(scans,function(scan)scan_footers(con,scan))
  # the metadata attribute is the left-hand table's, which need not be from parquet_tbl
  paths <- attr(tbl,"r2parquet_paths")
  left <- match(list(paths),lapply(scans,function(scan)scan$paths))
  left_metadata <- if (!read_metadata || is.null(paths)) NULL else if (is.na(left)) scan_footers(con,left_scan(tbl,scans)) else metadata[[left]]
  # in a language, the columns in that language take the place of their plain columns and the other
  # languages' columns get left out, before collecting so they don't get read; select_language() may have
  # done that already, its selection maps the columns back to the stored ones for the labels
  columns <- colnames(tbl)
  note_unselected_columns(columns,scans)
  selection <- NULL
  recorded <- attr(tbl,"r2parquet_selection")
  if (!is.null(language)) {
    languages <- unlist(lapply(c(list(left_metadata),metadata),metadata_languages))
    selection <- language_columns(columns,language,languages)
    if (!is.null(selection)) tbl <- dplyr::select(tbl,dplyr::all_of(selection))
  }
  # the files get read now, failures get explained as in parquet_tbl
  geometry_columns <- NULL
  data <- tryCatch({
    geometry_columns <- duckdb_geometry_columns(tbl)
    dplyr::collect(tbl)
  },error=function(e){
    stop(read_error(e,unique(unlist(c(attr(tbl,"r2parquet_paths"),lapply(table_scans(tbl),function(scan)scan$paths)))),call))
  })
  for (column in names(geometry_columns)) {
    data[[column]] <- wkb_to_sfc(data[[column]],geometry_columns[[column]])
  }
  if (length(geometry_columns)>0) {
    data <- sf::st_as_sf(data,sf_column_name=duckdb_geometry_column(tbl,geometry_columns=geometry_columns)$name)
  }
  if (isTRUE(attach_metadata)) {
    # in a language the metadata describes the result: entries in that language in place of the plain ones,
    # columns as collected, so the labels are the plain ones of the resolved metadata
    if (is.null(language)) note_missing_language(c(list(left_metadata),metadata))
    if (!is.null(language)) {
      left_metadata <- resolve_language(left_metadata,language,columns,selection,recorded)
      metadata <- lapply(metadata,resolve_language,language)
    }
    data <- attach_parquet_metadata(data,left_metadata,metadata)
    if (length(scans)>0) attr(data,"parquet_sources") <- parquet_sources_table(scan_paths(scans),metadata)
  }
  data
}

# a message, once per session, when a table on R2, S3 or at a URL gets collected with every column it was
# read with (recorded by parquet_tbl() in its parquet_scans entry): parquet is stored by column and only the
# selected ones get downloaded, so the message gives the select() of the columns to paste in and prune.
# Tables recovered from their SQL have no columns recorded and give none
note_unselected_columns <- function(columns,scans){
  if (isTRUE(parquet_cache$columns_noted)) return(invisible(FALSE))
  for (scan in scans) {
    if (is.null(scan$columns) || !all(is_remote(scan$paths)) || !all(scan$columns %in% columns)) next
    parquet_cache$columns_noted <- TRUE
    message("Collecting all ",length(columns)," columns of ",paste0(scan$source,collapse=", "),". Parquet is stored by ",
            "column and only the selected columns get downloaded, so select the ones you need: add\n  select(",
            paste0(columns,collapse=", "),")\nto the query and remove the columns you don't need. This message is shown once per session.")
    return(invisible(TRUE))
  }
  invisible(FALSE)
}
