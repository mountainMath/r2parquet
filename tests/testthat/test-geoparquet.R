test_that("crs gets passed to DuckDB as EPSG code or WKT", {
  expect_equal(r2parquet:::duckdb_crs(sf::st_crs(4326)),"EPSG:4326")
  expect_null(r2parquet:::duckdb_crs(sf::NA_crs_))
})

test_that("invalid multi polygons get repaired and stay multi polygons", {
  bowtie <- sf::st_multipolygon(list(list(rbind(c(0,0),c(1,1),c(1,0),c(0,1),c(0,0)))))
  square <- sf::st_multipolygon(list(list(rbind(c(0,0),c(0,1),c(1,1),c(1,0),c(0,0)))))
  geometry <- sf::st_sfc(bowtie,square)
  expect_message(fixed <- r2parquet:::make_valid_geometries(geometry),"Repairing 1 invalid geometry")
  expect_true(all(sf::st_is_valid(fixed)))
  expect_equal(as.character(sf::st_geometry_type(fixed)),c("MULTIPOLYGON","MULTIPOLYGON"))
  expect_identical(fixed[[2]],square)
  expect_equal(attr(fixed,"repaired"),c(TRUE,FALSE))
  expect_equal(attr(r2parquet:::make_valid_geometries(sf::st_sfc(square)),"repaired"),FALSE)
})

test_that("geometry repairs get recorded in the metadata", {
  skip_if_no_duckdb_extensions()
  bowtie <- sf::st_multipolygon(list(list(rbind(c(0,0),c(1,1),c(1,0),c(0,1),c(0,0)))))
  square <- sf::st_multipolygon(list(list(rbind(c(0,0),c(0,1),c(1,1),c(1,0),c(0,0)))))
  data <- sf::st_sf(name=c("a","b","b"),DBUID=c("x1","x2","x3"),geometry=sf::st_sfc(square,bowtie,square,crs=3347))
  path <- withr::local_tempfile(fileext=".parquet")
  # listed by the first unique column named like an id
  expect_message(data_to_parquet(data,path,metadata=list(source="s")),"Repairing 1 invalid geometry")
  metadata <- parquet_metadata(path)
  expect_match(metadata$processing,"^Repaired 1 of 3 geometries that were invalid, via sf::st_make_valid \\(GEOS .*by DBUID\\.$")
  expect_equal(metadata$geometry_repairs,list(columns="DBUID",rows=list("x2")))
  expect_equal(metadata$source,"s")
  # by the declared primary key, of several columns
  suppressMessages(data_to_parquet(data,path,metadata=list(primary_key=c("name","DBUID"))))
  metadata <- parquet_metadata(path)
  expect_equal(metadata$geometry_repairs,list(columns=c("name","DBUID"),rows=list(c("b","x2"))))
  # only counted without a column that identifies rows
  data$DBUID <- "x"
  expect_warning(suppressMessages(data_to_parquet(data,path)),"no primary key")
  metadata <- parquet_metadata(path)
  expect_match(metadata$processing,"^Repaired 1 of 3 geometries.*geometry types\\.$")
  expect_null(metadata$geometry_repairs)
  # entries given in the metadata are kept
  suppressWarnings(suppressMessages(data_to_parquet(data,path,metadata=list(processing="own"))))
  expect_equal(parquet_metadata(path)$processing,"own")
  # valid data gets no entries
  data_to_parquet(data[c(1,3),],path)
  expect_null(parquet_metadata(path)$processing)
})

test_that("spatial filter matches sf", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  tbl <- parquet_tbl(path)
  # query in a different crs than the data
  y <- sf::st_transform(sf::st_buffer(sf::st_centroid(sf::st_geometry(data[data$NAME=="Wake",])),20000),3857)
  # nc is longitude/latitude, compare with planar sf
  withr::local_options(sf_use_s2=FALSE)
  expected <- sort(data$NAME[lengths(suppressMessages(sf::st_intersects(data,sf::st_transform(y,sf::st_crs(data)))))>0])
  result <- collect_parquet(sf::st_filter(tbl,y))
  expect_equal(sort(result$NAME),expected)
  bbox_result <- collect_parquet(sf::st_filter(tbl,y,exact=FALSE))
  expect_true(all(expected %in% bbox_result$NAME))
  expect_gte(nrow(bbox_result),nrow(result))
  # other predicates, against several features of y without a union
  wake <- data[data$NAME %in% c("Wake","Durham"),]
  sf_names <- function(predicate,...)sort(suppressMessages(sf::st_filter(data,wake,.predicate=predicate,...))$NAME)
  duckdb_names <- function(predicate,...)sort(collect_parquet(sf::st_filter(tbl,wake,.predicate=predicate,...))$NAME)
  for (predicate in list(sf::st_touches,sf::st_within,sf::st_covered_by,sf::st_equals)) {
    expect_equal(duckdb_names(predicate),sf_names(predicate))
  }
  expect_equal(duckdb_names(sf::st_disjoint),sf_names(sf::st_disjoint))
  expect_error(sf::st_filter(tbl,wake,.predicate=sf::st_is_within_distance),"needs dist")
  expect_error(sf::st_filter(tbl,wake,.predicate=function(x,y)TRUE),"has to be one of")
  expect_error(sf::st_filter(tbl,wake,foo=1),"Unsupported arguments")
  expect_equal(nrow(collect_parquet(sf::st_filter(tbl,wake[0,]))),0)
  # within distance on projected data, sf measures distances on longitude/latitude data along great circles
  projected <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(sf::st_transform(data,3347),projected)
  near <- collect_parquet(sf::st_filter(parquet_tbl(projected),wake,.predicate=sf::st_is_within_distance,dist=units::set_units(10,km)))
  expect_equal(sort(near$NAME),
               sort(data$NAME[lengths(sf::st_is_within_distance(sf::st_transform(data,3347),sf::st_transform(wake,3347),10000))>0]))
})

test_that("spatial filters on longitude/latitude data warn with s2 on", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  wake <- data[data$NAME=="Wake",]
  withr::local_options(sf_use_s2=TRUE)
  expect_warning(sf::st_filter(parquet_tbl(path),wake),"planar")
  withr::local_options(sf_use_s2=FALSE)
  expect_no_warning(sf::st_filter(parquet_tbl(path),wake))
})

test_that("local directories of partitioned parquet files get read", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  data$high_sid <- as.integer(data$SID74>5)
  dir <- withr::local_tempdir()
  data_to_parquet(data,file.path(dir,"nc"),partition_by="high_sid")
  result <- collect_parquet(parquet_tbl(file.path(dir,"nc")))
  expect_equal(nrow(result),nrow(data))
  # partition values come back as strings
  expect_equal(sort(result$high_sid),as.character(sort(data$high_sid)))
})

test_that("collecting no rows gives an empty sf object with the crs", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  result <- collect_parquet(dplyr::filter(parquet_tbl(path),NAME=="nowhere"))
  expect_s3_class(result,"sf")
  expect_equal(nrow(result),0)
  expect_true(sf::st_crs(result)==sf::st_crs(data))
})

test_that("the geometry column named geometry is the active one, otherwise the first", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  # centroid comes before geometry in the table
  tbl <- dplyr::mutate(parquet_tbl(path),centroid=dplyr::sql("ST_Centroid(geometry)"),.before="geometry")
  expect_equal(names(duckdb_geometry_columns(tbl)),c("centroid","geometry"))
  result <- collect_parquet(tbl)
  expect_equal(attr(result,"sf_column"),"geometry")
  expect_s3_class(result$centroid,"sfc_POINT")
  expect_true(sf::st_crs(result$centroid)==sf::st_crs(data))
  expect_equal(attr(result,"sf_column"),"geometry")
  # without a column named geometry the first one is active, also for the spatial filter
  renamed <- dplyr::rename(tbl,geom=geometry)
  expect_equal(attr(collect_parquet(renamed),"sf_column"),"centroid")
  wake <- data[data$NAME=="Wake",]
  withr::local_options(sf_use_s2=FALSE)
  expect_equal(nrow(collect_parquet(sf::st_filter(renamed,wake))),nrow(collect_parquet(sf::st_filter(tbl,wake,geometry_column="centroid"))))
})

test_that("missing geometries get collected as empty geometries", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  tbl <- parquet_tbl(path)
  # NULL geometries as a left join without a match would produce them
  nulled <- dplyr::mutate(tbl,geometry=dplyr::sql("CASE WHEN NAME = 'Wake' THEN NULL ELSE geometry END"))
  result <- collect_parquet(nulled)
  expect_s3_class(result,"sf")
  expect_s3_class(result$geometry,"sfc_MULTIPOLYGON")
  expect_equal(result$NAME[sf::st_is_empty(result)],"Wake")
  expect_true(sf::st_crs(result)==sf::st_crs(data))
  expect_equal(nrow(result),nrow(data))
  # all missing
  all_null <- dplyr::mutate(tbl,geometry=dplyr::sql("CAST(NULL AS GEOMETRY)"))
  result <- collect_parquet(all_null)
  expect_s3_class(result,"sf")
  expect_true(all(sf::st_is_empty(result)))
  expect_equal(nrow(result),nrow(data))
})
