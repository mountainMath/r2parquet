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
})

test_that("spatial filter matches sf", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  # query in a different crs than the data
  y <- sf::st_transform(sf::st_buffer(sf::st_centroid(sf::st_geometry(data[data$NAME=="Wake",])),20000),3857)
  expected <- sort(data$NAME[lengths(sf::st_intersects(data,sf::st_transform(y,sf::st_crs(data))))>0])
  result <- collect_parquet(filter_spatial(parquet_tbl(path),y))
  expect_equal(sort(result$NAME),expected)
  bbox_result <- collect_parquet(filter_spatial(parquet_tbl(path),y,exact=FALSE))
  expect_true(all(expected %in% bbox_result$NAME))
  expect_gte(nrow(bbox_result),nrow(result))
})

test_that("local directories of partitioned parquet files get read", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  data$high_sid <- as.integer(data$SID74>5)
  dir <- withr::local_tempdir()
  data_to_parquet(data,file.path(dir,"nc"),partition_by="high_sid")
  result <- collect_parquet(parquet_tbl(file.path(dir,"nc")))
  expect_equal(nrow(result),nrow(data))
  expect_equal(sort(result$high_sid),sort(data$high_sid))
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
  expect_equal(nrow(collect_parquet(filter_spatial(renamed,wake))),nrow(collect_parquet(filter_spatial(tbl,wake,geometry_column="centroid"))))
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
