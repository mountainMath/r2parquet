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
  result <- collect_sf(filter_spatial(parquet_tbl(path),y))
  expect_equal(sort(result$NAME),expected)
  bbox_result <- collect_sf(filter_spatial(parquet_tbl(path),y,exact=FALSE))
  expect_true(all(expected %in% bbox_result$NAME))
  expect_gte(nrow(bbox_result),nrow(result))
})

test_that("local directories of partitioned parquet files get read", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  data$high_sid <- as.integer(data$SID74>5)
  dir <- withr::local_tempdir()
  data_to_parquet(data,file.path(dir,"nc"),partition_by="high_sid")
  result <- collect_sf(parquet_tbl(file.path(dir,"nc")))
  expect_equal(nrow(result),nrow(data))
  expect_equal(sort(result$high_sid),sort(data$high_sid))
})
