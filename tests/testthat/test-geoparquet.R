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

test_that("a non-geometry column named geometry is rejected", {
  data <- nc_data()
  data <- sf::st_set_geometry(data,"geom")
  data$geometry <- 1
  expect_error(sf_to_geoparquet(data,tempfile(fileext=".parquet")),"non-geometry column named geometry")
})

test_that("GeoParquet round trip keeps attributes, geometry and crs", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  sf_to_geoparquet(data,path,sort_by="SID74")
  result <- collect_sf(parquet_tbl(path))
  expect_s3_class(result,"sf")
  expect_equal(attr(result,"sf_column"),"geometry")
  expect_true(sf::st_crs(result)==sf::st_crs(data))
  expect_equal(nrow(result),nrow(data))
  expect_equal(result$SID74,sort(data$SID74))
  result <- result[order(result$FIPS),]
  data <- data[order(data$FIPS),]
  expect_equal(sf::st_area(result),sf::st_area(data))
})

test_that("geometry column gets renamed and bbox column added", {
  skip_if_no_duckdb_extensions()
  data <- sf::st_set_geometry(nc_data(),"geom")
  path <- withr::local_tempfile(fileext=".parquet")
  expect_message(sf_to_geoparquet(data,path,bbox_column=TRUE),"Renaming geometry column geom")
  tbl <- parquet_tbl(path)
  expect_true(all(c("geometry","bbox") %in% colnames(tbl)))
  expect_false("geom" %in% colnames(tbl))
})

test_that("spatial filter matches sf", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  sf_to_geoparquet(data,path,bbox_column=TRUE)
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
  dir <- withr::local_tempdir()
  for (sid in c(0,1)) {
    dir.create(file.path(dir,paste0("high_sid=",sid)))
    sf_to_geoparquet(data[(data$SID74>5)==sid,],file.path(dir,paste0("high_sid=",sid),"data.parquet"))
  }
  result <- collect_sf(parquet_tbl(dir))
  expect_equal(nrow(result),nrow(data))
  expect_true("high_sid" %in% names(result))
})
