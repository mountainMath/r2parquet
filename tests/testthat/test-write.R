test_that("a non-geometry column named geometry is rejected", {
  data <- nc_data()
  data <- sf::st_set_geometry(data,"geom")
  data$geometry <- 1
  expect_error(data_to_parquet(data,tempfile(fileext=".parquet")),"non-geometry column named geometry")
})

test_that("data frames with geometry columns that are not sf get rejected", {
  data <- nc_data()
  df <- data.frame(NAME=data$NAME,geom=sf::st_geometry(data))
  expect_error(data_to_parquet(df,tempfile(fileext=".parquet")),"convert data to sf")
})

test_that("sort_by and partition_by columns have to exist", {
  expect_error(data_to_parquet(data.frame(a=1),tempfile(fileext=".parquet"),sort_by="b"),"Columns b not found")
  expect_error(data_to_parquet(data.frame(a=1),tempfile(fileext=".parquet"),partition_by="b"),"Columns b not found")
})

test_that("data frames get written as sorted parquet without extensions", {
  df <- data.frame(id=c("003","001","002"),value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,sort_by="id")
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  result <- DBI::dbGetQuery(con,paste0("SELECT * FROM read_parquet('",path,"')"))
  expect_equal(result,df[order(df$id),],ignore_attr=TRUE)
})

test_that("partitioned data frames keep string partition values and the sort order", {
  df <- data.frame(group=rep(c("001","010"),each=5000),value=sample.int(10000))
  dir <- withr::local_tempdir()
  data_to_parquet(df,file.path(dir,"data"),sort_by="value",partition_by="group")
  expect_setequal(dir(file.path(dir,"data")),c("group=001","group=010"))
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  for (group in c("001","010")) {
    value <- DBI::dbGetQuery(con,paste0("SELECT value FROM read_parquet('",file.path(dir,"data",paste0("group=",group),"*.parquet"),"')"))$value
    expect_equal(value,sort(df$value[df$group==group]))
  }
  result <- dplyr::collect(parquet_tbl(file.path(dir,"data")))
  expect_type(result$group,"character")
  # writing again replaces the directory
  data_to_parquet(df[df$group=="001",],file.path(dir,"data"),partition_by="group")
  expect_equal(dir(file.path(dir,"data")),"group=001")
})

test_that("GeoParquet round trip keeps attributes, geometry and crs", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path,sort_by="SID74")
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

test_that("geometry column gets renamed, polygons get a bbox column that collect_sf drops", {
  skip_if_no_duckdb_extensions()
  data <- sf::st_set_geometry(nc_data(),"geom")
  path <- withr::local_tempfile(fileext=".parquet")
  expect_message(data_to_parquet(data,path),"Renaming geometry column geom")
  tbl <- parquet_tbl(path)
  expect_true(all(c("geometry","bbox") %in% colnames(tbl)))
  expect_false("geom" %in% colnames(tbl))
  expect_false("bbox" %in% names(collect_sf(tbl)))
})

test_that("points get no bbox column", {
  skip_if_no_duckdb_extensions()
  data <- sf::st_centroid(sf::st_set_agr(nc_data(),"constant"))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  expect_false("bbox" %in% colnames(parquet_tbl(path)))
})

test_that("an existing bbox column of the data is kept", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  data$bbox <- "user data"
  path <- withr::local_tempfile(fileext=".parquet")
  expect_message(data_to_parquet(data,path),"already has a bbox column")
  result <- collect_sf(parquet_tbl(path))
  expect_equal(unique(result$bbox),"user data")
  # filter_spatial does not mistake it for a bounding box column
  wake <- data[data$NAME=="Wake",]
  expect_true("Wake" %in% collect_sf(filter_spatial(parquet_tbl(path),wake))$NAME)
})

test_that("lazy DuckDB tables get written without collecting, keeping geometry and crs", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path)
  subset_path <- withr::local_tempfile(fileext=".parquet")
  dplyr::filter(parquet_tbl(path),BIR74>10000) |>
    data_to_parquet(subset_path,sort_by="NAME")
  tbl <- parquet_tbl(subset_path)
  # the bbox column gets recomputed rather than duplicated
  expect_equal(sum(colnames(tbl)=="bbox"),1)
  result <- collect_sf(tbl)
  expect_equal(result$NAME,sort(data$NAME[data$BIR74>10000]))
  expect_true(sf::st_crs(result)==sf::st_crs(data))
})

test_that("partitioned GeoParquet keeps the Hilbert order within partitions", {
  skip_if_no_duckdb_extensions()
  set.seed(1)
  points <- sf::st_as_sf(data.frame(group=rep(c("a","b"),each=20000),x=runif(40000),y=runif(40000)),
                         coords=c("x","y"),crs=3857)
  dir <- withr::local_tempdir()
  data_to_parquet(points,file.path(dir,"points"),partition_by="group")
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  DBI::dbExecute(con,"LOAD spatial")
  # the Hilbert curve spans the extent of all the data
  extent <- sprintf("%.17g",sf::st_bbox(points))
  for (group in c("a","b")) {
    files <- file.path(dir,"points",paste0("group=",group),"*.parquet")
    h <- DBI::dbGetQuery(con,paste0("SELECT ST_Hilbert(geometry, {min_x: ",extent[1],", min_y: ",extent[2],
                                    ", max_x: ",extent[3],", max_y: ",extent[4],"}::BOX_2D) AS h ",
                                    "FROM read_parquet('",files,"')"))$h
    expect_false(is.unsorted(h))
  }
})
