# integration tests against a real R2 bucket, the bucket has to exist
# all files get written under a random prefix that is removed at the end

test_that("parquet files can be uploaded, queried and removed", {
  skip_if_no_r2()
  bucket <- Sys.getenv("R2_TEST_BUCKET")
  prefix <- paste0("r2parquet-tests/",format(Sys.time(),"%Y%m%d%H%M%S"),"-",sample.int(1e6,1))
  data <- nc_data()

  # single GeoParquet file
  r2_path <- paste0(prefix,"/nc.parquet")
  expect_message(sf_to_r2_geoparquet(data,bucket,r2_path),"Uploading")
  y <- sf::st_buffer(sf::st_centroid(sf::st_geometry(data[data$NAME=="Wake",])),20000)
  result <- collect_sf(filter_spatial(r2_parquet_tbl(bucket,r2_path),y))
  expect_equal(sort(result$NAME),sort(data$NAME[lengths(sf::st_intersects(data,y))>0]))
  expect_true(sf::st_crs(result)==sf::st_crs(data))

  # directory of partitioned files, named like a parquet file
  dir <- withr::local_tempdir()
  df <- sf::st_drop_geometry(data)[,c("NAME","SID74","BIR74")]
  for (sid in c(0,1)) {
    dir.create(file.path(dir,paste0("high_sid=",sid)))
    duckdb_con <- DBI::dbConnect(duckdb::duckdb())
    duckdb::duckdb_register(duckdb_con,"df",df[(df$SID74>5)==sid,])
    DBI::dbExecute(duckdb_con,paste0("COPY df TO '",file.path(dir,paste0("high_sid=",sid),"data.parquet"),"' (FORMAT parquet)"))
    DBI::dbDisconnect(duckdb_con,shutdown=TRUE)
  }
  dir_path <- paste0(prefix,"/partitioned.parquet")
  uploaded <- parquet_to_r2(dir,bucket,dir_path)
  expect_length(uploaded,2)
  tbl <- r2_parquet_tbl(bucket,dir_path)
  expect_equal(dplyr::pull(dplyr::count(tbl)),nrow(df))
  expect_equal(dplyr::pull(dplyr::count(dplyr::filter(tbl,high_sid==1L))),sum(df$SID74>5))

  if (requireNamespace("arrow",quietly=TRUE)) {
    expect_equal(nrow(dplyr::collect(r2_parquet_arrow(bucket,dir_path))),nrow(df))
  }
  if (requireNamespace("polars",quietly=TRUE)) {
    expect_equal(nrow(r2_parquet_polars(bucket,dir_path)$collect()),nrow(df))
  }

  expect_message(removed <- remove_parquet_from_r2(bucket,prefix,purge_cache=FALSE),"Removed 3 files")
  expect_setequal(removed,c(r2_path,uploaded))
  expect_error(remove_parquet_from_r2(bucket,prefix,purge_cache=FALSE),"does not exist")
})
