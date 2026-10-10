# integration tests against a real R2 bucket, the bucket has to exist
# all files get written under a random prefix that is removed at the end

test_that("parquet files can be uploaded, queried and removed", {
  skip_if_no_r2()
  bucket <- Sys.getenv("R2_TEST_BUCKET")
  prefix <- paste0("r2parquet-tests/",format(Sys.time(),"%Y%m%d%H%M%S"),"-",sample.int(1e6,1))
  data <- nc_data()

  # single GeoParquet file
  r2_path <- paste0(prefix,"/nc.parquet")
  expect_message(data_to_r2_parquet(data,bucket,r2_path),"Uploading")
  y <- sf::st_buffer(sf::st_centroid(sf::st_geometry(data[data$NAME=="Wake",])),20000)
  result <- collect_sf(filter_spatial(r2_parquet_tbl(bucket,r2_path),y))
  expect_equal(sort(result$NAME),sort(data$NAME[lengths(sf::st_intersects(data,y))>0]))
  expect_true(sf::st_crs(result)==sf::st_crs(data))

  # directory of partitioned files, named like a parquet file
  df <- sf::st_drop_geometry(data)[,c("NAME","SID74","BIR74")]
  df$high_sid <- as.integer(df$SID74>5)
  df$group <- as.integer(df$BIR74>5000)
  dir_path <- paste0(prefix,"/partitioned.parquet")
  # stale partitions get removed when uploading again
  stale <- data_to_r2_parquet(df,bucket,dir_path,partition_by="group")
  expect_length(stale,2)
  uploaded <- data_to_r2_parquet(df,bucket,dir_path,partition_by="high_sid")
  expect_length(uploaded,2)
  tbl <- r2_parquet_tbl(bucket,dir_path)
  # stale group= partitions would be read too and double the row count
  expect_equal(dplyr::pull(dplyr::count(tbl)),nrow(df))
  expect_equal(dplyr::pull(dplyr::count(dplyr::filter(tbl,high_sid==1L))),sum(df$SID74>5))

  if (requireNamespace("arrow",quietly=TRUE)) {
    expect_equal(nrow(dplyr::collect(r2_parquet_arrow(bucket,dir_path))),nrow(df))
  }
  if (requireNamespace("polars",quietly=TRUE)) {
    expect_equal(nrow(r2_parquet_polars(bucket,dir_path)$collect()),nrow(df))
  }

  # presigned urls work without credentials
  response <- httr::GET(r2_presigned_url(bucket,r2_path,expires=60))
  expect_equal(httr::status_code(response),200)
  expect_equal(rawToChar(httr::content(response,"raw")[1:4]),"PAR1")
  put_path <- paste0(prefix,"/presigned put.parquet")
  response <- httr::PUT(r2_presigned_url(bucket,put_path,method="PUT"),body=httr::content(response,"raw"))
  expect_equal(httr::status_code(response),200)
  expect_equal(dplyr::pull(dplyr::count(r2_parquet_tbl(bucket,put_path))),nrow(data))

  expect_message(removed <- remove_parquet_from_r2(bucket,prefix,purge_cache=FALSE),"Removed 4 files")
  expect_setequal(removed,c(r2_path,put_path,uploaded))
  expect_error(remove_parquet_from_r2(bucket,prefix,purge_cache=FALSE),"does not exist")
})
