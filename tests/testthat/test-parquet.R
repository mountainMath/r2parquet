# the duckdb engine of parquet_tbl and the connection it works on, plain parquet so no extensions are needed

test_that("parquet_tbl uses the shared connection or one of its own", {
  df <- data.frame(id=1:3,value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path)
  expect_true(identical(parquet_connection(),parquet_connection()))
  expect_true(identical(dbplyr::remote_con(parquet_tbl(path)),parquet_connection()))
  # a connection of the user's own, with data to join against
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  tbl <- parquet_tbl(path,con=con)
  expect_true(identical(dbplyr::remote_con(tbl),con))
  names <- dplyr::copy_to(con,data.frame(id=1:2,name=c("a","b")),"names")
  joined <- collect_parquet(dplyr::inner_join(tbl,names,by="id"))
  expect_equal(nrow(joined),2)
  expect_equal(sort(joined$name),c("a","b"))
  expect_error(parquet_tbl(path,engine="arrow",con=con),"duckdb engine")
  expect_error(parquet_tbl(path,con="con"),"DuckDB connection")
  expect_error(parquet_tbl(character()),"character vector")
})

test_that("a disconnected shared connection gets replaced", {
  df <- data.frame(id=1:3)
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path)
  old <- parquet_connection()
  DBI::dbDisconnect(old,shutdown=TRUE)
  expect_false(identical(parquet_connection(),old))
  expect_equal(nrow(collect_parquet(parquet_tbl(path))),3)
})
