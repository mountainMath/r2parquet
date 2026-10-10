test_that("missing credentials are reported by name", {
  withr::local_envvar(R2_ACCOUNT_ID="abc",R2_ACCESS_KEY_ID=NA,R2_SECRET_ACCESS_KEY=NA)
  expect_error(r2parquet:::r2_credentials(c("R2_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY")),
               "R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
  expect_equal(r2parquet:::r2_endpoint(),"https://abc.r2.cloudflarestorage.com")
})

test_that("directory paths get globbed without contacting R2", {
  expect_equal(r2parquet:::r2_parquet_glob("bucket","data/"),"data/**/*.parquet")
  expect_equal(r2parquet:::r2_parquet_glob("bucket","/data"),"data/**/*.parquet")
  expect_equal(r2parquet:::r2_parquet_glob("bucket",""),"**/*.parquet")
})

test_that("r2 paths get split into bucket and path", {
  expect_equal(r2parquet:::r2_split_path("r2://bucket/data/file.parquet"),list(bucket="bucket",path="data/file.parquet"))
  expect_equal(r2parquet:::r2_split_path("r2://bucket"),list(bucket="bucket",path=""))
  expect_error(r2parquet:::r2_split_path("r2:///data"),"r2://bucket/path")
})

test_that("arrow and polars engines can't mix R2 with other paths", {
  expect_error(parquet_tbl(c("r2://bucket/a.parquet","b.parquet"),engine="arrow"),"can't mix")
  expect_error(parquet_tbl(c("r2://bucket/a.parquet","b.parquet"),engine="polars"),"can't mix")
})
