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
