test_that("missing credentials are reported by name", {
  withr::local_envvar(R2_ACCOUNT_ID="abc",R2_ACCESS_KEY_ID=NA,R2_SECRET_ACCESS_KEY=NA)
  expect_error(r2parquet:::r2_credentials(c("R2_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY")),
               "R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
  expect_equal(r2parquet:::r2_endpoint(),"https://abc.r2.cloudflarestorage.com")
})

test_that("directory paths get globbed without contacting R2", {
  expect_equal(r2parquet:::r2_parquet_glob("bucket","data",is_directory=TRUE),"data/**/*.parquet")
  expect_equal(r2parquet:::r2_parquet_glob("bucket","data"),"data/**/*.parquet")
  expect_equal(r2parquet:::r2_parquet_glob("bucket",""),"**/*.parquet")
})

test_that("r2 paths get split into bucket and path", {
  expect_equal(r2parquet:::r2_split_path("r2://bucket/data/file.parquet"),list(bucket="bucket",path="data/file.parquet",directory=FALSE))
  expect_equal(r2parquet:::r2_split_path("r2://bucket"),list(bucket="bucket",path="",directory=FALSE))
  expect_equal(r2parquet:::r2_split_path("r2://bucket//data/"),list(bucket="bucket",path="data",directory=TRUE))
  expect_error(r2parquet:::r2_split_path("r2:///data"),"r2://bucket/path")
  expect_error(r2parquet:::r2_split_path(c("r2://bucket/a.parquet","r2://bucket/b.parquet")),"single r2:// or s3:// path")
})

test_that("s3 paths keep the scheme in the bucket", {
  expect_equal(r2parquet:::r2_split_path("s3://bucket/data/"),list(bucket="s3://bucket",path="data",directory=TRUE))
  expect_equal(r2parquet:::bucket_url("s3://bucket","data/a.parquet"),"s3://bucket/data/a.parquet")
  expect_equal(r2parquet:::bucket_url("bucket","data/a.parquet"),"r2://bucket/data/a.parquet")
  expect_true(all(r2parquet:::is_bucket_path(c("r2://b/x","s3://b/x"))))
  expect_false(r2parquet:::is_bucket_path("https://b/x"))
})

test_that("s3 buckets are addressed by the AWS SDK environment variables", {
  withr::local_envvar(AWS_ACCESS_KEY_ID="key",AWS_SECRET_ACCESS_KEY="secret",AWS_SESSION_TOKEN=NA,AWS_REGION="ca-central-1",
                      AWS_DEFAULT_REGION=NA,AWS_ENDPOINT_URL=NA,AWS_ENDPOINT_URL_S3=NA,R2PARQUET_S3_URL_STYLE=NA)
  store <- r2parquet:::bucket_store("s3://bucket")
  expect_equal(store[c("endpoint","region","url_style","token")],
               list(endpoint="https://bucket.s3.ca-central-1.amazonaws.com",region="ca-central-1",url_style="vhost",token=""))
  # dots in bucket names don't match the certificates of virtual hosts
  expect_equal(r2parquet:::bucket_store("s3://my.bucket")$endpoint,"https://s3.ca-central-1.amazonaws.com")
  expect_equal(r2parquet:::s3_cache_host(),"aws")
  withr::local_envvar(AWS_ENDPOINT_URL="http://localhost:9000/",AWS_SESSION_TOKEN="token")
  store <- r2parquet:::bucket_store("s3://bucket")
  expect_equal(store[c("endpoint","url_style","token")],list(endpoint="http://localhost:9000",url_style="path",token="token"))
  expect_equal(r2parquet:::s3_cache_host(),"localhost_9000")
  # temporary credentials go into the signed query of presigned urls
  expect_match(presigned_url("s3://bucket/a.parquet"),"^http://localhost:9000/bucket/a.parquet\\?.*X-Amz-Security-Token=token")
  withr::local_envvar(AWS_ACCESS_KEY_ID=NA)
  expect_error(r2parquet:::bucket_store("s3://bucket"),"for S3 access: AWS_ACCESS_KEY_ID")
})

test_that("numeric character references get unescaped", {
  expect_equal(r2parquet:::xml_unescape(c("&#34;abc&#34;","a&amp;#34;b","&lt;&#x41;&apos;",NA)),c("\"abc\"","a&#34;b","<A'",NA))
})

test_that("ports of cached hosts survive the round trip", {
  cache_dir <- "/cache"
  location <- r2parquet:::cache_location("http://localhost:9000/data/a.parquet",cache_dir)
  expect_equal(location$local,"/cache/http/localhost_9000/data/a.parquet")
  expect_equal(r2parquet:::cache_remote_path(location$local,cache_dir),"http://localhost:9000/data/a.parquet")
})
