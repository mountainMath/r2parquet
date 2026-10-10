# examples from https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
# and https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
aws_access_key <- "AKIAIOSFODNN7EXAMPLE"
aws_secret <- "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

test_that("header signature matches the AWS GET object example", {
  headers <- r2parquet:::sigv4_headers("GET","examplebucket.s3.amazonaws.com","/test.txt",
                                       headers=list(Range="bytes=0-9"),
                                       access_key=aws_access_key,secret=aws_secret,region="us-east-1",
                                       datetime="20130524T000000Z")
  expect_equal(headers$authorization,
               paste0("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,",
                      "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,",
                      "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
  expect_null(headers$host)
})

test_that("query signature matches the AWS presigned URL example", {
  url <- r2parquet:::sigv4_presigned_url("GET","https://examplebucket.s3.amazonaws.com","/test.txt",86400,
                                         access_key=aws_access_key,secret=aws_secret,region="us-east-1",
                                         datetime="20130524T000000Z")
  expect_equal(url,
               paste0("https://examplebucket.s3.amazonaws.com/test.txt",
                      "?X-Amz-Algorithm=AWS4-HMAC-SHA256",
                      "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request",
                      "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host",
                      "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"))
})

test_that("object paths and query strings get encoded", {
  expect_equal(r2parquet:::r2_canonical_path("bucket","dir/year=2021/a b.parquet"),"/bucket/dir/year%3D2021/a%20b.parquet")
  expect_equal(r2parquet:::r2_canonical_path("bucket"),"/bucket")
  expect_equal(r2parquet:::sigv4_query_string(list(delete="")),"delete=")
  expect_equal(r2parquet:::sigv4_query_string(list(b="2",a="x/y")),"a=x%2Fy&b=2")
})

test_that("presigned R2 URLs use the account endpoint and validate arguments", {
  withr::local_envvar(R2_ACCOUNT_ID="abc",R2_ACCESS_KEY_ID="key",R2_SECRET_ACCESS_KEY="secret")
  url <- presigned_url("r2://bucket/dir/file.parquet",expires=60)
  expect_match(url,"^https://abc\\.r2\\.cloudflarestorage\\.com/bucket/dir/file\\.parquet\\?X-Amz-Algorithm=")
  expect_match(url,"X-Amz-Credential=key%2F[0-9]{8}%2Fauto%2Fs3%2Faws4_request")
  expect_match(url,"X-Amz-Expires=60&")
  expect_error(presigned_url("r2://bucket/file.parquet",expires=604801),"between 1 and 604800")
  expect_error(presigned_url("r2://bucket/dir/"),"path of a file")
  expect_error(presigned_url("bucket/file.parquet"),"r2://bucket/path")
  expect_error(presigned_url("r2://bucket/file.parquet",method="DELETE"))
})
