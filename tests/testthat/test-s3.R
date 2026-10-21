# a bucket whose policy and Block Public Access settings are kept in memory, recording the requests
policy_bucket <- function(policy=NULL,access_block=NULL,envir=parent.frame()){
  state <- new.env()
  state$policy <- policy
  state$requests <- list()
  withr::local_envvar(AWS_ACCESS_KEY_ID="key",AWS_SECRET_ACCESS_KEY="secret",AWS_REGION="ca-central-1",
                      AWS_ENDPOINT_URL=NA,AWS_ENDPOINT_URL_S3=NA,R2PARQUET_S3_URL_STYLE=NA,.local_envir=envir)
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    state$requests[[length(state$requests)+1]] <- list(verb=verb,query=names(query))
    if (identical(names(query),"publicAccessBlock")) {
      if (is.null(access_block)) return(fake_response(404,"<Error><Code>NoSuchPublicAccessBlockConfiguration</Code></Error>"))
      return(fake_response(200,access_block))
    }
    if (verb=="GET") return(if (is.null(state$policy)) fake_response(404,"<Error/>") else fake_response(200,state$policy))
    if (verb=="PUT") state$policy <- rawToChar(body)
    if (verb=="DELETE") state$policy <- NULL
    fake_response(if (verb=="DELETE") 204 else 200,"")
  },.env=envir)
  state
}

test_that("public access to a prefix is added to and removed from the bucket policy", {
  other <- '{"Version":"2012-10-17","Statement":{"Sid":"Other","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::1:root"},"Action":"s3:ListBucket","Resource":["arn:aws:s3:::bucket"]}}'
  bucket <- policy_bucket(other)
  expect_message(url <- set_s3_public_access("s3://bucket/data/public/"),"publicly readable")
  expect_equal(url,"https://bucket.s3.ca-central-1.amazonaws.com/data/public/")
  policy <- jsonlite::fromJSON(bucket$policy,simplifyVector=FALSE)
  # the other statement is kept as it was, arrays stay arrays
  expect_length(policy$Statement,2)
  expect_equal(policy$Statement[[1]]$Resource,list("arn:aws:s3:::bucket"))
  expect_equal(policy$Statement[[2]][c("Effect","Principal","Action","Resource")],
               list(Effect="Allow",Principal="*",Action="s3:GetObject",Resource="arn:aws:s3:::bucket/data/public/*"))
  expect_message(set_s3_public_access("s3://bucket/data/public"),"already public")
  expect_length(jsonlite::fromJSON(bucket$policy,simplifyVector=FALSE)$Statement,2)
  expect_message(set_s3_public_access("s3://bucket/data/public",public=FALSE),"Removed public access")
  expect_equal(jsonlite::fromJSON(bucket$policy,simplifyVector=FALSE)$Statement[[1]]$Sid,"Other")
})

test_that("the policy goes when its last statement is removed", {
  bucket <- policy_bucket()
  expect_message(url <- set_s3_public_access("s3://bucket"),"publicly readable")
  expect_equal(url,"https://bucket.s3.ca-central-1.amazonaws.com/")
  expect_equal(jsonlite::fromJSON(bucket$policy,simplifyVector=FALSE)$Statement[[1]]$Resource,"arn:aws:s3:::bucket/*")
  expect_message(set_s3_public_access("s3://bucket",public=FALSE),"Removed")
  expect_null(bucket$policy)
  expect_equal(bucket$requests[[length(bucket$requests)]]$verb,"DELETE")
  expect_message(set_s3_public_access("s3://bucket",public=FALSE),"has no public access")
})

test_that("Block Public Access stops before the policy is written", {
  bucket <- policy_bucket(access_block=paste0("<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy>",
                                              "<RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>"))
  expect_error(set_s3_public_access("s3://bucket/public"),"BlockPublicPolicy turned on")
  expect_null(bucket$policy)
})

test_that("public access is only set on S3 prefixes", {
  expect_error(set_s3_public_access("r2://bucket/public"),"create_r2_bucket")
  withr::local_envvar(AWS_ACCESS_KEY_ID="key",AWS_SECRET_ACCESS_KEY="secret",AWS_ENDPOINT_URL="http://localhost:9000")
  expect_equal(r2parquet:::s3_public_url("s3://bucket","a b/c"),"http://localhost:9000/bucket/a%20b/c/")
})
