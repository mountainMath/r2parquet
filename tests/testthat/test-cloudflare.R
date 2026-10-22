# the Cloudflare REST API gets mocked at cloudflare_api(), the tests record the requests made

# mock answering each "VERB path" from responses, recording requests in the calling environment
mock_cloudflare_api <- function(responses,env=parent.frame()){
  env$requests <- list()
  local_mocked_bindings(cloudflare_api=function(verb,path,body=NULL,query=NULL){
    env$requests[[length(env$requests)+1]] <- list(verb=verb,path=path,body=body,query=query)
    key <- paste(verb,path)
    if (is.null(responses[[key]])) stop("Unexpected Cloudflare API request ",key)
    responses[[key]]
  },.env=env)
}

buckets <- "/accounts/abc/r2/buckets"

test_that("existing buckets are left alone", {
  withr::local_envvar(R2_ACCOUNT_ID="abc")
  mock_cloudflare_api(list("GET /accounts/abc/r2/buckets/b"=cloudflare_response(list(name="b"))))
  expect_message(url <- create_r2_bucket("b"),"already exists")
  expect_null(url)
  expect_length(requests,1)
  # the bucket can be given as r2://bucket like everywhere else
  expect_message(create_r2_bucket("r2://b/"),"already exists")
  expect_equal(requests[[2]]$path,"/accounts/abc/r2/buckets/b")
  expect_error(create_r2_bucket("r2://b/path"),"bucket name or r2://bucket")
  expect_error(create_r2_bucket("r2://"),"bucket name or r2://bucket")
  expect_error(create_r2_bucket(c("a","b")),"single bucket name")
})

test_that("new public buckets get created with the r2.dev domain", {
  withr::local_envvar(R2_ACCOUNT_ID="abc")
  mock_cloudflare_api(list(
    "GET /accounts/abc/r2/buckets/b"=cloudflare_response(errors=list(list(code=10006,message="bucket not found")),status=404L),
    "POST /accounts/abc/r2/buckets"=cloudflare_response(list(name="b")),
    "PUT /accounts/abc/r2/buckets/b/domains/managed"=cloudflare_response(list(domain="pub-123.r2.dev",enabled=TRUE))))
  expect_message(url <- create_r2_bucket("b",public=TRUE),"Creating R2 bucket b")
  expect_equal(url,"https://pub-123.r2.dev")
  expect_equal(requests[[2]]$body,list(name="b"))
  expect_equal(requests[[3]]$body,list(enabled=TRUE))
})

test_that("custom domains get connected via the zone of the domain", {
  withr::local_envvar(R2_ACCOUNT_ID="abc")
  requests <- list()
  local_mocked_bindings(cloudflare_api=function(verb,path,body=NULL,query=NULL){
    requests[[length(requests)+1]] <<- list(verb=verb,path=path,body=body,query=query)
    if (path=="/zones") {
      # the zone lookup walks up the domain labels until it finds a zone
      if (query$name=="example.com") return(cloudflare_response(list(list(id="z1",name="example.com",status="active"))))
      return(cloudflare_response(list()))
    }
    switch(paste(verb,path),
           "GET /accounts/abc/r2/buckets/b"=cloudflare_response(list(name="b")),
           "GET /accounts/abc/r2/buckets/b/domains/custom"=cloudflare_response(list(domains=list())),
           "POST /accounts/abc/r2/buckets/b/domains/custom"=cloudflare_response(list(domain="data.example.com")),
           stop("Unexpected Cloudflare API request ",verb," ",path))
  })
  expect_message(url <- create_r2_bucket("b",custom_domain="https://Data.Example.com/"),"Connected custom domain data.example.com")
  expect_equal(url,"https://data.example.com")
  zone_requests <- Filter(function(r)r$path=="/zones",requests)
  expect_equal(vapply(zone_requests,function(r)r$query$name,""),c("data.example.com","example.com"))
  expect_equal(zone_requests[[1]]$query$account.id,"abc")
  expect_equal(requests[[length(requests)]]$body,list(domain="data.example.com",zoneId="z1",enabled=TRUE))
})

test_that("disabled custom domains get enabled, connected ones are left alone", {
  withr::local_envvar(R2_ACCOUNT_ID="abc")
  mock_cloudflare_api(list(
    "GET /accounts/abc/r2/buckets/b"=cloudflare_response(list(name="b")),
    "GET /accounts/abc/r2/buckets/b/domains/custom"=cloudflare_response(list(domains=list(list(domain="data.example.com",enabled=FALSE)))),
    "PUT /accounts/abc/r2/buckets/b/domains/custom/data.example.com"=cloudflare_response(list(domain="data.example.com"))))
  expect_message(create_r2_bucket("b",custom_domain="data.example.com"),"Enabled custom domain")
  expect_equal(requests[[3]]$body,list(enabled=TRUE))

  mock_cloudflare_api(list(
    "GET /accounts/abc/r2/buckets/b"=cloudflare_response(list(name="b")),
    "GET /accounts/abc/r2/buckets/b/domains/custom"=cloudflare_response(list(domains=list(list(domain="data.example.com",enabled=TRUE))))))
  expect_message(create_r2_bucket("b",custom_domain="data.example.com"),"already connected")
  expect_length(requests,2)
})

test_that("zones have to exist and be active", {
  withr::local_envvar(R2_ACCOUNT_ID="abc")
  local_mocked_bindings(cloudflare_api=function(verb,path,body=NULL,query=NULL){
    if (query$name=="pending.org") return(cloudflare_response(list(list(id="z2",name="pending.org",status="pending"))))
    cloudflare_response(list())
  })
  expect_error(r2parquet:::r2_zone_for_domain("data.pending.org"),"not active yet \\(status pending\\)")
  expect_error(r2parquet:::r2_zone_for_domain("data.example.com"),"No Cloudflare zone found for data.example.com")
})

test_that("Cloudflare API errors are reported with their messages", {
  withr::local_envvar(R2_ACCOUNT_ID="abc")
  mock_cloudflare_api(list(
    "GET /accounts/abc/r2/buckets/b"=cloudflare_response(errors=list(list(code=10000,message="Authentication error")),status=403L)))
  expect_error(create_r2_bucket("b"),"status 403. Authentication error \\(code 10000\\)")
})

test_that("cache purges go to the zones of enabled custom domains in batches", {
  withr::local_envvar(R2_ACCOUNT_ID="abc")
  mock_cloudflare_api(list(
    "GET /accounts/abc/r2/buckets/b/domains/custom"=cloudflare_response(list(domains=list(
      list(domain="data.example.com",zoneId="z1",enabled=TRUE),
      list(domain="old.example.com",zoneId="z1",enabled=FALSE),
      list(domain="data.example.org",zoneId="z3",enabled=TRUE)))),
    "POST /zones/z1/purge_cache"=cloudflare_response(list(id="z1")),
    "POST /zones/z3/purge_cache"=cloudflare_response(list(id="z3"))))
  paths <- c("dir/a b.parquet",paste0("dir/year=2021/data_",seq_len(59),".parquet"))
  r2parquet:::r2_purge_cache("b",paths)
  purges <- Filter(function(r)r$verb=="POST",requests)
  expect_equal(vapply(purges,function(r)r$path,""),rep(c("/zones/z1/purge_cache","/zones/z3/purge_cache"),each=2))
  expect_equal(lengths(lapply(purges,function(r)r$body$files)),rep(30,4))
  expect_equal(purges[[1]]$body$files[[1]],"https://data.example.com/dir/a%20b.parquet")
  expect_equal(purges[[3]]$body$files[[1]],"https://data.example.org/dir/a%20b.parquet")
})

test_that("cache purges also purge the copies cached per origin in R2PARQUET_PURGE_ORIGINS", {
  withr::local_envvar(R2_ACCOUNT_ID="abc",R2PARQUET_PURGE_ORIGINS="https://a.example.com/, https://b.example.com,,")
  expect_equal(r2parquet:::purge_origins(),c("https://a.example.com","https://b.example.com"))
  mock_cloudflare_api(list(
    "GET /accounts/abc/r2/buckets/b/domains/custom"=cloudflare_response(list(domains=list(
      list(domain="data.example.com",zoneId="z1",enabled=TRUE)))),
    "POST /zones/z1/purge_cache"=cloudflare_response(list(id="z1"))))
  paths <- paste0("dir/f",seq_len(12),".parquet")
  r2parquet:::r2_purge_cache("b",paths)
  files <- unlist(lapply(Filter(function(r)r$verb=="POST",requests),function(r)r$body$files),recursive=FALSE)
  expect_equal(lengths(lapply(Filter(function(r)r$verb=="POST",requests),function(r)r$body$files)),c(30,6))
  urls <- paste0("https://data.example.com/",paths)
  expect_equal(files[1:12],as.list(urls))
  expect_equal(files[[13]],list(url=urls[1],headers=list(Origin="https://a.example.com")))
  expect_equal(files[[36]],list(url=urls[12],headers=list(Origin="https://b.example.com")))
})

test_that("cache purges without R2PARQUET_PURGE_ORIGINS only purge the plain urls", {
  withr::local_envvar(R2PARQUET_PURGE_ORIGINS=NA)
  expect_equal(r2parquet:::purge_origins(),character(0))
})

test_that("purging after changes is skipped without a token and only warns on failure", {
  withr::local_envvar(R2_ACCOUNT_ID="abc",R2_API_TOKEN=NA)
  local_mocked_bindings(cloudflare_api=function(...)stop("should not be called"))
  expect_no_warning(r2parquet:::r2_purge_cache_after_change("b","a.parquet"))
  withr::local_envvar(R2_API_TOKEN="token")
  mock_cloudflare_api(list(
    "GET /accounts/abc/r2/buckets/b/domains/custom"=cloudflare_response(errors=list(list(code=10000,message="Authentication error")),status=403L)))
  expect_warning(r2parquet:::r2_purge_cache_after_change("b","a.parquet"),"Purging the Cloudflare cache for R2 bucket b failed")
})
