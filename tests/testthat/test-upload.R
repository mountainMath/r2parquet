# the signed requests get mocked, the tests check which local files end up at which destinations

test_that("upload destinations get resolved for files and directories", {
  uploads <- list()
  purged <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    uploads[[length(uploads)+1]] <<- list(verb=verb,destination=paste0(r2_bucket,"/",r2_path),size=length(body))
    fake_response(200)
  },r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths)
  dir <- withr::local_tempdir()
  writeBin(as.raw(1),file.path(dir,"a.parquet"))
  file.create(file.path(dir,"notes.txt"))
  dir.create(file.path(dir,"year=2021"))
  writeBin(as.raw(1:2),file.path(dir,"year=2021","data_0.parquet"))

  expect_equal(upload_parquet(file.path(dir,"a.parquet"),"r2://bucket"),"r2://bucket/a.parquet")
  expect_equal(upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/dir/"),"r2://bucket/dir/a.parquet")
  expect_equal(upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/dir/b.parquet",purge_cache=FALSE),"r2://bucket/dir/b.parquet")
  expect_equal(upload_parquet(dir,"r2://bucket/data/"),c("r2://bucket/data/a.parquet","r2://bucket/data/year=2021/data_0.parquet"))
  expect_equal(upload_parquet(dir,"r2://bucket"),c("r2://bucket/a.parquet","r2://bucket/year=2021/data_0.parquet"))
  # the file contents go up with PUT requests
  expect_true(all(vapply(uploads,`[[`,"",i="verb")=="PUT"))
  expect_equal(vapply(uploads,`[[`,"",i="destination"),
               c("bucket/a.parquet","bucket/dir/a.parquet","bucket/dir/b.parquet",
                 "bucket/data/a.parquet","bucket/data/year=2021/data_0.parquet",
                 "bucket/a.parquet","bucket/year=2021/data_0.parquet"))
  expect_equal(vapply(uploads,`[[`,1L,i="size"),c(1L,1L,1L,1L,2L,1L,2L))
  # the purge gets the uploaded paths, unless it is turned off
  expect_equal(purged,list("a.parquet","dir/a.parquet",c("data/a.parquet","data/year=2021/data_0.parquet"),
                           c("a.parquet","year=2021/data_0.parquet")))

  expect_error(upload_parquet(file.path(dir,"missing.parquet"),"r2://bucket"),"does not exist")
  expect_error(upload_parquet(withr::local_tempdir(),"r2://bucket"),"No parquet files found")
  expect_error(upload_parquet(file.path(dir,"a.parquet"),"bucket/a.parquet"),"r2://bucket/path")
})

test_that("failed uploads are reported", {
  local_mocked_bindings(r2_request=function(...)fake_response(403,"<Error><Code>AccessDenied</Code></Error>"))
  file <- withr::local_tempfile(fileext=".parquet")
  file.create(file)
  expect_error(upload_parquet(file,"r2://bucket/a.parquet"),"r2://bucket/a.parquet failed with status 403")
})
