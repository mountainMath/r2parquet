# the arrow filesystem gets mocked, the tests check which local files end up at which destinations

test_that("upload destinations get resolved for files and directories", {
  skip_if_not_installed("arrow")
  uploads <- character()
  purged <- list()
  local_mocked_bindings(r2_filesystem=function()"filesystem",
                        file_to_filesystem=function(path,filesystem,destination,...)uploads[[length(uploads)+1]] <<- destination,
                        r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths)
  dir <- withr::local_tempdir()
  file.create(file.path(dir,"a.parquet"),file.path(dir,"notes.txt"))
  dir.create(file.path(dir,"year=2021"))
  file.create(file.path(dir,"year=2021","data_0.parquet"))

  expect_equal(upload_parquet(file.path(dir,"a.parquet"),"r2://bucket"),"r2://bucket/a.parquet")
  expect_equal(upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/dir/"),"r2://bucket/dir/a.parquet")
  expect_equal(upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/dir/b.parquet",purge_cache=FALSE),"r2://bucket/dir/b.parquet")
  expect_equal(upload_parquet(dir,"r2://bucket/data/"),c("r2://bucket/data/a.parquet","r2://bucket/data/year=2021/data_0.parquet"))
  expect_equal(upload_parquet(dir,"r2://bucket"),c("r2://bucket/a.parquet","r2://bucket/year=2021/data_0.parquet"))
  expect_equal(uploads,c("bucket/a.parquet","bucket/dir/a.parquet","bucket/dir/b.parquet",
                         "bucket/data/a.parquet","bucket/data/year=2021/data_0.parquet",
                         "bucket/a.parquet","bucket/year=2021/data_0.parquet"))
  # the purge gets the uploaded paths, unless it is turned off
  expect_equal(purged,list("a.parquet","dir/a.parquet",c("data/a.parquet","data/year=2021/data_0.parquet"),
                           c("a.parquet","year=2021/data_0.parquet")))

  expect_error(upload_parquet(file.path(dir,"missing.parquet"),"r2://bucket"),"does not exist")
  expect_error(upload_parquet(withr::local_tempdir(),"r2://bucket"),"No parquet files found")
  expect_error(upload_parquet(file.path(dir,"a.parquet"),"bucket/a.parquet"),"r2://bucket/path")
})
