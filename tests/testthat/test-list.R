# listing files on R2 with a mocked arrow filesystem

test_that("list_parquet on R2 lists files, directories and nothing for missing paths", {
  skip_if_not_installed("arrow")
  info <- function(path,type,size=NA_real_,mtime=NULL)list(path=path,type=type,size=size,mtime=mtime)
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- list("bucket/a.parquet"=info("bucket/a.parquet",arrow::FileType$File,10,now),
                  "bucket/data"=info("bucket/data",arrow::FileType$Directory),
                  "bucket"=info("bucket",arrow::FileType$Directory))
  listing <- list(info("bucket/data/year=2021/data_0.parquet",arrow::FileType$File,20,now),
                  info("bucket/data/year=2022/data_0.parquet",arrow::FileType$File,30,now),
                  info("bucket/data/notes.txt",arrow::FileType$File,1,now),
                  info("bucket/data/year=2021",arrow::FileType$Directory))
  filesystem <- list(GetFileInfo=function(selector){
    if (is.character(selector)) return(list(if (is.null(objects[[selector]])) info(selector,arrow::FileType$NotFound) else objects[[selector]]))
    # a recursive selector lists everything below its base directory
    Filter(function(i)startsWith(i$path,paste0(selector$base_dir,"/")),c(listing,list(objects[["bucket/a.parquet"]])))
  })
  local_mocked_bindings(r2_filesystem=function()filesystem)
  files <- list_parquet("r2://bucket/data/")
  expect_equal(files$path,c("r2://bucket/data/year=2021/data_0.parquet","r2://bucket/data/year=2022/data_0.parquet"))
  expect_equal(files$size,c(20,30))
  expect_equal(files$modified,c(now,now))
  single <- list_parquet("r2://bucket/a.parquet")
  expect_equal(single$path,"r2://bucket/a.parquet")
  expect_equal(single$size,10)
  expect_equal(nrow(list_parquet("r2://bucket/missing.parquet")),0)
  expect_equal(nrow(list_parquet("r2://bucket")),3)
  expect_equal(nrow(list_parquet(c("r2://bucket/a.parquet","r2://bucket/data","r2://bucket/a.parquet"))),3)
})
