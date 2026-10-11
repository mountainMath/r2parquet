# listing objects on R2 with mocked signed requests

test_that("r2_list_objects pages through listings and unescapes keys", {
  requests <- list()
  object <- function(key,modified,size){
    paste0("<Contents><Key>",key,"</Key><LastModified>",modified,"</LastModified><ETag>\"abc\"</ETag><Size>",size,
           "</Size><StorageClass>STANDARD</StorageClass></Contents>")
  }
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    requests[[length(requests)+1]] <<- list(verb=verb,r2_bucket=r2_bucket,r2_path=r2_path,query=query)
    if (is.null(query[["continuation-token"]])) {
      fake_response(200,paste0("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket</Name><Prefix>data</Prefix>",
                               "<IsTruncated>true</IsTruncated><NextContinuationToken>tok/en=1</NextContinuationToken>",
                               object("data/a&amp;b.parquet","2026-10-10T12:00:00.000Z",10),
                               object("data/year=2021/data_0.parquet","2026-10-10T12:15:30.500Z",20),
                               "</ListBucketResult>"))
    } else {
      fake_response(200,paste0("<ListBucketResult><IsTruncated>false</IsTruncated>",
                               object("data/c.parquet","2026-10-10T12:30:00.000Z",30),"</ListBucketResult>"))
    }
  })
  objects <- r2parquet:::r2_list_objects("bucket","data")
  expect_equal(objects$key,c("data/a&b.parquet","data/year=2021/data_0.parquet","data/c.parquet"))
  expect_equal(objects$size,c(10,20,30))
  expect_equal(objects$modified,as.POSIXct(c("2026-10-10 12:00:00","2026-10-10 12:15:30.5","2026-10-10 12:30:00"),tz="UTC"))
  expect_length(requests,2)
  expect_equal(requests[[1]]$verb,"GET")
  expect_equal(requests[[1]]$r2_path,"")
  expect_equal(requests[[1]]$query,list("list-type"="2",prefix="data"))
  expect_equal(requests[[2]]$query[["continuation-token"]],"tok/en=1")
  # the whole bucket, an empty listing and failures
  requests <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    requests[[length(requests)+1]] <<- list(query=query)
    fake_response(200,"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
  })
  empty <- r2parquet:::r2_list_objects("bucket")
  expect_equal(nrow(empty),0)
  expect_named(empty,c("key","size","modified"))
  expect_equal(requests[[1]]$query,list("list-type"="2"))
  local_mocked_bindings(r2_request=function(...)fake_response(404,"<Error><Code>NoSuchBucket</Code></Error>"))
  expect_error(r2parquet:::r2_list_objects("bucket"),"Listing R2 bucket bucket failed with status 404")
})

test_that("list_parquet on R2 lists files, directories and nothing for missing paths", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- data.frame(key=c("a.parquet","a.parquet.bak","data/year=2021/data_0.parquet","data/year=2022/data_0.parquet",
                              "data/notes.txt","data2/x.parquet"),
                        size=c(10,1,20,30,1,5),modified=now)
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix="")objects[r2_bucket=="bucket" & startsWith(objects$key,prefix),])
  files <- list_parquet("r2://bucket/data/")
  expect_equal(files$path,c("r2://bucket/data/year=2021/data_0.parquet","r2://bucket/data/year=2022/data_0.parquet"))
  expect_equal(files$size,c(20,30))
  expect_equal(files$modified,c(now,now))
  # a file is listed as is, objects that merely share the prefix are not
  single <- list_parquet("r2://bucket/a.parquet")
  expect_equal(single$path,"r2://bucket/a.parquet")
  expect_equal(single$size,10)
  expect_equal(nrow(list_parquet("r2://bucket/missing.parquet")),0)
  expect_equal(nrow(list_parquet("r2://bucket/data2.parquet")),0)
  expect_equal(nrow(list_parquet("r2://other/a.parquet")),0)
  expect_equal(nrow(list_parquet("r2://bucket")),4)
  expect_equal(nrow(list_parquet(c("r2://bucket/a.parquet","r2://bucket/data","r2://bucket/a.parquet"))),3)
})
