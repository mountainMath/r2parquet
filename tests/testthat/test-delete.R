test_that("deletes get batched by 1000 with escaped keys", {
  requests <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    requests[[length(requests)+1]] <<- list(verb=verb,r2_bucket=r2_bucket,query=query,headers=headers,body=rawToChar(body))
    fake_response(200,"<DeleteResult></DeleteResult>")
  })
  paths <- c("a&b<c>.parquet",paste0("dir/part-",seq_len(2499),".parquet"))
  r2parquet:::r2_delete_objects("bucket",paths)
  expect_length(requests,3)
  expect_equal(requests[[1]]$verb,"POST")
  expect_equal(requests[[1]]$query,list(delete=""))
  expect_equal(lengths(regmatches(vapply(requests,`[[`,"",i="body"),gregexpr("<Key>",vapply(requests,`[[`,"",i="body")))),
               c(1000,1000,500))
  expect_match(requests[[1]]$body,"<Key>a&amp;b&lt;c&gt;.parquet</Key>",fixed=TRUE)
  expect_equal(requests[[1]]$headers[["content-md5"]],
               jsonlite::base64_enc(digest::digest(charToRaw(requests[[1]]$body),algo="md5",serialize=FALSE,raw=TRUE)))
})

test_that("failed deletes of individual objects are reported", {
  local_mocked_bindings(r2_request=function(...){
    fake_response(200,paste0("<DeleteResult>\n<Error>\n<Key>a&amp;b.parquet</Key><Code>AccessDenied</Code>",
                             "<Message>Access Denied</Message></Error></DeleteResult>"))
  })
  expect_error(r2parquet:::r2_delete_objects("bucket",c("a&b.parquet","c.parquet")),
               "Removing 1 file from R2 bucket bucket failed. a&b.parquet: Access Denied \\(AccessDenied\\)")
})

test_that("failed delete requests are reported", {
  local_mocked_bindings(r2_request=function(...)fake_response(403,"<Error><Code>InvalidAccessKeyId</Code></Error>"))
  expect_error(r2parquet:::r2_delete_objects("bucket","a.parquet"),"failed with status 403")
})

test_that("remove_parquet resolves all paths first and removes per bucket", {
  skip_if_not_installed("arrow")
  deleted <- list()
  purged <- list()
  objects <- list("bucket/a.parquet"=arrow::FileType$File,"bucket/data"=arrow::FileType$Directory,
                  "other/b.parquet"=arrow::FileType$File,"bucket/empty"=arrow::FileType$Directory)
  filesystem <- list(GetFileInfo=function(path)list(list(type=if (is.null(objects[[path]])) arrow::FileType$NotFound else objects[[path]])))
  local_mocked_bindings(r2_filesystem=function()filesystem,
                        r2_list_parquet=function(filesystem,r2_bucket,r2_path)if (r2_path=="data") c("data/year=2021/data_0.parquet","data/year=2022/data_0.parquet") else character(),
                        r2_delete_objects=function(r2_bucket,r2_paths)deleted[[r2_bucket]] <<- r2_paths,
                        r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[r2_bucket]] <<- r2_paths)
  expect_message(expect_message(
    removed <- remove_parquet(c("r2://bucket/a.parquet","r2://other/b.parquet","r2://bucket/data/","r2://bucket/a.parquet")),
    "Removed 3 files from R2 bucket bucket"),"Removed 1 file from R2 bucket other")
  # files of one bucket go in one batch, duplicates get dropped
  expect_equal(deleted,list(bucket=c("a.parquet","data/year=2021/data_0.parquet","data/year=2022/data_0.parquet"),other="b.parquet"))
  expect_equal(purged,deleted)
  expect_equal(removed,c("r2://bucket/a.parquet","r2://bucket/data/year=2021/data_0.parquet",
                         "r2://bucket/data/year=2022/data_0.parquet","r2://other/b.parquet"))
  # a missing path stops the call before anything gets removed
  deleted <- list()
  expect_error(remove_parquet(c("r2://bucket/a.parquet","r2://bucket/missing.parquet")),"missing.parquet does not exist")
  expect_error(remove_parquet(c("r2://bucket/a.parquet","r2://bucket/empty")),"No parquet files found in empty")
  expect_error(remove_parquet("r2://bucket"),"specify the path")
  expect_error(remove_parquet(character()),"r2://bucket/path")
  expect_length(deleted,0)
  # no purge when turned off
  purged <- list()
  remove_parquet("r2://other/b.parquet",purge_cache=FALSE)
  expect_length(purged,0)
})
