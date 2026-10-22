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
  expect_error(r2parquet:::r2_delete_objects("bucket","a.parquet"),"failed with status 403.*InvalidAccessKeyId",class="r2parquet_access_denied")
})

test_that("remove_parquet resolves all paths first and removes per bucket", {
  deleted <- list()
  purged <- list()
  keys <- list(bucket=c("a.parquet","data/year=2021/data_0.parquet","data/year=2022/data_0.parquet","data/notes.txt","empty/readme.txt"),
               other="b.parquet")
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix=""){
                          key <- keys[[r2_bucket]]
                          key <- key[startsWith(key,prefix)]
                          data.frame(key=key,size=rep(1,length(key)),modified=rep(Sys.time(),length(key)),etag=rep("e",length(key)))
                        },
                        r2_delete_objects=function(r2_bucket,r2_paths)deleted[[r2_bucket]] <<- r2_paths,
                        r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[r2_bucket]] <<- r2_paths,
                        # no manifests to update
                        r2_get_manifest=function(r2_bucket,key)NULL)
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

test_that("remove_parquet removes manifests with directories and updates them for single files", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- data.frame(key=c("data/a.parquet","data/b.parquet","data/_manifest.json","data/sub/c.parquet","data/sub/_manifest.json",
                              "nested/x.parquet","nested/_manifest.json","plain/y.parquet","plain/z.parquet"),
                        size=1,modified=now,etag="e")
  bodies <- list("data/_manifest.json"=manifest_body(c("a.parquet","b.parquet","sub/c.parquet")),
                 "data/sub/_manifest.json"=manifest_body("c.parquet"),"nested/_manifest.json"=manifest_body("x.parquet"))
  deleted <- list()
  puts <- list()
  purged <- list()
  local_mocked_bindings(r2_request=manifest_bucket(environment()),
                        r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
                        r2_delete_objects=function(r2_bucket,r2_paths){
                          deleted[[length(deleted)+1]] <<- r2_paths
                          objects <<- objects[!objects$key %in% r2_paths,]
                        },r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
                        parquet_footers=fake_footers())
  # a directory takes its manifests with it, a single file gets its directory's manifest patched
  removed <- remove_parquet(c("r2://bucket/data","r2://bucket/nested/x.parquet","r2://bucket/plain/y.parquet"))
  expect_equal(removed,c("r2://bucket/data/a.parquet","r2://bucket/data/b.parquet","r2://bucket/data/sub/c.parquet",
                         "r2://bucket/nested/x.parquet","r2://bucket/plain/y.parquet"))
  expect_equal(deleted[[1]],c("data/a.parquet","data/b.parquet","data/sub/c.parquet","nested/x.parquet","plain/y.parquet",
                              "data/_manifest.json","data/sub/_manifest.json"))
  # the last parquet file of nested went, so its manifest gets removed too, plain never had one
  expect_equal(deleted[[2]],"nested/_manifest.json")
  expect_length(puts,0)
  expect_equal(purged,list(c("data/a.parquet","data/b.parquet","data/sub/c.parquet","nested/x.parquet","plain/y.parquet",
                             "data/_manifest.json","data/sub/_manifest.json","nested/_manifest.json")))
  # a single file out of a directory with a manifest and remaining files drops it from the manifest, without
  # listing the directory
  objects <- data.frame(key=c("data/a.parquet","data/b.parquet","data/_manifest.json"),size=1,modified=now,etag="e")
  bodies <- list("data/_manifest.json"=manifest_body(c("a.parquet","b.parquet")))
  deleted <- list()
  purged <- list()
  listings <- 0
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix=""){
    listings <<- listings+1
    objects[startsWith(objects$key,prefix),]
  })
  remove_parquet("r2://bucket/data/a.parquet")
  expect_equal(deleted,list("data/a.parquet"))
  expect_equal(puts,list("data/_manifest.json"="b.parquet"))
  expect_equal(purged,list(c("data/a.parquet","data/_manifest.json")))
  expect_equal(listings,1)
})

test_that("remove_parquet keeps the manifests above removed files and partitions current", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  # a partitioned dataset with its manifest at the root, as data_to_parquet writes it, below a directory with
  # a manifest of its own
  partitions <- paste0("data/year=",2021:2023,"/data_0.parquet")
  objects <- data.frame(key=c(partitions,"data/_manifest.json","other.parquet","_manifest.json"),size=1,modified=now,etag="e")
  bodies <- list("data/_manifest.json"=manifest_body(substring(partitions,6)),"_manifest.json"=manifest_body(c(partitions,"other.parquet")))
  puts <- list()
  purged <- list()
  gets <- character()
  local_mocked_bindings(r2_request=manifest_bucket(environment()),
                        r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
                        r2_delete_objects=function(r2_bucket,r2_paths)objects <<- objects[!objects$key %in% r2_paths,],
                        r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
                        parquet_footers=fake_footers())
  # a file out of a partition: the partition directory has no manifest, the dataset and the bucket root do
  remove_parquet("r2://bucket/data/year=2021/data_0.parquet")
  expect_equal(gets,c("data/year=2021/_manifest.json","data/_manifest.json","_manifest.json"))
  expect_equal(puts,list("data/_manifest.json"=c("year=2022/data_0.parquet","year=2023/data_0.parquet"),
                         "_manifest.json"=c("data/year=2022/data_0.parquet","data/year=2023/data_0.parquet","other.parquet")))
  expect_equal(purged,list(c("data/year=2021/data_0.parquet","data/_manifest.json","_manifest.json")))
  # a whole partition
  puts <- list()
  purged <- list()
  remove_parquet("r2://bucket/data/year=2022")
  expect_equal(puts,list("data/_manifest.json"="year=2023/data_0.parquet",
                         "_manifest.json"=c("data/year=2023/data_0.parquet","other.parquet")))
  expect_equal(purged,list(c("data/year=2022/data_0.parquet","data/_manifest.json","_manifest.json")))
  # the dataset and a partition in it at once: the dataset's manifest goes with it and is not rewritten
  puts <- list()
  purged <- list()
  remove_parquet(c("r2://bucket/data/year=2023","r2://bucket/data"))
  expect_equal(puts,list("_manifest.json"="other.parquet"))
  expect_equal(purged,list(c("data/year=2023/data_0.parquet","data/_manifest.json","_manifest.json")))
})

test_that("remove_parquet removes local files, directories and the paths of lazy tables", {
  root <- withr::local_tempdir()
  file <- file.path(root,"iris.parquet")
  data_to_parquet(iris,file,metadata=list())
  expect_message(expect_equal(remove_parquet(parquet_tbl(file)),file),"Removed 1 local file")
  expect_false(file.exists(file))
  expect_error(remove_parquet(file),"does not exist")

  dataset <- file.path(root,"dataset")
  data_to_parquet(iris,dataset,partition_by="Species",metadata=list())
  writeLines("{}",file.path(dataset,"_manifest.json"))
  writeLines("notes",file.path(dataset,"Species=setosa","notes.txt"))
  index <- file.path(root,"index.json")
  writeLines("{}",index)
  expected <- dir(dataset,"\\.parquet$",recursive=TRUE,full.names=TRUE)
  expect_message(removed <- remove_parquet(paste0(dataset,"/")),"Removed 3 local files")
  expect_setequal(removed,expected)
  # other files stay, with the directories holding them
  expect_equal(dir(dataset,recursive=TRUE,all.files=TRUE),"Species=setosa/notes.txt")
  unlink(file.path(dataset,"Species=setosa","notes.txt"))
  expect_error(remove_parquet(dataset),"No parquet files found")

  # nothing gets removed when a path does not resolve
  data_to_parquet(iris,file,metadata=list())
  expect_error(remove_parquet(c(file,file.path(root,"missing"))),"missing does not exist")
  expect_true(file.exists(file))
  expect_error(remove_parquet(c(file,index)),"index.json is not a parquet file")
  expect_true(file.exists(file))
  # a binary file starting with a nul byte is not a parquet file either
  binary <- file.path(root,"data.bin")
  writeBin(as.raw(c(0,0,0,0,1)),binary)
  expect_error(remove_parquet(binary),"data.bin is not a parquet file")
  expect_true(file.exists(binary))
  expect_error(remove_parquet("https://example.com/a.parquet"),"Files at URLs can't be removed")
  # also for a table read from a URL
  tbl <- parquet_tbl(file)
  attr(tbl,"r2parquet_source") <- "https://example.com/a.parquet"
  expect_error(remove_parquet(tbl),"Files at URLs can't be removed.*https://example.com/a.parquet")
  expect_true(file.exists(file))
})

test_that("remove_parquet removes the remote files of a table read from the cache", {
  remote <- withr::local_tempdir()
  data_to_parquet(data.frame(year=c(2021L,2022L),value=1:2),file.path(remote,"data"),partition_by="year",metadata=list())
  files <- dir(remote,"\\.parquet$",recursive=TRUE)
  objects <- data.frame(key=files,size=file.size(file.path(remote,files)),modified=as.POSIXct("2026-10-10 12:00:00",tz="UTC"),
                        etag=vapply(file.path(remote,files),digest::digest,"",algo="md5",file=TRUE,USE.NAMES=FALSE))
  deleted <- character()
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
                        r2_get_object=function(r2_bucket,r2_path,file)file.copy(file.path(remote,r2_path),file,overwrite=TRUE),
                        r2_delete_objects=function(r2_bucket,r2_paths)deleted <<- c(deleted,r2_paths),
                        r2_purge_cache_after_change=function(r2_bucket,r2_paths)NULL,
                        r2_get_manifest=function(r2_bucket,key)NULL)
  cache <- local_cache_dir()
  withr::local_envvar(R2_ACCOUNT_ID="acct",R2_ACCESS_KEY_ID="key",R2_SECRET_ACCESS_KEY="secret",R2PARQUET_CACHE_PATH=cache)
  suppressMessages(local <- cache_parquet("r2://bucket/data"))
  tbl <- parquet_tbl("r2://bucket/data",cache="offline")
  expect_equal(attr(tbl,"r2parquet_paths"),file.path(local,"**","*.parquet"))
  expect_message(removed <- remove_parquet(tbl),"Removed 2 files from R2 bucket bucket")
  expect_equal(removed,paste0("r2://bucket/",files))
  expect_equal(deleted,files)
  # the copies stay for remove_parquet_from_cache
  expect_setequal(dir(local,"\\.parquet$",recursive=TRUE),sub("^data/","",files))
})

test_that("remove_parquet leaves copies in the cache directory to remove_parquet_from_cache", {
  cache <- local_cache_dir()
  withr::local_envvar(R2PARQUET_CACHE_PATH=cache)
  dir.create(file.path(cache,"r2"),recursive=TRUE)
  data_to_parquet(iris,file.path(cache,"r2","a.parquet"),metadata=list())
  expect_error(remove_parquet(file.path(cache,"r2","a.parquet")),"remove_parquet_from_cache")
  expect_error(remove_parquet(cache),"remove_parquet_from_cache")
  expect_true(file.exists(file.path(cache,"r2","a.parquet")))
})
