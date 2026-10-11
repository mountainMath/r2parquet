# local copies of remote parquet files, with the R2 listings and downloads, and the requests to URLs, mocked

# parquet files to serve, with their md5 as etag like R2 does for single request uploads
cache_fixture <- function(){
  remote <- withr::local_tempdir(.local_envir=parent.frame())
  df <- data.frame(year=c(2021L,2021L,2022L),value=1:3)
  data_to_parquet(df,file.path(remote,"data"),partition_by="year")
  data_to_parquet(df,file.path(remote,"single.parquet"))
  files <- dir(remote,"\\.parquet$",recursive=TRUE)
  info <- file.info(file.path(remote,files))
  list(dir=remote,
       objects=data.frame(key=paste0("census/",files),size=info$size,modified=as.POSIXct("2026-10-10 12:00:00",tz="UTC"),
                          etag=vapply(file.path(remote,files),digest::digest,"",algo="md5",file=TRUE,USE.NAMES=FALSE)))
}

test_that("cache_parquet mirrors R2 paths and only downloads what changed", {
  fixture <- cache_fixture()
  objects <- fixture$objects
  downloads <- character()
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
                        r2_get_object=function(r2_bucket,r2_path,file){
                          downloads <<- c(downloads,r2_path)
                          file.copy(file.path(fixture$dir,sub("^census/","",r2_path)),file,overwrite=TRUE)
                        })
  withr::local_envvar(R2_ACCOUNT_ID="acct",R2_ACCESS_KEY_ID="key",R2_SECRET_ACCESS_KEY="secret")
  cache <- local_cache_dir()

  expect_message(local <- cache_parquet("r2://bucket/census/data",cache),"Downloading 2 files")
  expect_equal(local,file.path(cache,"r2","acct","bucket","census","data"))
  expect_setequal(dir(local,recursive=TRUE),c("_manifest.json","year=2021/data_0.parquet","year=2022/data_0.parquet"))
  expect_equal(downloads,c("census/data/year=2021/data_0.parquet","census/data/year=2022/data_0.parquet"))
  manifest <- jsonlite::fromJSON(file.path(local,"_manifest.json"))$files
  expect_equal(manifest$path,c("year=2021/data_0.parquet","year=2022/data_0.parquet"))
  expect_equal(manifest$etag,objects$etag[startsWith(objects$key,"census/data/")])
  # nothing to do the second time, the table reads the local copy
  downloads <- character()
  expect_no_message(expect_equal(cache_parquet("r2://bucket/census/data",cache),local))
  expect_length(downloads,0)
  tbl <- parquet_tbl("r2://bucket/census/data",cache=cache)
  expect_equal(dplyr::pull(dplyr::count(tbl)),3)
  expect_equal(dplyr::pull(dplyr::count(dplyr::filter(tbl,year=="2022"))),1)
  expect_equal(list_parquet(tbl)$path,file.path(local,c("year=2021","year=2022"),"data_0.parquet"))
  # a changed file gets downloaded again, a removed one goes
  objects$etag[objects$key=="census/data/year=2021/data_0.parquet"] <- "changed"
  objects <- objects[objects$key!="census/data/year=2022/data_0.parquet",]
  expect_message(cache_parquet("r2://bucket/census/data",cache),"Downloading 1 file ")
  expect_equal(downloads,"census/data/year=2021/data_0.parquet")
  expect_setequal(dir(local,recursive=TRUE),c("_manifest.json","year=2021/data_0.parquet"))
  expect_false(dir.exists(file.path(local,"year=2022")))
  # single files keep a manifest in their directory, next to entries of other files
  downloads <- character()
  objects <- fixture$objects
  expect_message(single <- cache_parquet("r2://bucket/census/single.parquet",cache),"Downloading 1 file")
  expect_equal(single,file.path(cache,"r2","acct","bucket","census","single.parquet"))
  expect_true(file.exists(single))
  expect_equal(jsonlite::fromJSON(file.path(dirname(single),"_manifest.json"))$files$path,"single.parquet")
  expect_no_message(cache_parquet("r2://bucket/census/single.parquet",cache))
  expect_equal(downloads,"census/single.parquet")
  # several paths, local paths pass through, missing paths and bad arguments
  expect_message(expect_equal(cache_parquet(c("r2://bucket/census/data",fixture$dir,"r2://bucket/census/single.parquet"),cache),
                              c(local,fixture$dir,single)),"Downloading 2 files")
  expect_error(cache_parquet("r2://bucket/census/missing.parquet",cache),"does not exist")
  expect_error(cache_parquet("r2://bucket",cache),"specify the path")
  expect_error(cache_parquet("r2://bucket/census/data",cache=""),"cache has to be")
  expect_error(cache_parquet(1,cache),"character vector")
  expect_error(parquet_tbl("r2://bucket/census/data",cache=NA),"cache has to be")
})

test_that("cache=TRUE uses the cache path from the environment and the cache survives being offline", {
  fixture <- cache_fixture()
  objects <- fixture$objects
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
                        r2_get_object=function(r2_bucket,r2_path,file){
                          file.copy(file.path(fixture$dir,sub("^census/","",r2_path)),file,overwrite=TRUE)
                        })
  cache <- local_cache_dir()
  withr::local_envvar(R2_ACCOUNT_ID="acct",R2_ACCESS_KEY_ID="key",R2_SECRET_ACCESS_KEY="secret",R2PARQUET_CACHE_PATH=cache)
  expect_message(local <- cache_parquet("r2://bucket/census/data"),"Downloading")
  expect_equal(local,file.path(cache,"r2","acct","bucket","census","data"))
  expect_message(single <- cache_parquet("r2://bucket/census/single.parquet",TRUE),"Downloading")
  expect_equal(dplyr::pull(dplyr::count(parquet_tbl("r2://bucket/census/single.parquet",cache=TRUE))),3)
  # without the environment variable the user cache directory is used
  withr::local_envvar(R2PARQUET_CACHE_PATH="")
  expect_equal(r2parquet:::cache_directory(TRUE),tools::R_user_dir("r2parquet","cache"))
  withr::local_envvar(R2PARQUET_CACHE_PATH=cache)
  # the remote can't be reached: cached copies get used with a warning, anything else fails
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix="")stop("Could not resolve host"))
  expect_warning(expect_equal(cache_parquet("r2://bucket/census/data"),local),"using the cached copy")
  expect_warning(expect_equal(cache_parquet("r2://bucket/census/single.parquet"),single),"Could not resolve host")
  expect_error(cache_parquet("r2://bucket/census/other.parquet"),"Could not resolve host")
  expect_warning(expect_equal(dplyr::pull(dplyr::count(parquet_tbl("r2://bucket/census/data",cache=TRUE))),3),"cached copy")
})

test_that("downloads that do not match their etag are rejected", {
  fixture <- cache_fixture()
  objects <- fixture$objects[fixture$objects$key=="census/single.parquet",]
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
                        r2_get_object=function(r2_bucket,r2_path,file)writeBin(as.raw(1:10),file))
  withr::local_envvar(R2_ACCOUNT_ID="acct",R2_ACCESS_KEY_ID="key",R2_SECRET_ACCESS_KEY="secret")
  cache <- local_cache_dir()
  expect_error(suppressMessages(cache_parquet("r2://bucket/census/single.parquet",cache)),"does not match its etag")
  local <- file.path(cache,"r2","acct","bucket","census")
  expect_false(file.exists(file.path(local,"single.parquet")))
  expect_length(dir(local,all.files=TRUE,no..=TRUE),0)
  # etags that are not md5 hashes, like for multipart uploads, are not checked
  objects$etag <- "abc-2"
  expect_message(cache_parquet("r2://bucket/census/single.parquet",cache),"Downloading")
  expect_true(file.exists(file.path(local,"single.parquet")))
})

test_that("URLs get cached by host, files via their headers and directories via their manifest", {
  fixture <- cache_fixture()
  objects <- fixture$objects
  # the response to a request for the first byte, with the total size in the content range
  url_headers <- function(key){
    object <- objects[objects$key==key,]
    structure(list(etag=paste0("\"",object$etag,"\""),"content-length"="1","content-range"=paste0("bytes 0-0/",object$size),
                   "last-modified"="Fri, 10 Oct 2026 12:00:00 GMT"),class=c("insensitive","list"))
  }
  requests <- list()
  local_mocked_bindings(http_probe=function(url){
    requests[[length(requests)+1]] <<- list(verb="HEAD",url=url)
    key <- sub("^https://data.example.com/","",sub("\\?.*$","",url))
    if (key %in% objects$key) {
      structure(list(url=url,status_code=206L,headers=url_headers(key),content=as.raw(80)),class="response")
    } else fake_response(404)
  },http_get=function(url){
    requests[[length(requests)+1]] <<- list(verb="GET",url=url)
    key <- sub("^https://data.example.com/","",url)
    if (key=="census/data/_manifest.json") {
      files <- objects[startsWith(objects$key,"census/data/"),]
      fake_response(200,as.character(r2parquet:::manifest_json(r2parquet:::manifest_files(
        substring(files$key,13),files$size,files$etag,files$modified))))
    } else fake_response(404)
  },http_download=function(url,file){
    requests[[length(requests)+1]] <<- list(verb="DOWNLOAD",url=url)
    file.copy(file.path(fixture$dir,sub("^https://data.example.com/census/","",sub("\\?.*$","",url))),file,overwrite=TRUE)
  })
  cache <- local_cache_dir()
  withr::local_envvar(R2PARQUET_CACHE_PATH=cache)
  # files and directories at URLs get listed via their headers and manifest
  files <- list_parquet("https://data.example.com/census/data")
  expect_equal(files$path,paste0("https://data.example.com/census/data/year=",2021:2022,"/data_0.parquet"))
  expect_equal(files$etag,objects$etag[startsWith(objects$key,"census/data/")])
  expect_equal(files$cache,rep("not cached",2))
  single_file <- list_parquet("https://data.example.com/census/single.parquet?X-Amz-Signature=abc")
  expect_equal(single_file$path,"https://data.example.com/census/single.parquet?X-Amz-Signature=abc")
  expect_equal(single_file$sizeRaw,objects$size[objects$key=="census/single.parquet"])
  expect_equal(single_file$file,"single.parquet")
  expect_equal(single_file$modified,as.POSIXct("2026-10-10 12:00:00",tz="UTC"))
  expect_equal(single_file$cache,"not cached")
  expect_true(is.na(list_parquet("https://data.example.com/census/missing.parquet")$etag))
  # a file, also with a query string like presigned urls have
  requests <- list()
  expect_message(single <- cache_parquet("https://data.example.com/census/single.parquet?X-Amz-Signature=abc",cache),"Downloading 1 file")
  expect_equal(single,file.path(cache,"https","data.example.com","census","single.parquet"))
  expect_equal(vapply(requests,`[[`,"",i="verb"),c("HEAD","DOWNLOAD"))
  expect_equal(requests[[2]]$url,"https://data.example.com/census/single.parquet?X-Amz-Signature=abc")
  manifest <- jsonlite::fromJSON(file.path(dirname(single),"_manifest.json"))$files
  expect_equal(manifest$etag,objects$etag[objects$key=="census/single.parquet"])
  expect_equal(manifest$modified,"2026-10-10T12:00:00Z")
  requests <- list()
  expect_no_message(cache_parquet("https://data.example.com/census/single.parquet",cache))
  expect_equal(vapply(requests,`[[`,"",i="verb"),"HEAD")
  expect_equal(list_parquet("https://data.example.com/census/single.parquet")$cache,"current")
  # a directory via its manifest
  requests <- list()
  expect_message(local <- cache_parquet("https://data.example.com/census/data",cache),"Downloading 2 files")
  expect_equal(local,file.path(cache,"https","data.example.com","census","data"))
  expect_equal(vapply(requests,`[[`,"",i="verb"),c("HEAD","GET","DOWNLOAD","DOWNLOAD"))
  expect_equal(requests[[2]]$url,"https://data.example.com/census/data/_manifest.json")
  expect_equal(requests[[3]]$url,"https://data.example.com/census/data/year=2021/data_0.parquet")
  expect_equal(dplyr::pull(dplyr::count(parquet_tbl("https://data.example.com/census/data/",cache=cache))),3)
  expect_equal(list_parquet("https://data.example.com/census/data")$cache,rep("current",2))
  # a trailing slash skips the request for the file
  requests <- list()
  cache_parquet("https://data.example.com/census/data/",cache)
  expect_equal(vapply(requests,`[[`,"",i="verb"),"GET")
  # the cache lists the copies by their URLs
  cached <- list_cache()
  expect_equal(cached$path,c("https://data.example.com/census/data/year=2021/data_0.parquet",
                             "https://data.example.com/census/data/year=2022/data_0.parquet",
                             "https://data.example.com/census/single.parquet"))
  expect_equal(cached$local,c(file.path(local,c("year=2021","year=2022"),"data_0.parquet"),single))
  expect_equal(list_cache("https://data.example.com/census/single.parquet?X-Amz-Signature=abc")$path,
               "https://data.example.com/census/single.parquet")
  # directories without a manifest, and URLs that are not files on a host
  expect_error(cache_parquet("https://data.example.com/census/",cache),"no manifest")
  expect_error(cache_parquet("https://data.example.com/",cache),"has to point to")
  expect_error(cache_parquet("https://data.example.com/../x.parquet",cache),"has to point to")
})

test_that("list_cache and remove_from_cache work on the cache directory and keep its manifests in line", {
  fixture <- cache_fixture()
  objects <- fixture$objects
  local_mocked_bindings(r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
                        r2_get_object=function(r2_bucket,r2_path,file){
                          file.copy(file.path(fixture$dir,sub("^census/","",r2_path)),file,overwrite=TRUE)
                        })
  cache <- local_cache_dir()
  withr::local_envvar(R2_ACCOUNT_ID="acct",R2_ACCESS_KEY_ID="key",R2_SECRET_ACCESS_KEY="secret",R2PARQUET_CACHE_PATH=cache)
  expect_named(list_cache(),c("file","size","sizeRaw","modified","etag","local","path"))
  expect_equal(nrow(list_cache()),0)
  expect_equal(list_parquet("r2://bucket/census/data")$cache,rep("not cached",2))
  suppressMessages({
    local <- cache_parquet("r2://bucket/census/data")
    single <- cache_parquet("r2://bucket/census/single.parquet")
  })
  # everything in the cache, or the copies of given paths, by their remote paths
  cached <- list_cache()
  expect_s3_class(cached,"tbl_df")
  expect_equal(cached$path,paste0("r2://bucket/census/",c("data/year=2021/data_0.parquet","data/year=2022/data_0.parquet","single.parquet")))
  expect_equal(cached$local,c(file.path(local,c("year=2021","year=2022"),"data_0.parquet"),single))
  expect_equal(cached$etag,objects$etag[match(sub("^r2://bucket/","",cached$path),objects$key)])
  expect_equal(cached$sizeRaw,objects$size[match(sub("^r2://bucket/","",cached$path),objects$key)])
  expect_equal(cached$file,c("data_0.parquet","data_0.parquet","single.parquet"))
  expect_equal(cached$modified,rep(objects$modified[1],3))
  expect_equal(list_cache("r2://bucket/census/data/")$path,cached$path[1:2])
  expect_equal(list_cache(c("r2://bucket/census/single.parquet","r2://bucket/census/data/year=2022"))$path,cached$path[c(2,3)])
  expect_equal(nrow(list_cache("r2://bucket/census/other.parquet")),0)
  expect_equal(nrow(list_cache("r2://other/census")),0)
  expect_error(list_cache(fixture$dir),"r2://bucket/path or a URL")
  expect_error(list_cache("r2://bucket"),"specify the path")
  # the cache column of list_parquet compares the etags of the copies with the remote files
  expect_equal(list_parquet("r2://bucket/census/data")$cache,rep("current",2))
  expect_equal(list_parquet("r2://bucket/census")$cache,rep("current",3))
  objects$etag[objects$key=="census/data/year=2021/data_0.parquet"] <- "changed"
  expect_equal(list_parquet("r2://bucket/census/data")$cache,c("stale","current"))
  expect_equal(list_parquet(c("r2://bucket/census/single.parquet",fixture$dir))$cache,c("current",NA,NA,NA))
  objects <- fixture$objects
  # removing a file inside a cached directory leaves a manifest that no longer covers the directory
  expect_message(removed <- remove_from_cache("r2://bucket/census/data/year=2021/data_0.parquet"),"Removed 1 file from the cache")
  expect_equal(removed,"r2://bucket/census/data/year=2021/data_0.parquet")
  expect_false(dir.exists(file.path(local,"year=2021")))
  manifest <- jsonlite::fromJSON(file.path(local,"_manifest.json"))
  expect_false(manifest$directory)
  expect_equal(manifest$files$path,"year=2022/data_0.parquet")
  expect_equal(list_parquet("r2://bucket/census/data")$cache,c("not cached","current"))
  expect_null(r2parquet:::cached_copy(r2parquet:::cache_location("r2://bucket/census/data",cache)))
  expect_message(cache_parquet("r2://bucket/census/data"),"Downloading 1 file")
  expect_true(jsonlite::fromJSON(file.path(local,"_manifest.json"))$directory)
  # a directory goes with its manifest, a single file leaves the directory manifest without its entry
  expect_message(removed <- remove_from_cache(c("r2://bucket/census/data","r2://bucket/census/data/year=2022")),"Removed 2 files")
  expect_equal(removed,cached$path[1:2])
  expect_false(dir.exists(local))
  expect_equal(list_cache()$path,"r2://bucket/census/single.parquet")
  expect_message(remove_from_cache("r2://bucket/census/single.parquet"),"Removed 1 file")
  expect_equal(nrow(list_cache()),0)
  # empty directories are removed up to the cache directory, which stays
  expect_length(dir(cache,all.files=TRUE,no..=TRUE),0)
  expect_true(dir.exists(cache))
  # nothing gets removed unless all paths are in the cache
  suppressMessages(cache_parquet("r2://bucket/census/single.parquet"))
  expect_error(remove_from_cache(c("r2://bucket/census/single.parquet","r2://bucket/census/data")),"Not in the cache: r2://bucket/census/data")
  expect_equal(nrow(list_cache()),1)
  expect_error(remove_from_cache(fixture$dir),"r2://bucket/path or a URL")
  expect_error(remove_from_cache(character()),"character vector")
})
