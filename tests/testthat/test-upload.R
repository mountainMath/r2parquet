# the signed requests get mocked, the tests check which local files end up at which destinations

test_that("upload destinations get resolved for files and directories", {
  uploads <- list()
  purged <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    uploads[[length(uploads)+1]] <<- list(verb=verb,destination=paste0(r2_bucket,"/",r2_path),size=length(body))
    fake_response(200)
  },r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
  r2_update_manifest=function(r2_bucket,r2_dir,create=FALSE)NULL)
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

test_that("directory uploads write a manifest, single files update the manifest of their directory", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  # what the bucket holds after the uploads, listings come from here
  objects <- data.frame(key=c("data/a.parquet","data/year=2021/data_0.parquet","data/notes.txt","data/_manifest.json",
                              "other/b.parquet","single/a.parquet"),
                        size=c(1,2,3,4,5,1),modified=now,etag=c("e1","e2","e3","e4","e5","e6"))
  puts <- list()
  purged <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") puts[[r2_path]] <<- list(type=headers[["content-type"]],body=rawToChar(body))
    fake_response(200)
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths)
  dir <- withr::local_tempdir()
  writeBin(as.raw(1),file.path(dir,"a.parquet"))
  dir.create(file.path(dir,"year=2021"))
  writeBin(as.raw(1:2),file.path(dir,"year=2021","data_0.parquet"))

  upload_parquet(dir,"r2://bucket/data")
  expect_setequal(names(puts),c("data/a.parquet","data/year=2021/data_0.parquet","data/_manifest.json"))
  expect_equal(puts[["data/_manifest.json"]]$type,"application/json")
  manifest <- jsonlite::fromJSON(puts[["data/_manifest.json"]]$body)$files
  expect_equal(manifest$path,c("a.parquet","year=2021/data_0.parquet"))
  expect_equal(manifest$size,c(1,2))
  expect_equal(manifest$etag,c("e1","e2"))
  expect_equal(manifest$modified,rep("2026-10-10T12:00:00Z",2))
  expect_equal(purged,list(c("data/a.parquet","data/year=2021/data_0.parquet","data/_manifest.json")))
  # no new manifest when turned off, but an existing one gets updated; into the bucket itself the manifest is at the root
  puts <- list()
  upload_parquet(dir,"r2://bucket/other",manifest=FALSE)
  expect_setequal(names(puts),c("other/a.parquet","other/year=2021/data_0.parquet"))
  puts <- list()
  upload_parquet(dir,"r2://bucket/data",manifest=FALSE)
  expect_setequal(names(puts),c("data/a.parquet","data/year=2021/data_0.parquet","data/_manifest.json"))
  puts <- list()
  upload_parquet(dir,"r2://bucket")
  expect_true("_manifest.json" %in% names(puts))
  expect_equal(jsonlite::fromJSON(puts[["_manifest.json"]]$body)$files$path,
               c("data/a.parquet","data/year=2021/data_0.parquet","other/b.parquet","single/a.parquet"))
  # a single file updates an existing manifest, but does not create one
  puts <- list()
  purged <- list()
  upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/data/")
  expect_setequal(names(puts),c("data/a.parquet","data/_manifest.json"))
  expect_equal(purged,list(c("data/a.parquet","data/_manifest.json")))
  puts <- list()
  upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/single/a.parquet")
  expect_equal(names(puts),"single/a.parquet")
})

test_that("uploads keep the manifests above the destination current", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  # a partitioned dataset with its manifest at the root, as data_to_parquet writes it
  objects <- data.frame(key=c("data/year=2021/data_0.parquet","data/_manifest.json"),size=1,modified=now,etag="e")
  puts <- list()
  purged <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") {
      puts[[r2_path]] <<- if (headers[["content-type"]]=="application/json") jsonlite::fromJSON(rawToChar(body))$files$path else "data"
      if (!(r2_path %in% objects$key)) objects <<- rbind(objects,data.frame(key=r2_path,size=1,modified=now,etag="e"))
    }
    fake_response(if (verb=="HEAD" && !(r2_path %in% objects$key)) 404 else 200)
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths)
  dir <- withr::local_tempdir()
  writeBin(as.raw(1),file.path(dir,"data_0.parquet"))
  # a file into a new partition: the partition has no manifest, the dataset's lists the new file
  upload_parquet(file.path(dir,"data_0.parquet"),"r2://bucket/data/year=2022/")
  expect_equal(puts,list("data/year=2022/data_0.parquet"="data",
                         "data/_manifest.json"=c("year=2021/data_0.parquet","year=2022/data_0.parquet")))
  expect_equal(purged,list(c("data/year=2022/data_0.parquet","data/_manifest.json")))
  # a directory as a new partition gets a manifest of its own, and the dataset's manifest lists its files too
  puts <- list()
  purged <- list()
  upload_parquet(dir,"r2://bucket/data/year=2023")
  expect_equal(puts,list("data/year=2023/data_0.parquet"="data","data/year=2023/_manifest.json"="data_0.parquet",
                         "data/_manifest.json"=c("year=2021/data_0.parquet","year=2022/data_0.parquet","year=2023/data_0.parquet")))
  expect_equal(purged,list(c("data/year=2023/data_0.parquet","data/year=2023/_manifest.json","data/_manifest.json")))
})

test_that("write_manifest writes manifests for directories", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- data.frame(key=c("data/a.parquet","data/year=2021/data_0.parquet","data/notes.txt","single.parquet","empty/notes.txt"),
                        size=1,modified=now,etag="e")
  puts <- list()
  purged <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") puts[[r2_path]] <<- rawToChar(body)
    fake_response(200)
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths)
  expect_equal(write_manifest("r2://bucket/data/"),"r2://bucket/data/_manifest.json")
  expect_equal(jsonlite::fromJSON(puts[["data/_manifest.json"]])$files$path,c("a.parquet","year=2021/data_0.parquet"))
  expect_equal(purged,list("data/_manifest.json"))
  expect_equal(write_manifest("r2://bucket",purge_cache=FALSE),"r2://bucket/_manifest.json")
  expect_equal(jsonlite::fromJSON(puts[["_manifest.json"]])$files$path,c("data/a.parquet","data/year=2021/data_0.parquet","single.parquet"))
  expect_length(purged,1)
  expect_error(write_manifest("r2://bucket/single.parquet"),"is a file")
  expect_error(write_manifest("r2://bucket/empty"),"No parquet files found")
  expect_error(write_manifest("r2://bucket/missing"),"No parquet files found")
  expect_error(write_manifest(character()),"r2://bucket/path")
})
