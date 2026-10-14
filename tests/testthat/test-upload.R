# the signed requests get mocked, the tests check which local files end up at which destinations

test_that("upload destinations get resolved for files and directories", {
  uploads <- list()
  purged <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    uploads[[length(uploads)+1]] <<- list(verb=verb,destination=paste0(r2_bucket,"/",r2_path),size=length(body))
    fake_response(200)
  },r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
  r2_update_manifest=function(r2_bucket,r2_dir,create=FALSE,uploaded=list())NULL,parquet_footers=fake_footers())
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
  local_mocked_bindings(r2_request=function(...)fake_response(403,"<Error><Code>AccessDenied</Code></Error>"),parquet_footers=fake_footers())
  file <- withr::local_tempfile(fileext=".parquet")
  file.create(file)
  expect_error(upload_parquet(file,"r2://bucket/a.parquet"),"r2://bucket/a.parquet failed with status 403")
})

test_that("directory uploads write a manifest with the footer metadata, single files update the manifest of their directory", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  # what the bucket holds after the uploads, listings come from here
  objects <- data.frame(key=c("data/a.parquet","data/year=2021/data_0.parquet","data/notes.txt","data/_manifest.json",
                              "other/b.parquet","single/a.parquet"),
                        size=c(1,2,3,4,5,1),modified=now,etag=c("e1","e2","e3","e4","e5","e6"))
  puts <- list()
  purged <- list()
  footers <- new.env()
  columns <- list(a=list(label="A"),b=list(label="B",description="the b column"))
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") {
      puts[[r2_path]] <<- list(type=headers[["content-type"]],body=rawToChar(body))
      if (!(r2_path %in% objects$key)) objects <<- rbind(objects,data.frame(key=r2_path,size=1,modified=now,etag="e"))
    }
    if (verb=="GET") return(fake_response(if (r2_path %in% objects$key) 200 else 404,puts[[r2_path]]$body %||% ""))
    fake_response(200)
  },r2_list_objects=function(r2_bucket,prefix=""){listed <- objects[startsWith(objects$key,prefix),]; listed[order(listed$key),]},
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
  parquet_footers=fake_footers(list("a.parquet"=list(source="s",year="2021",columns=columns),
                                    "data_0.parquet"=list(source="s",year="2022",columns=columns,notes=list("x","y"))),calls=footers))
  dir <- withr::local_tempdir()
  writeBin(as.raw(1),file.path(dir,"a.parquet"))
  dir.create(file.path(dir,"year=2021"))
  writeBin(as.raw(1:2),file.path(dir,"year=2021","data_0.parquet"))

  upload_parquet(dir,"r2://bucket/data")
  expect_setequal(names(puts),c("data/a.parquet","data/year=2021/data_0.parquet","data/_manifest.json"))
  expect_equal(puts[["data/a.parquet"]]$type,"application/octet-stream")
  expect_equal(puts[["data/_manifest.json"]]$type,"application/json")
  manifest <- jsonlite::fromJSON(puts[["data/_manifest.json"]]$body,simplifyVector=FALSE)
  expect_equal(names(manifest),c("directory","metadata","files"))
  expect_true(manifest$directory)
  # the entries all files share are the dataset's metadata, the rest goes with the files
  expect_equal(manifest$metadata,list(source="s",columns=columns))
  expect_equal(manifest$files,list(list(path="a.parquet",size=1L,etag="e1",modified="2026-10-10T12:00:00Z",metadata=list(year="2021")),
                                   list(path="year=2021/data_0.parquet",size=2L,etag="e2",modified="2026-10-10T12:00:00Z",
                                        metadata=list(year="2022",notes=list("x","y")))))
  # the metadata came from the local files, nothing got read from R2
  expect_equal(footers$paths,file.path(dir,c("a.parquet","year=2021/data_0.parquet")))
  expect_equal(purged,list(c("data/a.parquet","data/year=2021/data_0.parquet","data/_manifest.json")))
  # into the bucket itself the manifest is at the root, listing everything; the footers of the files that are
  # not part of the upload and not in an existing manifest get read from R2, the column descriptions are only
  # recorded when all files share them
  puts <- list()
  footers$paths <- character()
  upload_parquet(dir,"r2://bucket")
  manifest <- jsonlite::fromJSON(puts[["_manifest.json"]]$body,simplifyVector=FALSE)
  expect_equal(vapply(manifest$files,`[[`,"",i="path"),
               c("a.parquet","data/a.parquet","data/year=2021/data_0.parquet","other/b.parquet","single/a.parquet","year=2021/data_0.parquet"))
  expect_equal(manifest$metadata,structure(list(),names=character()))
  expect_equal(manifest$files[[1]]$metadata,list(source="s",year="2021",columns=columns))
  expect_null(manifest$files[[4]]$metadata)
  expect_equal(footers$paths,c(file.path(dir,c("a.parquet","year=2021/data_0.parquet")),
                               paste0("r2://bucket/",c("data/a.parquet","data/year=2021/data_0.parquet","other/b.parquet","single/a.parquet"))))
  # a single file updates the existing manifests of its directory and the directories above, but does not create one
  puts <- list()
  purged <- list()
  upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/data/")
  expect_equal(names(puts),c("data/a.parquet","data/_manifest.json","_manifest.json"))
  expect_equal(purged,list(c("data/a.parquet","data/_manifest.json","_manifest.json")))
  puts <- list()
  upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/single/a.parquet")
  expect_equal(names(puts),c("single/a.parquet","_manifest.json"))
})

test_that("single files of any type go up with the content type for their extension", {
  puts <- list()
  purged <- list()
  requests <- character()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    requests <<- c(requests,paste(verb,r2_path))
    if (verb=="PUT") puts[[r2_path]] <<- list(type=headers[["content-type"]],body=rawToChar(body))
    fake_response(if (verb=="PUT") 200 else 404)
  },r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
  parquet_footers=fake_footers())
  dir <- withr::local_tempdir()
  writeLines('{"files":[]}',file.path(dir,"index.json"))
  writeLines("a,b",file.path(dir,"table.csv"))
  writeLines("notes",file.path(dir,"README"))
  writeLines("{}",file.path(dir,"_manifest.json"))

  expect_equal(upload_parquet(file.path(dir,"index.json"),"r2://bucket/data/"),"r2://bucket/data/index.json")
  expect_equal(puts[["data/index.json"]],list(type="application/json",body='{"files":[]}\n'))
  expect_equal(purged,list("data/index.json"))
  # no manifest gets touched for files that are not parquet
  expect_equal(requests,"PUT data/index.json")
  upload_parquet(file.path(dir,"table.csv"),"r2://bucket",purge_cache=FALSE)
  expect_equal(puts[["table.csv"]]$type,"text/csv")
  upload_parquet(file.path(dir,"README"),"r2://bucket/docs/readme")
  expect_equal(puts[["docs/readme"]]$type,"application/octet-stream")
  expect_equal(purged,list("data/index.json","docs/readme"))
  # manifests are written by the package
  expect_error(upload_parquet(file.path(dir,"_manifest.json"),"r2://bucket/data/"),"write_manifest")
  expect_error(upload_parquet(file.path(dir,"index.json"),"r2://bucket/data/_manifest.json"),"write_manifest")
  expect_length(puts,3)
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
    fake_response(if (verb=="GET" && !(r2_path %in% objects$key)) 404 else 200)
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
  parquet_footers=fake_footers())
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
  footers <- new.env()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") puts[[r2_path]] <<- rawToChar(body)
    fake_response(if (verb=="GET" && !(r2_path %in% objects$key)) 404 else 200)
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
  parquet_footers=fake_footers(list("a.parquet"=list(source="s"),"data_0.parquet"=list(source="s")),calls=footers))
  expect_equal(write_manifest("r2://bucket/data/"),"r2://bucket/data/_manifest.json")
  manifest <- jsonlite::fromJSON(puts[["data/_manifest.json"]],simplifyVector=FALSE)
  expect_equal(vapply(manifest$files,`[[`,"",i="path"),c("a.parquet","year=2021/data_0.parquet"))
  # the footers get read from R2
  expect_equal(manifest$metadata,list(source="s"))
  expect_equal(footers$paths,c("r2://bucket/data/a.parquet","r2://bucket/data/year=2021/data_0.parquet"))
  expect_equal(purged,list("data/_manifest.json"))
  expect_equal(write_manifest("r2://bucket",purge_cache=FALSE),"r2://bucket/_manifest.json")
  expect_equal(jsonlite::fromJSON(puts[["_manifest.json"]])$files$path,c("data/a.parquet","data/year=2021/data_0.parquet","single.parquet"))
  expect_length(purged,1)
  expect_error(write_manifest("r2://bucket/single.parquet"),"is a file")
  expect_error(write_manifest("r2://bucket/empty"),"No parquet files found")
  expect_error(write_manifest("r2://bucket/missing"),"No parquet files found")
  expect_error(write_manifest(character()),"r2://bucket/path")
})

test_that("remove_stale removes the parquet files below the destination that are not in the upload", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- data.frame(key=c("data/a.parquet","data/old.parquet","data/region=1/data_0.parquet","data/notes.txt",
                              "data/_manifest.json","data.parquet","other/b.parquet"),
                        size=1,modified=now,etag="e")
  puts <- list()
  deleted <- list()
  purged <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") {
      puts[[r2_path]] <<- rawToChar(body)
      if (!(r2_path %in% objects$key)) objects <<- rbind(objects,data.frame(key=r2_path,size=1,modified=now,etag="e"))
    }
    fake_response(if (verb=="GET" && !(r2_path %in% objects$key)) 404 else 200)
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_delete_objects=function(r2_bucket,r2_paths){
    deleted[[length(deleted)+1]] <<- r2_paths
    objects <<- objects[!(objects$key %in% r2_paths),]
  },
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)purged[[length(purged)+1]] <<- r2_paths,
  parquet_footers=fake_footers())
  dir <- withr::local_tempdir()
  writeBin(as.raw(1),file.path(dir,"a.parquet"))
  writeBin(as.raw(1:2),file.path(dir,"new.parquet"))

  # the default keeps them
  upload_parquet(dir,"r2://bucket/data")
  expect_length(deleted,0)
  expect_setequal(jsonlite::fromJSON(puts[["data/_manifest.json"]])$files$path,
                  c("a.parquet","new.parquet","old.parquet","region=1/data_0.parquet"))
  # stale files and partitions go, other files, files with the same prefix and files elsewhere stay
  puts <- list()
  purged <- list()
  expect_message(upload_parquet(dir,"r2://bucket/data",remove_stale=TRUE),"Removing 2 stale parquet files from r2://bucket/data")
  expect_equal(deleted,list(c("data/old.parquet","data/region=1/data_0.parquet")))
  expect_equal(jsonlite::fromJSON(puts[["data/_manifest.json"]])$files$path,c("a.parquet","new.parquet"))
  expect_equal(purged,list(c("data/a.parquet","data/new.parquet","data/old.parquet","data/region=1/data_0.parquet","data/_manifest.json")))
  expect_setequal(objects$key,c("data/a.parquet","data/new.parquet","data/notes.txt","data/_manifest.json","data.parquet","other/b.parquet"))
  # nothing stale, no message
  expect_no_message(upload_parquet(dir,"r2://bucket/data",remove_stale=TRUE,purge_cache=FALSE))
  expect_length(deleted,1)
  # single file uploads never remove anything
  upload_parquet(file.path(dir,"a.parquet"),"r2://bucket/other/",remove_stale=TRUE)
  expect_length(deleted,1)
})


test_that("manifest rewrites keep the metadata of unchanged files and read the footers of changed ones", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- data.frame(key=c("data/a.parquet","data/b.parquet","data/c.parquet","data/_manifest.json","_manifest.json"),
                        size=1,modified=now,etag=c("ea","eb","ec","em","er"))
  # the dataset's manifest knows a and b, b has changed since; the manifest at the bucket root predates the metadata
  bodies <- list("data/_manifest.json"=as.character(manifest_json(manifest_files(c("a.parquet","b.parquet"),1,c("ea","old"),now,
                   list(list(source="s",year="2021",columns=list(a="A")),list(source="s",year="2022",columns=list(a="A")))))),
                 "_manifest.json"='{"directory":true,"files":[{"path":"data/a.parquet","size":1,"etag":"ea","modified":"2026-10-10T12:00:00Z"}]}')
  puts <- list()
  footers <- new.env()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") {
      if (headers[["content-type"]]=="application/json") puts[[r2_path]] <<- jsonlite::fromJSON(rawToChar(body),simplifyVector=FALSE)
      if (!(r2_path %in% objects$key)) objects <<- rbind(objects,data.frame(key=r2_path,size=1,modified=now,etag="e"))
    }
    fake_response(if (verb=="GET" && !(r2_path %in% objects$key)) 404 else 200,bodies[[r2_path]] %||% "")
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)NULL,
  parquet_footers=fake_footers(list("a.parquet"=list(source="s",year="2021",columns=list(a="A")),"b.parquet"=list(source="s",year="2023",columns=list(a="A")),
                                    "c.parquet"=list(source="s",year="2024",columns=list(a="A")),"d.parquet"=list(source="s",year="2025",columns=list(a="A"))),
                               calls=footers))
  dir <- withr::local_tempdir()
  writeBin(as.raw(1),file.path(dir,"d.parquet"))
  upload_parquet(file.path(dir,"d.parquet"),"r2://bucket/data/")
  # a is carried over, b and c get read from R2, d comes from the local file
  manifest <- puts[["data/_manifest.json"]]
  expect_equal(manifest$metadata,list(source="s",columns=list(a="A")))
  expect_equal(lapply(manifest$files,`[[`,"metadata"),list(list(year="2021"),list(year="2023"),list(year="2024"),list(year="2025")))
  expect_equal(footers$paths[1:3],c(file.path(dir,"d.parquet"),"r2://bucket/data/b.parquet","r2://bucket/data/c.parquet"))
  # the manifest at the root has no metadata to carry over, so everything but d gets read
  expect_equal(footers$paths[-(1:3)],paste0("r2://bucket/data/",c("a","b","c"),".parquet"))
  expect_equal(puts[["_manifest.json"]]$metadata,list(source="s",columns=list(a="A")))
  # write_manifest carries over what the existing manifest covers the same way, column descriptions only stay when all files share them
  bodies[["data/_manifest.json"]] <- as.character(manifest_json(manifest_files("b.parquet",1,"eb",now,list(list(source="s",year="2023")))))
  puts <- list()
  footers$paths <- character()
  write_manifest("r2://bucket/data",purge_cache=FALSE)
  expect_equal(footers$paths,paste0("r2://bucket/data/",c("a","c","d"),".parquet"))
  expect_equal(puts[["data/_manifest.json"]]$metadata,list(source="s"))
  expect_equal(lapply(puts[["data/_manifest.json"]]$files,`[[`,"metadata"),
               list(list(year="2021",columns=list(a="A")),list(year="2023"),list(year="2024",columns=list(a="A")),list(year="2025",columns=list(a="A"))))
})

test_that("the manifest metadata comes from the parquet footers", {
  skip_on_cran()
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- data.frame(key=character(),size=numeric(),modified=now[0],etag=character())
  puts <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") {
      if (headers[["content-type"]]=="application/json") puts[[r2_path]] <<- jsonlite::fromJSON(rawToChar(body),simplifyVector=FALSE)
      objects <<- rbind(objects,data.frame(key=r2_path,size=length(body),modified=now,etag="e"))
    }
    fake_response(if (verb=="GET" && !(r2_path %in% objects$key)) 404 else 200)
  },r2_list_objects=function(r2_bucket,prefix="")objects[startsWith(objects$key,prefix),],
  r2_purge_cache_after_change=function(r2_bucket,r2_paths)NULL)
  dir <- withr::local_tempdir()
  df <- data.frame(year=c(2021L,2021L,2022L),value=1:3)
  data_to_parquet(df,file.path(dir,"data.parquet"),partition_by="year",
                  metadata=list(source="census",license="OGL",columns=list(value="The value")))
  data_to_parquet(df[1:2,],file.path(dir,"other.parquet"),metadata=list(source="census",notes=list(a=1,b="two")))
  upload_parquet(dir,"r2://bucket/data")
  manifest <- puts[["data/_manifest.json"]]
  expect_equal(manifest$metadata,list(source="census"))
  expect_equal(vapply(manifest$files,`[[`,"",i="path"),c("data.parquet/year=2021/data_0.parquet","data.parquet/year=2022/data_0.parquet","other.parquet"))
  expect_equal(manifest$files[[1]]$metadata,list(license="OGL",columns=list(value=list(label="The value"))))
  expect_equal(manifest$files[[3]]$metadata,list(notes=list(a=1L,b="two")))
  # the partitioned dataset on its own shares everything, column descriptions included
  puts <- list()
  upload_parquet(file.path(dir,"data.parquet"),"r2://bucket/data.parquet")
  manifest <- puts[["data.parquet/_manifest.json"]]
  expect_equal(manifest$metadata,list(source="census",license="OGL",columns=list(value=list(label="The value"))))
  expect_null(manifest$files[[1]]$metadata)
  expect_equal(parquet_footers(file.path(dir,"other.parquet"))[[1]],list(source="census",notes=list(a=1L,b="two")))
})
