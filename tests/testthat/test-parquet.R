# parquet_tbl and the connection it works on, plain parquet so no extensions are needed

test_that("parquet_tbl uses the shared connection or one of its own", {
  df <- data.frame(id=1:3,value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path)
  expect_true(identical(parquet_connection(),parquet_connection()))
  expect_true(identical(dbplyr::remote_con(parquet_tbl(path)),parquet_connection()))
  # a connection of the user's own, with data to join against
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  tbl <- parquet_tbl(path,con=con)
  expect_true(identical(dbplyr::remote_con(tbl),con))
  names <- dplyr::copy_to(con,data.frame(id=1:2,name=c("a","b")),"names")
  joined <- collect_parquet(dplyr::inner_join(tbl,names,by="id"))
  expect_equal(nrow(joined),2)
  expect_equal(sort(joined$name),c("a","b"))
  expect_error(parquet_tbl(path,con="con"),"DuckDB connection")
  expect_error(parquet_tbl(character()),"character vector")
})

test_that("a disconnected shared connection gets replaced", {
  df <- data.frame(id=1:3)
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path)
  old <- parquet_connection()
  DBI::dbDisconnect(old,shutdown=TRUE)
  expect_false(identical(parquet_connection(),old))
  expect_equal(nrow(collect_parquet(parquet_tbl(path))),3)
})

test_that("list_parquet lists local files and the files of a table", {
  dir <- withr::local_tempdir()
  df <- data.frame(group=c("a","b"),value=1:2)
  data_to_parquet(df,file.path(dir,"data"),partition_by="group")
  data_to_parquet(df,file.path(dir,"single.parquet"))
  writeLines("x",file.path(dir,"data","notes.txt"))
  files <- list_parquet(file.path(dir,"data"))
  expect_s3_class(files,"tbl_df")
  expect_named(files,c("file","size","sizeRaw","modified","cache","etag","path"))
  expect_equal(files$file,rep("data_0.parquet",2))
  expect_true(all(is.na(files$cache)))
  expect_setequal(files$path,file.path(dir,"data",c("group=a","group=b"),"data_0.parquet"))
  expect_true(all(files$sizeRaw>0))
  expect_true(all(grepl("^[0-9.]+ k?B$",files$size)))
  expect_s3_class(files$modified,"POSIXct")
  expect_equal(attr(files$modified,"tzone"),"UTC")
  # files, several paths with duplicates, and missing paths
  expect_equal(list_parquet(file.path(dir,"single.parquet"))$path,file.path(dir,"single.parquet"))
  expect_equal(nrow(list_parquet(c(file.path(dir,"data"),file.path(dir,"single.parquet"),dir))),3)
  expect_equal(nrow(list_parquet(file.path(dir,"missing"))),0)
  expect_named(list_parquet(character()),c("file","size","sizeRaw","modified","cache","etag","path"))
  # the files a table reads, a URL that answers neither for a file nor with a manifest is listed as it is
  expect_equal(list_parquet(parquet_tbl(file.path(dir,"data"))),files)
  expect_equal(list_parquet(parquet_tbl(file.path(dir,"single.parquet")))$path,file.path(dir,"single.parquet"))
  tbl <- parquet_tbl(file.path(dir,"single.parquet"))
  attr(tbl,"r2parquet_paths") <- attr(tbl,"r2parquet_source") <- "https://example.com/data.parquet"
  local_mocked_bindings(http_probe=function(url)fake_response(404),http_get=function(url)fake_response(404))
  url <- list_parquet(tbl)
  expect_equal(url$path,"https://example.com/data.parquet")
  expect_true(is.na(url$size))
  expect_true(is.na(url$sizeRaw))
  expect_equal(url$file,"data.parquet")
  expect_true(is.na(url$etag))
  expect_error(list_parquet(dplyr::tbl(parquet_connection(),dplyr::sql("SELECT 1"))),"not created by parquet_tbl")
  expect_error(list_parquet(1),"path")
})

test_that("directories at URLs get read through their manifest", {
  skip_if_no_duckdb_extensions()
  root <- withr::local_tempdir()
  df <- data.frame(year=c(2021L,2021L,2022L),value=1:3)
  # a directory, and one named like a parquet file
  for (name in c("data","data.parquet")) {
    data_to_parquet(df,file.path(root,name),partition_by="year",metadata=list(source="s"))
    files <- dir(file.path(root,name),"\\.parquet$",recursive=TRUE)
    info <- file.info(file.path(root,name,files))
    etags <- vapply(file.path(root,name,files),digest::digest,"",algo="md5",file=TRUE,USE.NAMES=FALSE)
    footers <- unname(r2parquet:::parquet_footers(file.path(root,name,files)))
    writeLines(as.character(r2parquet:::manifest_json(r2parquet:::manifest_files(files,info$size,etags,info$mtime,footers))),
               file.path(root,name,"_manifest.json"))
  }
  base <- local_http_server(root)
  tbl <- parquet_tbl(paste0(base,"/data/"))
  expect_equal(dplyr::pull(dplyr::count(tbl)),3)
  # the partition columns come from the paths of the files, as for local directories
  expect_equal(sort(dplyr::pull(dplyr::distinct(tbl,year))),sort(dplyr::pull(dplyr::distinct(parquet_tbl(file.path(root,"data")),year))))
  expect_equal(attr(tbl,"r2parquet_paths"),paste0(base,"/data/year=",2021:2022,"/data_0.parquet"))
  expect_equal(list_parquet(tbl)$path,paste0(base,"/data/year=",2021:2022,"/data_0.parquet"))
  expect_equal(parquet_metadata(tbl)$source,"s")
  expect_equal(parquet_metadata(tbl,from_files=TRUE)$source,"s")
  # the metadata of a directory, of a partition and of a file comes from the manifest, or from the footers
  expect_equal(unclass(parquet_metadata(paste0(base,"/data/"))),list(source="s"))
  expect_equal(unclass(parquet_metadata(paste0(base,"/data.parquet"))),list(source="s"))
  expect_equal(unclass(parquet_metadata(paste0(base,"/data/year=2022"))),list(source="s"))
  expect_equal(unclass(parquet_metadata(paste0(base,"/data/year=2022/data_0.parquet"))),list(source="s"))
  expect_equal(unclass(parquet_metadata(paste0(base,"/data/year=2022/data_0.parquet"),from_files=TRUE)),list(source="s"))
  expect_equal(unclass(parquet_metadata(paste0(base,"/data/year=2022/data_0.parquet?x=1"))),list(source="s"))
  expect_error(parquet_metadata(paste0(base,"/missing/")),"no manifest")
  # without the trailing slash the files get tried first, a file is read as it is
  expect_equal(dplyr::pull(dplyr::count(parquet_tbl(paste0(base,"/data.parquet")))),3)
  expect_equal(dplyr::pull(dplyr::count(parquet_tbl(c(paste0(base,"/data"),paste0(base,"/data/year=2022/data_0.parquet"))))),4)
  # without a manifest the error of the read stands, or says what is missing for a directory
  expect_error(parquet_tbl(paste0(base,"/missing.parquet")),"No file at .*missing.parquet and no manifest",class="r2parquet_not_found")
  expect_error(parquet_tbl(paste0(base,"/missing/")),"no manifest",class="r2parquet_not_found")
})

test_that("whole numbers in comparisons are rendered as integers", {
  dir <- withr::local_tempdir()
  path <- file.path(dir,"numbers.parquet")
  data <- data.frame(i=1:5,d=c(1,1.5,2,2.5,3),day=as.Date("2024-01-01")+0:4)
  data_to_parquet(data,path)
  tbl <- parquet_tbl(path)
  expect_s4_class(dbplyr::remote_con(tbl),"r2parquet_connection")
  where <- function(t)sub(".*WHERE ","",gsub("\\s+"," ",as.character(dbplyr::remote_query(t))))
  assign("integer_literal_noted",FALSE,envir=r2parquet:::parquet_cache)
  expect_message(sql <- where(dplyr::filter(tbl,i==2)),"rendered as the integer 2L")
  expect_equal(sql,"(i = 2)")
  # the message is shown once per session
  expect_silent(expect_equal(where(dplyr::filter(tbl,i!=2,i<3,i<=3,i>1,i>=1)),"(i <> 2) AND (i < 3) AND (i <= 3) AND (i > 1) AND (i >= 1)"))
  expect_equal(where(dplyr::filter(tbl,i %in% c(1,2))),"(i IN (1, 2))")
  expect_equal(where(dplyr::filter(tbl,dplyr::between(i,1,3))),"(i BETWEEN 1 AND 3)")
  # fractions, numbers outside the integer range, dates and arithmetic stay as they are
  expect_equal(where(dplyr::filter(tbl,d==1.5)),"(d = 1.5)")
  expect_equal(where(dplyr::filter(tbl,i==3e9)),"(i = 3000000000.0)")
  expect_match(where(dplyr::filter(tbl,day==as.Date("2024-01-02"))),"TRY_CAST('2024-01-02' AS DATE)",fixed=TRUE)
  expect_equal(where(dplyr::filter(dplyr::mutate(tbl,y=i*1000),y>2)),"(y > 2)")
  expect_match(gsub("\\s+"," ",dbplyr::remote_query(dplyr::mutate(tbl,y=i*1000))),"i \\* 1000.0",fixed=FALSE)
  # and the results are right either way
  expect_equal(dplyr::collect(dplyr::filter(tbl,i==2))$d,1.5)
  expect_equal(dplyr::collect(dplyr::filter(tbl,d==2))$i,3L)
  expect_equal(dplyr::collect(dplyr::filter(tbl,d==1.5))$i,2L)
  expect_equal(dplyr::collect(dplyr::filter(tbl,day==as.Date("2024-01-02")))$i,2L)
  expect_equal(dplyr::collect(dplyr::filter(tbl,i %in% c(1,2)))$i,1:2)
  # a user's own connection keeps DuckDB's translation, so its tables stay joinable with the user's others
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  own <- parquet_tbl(path,con=con)
  expect_false(methods::is(dbplyr::remote_con(own),"r2parquet_connection"))
  expect_equal(where(dplyr::filter(own,i==2)),"(i = 2.0)")
  # the session connection's tables join with data copied there
  copied <- dplyr::copy_to(parquet_connection(),data.frame(i=2L,label="two"),"labels",overwrite=TRUE)
  expect_equal(dplyr::collect(dplyr::inner_join(tbl,copied,by="i"))$label,"two")
  # a lazy table still goes into data_to_parquet
  expect_silent(data_to_parquet(dplyr::filter(tbl,i>2L),file.path(dir,"subset.parquet")))
  expect_equal(nrow(dplyr::collect(parquet_tbl(file.path(dir,"subset.parquet")))),3L)
})

test_that("numbers compared with partition columns are rendered as strings", {
  dir <- withr::local_tempdir()
  data_to_parquet(data.frame(PRUID=c("10","59","59"),i=1:3,d=c(1,2,3)),file.path(dir,"parts"),partition_by="PRUID")
  tbl <- parquet_tbl(file.path(dir,"parts"))
  expect_equal(attr(tbl,"r2parquet_partitions"),"PRUID")
  expect_true(exists("PRUID",envir=r2parquet:::parquet_partitions))
  where <- function(t)sub(".*WHERE ","",gsub("\\s+"," ",as.character(dbplyr::remote_query(t))))
  assign("partition_literal_noted",FALSE,envir=r2parquet:::parquet_cache)
  assign("integer_literal_noted",TRUE,envir=r2parquet:::parquet_cache)
  expect_message(sql <- where(dplyr::filter(tbl,PRUID==59)),'rendered as the string "59"')
  expect_equal(sql,"(PRUID = '59')")
  # once per session, on either side, for integers and in %in%
  expect_silent(expect_equal(where(dplyr::filter(tbl,59L==PRUID,PRUID!=10)),"('59' = PRUID) AND (PRUID <> '10')"))
  expect_silent(expect_equal(where(dplyr::filter(tbl,10==PRUID)),"('10' = PRUID)"))
  expect_equal(where(dplyr::filter(tbl,PRUID %in% c(10,59))),"(PRUID IN ('10', '59'))")
  expect_equal(where(dplyr::filter(tbl,PRUID %in% 59L)),"(PRUID IN ('59'))")
  # order comparisons and other columns keep the integer rendering, DuckDB asks for an explicit cast on the
  # string column rather than comparing lexicographically
  expect_equal(where(dplyr::filter(tbl,PRUID>10,i==2)),"(PRUID > 10) AND (i = 2)")
  expect_error(dplyr::collect(dplyr::filter(tbl,PRUID>10)),"explicit cast")
  expect_equal(dplyr::collect(dplyr::filter(tbl,as.integer(PRUID)>10))$i,2:3)
  expect_equal(where(dplyr::filter(tbl,PRUID=="59")),"(PRUID = '59')")
  # the string literal lets DuckDB skip the other partition
  plan <- function(t)DBI::dbGetQuery(dbplyr::remote_con(t),paste0("EXPLAIN ",dbplyr::remote_query(t)))[[2]]
  expect_match(plan(dplyr::filter(tbl,PRUID==59)),"Scanning Files: 1/2")
  expect_equal(dplyr::collect(dplyr::filter(tbl,PRUID==59))$i,2:3)
  expect_equal(dplyr::collect(dplyr::filter(tbl,PRUID %in% c(10,59)))$i,1:3)
  # a numeric column of another table with the same name is unaffected, DuckDB casts the literal
  other <- parquet_tbl(local({p <- file.path(dir,"other.parquet"); data_to_parquet(data.frame(PRUID=c(10L,59L),v=1:2),p); p}))
  expect_equal(where(dplyr::filter(other,PRUID==59)),"(PRUID = '59')")
  expect_equal(dplyr::collect(dplyr::filter(other,PRUID==59))$v,2L)
  # the partition column is known for a single file read with its directories too
  file <- parquet_tbl(dir(file.path(dir,"parts"),pattern="\\.parquet$",recursive=TRUE,full.names=TRUE)[1])
  expect_equal(attr(file,"r2parquet_partitions"),"PRUID")
  expect_equal(attr(parquet_tbl(file.path(dir,"parts"),hive_partitioning=FALSE),"r2parquet_partitions"),character(0))
})

test_that("missing local paths get explained", {
  root <- withr::local_tempdir()
  expect_error(parquet_tbl(file.path(root,"census.parquet")),"No file or directory at .*census.parquet\\.$",class="r2parquet_not_found")
  withr::local_dir(root)
  expect_error(parquet_tbl("census.parquet"),"relative to the working directory",class="r2parquet_not_found")
  dir.create("empty")
  expect_error(parquet_tbl("empty"),"No parquet files in the directory empty",class="r2parquet_not_found")
  # with several paths the one that is missing gets named
  data_to_parquet(data.frame(x=1:2),"data.parquet",metadata=list())
  expect_error(parquet_tbl(c("data.parquet","census.parquet")),"No file or directory at census.parquet",class="r2parquet_not_found")
})

test_that("failed remote reads get explained", {
  denied <- function(target)simpleError(paste0("HTTP Error: HTTP GET error reading '",target,"' in region 'auto' (HTTP 403 Forbidden)\n\n",
                                               "SignatureDoesNotMatch: The request signature we calculated does not match."))
  e <- r2parquet:::read_error(denied("r2://bucket/data"),c("r2://bucket/data","r2://bucket/other"))
  expect_s3_class(e,"r2parquet_access_denied")
  expect_match(conditionMessage(e),"Reading r2://bucket/data failed with status 403. Access was denied: check that R2_ACCESS_KEY_ID")
  expect_match(conditionMessage(e),"SignatureDoesNotMatch",fixed=TRUE)
  e <- r2parquet:::read_error(simpleError("HTTP Error: HTTP GET error reading 's3://bucket/a.parquet' (HTTP 404 Not Found)\n\nNoSuchBucket: The specified bucket does not exist."),"s3://bucket/a.parquet")
  expect_s3_class(e,"r2parquet_not_found")
  expect_equal(conditionMessage(e),"The S3 bucket bucket does not exist.")
  e <- r2parquet:::read_error(simpleError('IO Error: No files found that match the pattern "r2://bucket/data/**/*.parquet"'),"r2://bucket/data")
  expect_equal(conditionMessage(e),"No parquet file or directory of parquet files at r2://bucket/data.")
  # other errors pass unchanged
  e <- simpleError("Invalid Input Error: not a parquet file")
  expect_identical(r2parquet:::read_error(e,"r2://bucket/data"),e)
})

test_that("denied requests say what access depends on", {
  expect_equal(r2parquet:::access_hint(404,"r2://bucket/a"),"")
  expect_match(r2parquet:::access_hint(403,"r2://bucket/a"),"R2_SECRET_ACCESS_KEY .* bucket bucket\\.$")
  expect_match(r2parquet:::access_hint(403,"s3://bucket/a"),"AWS_SECRET_ACCESS_KEY")
  expect_match(r2parquet:::access_hint(401,"https://example.com/a.parquet"),"not public")
  presigned <- function(date)paste0("https://example.com/a.parquet?X-Amz-Date=",date,"&X-Amz-Expires=3600&X-Amz-Signature=abc")
  expect_match(r2parquet:::access_hint(403,presigned("20200101T000000Z")),"expired at 2020-01-01 01:00:00 UTC")
  expect_match(r2parquet:::access_hint(403,presigned(format(Sys.time(),"%Y%m%dT%H%M%SZ",tz="UTC"))),"is not valid")
  withr::local_envvar(R2_ACCOUNT_ID="account",R2_ACCESS_KEY_ID="key",R2_SECRET_ACCESS_KEY="secret")
  local_mocked_bindings(r2_request=function(...)fake_response(403))
  expect_error(r2parquet:::r2_object_exists("bucket","a.parquet"),"failed with status 403. Access was denied",class="r2parquet_access_denied")
  local_mocked_bindings(http_probe=function(url)fake_response(403))
  expect_error(r2parquet:::url_cache_source(list(url="https://example.com/a.parquet")),"not public",class="r2parquet_access_denied")
})

test_that("servers that can't be reached fail with a network error", {
  # curl errors of requests
  unreachable <- function(...)stop(structure(class=c("curl_error_couldnt_resolve_host","curl_error","error","condition"),
                                             list(message="Could not resolve hostname [example.com]:\nCould not resolve host: example.com",call=NULL)))
  local_mocked_bindings(GET=unreachable,HEAD=unreachable,.package="httr")
  expect_error(r2parquet:::url_cache_source(list(url="https://example.com/a.parquet")),
               "^Could not reach example.com, check the internet connection. Could not resolve host: example.com$",
               class="r2parquet_network_error")
  expect_error(r2parquet:::url_cache_source(list(url="https://example.com/a/")),class="r2parquet_network_error")
  # list_parquet lists a URL it can't look up as it is, but not one it can't reach
  expect_error(list_parquet("https://example.com/a/"),class="r2parquet_network_error")
  # connection failures of DuckDB reads
  e <- r2parquet:::read_error(simpleError("IO Error: Could not resolve hostname error for HTTP HEAD to 'https://example.com:8080/a.parquet'"),
                              c("https://example.com:8080/a.parquet","b.parquet"))
  expect_s3_class(e,"r2parquet_network_error")
  expect_equal(conditionMessage(e),"Could not reach example.com:8080, check the internet connection. Could not resolve hostname.")
})

test_that("extensions that can't be downloaded fail with a network error", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con,shutdown=TRUE))
  DBI::dbExecute(con,paste0("SET extension_directory = ",DBI::dbQuoteString(con,withr::local_tempdir())))
  DBI::dbExecute(con,"SET custom_extension_repository = 'http://nonexistent.invalid'")
  expect_error(r2parquet:::load_extension(con,"httpfs"),"Could not reach nonexistent.invalid.*extension httpfs gets downloaded",
               class="r2parquet_network_error")
})
