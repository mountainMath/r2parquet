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

# a local http server for the files of a directory, started in the background with python, NULL if that fails
local_http_server <- function(root,envir=parent.frame()){
  skip_on_os("windows")
  skip_if(Sys.which("python3")=="","python3 not available")
  port <- sample(20000:40000,1)
  pid <- system(paste0("python3 -m http.server ",port," --bind 127.0.0.1 --directory ",shQuote(root)," >/dev/null 2>&1 & echo $!"),intern=TRUE)
  withr::defer(tools::pskill(as.integer(pid)),envir=envir)
  base <- paste0("http://127.0.0.1:",port)
  for (i in 1:100) {
    if (tryCatch(httr::status_code(httr::GET(paste0(base,"/"),httr::timeout(1)))<500,error=function(e)FALSE)) return(base)
    Sys.sleep(0.1)
  }
  skip("could not start a local http server")
}

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
  expect_equal(parquet_metadata(paste0(base,"/data/")),list(source="s"))
  expect_equal(parquet_metadata(paste0(base,"/data.parquet")),list(source="s"))
  expect_equal(parquet_metadata(paste0(base,"/data/year=2022")),list(source="s"))
  expect_equal(parquet_metadata(paste0(base,"/data/year=2022/data_0.parquet")),list(source="s"))
  expect_equal(parquet_metadata(paste0(base,"/data/year=2022/data_0.parquet"),from_files=TRUE),list(source="s"))
  expect_equal(parquet_metadata(paste0(base,"/data/year=2022/data_0.parquet?x=1")),list(source="s"))
  expect_error(parquet_metadata(paste0(base,"/missing/")),"no manifest")
  # without the trailing slash the files get tried first, a file is read as it is
  expect_equal(dplyr::pull(dplyr::count(parquet_tbl(paste0(base,"/data.parquet")))),3)
  expect_equal(dplyr::pull(dplyr::count(parquet_tbl(c(paste0(base,"/data"),paste0(base,"/data/year=2022/data_0.parquet"))))),4)
  # without a manifest the error of the read stands, or says what is missing for a directory
  expect_error(parquet_tbl(paste0(base,"/missing.parquet")),"404")
  expect_error(parquet_tbl(paste0(base,"/missing/")),"no manifest")
})
