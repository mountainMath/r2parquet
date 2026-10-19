test_that("metadata has to be a named list without a geo entry", {
  df <- data.frame(a=1)
  path <- tempfile(fileext=".parquet")
  expect_error(data_to_parquet(df,path,metadata="x"),"named list")
  expect_error(data_to_parquet(df,path,metadata=list("x")),"named list")
  expect_error(data_to_parquet(df,path,metadata=list(geo="x")),"reserved")
  expect_error(data_to_parquet(df,path,metadata=list(columns=list(b="B"))),"Columns b in metadata\\$columns")
  expect_error(data_to_parquet(df,path,metadata=list(columns="B")),"named list with one entry per column")
})

test_that("metadata round trips through the parquet footer", {
  df <- data.frame(id=c("003","001","002"),value=c(3,1,2))
  attr(df$value,"label") <- "The value"
  path <- withr::local_tempfile(fileext=".parquet")
  metadata <- list(source="https://example.org/data",license="OGL-Canada-2.0",notes=c("first","second"),
                   columns=list(id=list(label="Identifier",description="Three digit code"),value="Overridden"),
                   skipped=NULL,missing=NA)
  # NULL and NA entries get dropped with a warning instead of ending up as strings in the footer
  expect_warning(data_to_parquet(df,path,sort_by="id",metadata=metadata),"Dropping the metadata entries skipped, missing")
  result <- parquet_metadata(path)
  expect_false("missing" %in% names(result))
  expect_equal(result$source,"https://example.org/data")
  expect_equal(result$license,"OGL-Canada-2.0")
  expect_equal(result$notes,c("first","second"))
  expect_equal(result$columns$id,list(label="Identifier",description="Three digit code"))
  # a string is short for the label, and takes precedence over the label attribute
  expect_equal(result$columns$value,list(label="Overridden"))
  expect_false("skipped" %in% names(result))
  expect_setequal(names(result),c("source","license","notes","columns"))
  # the entries are plain strings in the footer for other tools
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  kv <- DBI::dbGetQuery(con,paste0("SELECT decode(key) AS k, decode(value) AS v FROM parquet_kv_metadata('",path,"')"))
  expect_equal(kv$v[kv$k=="source"],"https://example.org/data")
  expect_equal(kv$v[kv$k=="notes"],'["first","second"]')
})

test_that("strings that look like JSON come back as strings", {
  path <- withr::local_tempfile(fileext=".parquet")
  metadata <- list(array="[1]",object='{"a": 1}',quoted='"x"',spaced=" [1]",plain="a [1]")
  data_to_parquet(data.frame(a=1),path,metadata=metadata)
  expect_equal(parquet_metadata(path),metadata)
  # structured entries still get parsed
  data_to_parquet(data.frame(a=1),path,metadata=list(notes=list(a=1L,b="[1]")))
  expect_equal(parquet_metadata(path)$notes,list(a=1L,b="[1]"))
})

test_that("column labels get picked up from label attributes", {
  df <- data.frame(id=1:3,value=c(3,1,2))
  attr(df$value,"label") <- "The value"
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path)
  expect_equal(parquet_metadata(path),list(columns=list(value=list(label="The value"))))
  # files without metadata have none
  data_to_parquet(data.frame(a=1),path)
  expect_equal(parquet_metadata(path),list())
})

test_that("collect_parquet attaches metadata and labels, also after dplyr verbs", {
  df <- data.frame(id=c("003","001","002"),value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(source="s",columns=list(id="Identifier",value="Value")))
  tbl <- parquet_tbl(path)
  expect_equal(parquet_metadata(tbl),list(source="s",columns=list(id=list(label="Identifier"),value=list(label="Value"))))
  result <- collect_parquet(tbl)
  expect_s3_class(result,"tbl_df")
  expect_false(inherits(result,"sf"))
  expect_equal(attr(result,"metadata")$source,"s")
  expect_equal(attr(result$id,"label"),"Identifier")
  expect_equal(attr(result$value,"label"),"Value")
  # collected data carries the metadata, sf results included
  expect_equal(parquet_metadata(result),parquet_metadata(tbl))
  expect_equal(parquet_metadata(data.frame(a=1)),list())
  filtered <- collect_parquet(dplyr::select(dplyr::filter(tbl,value>1),id))
  expect_equal(nrow(filtered),2)
  expect_equal(attr(filtered,"metadata")$source,"s")
  expect_equal(attr(filtered$id,"label"),"Identifier")
  plain <- collect_parquet(tbl,attach_metadata=FALSE)
  expect_null(attr(plain,"metadata"))
  expect_null(attr(plain$id,"label"))
  expect_equal(parquet_metadata(plain),list())
  # lazy tables not from parquet_tbl have no metadata
  other <- dplyr::tbl(dbplyr::remote_con(tbl),dplyr::sql("SELECT 1 AS a"))
  expect_null(attr(collect_parquet(other),"metadata"))
  expect_error(parquet_metadata(1),"path")
  expect_error(parquet_metadata(other),"not created by parquet_tbl")
})

test_that("partitioned directories share their metadata and disagreeing files warn", {
  df <- data.frame(group=rep(c("a","b"),each=3),value=1:6)
  dir <- withr::local_tempdir()
  data_to_parquet(df,file.path(dir,"data"),partition_by="group",metadata=list(source="s"))
  expect_equal(parquet_metadata(file.path(dir,"data")),list(source="s"))
  expect_equal(parquet_metadata(parquet_tbl(file.path(dir,"data"))),list(source="s"))
  data_to_parquet(df[df$group=="b",],file.path(dir,"data"),partition_by="group",remove_stale=FALSE,metadata=list(source="t",license="l"))
  expect_warning(result <- parquet_metadata(file.path(dir,"data")),"disagree on the metadata entry source")
  expect_equal(result,list(source="s",license="l"))
})

test_that("GeoParquet keeps the geo entry out of the metadata and collect_parquet returns sf", {
  skip_if_no_duckdb_extensions()
  data <- nc_data()
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data,path,metadata=list(source="https://example.org/nc",columns=list(NAME="County name")))
  metadata <- parquet_metadata(path)
  expect_equal(names(metadata),c("source","columns"))
  result <- collect_parquet(parquet_tbl(path))
  expect_s3_class(result,"sf")
  expect_true(sf::st_crs(result)==sf::st_crs(data))
  expect_equal(attr(result,"metadata")$source,"https://example.org/nc")
  expect_equal(attr(result$NAME,"label"),"County name")
  expect_equal(attr(result,"sf_column"),"geometry")
})

test_that("primary keys get checked and written as arrays", {
  df <- data.frame(DAUID=c("1","1","2"),characteristic=c(1L,2L,1L),value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(primary_key=c("DAUID","characteristic")))
  expect_equal(parquet_metadata(path)$primary_key,c("DAUID","characteristic"))
  # a single column is an array in the footer too
  data_to_parquet(df[1:2,],path,metadata=list(primary_key="characteristic"))
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con,shutdown=TRUE))
  kv <- DBI::dbGetQuery(con,paste0("SELECT decode(key) AS k, decode(value) AS v FROM parquet_kv_metadata('",path,"')"))
  expect_equal(kv$v[kv$k=="primary_key"],'["characteristic"]')
  expect_error(data_to_parquet(df,path,metadata=list(primary_key="DAUID")),"does not identify rows, 1 rows repeat a key")
  expect_error(data_to_parquet(df,path,metadata=list(primary_key=c("DAUID","other"))),"Columns other in metadata\\$primary_key")
  expect_error(data_to_parquet(df,path,metadata=list(primary_key=1)),"vector of column names")
  df$characteristic[3] <- NA
  expect_error(data_to_parquet(df,path,metadata=list(primary_key=c("DAUID","characteristic"))),"missing values in 1 rows")
  # lazy tables get checked as well
  data_to_parquet(df[1:2,],path)
  expect_error(data_to_parquet(parquet_tbl(path),withr::local_tempfile(fileext=".parquet"),metadata=list(primary_key="DAUID")),
               "does not identify rows")
})

test_that("manifests keep one element arrays as arrays", {
  df <- data.frame(id=c("a","b"),value=1:2)
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(primary_key="id",notes=list("one"),repairs=list(columns="id",rows=list(list("a")))))
  footers <- unname(r2parquet:::parquet_footers(path))
  json <- as.character(r2parquet:::manifest_json(r2parquet:::manifest_files("data.parquet",1,"e",Sys.time(),footers)))
  manifest <- jsonlite::fromJSON(json,simplifyVector=FALSE)
  expect_equal(manifest$metadata$primary_key,list("id"))
  expect_equal(manifest$metadata$notes,list("one"))
  expect_equal(manifest$metadata$repairs,list(columns="id",rows=list(list("a"))))
  # and keep them through a rewrite of the manifest
  rewritten <- as.character(r2parquet:::manifest_json(r2parquet:::parse_manifest(json)))
  expect_equal(jsonlite::fromJSON(rewritten,simplifyVector=FALSE)$metadata,manifest$metadata)
  # the user gets them simplified, the same from the footers and from the manifest
  expected <- list(primary_key="id",notes="one",repairs=list(columns="id",rows=list("a")))
  expect_equal(parquet_metadata(path),expected)
  expect_equal(r2parquet:::merge_footers(r2parquet:::parse_manifest(json)$metadata),expected)
})

test_that("lazy tables get written with metadata", {
  df <- data.frame(id=1:3,value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(source="s",columns=list(value="Value")))
  path2 <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(dplyr::filter(parquet_tbl(path),value>1),path2,metadata=list(source="s2"))
  expect_equal(parquet_metadata(path2),list(source="s2"))
  expect_error(data_to_parquet(parquet_tbl(path),path2,metadata=list(columns=list(other="x"))),"Columns other")
})

test_that("remote metadata comes from the nearest manifest, or from the footers without one", {
  now <- as.POSIXct("2026-10-10 12:00:00",tz="UTC")
  objects <- data.frame(key=c("data/year=2021/data_0.parquet","data/year=2022/data_0.parquet","data/_manifest.json",
                              "plain/a.parquet","old/a.parquet","old/_manifest.json"),size=1,modified=now,etag="e")
  bodies <- list("data/_manifest.json"=manifest_body(c("year=2021/data_0.parquet","year=2022/data_0.parquet"),
                                                     metadata=list(list(source="s",license="l"),list(source="s",license="m"))),
                 # a manifest from before manifests recorded metadata
                 "old/_manifest.json"='{"directory":true,"files":[{"path":"a.parquet","size":1,"etag":"e","modified":"2026-10-10T12:00:00Z"}]}')
  gets <- character()
  footers <- character()
  read_footers <- r2parquet:::read_parquet_footers
  local_mocked_bindings(r2_request=manifest_bucket(environment()),
                        duckdb_parquet_paths=function(con,path)path,
                        # remote footers are faked, local ones read
                        read_parquet_footers=function(con,path){
                          if (!any(startsWith(path,"r2://"))) return(read_footers(con,path))
                          footers <<- c(footers,path)
                          stats::setNames(rep(list(list(source="f")),length(path)),path)
                        })
  # a directory: one request, the entries of its files merged like footers
  expect_warning(result <- parquet_metadata("r2://bucket/data"),"disagree on the metadata entry license")
  expect_equal(result,list(source="s",license="l"))
  expect_equal(gets,"data/_manifest.json")
  # a partition and a file in it are covered by the manifest above them
  gets <- character()
  expect_equal(parquet_metadata("r2://bucket/data/year=2022"),list(source="s",license="m"))
  expect_equal(parquet_metadata("r2://bucket/data/year=2022/data_0.parquet"),list(source="s",license="m"))
  expect_equal(gets,c("data/year=2022/_manifest.json","data/_manifest.json",
                      "data/year=2022/data_0.parquet/_manifest.json","data/year=2022/_manifest.json","data/_manifest.json"))
  expect_length(footers,0)
  # the footers get read without a manifest, with one that predates the metadata, for a path the manifest does
  # not list, and with from_files = TRUE
  expect_equal(parquet_metadata("r2://bucket/plain/a.parquet"),list(source="f"))
  expect_equal(parquet_metadata("r2://bucket/old/a.parquet"),list(source="f"))
  expect_equal(parquet_metadata("r2://bucket/data/year=2023"),list(source="f"))
  expect_equal(parquet_metadata("r2://bucket/data",from_files=TRUE),list(source="f"))
  expect_equal(footers,c("r2://bucket/plain/a.parquet","r2://bucket/old/a.parquet","r2://bucket/data/year=2023","r2://bucket/data"))
  # paths with and without a manifest get merged like files
  footers <- character()
  expect_warning(result <- parquet_metadata(c("r2://bucket/plain/a.parquet","r2://bucket/data/year=2022")),"disagree on the metadata entry source")
  expect_equal(result,list(source="s",license="m"))
  expect_equal(footers,"r2://bucket/plain/a.parquet")
  # a table reads the manifests of the paths it was resolved from, or the footers of the files it reads
  dir <- withr::local_tempdir()
  data_to_parquet(data.frame(a=1),file.path(dir,"local.parquet"),metadata=list(source="local"))
  tbl <- parquet_tbl(file.path(dir,"local.parquet"))
  expect_equal(parquet_metadata(tbl),list(source="local"))
  attr(tbl,"r2parquet_source") <- "r2://bucket/data/year=2022"
  expect_equal(parquet_metadata(tbl),list(source="s",license="m"))
  expect_equal(parquet_metadata(tbl,from_files=TRUE),list(source="local"))
  attr(tbl,"r2parquet_source") <- "r2://bucket/plain/a.parquet"
  expect_equal(parquet_metadata(tbl),list(source="local"))
})

test_that("footer reads stay out of RStudio's Connections pane", {
  events <- character()
  enable <- getOption("duckdb.enable_rstudio_connection_pane")
  withr::local_options(duckdb.force_rstudio_connection_pane=TRUE,
                       connectionObserver=list(connectionOpened=function(type,host,...)events <<- c(events,paste("opened",host)),
                                               connectionClosed=function(type,host)events <<- c(events,paste("closed",host)),
                                               connectionUpdated=function(...)NULL))
  dir <- withr::local_tempdir()
  data_to_parquet(data.frame(a=1),file.path(dir,"a.parquet"),metadata=list(source="s"))
  events <- character()
  expect_equal(r2parquet:::parquet_footers(file.path(dir,"a.parquet")),stats::setNames(list(list(source="s")),file.path(dir,"a.parquet")))
  expect_equal(events,character())
  # the options are back to what they were
  expect_equal(getOption("duckdb.force_rstudio_connection_pane"),TRUE)
  expect_equal(getOption("duckdb.enable_rstudio_connection_pane"),enable)
  # the session connection does register
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbDisconnect(con,shutdown=TRUE)
  expect_equal(events,c("opened :memory:","closed :memory:"))
})
