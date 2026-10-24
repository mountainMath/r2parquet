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
  expect_equal(unclass(parquet_metadata(path)),metadata)
  # structured entries still get parsed
  data_to_parquet(data.frame(a=1),path,metadata=list(notes=list(a=1L,b="[1]")))
  expect_equal(parquet_metadata(path)$notes,list(a=1L,b="[1]"))
})

test_that("metadata prints one line per entry and the columns as a table", {
  metadata <- r2parquet:::new_parquet_metadata(list(
    title="A title",primary_key=c("a","b"),notes="line one\nline two",repairs=list(columns="a",rows=list("x","y")),
    columns=list(a=list(label="The a",description="Longer"),b=list(label="The b"),c=list(description="Only a description"))))
  lines <- format(metadata,width=80)
  expect_equal(lines[1:6],c("<parquet metadata: 5 entries>",
                            "title        A title",
                            "primary_key  a, b",
                            "notes        line one line two",
                            'repairs      {"columns":"a","rows":["x","y"]}',
                            "columns      <3 columns, see below>"))
  expect_equal(lines[8:11],c("columns","  a  The a: Longer","  b  The b","  c  Only a description"))
  expect_true(all(nchar(format(metadata,width=20)[-1],type="width")<=20))
  expect_match(format(metadata,max_columns=2)[11],"and 1 more")
  expect_output(print(metadata),"<parquet metadata: 5 entries>")
  expect_equal(format(r2parquet:::new_parquet_metadata(list())),"<parquet metadata: no entries>")
  # subsetting keeps the class, single entries come out as they are
  expect_s3_class(metadata[c("title","primary_key")],"parquet_metadata")
  expect_equal(names(metadata[c("title","primary_key")]),c("title","primary_key"))
  expect_equal(metadata$primary_key,c("a","b"))
})

test_that("metadata read back can be written again", {
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data.frame(a=1),path,metadata=list(source="s",primary_key="a",columns=list(a="The a")))
  copy <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data.frame(a=2),copy,metadata=parquet_metadata(path)[c("source","primary_key","columns")])
  expect_equal(unclass(parquet_metadata(copy)),list(source="s",primary_key="a",columns=list(a=list(label="The a"))))
})

test_that("column labels get picked up from label attributes", {
  df <- data.frame(id=1:3,value=c(3,1,2))
  attr(df$value,"label") <- "The value"
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path)
  expect_equal(unclass(parquet_metadata(path)),list(columns=list(value=list(label="The value"))))
  # files without metadata have none
  data_to_parquet(data.frame(a=1),path)
  expect_equal(unclass(parquet_metadata(path)),list())
})

test_that("collect_parquet attaches metadata and labels, also after dplyr verbs", {
  df <- data.frame(id=c("003","001","002"),value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(source="s",columns=list(id="Identifier",value="Value")))
  tbl <- parquet_tbl(path)
  expect_equal(unclass(parquet_metadata(tbl)),list(source="s",columns=list(id=list(label="Identifier"),value=list(label="Value"))))
  result <- collect_parquet(tbl)
  expect_s3_class(result,"tbl_df")
  expect_false(inherits(result,"sf"))
  expect_equal(attr(result,"metadata")$source,"s")
  expect_equal(attr(result$id,"label"),"Identifier")
  expect_equal(attr(result$value,"label"),"Value")
  # collected data carries the metadata, sf results included
  expect_equal(parquet_metadata(result),parquet_metadata(tbl))
  expect_equal(unclass(parquet_metadata(data.frame(a=1))),list())
  filtered <- collect_parquet(dplyr::select(dplyr::filter(tbl,value>1),id))
  expect_equal(nrow(filtered),2)
  expect_equal(attr(filtered,"metadata")$source,"s")
  expect_equal(attr(filtered$id,"label"),"Identifier")
  plain <- collect_parquet(tbl,attach_metadata=FALSE)
  expect_null(attr(plain,"metadata"))
  expect_null(attr(plain$id,"label"))
  expect_equal(unclass(parquet_metadata(plain)),list())
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
  expect_equal(unclass(parquet_metadata(file.path(dir,"data"))),list(source="s"))
  expect_equal(unclass(parquet_metadata(parquet_tbl(file.path(dir,"data")))),list(source="s"))
  data_to_parquet(df[df$group=="b",],file.path(dir,"data"),partition_by="group",remove_stale=FALSE,metadata=list(source="t",license="l"))
  expect_warning(result <- parquet_metadata(file.path(dir,"data")),"disagree on the metadata entry source")
  expect_equal(unclass(result),list(source="s",license="l"))
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
  expect_equal(unclass(parquet_metadata(path)),expected)
  expect_equal(r2parquet:::merge_footers(r2parquet:::parse_manifest(json)$metadata),expected)
})

test_that("lazy tables get written with metadata", {
  df <- data.frame(id=1:3,value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(source="s",columns=list(value="Value")))
  path2 <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(dplyr::filter(parquet_tbl(path),value>1),path2,metadata=list(source="s2"))
  expect_equal(unclass(parquet_metadata(path2)),list(source="s2"))
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
  expect_equal(unclass(result),list(source="s",license="l"))
  expect_equal(gets,"data/_manifest.json")
  # a partition and a file in it are covered by the manifest above them
  gets <- character()
  expect_equal(unclass(parquet_metadata("r2://bucket/data/year=2022")),list(source="s",license="m"))
  expect_equal(unclass(parquet_metadata("r2://bucket/data/year=2022/data_0.parquet")),list(source="s",license="m"))
  expect_equal(gets,c("data/year=2022/_manifest.json","data/_manifest.json",
                      "data/year=2022/data_0.parquet/_manifest.json","data/year=2022/_manifest.json","data/_manifest.json"))
  expect_length(footers,0)
  # the footers get read without a manifest, with one that predates the metadata, for a path the manifest does
  # not list, and with from_files = TRUE
  expect_equal(unclass(parquet_metadata("r2://bucket/plain/a.parquet")),list(source="f"))
  expect_equal(unclass(parquet_metadata("r2://bucket/old/a.parquet")),list(source="f"))
  expect_equal(unclass(parquet_metadata("r2://bucket/data/year=2023")),list(source="f"))
  expect_equal(unclass(parquet_metadata("r2://bucket/data",from_files=TRUE)),list(source="f"))
  expect_equal(footers,c("r2://bucket/plain/a.parquet","r2://bucket/old/a.parquet","r2://bucket/data/year=2023","r2://bucket/data"))
  # paths with and without a manifest get merged like files
  footers <- character()
  expect_warning(result <- parquet_metadata(c("r2://bucket/plain/a.parquet","r2://bucket/data/year=2022")),"disagree on the metadata entry source")
  expect_equal(unclass(result),list(source="s",license="m"))
  expect_equal(footers,"r2://bucket/plain/a.parquet")
  # a table reads the manifests of the paths it was given when the files it reads are remote, a table reading
  # copies from the cache the footers of the copies
  dir <- withr::local_tempdir()
  data_to_parquet(data.frame(a=1),file.path(dir,"local.parquet"),metadata=list(source="local"))
  tbl <- parquet_tbl(file.path(dir,"local.parquet"))
  expect_equal(unclass(parquet_metadata(tbl)),list(source="local"))
  attr(tbl,"r2parquet_source") <- "r2://bucket/data/year=2022"
  expect_equal(unclass(parquet_metadata(tbl)),list(source="local"))
  footers <- character()
  attr(tbl,"r2parquet_paths") <- "r2://bucket/data/year=2022/**/*.parquet"
  expect_equal(unclass(parquet_metadata(tbl)),list(source="s",license="m"))
  expect_length(footers,0)
  expect_equal(unclass(parquet_metadata(tbl,from_files=TRUE)),list(source="f"))
  attr(tbl,"r2parquet_source") <- "r2://bucket/plain/a.parquet"
  expect_equal(unclass(parquet_metadata(tbl)),list(source="f"))
  expect_equal(footers,rep("r2://bucket/data/year=2022/**/*.parquet",2))
  # collect_parquet attaches what parquet_metadata returns
  footers <- character()
  attr(tbl,"r2parquet_source") <- "r2://bucket/data/year=2022"
  expect_equal(attr(collect_parquet(tbl),"metadata"),list(source="s",license="m"))
  expect_length(footers,0)
})

test_that("remote metadata at URLs comes from the manifests, a denied request for one counts as none", {
  manifests <- list("https://data.example.com/census/data/_manifest.json"=
                      manifest_body(c("year=2021/data_0.parquet","year=2022/data_0.parquet"),
                                    metadata=list(list(source="s",license="l"),list(source="s",license="m"))))
  gets <- character()
  local_mocked_bindings(http_get=function(url){
                          gets <<- c(gets,url)
                          if (!is.null(manifests[[url]])) return(fake_response(200,manifests[[url]]))
                          # like S3 answers for missing objects without listing rights
                          fake_response(if (grepl("^https://data.example.com/census/",url)) 404 else 403)
                        },
                        duckdb_parquet_paths=function(con,path)path,
                        read_parquet_footers=function(con,path)stats::setNames(rep(list(list(source="f")),length(path)),path))
  expect_equal(unclass(parquet_metadata("https://data.example.com/census/data/year=2022/")),list(source="s",license="m"))
  expect_equal(gets,paste0("https://data.example.com/census/data/",c("year=2022/",""),"_manifest.json"))
  # without a manifest below the host the footers get read, a 403 along the way is not an error
  gets <- character()
  expect_equal(unclass(parquet_metadata("https://data.example.com/other/a.parquet")),list(source="f"))
  expect_equal(gets,paste0("https://data.example.com/",c("other/a.parquet/","other/",""),"_manifest.json"))
  # an unreadable manifest counts as one without metadata, with a warning
  manifests[["https://data.example.com/census/data/_manifest.json"]] <- "not json"
  expect_warning(result <- parquet_metadata("https://data.example.com/census/data/"),"is not a valid manifest")
  expect_equal(unclass(result),list(source="f"))
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

test_that("writing data read from parquet files with metadata warns unless metadata is given", {
  df <- data.frame(id=1:4,value=c(3,1,2,4))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(source="s",license="l",primary_key="id",columns=list(id="Identifier",value="Value")))
  copy <- withr::local_tempfile(fileext=".parquet")
  expect_warning(data_to_parquet(dplyr::filter(parquet_tbl(path),value>1),copy),
                 "metadata \\(source, license, primary_key, columns\\), which doesn't get written")
  expect_equal(unclass(parquet_metadata(copy)),list())
  # list() writes none on purpose
  expect_no_warning(data_to_parquet(parquet_tbl(path),copy,metadata=list()))
  expect_equal(unclass(parquet_metadata(copy)),list())
  # data without metadata in its source writes without a warning
  expect_no_warning(data_to_parquet(parquet_tbl(copy),path))
  expect_no_warning(data_to_parquet(df,path))
  # also for collected data
  data_to_parquet(df,path,metadata=list(source="s"))
  expect_warning(data_to_parquet(collect_parquet(parquet_tbl(path)),copy),"metadata \\(source\\)")
})

test_that("metadata of the source can be written again", {
  df <- data.frame(id=1:4,value=c(3,1,2,4))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(source="s",license="l",primary_key="id",columns=list(id="Identifier",value="Value")))
  source <- parquet_tbl(path)
  copy <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(source,copy,metadata=parquet_metadata(source))
  expect_equal(unclass(parquet_metadata(copy)),unclass(parquet_metadata(path)))
  # entries get changed with modifyList, column entries of dropped columns go
  changed <- dplyr::mutate(source,double=value*2)
  data_to_parquet(dplyr::select(changed,-value),copy,
                  metadata=modifyList(parquet_metadata(changed),list(source="t",license=NULL,columns=list(double="Doubled"))))
  expect_equal(unclass(parquet_metadata(copy)),
               list(source="t",primary_key="id",columns=list(id=list(label="Identifier"),double=list(label="Doubled"))))
  # unless written by hand
  expect_error(data_to_parquet(dplyr::select(source,id),copy,metadata=list(columns=list(value="Value"))),"Columns value")
  # the primary key gets checked
  doubled <- dplyr::union_all(source,source)
  expect_error(data_to_parquet(doubled,copy,metadata=parquet_metadata(doubled)),"does not identify rows")
  # collected data, given labels win over label attributes as always
  collected <- collect_parquet(source)
  attr(collected$value,"label") <- "New value"
  data_to_parquet(collected,copy,metadata=parquet_metadata(collected)[c("source","columns")])
  expect_equal(unclass(parquet_metadata(copy)),list(source="s",columns=list(id=list(label="Identifier"),value=list(label="Value"))))
})

test_that("parquet_sources lists the metadata of joined tables, also ones not assigned to a variable", {
  left <- withr::local_tempfile(fileext=".parquet")
  right <- withr::local_tempfile(fileext=".parquet")
  third <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data.frame(id=c("a","b"),value=1:2),left,metadata=list(attribution="Left",columns=list(id="Identifier")))
  data_to_parquet(data.frame(id=c("a","b"),count=3:4),right,metadata=list(attribution="Right",licence="OGL",columns=list(id="Other",count="Count")))
  data_to_parquet(data.frame(id="a"),third,metadata=list(attribution="Third"))
  tbl <- parquet_tbl(left)
  joined <- tbl |>
    dplyr::inner_join(parquet_tbl(right),by="id") |>
    dplyr::semi_join(parquet_tbl(third),by="id") |>
    dplyr::filter(value>0)
  sources <- parquet_sources(joined)
  expect_equal(sources$path,c(left,right,third))
  expect_equal(sources$attribution,c("Left","Right","Third"))
  expect_equal(sources$licence,c(NA,"OGL",NA))
  expect_equal(unclass(sources$metadata[[2]])$columns$count,list(label="Count"))
  result <- collect_parquet(joined)
  expect_equal(nrow(result),1)
  # the metadata stays the left-hand table's, the labels of the joined columns get added
  expect_equal(attr(result,"metadata")$attribution,"Left")
  expect_equal(attr(result$id,"label"),"Identifier")
  expect_equal(attr(result$count,"label"),"Count")
  expect_equal(parquet_sources(result),sources)
  # a table joined twice is one source, set operations count too
  self <- dplyr::left_join(tbl,tbl,by="id")
  expect_equal(parquet_sources(self)$path,left)
  combined <- dplyr::union_all(dplyr::select(tbl,id),dplyr::select(parquet_tbl(third),id))
  expect_equal(parquet_sources(combined)$attribution,c("Left","Third"))
  # tables not from parquet_tbl have no row, also on the left
  other <- dplyr::tbl(dbplyr::remote_con(tbl),dplyr::sql("SELECT 'a' AS id"))
  expect_equal(nrow(parquet_sources(other)),0)
  from_other <- collect_parquet(dplyr::left_join(other,parquet_tbl(right),by="id"))
  expect_null(attr(from_other,"metadata"))
  expect_equal(parquet_sources(from_other)$attribution,"Right")
  expect_equal(nrow(parquet_sources(data.frame(a=1))),0)
  expect_null(attr(collect_parquet(joined,attach_metadata=FALSE),"parquet_sources"))
  expect_error(parquet_sources(1),"lazy DuckDB table")
})

test_that("tables keep their sources when the session's record of them is reset, as by reloading the package", {
  root <- withr::local_tempdir()
  left <- file.path(root,"it's.parquet")
  right <- file.path(root,"right")
  data_to_parquet(data.frame(id=c("a","b"),value=1:2),left,metadata=list(attribution="Left"))
  data_to_parquet(data.frame(id=c("a","b"),part=c("x","y")),right,partition_by="part",metadata=list(attribution="Right"))
  tbl <- parquet_tbl(left)
  joined <- dplyr::inner_join(tbl,parquet_tbl(right),by="id")
  other <- file.path(root,"other.parquet")
  data_to_parquet(data.frame(id="c",value=3L),other,metadata=list())
  both <- parquet_tbl(c(left,other))
  expected <- parquet_sources(joined)
  scans <- r2parquet:::parquet_scans
  rm(list=ls(scans,all.names=TRUE),envir=scans)
  expect_equal(parquet_sources(joined)$path,c(left,right))
  expect_equal(parquet_sources_attribution(joined),"Left | Right")
  expect_equal(parquet_sources(joined)$metadata,expected$metadata)
  expect_equal(parquet_sources(both)$path,paste(c(left,other),collapse=", "))
  # other SQL is not a scan
  expect_equal(r2parquet:::scan_sql_paths("SELECT 'a' AS id"),character())
})

test_that("attributions that differ in their last word get combined", {
  expect_equal(combine_attributions(c("Statistics Canada Product ldb_000b21f_e","Statistics Canada Product 2021_92-151_X")),
               "Statistics Canada Product ldb_000b21f_e, 2021_92-151_X")
  # groups keep the order of their first member, repeats and missing attributions go
  expect_equal(combine_attributions(c("StatCan Table 1","City of Vancouver","StatCan Table 2","StatCan Table 1",NA," ")),
               "StatCan Table 1, 2 | City of Vancouver")
  # a shared first word is not enough, and single words don't match a prefix
  expect_equal(combine_attributions(c("Statistics Canada Product 1","Statistics Canada, Census"),sep="; "),
               "Statistics Canada Product 1; Statistics Canada, Census")
  expect_equal(combine_attributions(c("Statistics","Statistics Canada")),"Statistics | Statistics Canada")
  expect_null(combine_attributions(c(NA,"")))
  expect_null(combine_attributions(NULL))
  left <- withr::local_tempfile(fileext=".parquet")
  right <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data.frame(id="a",value=1),left,metadata=list(attribution="Statistics Canada Product A"))
  data_to_parquet(data.frame(id="a",count=2),right,metadata=list(attribution="Statistics Canada Product B"))
  joined <- dplyr::left_join(parquet_tbl(left),parquet_tbl(right),by="id")
  expect_equal(parquet_sources_attribution(joined),"Statistics Canada Product A, B")
  expect_equal(parquet_sources_attribution(collect_parquet(joined)),"Statistics Canada Product A, B")
  data_to_parquet(data.frame(id="a"),left,metadata=list(source="s"))
  expect_null(parquet_sources_attribution(parquet_tbl(left)))
})

test_that("writing joined data warns about the metadata of the joined tables", {
  left <- withr::local_tempfile(fileext=".parquet")
  right <- withr::local_tempfile(fileext=".parquet")
  plain <- withr::local_tempfile(fileext=".parquet")
  copy <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data.frame(id="a",value=1),left,metadata=list(source="s"))
  data_to_parquet(data.frame(id="a",count=2),right,metadata=list(attribution="r"))
  data_to_parquet(data.frame(id="a",other=3),plain,metadata=list())
  joined <- dplyr::left_join(parquet_tbl(left),parquet_tbl(right),by="id")
  # the paths are matched literally, Windows paths have backslashes
  joined_message <- paste0("Joined parquet tables have metadata too (",right,")")
  warning <- expect_warning(data_to_parquet(joined,copy),"metadata \\(source\\)")
  expect_match(conditionMessage(warning),joined_message,fixed=TRUE)
  expect_warning(data_to_parquet(collect_parquet(joined),copy),joined_message,fixed=TRUE)
  # only the joined table has metadata
  from_plain <- dplyr::left_join(parquet_tbl(plain),parquet_tbl(right),by="id")
  warning <- expect_warning(data_to_parquet(from_plain,copy),"^The data was read from parquet files with metadata, ")
  expect_match(conditionMessage(warning),joined_message,fixed=TRUE)
  expect_match(conditionMessage(warning),"metadata = list()",fixed=TRUE)
  expect_warning(data_to_parquet(collect_parquet(from_plain),copy),joined_message,fixed=TRUE)
  expect_no_warning(data_to_parquet(from_plain,copy,metadata=list()))
  # joined tables without metadata don't get mentioned
  expect_warning(data_to_parquet(dplyr::left_join(parquet_tbl(left),parquet_tbl(plain),by="id"),copy),"^(?!.*Joined)",perl=TRUE)
  expect_no_warning(data_to_parquet(dplyr::left_join(parquet_tbl(plain),parquet_tbl(plain),by="id"),copy))
})

test_that("labels in several languages round trip and get checked", {
  df <- data.frame(id=c("a","b"),name=c("Canada","Quebec"),name_fr=c("Canada","Québec"),rate_men=1:2)
  path <- withr::local_tempfile(fileext=".parquet")
  metadata <- list(language="en",columns=list(
    id=list(label="Identifier",label_fr="Identifiant"),
    name=list(label="Name",description="English name",label_fr="Nom",description_fr="Nom français"),
    name_fr=list(label_en="Name, French",label_fr="Nom, en français"),
    rate_men="Rate, men"))
  data_to_parquet(df,path,metadata=metadata)
  result <- parquet_metadata(path)
  expect_equal(result$language,"en")
  expect_equal(result$columns$id,list(label="Identifier",label_fr="Identifiant"))
  # a column without a plain label keeps just the suffixed ones, no language is privileged
  expect_equal(result$columns$name_fr,list(label_en="Name, French",label_fr="Nom, en français"))
  collected <- collect_parquet(parquet_tbl(path))
  # without a language the plain label, else the dataset language's, else none
  expect_equal(attr(collected$name,"label"),"Name")
  expect_equal(attr(collected$name_fr,"label"),"Name, French")
  # in French name_fr takes the place of name, with its own French label
  expect_equal(attr(collect_parquet(parquet_tbl(path),language="fr")$name,"label"),"Nom, en français")
  expect_error(data_to_parquet(df,path,metadata=list(columns=list(id=list(label=c("a","b"))))),"label has to be a single string")
  expect_error(data_to_parquet(df,path,metadata=list(columns=list(id=list(label="a",description_fr=1)))),"description_fr has to be a single string")
  expect_error(data_to_parquet(df,path,metadata=list(language="english")),'metadata\\$language has to be a language code')
  # printing shows one column per language, the dataset's language first
  lines <- format(result,width=100)
  expect_match(lines[grepl("^columns +en",lines)],"^columns +en +fr$")
  expect_match(lines[grepl("^  id ",lines)],"^  id +Identifier +Identifiant$")
  expect_match(lines[grepl("^  name ",lines)],"^  name +Name: English name +Nom: Nom français$")
})

test_that("metadata in several languages prints one column per language", {
  metadata <- r2parquet:::new_parquet_metadata(list(
    title="Census profile",title_fr="Profil du recensement",language="en",attribution="Statistics Canada",
    columns=list(id=list(label="Identifier",label_fr="Identifiant"),
                 name=list(label_en="Name",label_fr="Nom",description_en="English name",description_fr="Nom français",unit="text"),
                 value=list(label="Value"))))
  lines <- format(metadata,width=90)
  # top-level entries with variants consolidate into one row under a header of the languages
  expect_match(lines[2],"^ +en +fr$")
  expect_match(lines[3],"^title +Census profile +Profil du recensement$")
  expect_equal(lines[4:6],c("language     en","attribution  Statistics Canada","columns      <3 columns, see below>"))
  expect_match(lines[8],"^columns +en +fr$")
  expect_match(lines[9],"^  id +Identifier +Identifiant$")
  # label and description per language, other entries after the text in the dataset's language
  expect_match(lines[10],'^  name +Name: English name \\{"unit":"text"\\} +Nom: Nom français$')
  # a column without variants spans the width
  expect_equal(lines[11],"  value  Value")
  # the cells start at the same position in the header and the rows
  expect_equal(as.integer(regexpr("fr",lines[8])),as.integer(regexpr("Identifiant",lines[9])))
  expect_true(all(nchar(format(metadata,width=40),type="width")<=40))
  # without a language entry plain text gets its own column, first; top-level peers give the languages
  lines <- format(r2parquet:::new_parquet_metadata(list(title_en="Profile",title_fr="Profil",
    columns=list(id=list(label="Identifier",label_fr="Identifiant")))),width=80)
  expect_match(lines[2],"^ +en +fr$")
  expect_match(lines[3],"^title +Profile +Profil$")
  expect_match(lines[6],"^columns +plain +fr$")
  expect_match(lines[7],"^  id +Identifier +Identifiant$")
  # a three letter suffix next to its base is no language unless the dataset names it: licence_url stays an
  # entry of its own, next to the resolved licence
  resolved <- r2parquet:::new_parquet_metadata(list(licence="Open Licence",licence_url="https://example.org",title="Profile",language="en"))
  expect_equal(r2parquet:::print_languages(resolved),"en")
  lines <- format(resolved,width=80)
  expect_equal(lines[2:3],c("licence      Open Licence","licence_url  https://example.org"))
  expect_equal(r2parquet:::print_languages(list(title_eng="Profile",title_fra="Profil")),character(0))
  expect_equal(r2parquet:::print_languages(list(title_eng="Profile",title_fra="Profil",language="eng",columns=list(id=list(label_fra="Id")))),c("eng","fra"))
  # the resolver leaves licence_url alone as well
  stored <- list(licence="Open Licence",licence_fr="Licence ouverte",licence_url="https://example.org")
  expect_equal(r2parquet:::resolve_language(stored,"fr"),list(licence="Licence ouverte",licence_url="https://example.org",language="fr"))
})

test_that("collect_parquet picks the labels and columns of a language", {
  df <- data.frame(id=c("a","b"),name=c("Canada","Quebec"),name_fr=c("Canada","Québec"),name_de=c("Kanada","Quebec"),
                   rate=c(1,2),rate_men=c(3,4))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(language="en",columns=list(
    id=list(label="Identifier",label_fr="Identifiant"),name=list(label="Name",label_fr="Nom"),
    name_fr=list(label="Name, French",label_fr="Nom"),rate="Rate",rate_men=list(label="Rate, men",label_de="Rate, Männer"))))
  tbl <- parquet_tbl(path)
  # without a language the data comes as stored
  plain <- suppressMessages(collect_parquet(tbl))
  expect_equal(names(plain),names(df))
  expect_equal(attr(plain$id,"label"),"Identifier")
  # in French the French columns take the place of the plain ones, the other languages go
  french <- collect_parquet(tbl,language="fr")
  expect_equal(names(french),c("id","name","rate","rate_men"))
  expect_equal(as.character(french$name),c("Canada","Québec"))
  expect_equal(attr(french$id,"label"),"Identifiant")
  # the column that took the place of name has its own label
  expect_equal(attr(french$name,"label"),"Nom")
  # columns without a French label keep the plain one
  expect_equal(attr(french$rate,"label"),"Rate")
  # rate_men is no translation of rate, men is not a language of the dataset
  expect_equal(as.numeric(french$rate_men),c(3,4))
  # in the dataset's language the other languages' columns go and the plain columns stay
  english <- collect_parquet(tbl,language="en")
  expect_equal(names(english),c("id","name","rate","rate_men"))
  expect_equal(as.character(english$name),c("Canada","Quebec"))
  expect_equal(attr(english$name,"label"),"Name")
  # a language the data doesn't have gives the plain columns and labels
  spanish <- collect_parquet(tbl,language="es")
  expect_equal(names(spanish),c("id","name","rate","rate_men"))
  expect_equal(attr(spanish$id,"label"),"Identifier")
  # the selection happens in the query, columns selected away stay away
  expect_equal(names(collect_parquet(dplyr::select(tbl,id,name_fr),language="fr")),c("id","name_fr"))
  expect_equal(names(collect_parquet(dplyr::select(tbl,id,name),language="fr")),c("id","name"))
  # the option sets the language
  withr::with_options(list(r2parquet.language="FR"),{
    expect_equal(attr(collect_parquet(tbl)$id,"label"),"Identifiant")
  })
  expect_error(collect_parquet(tbl,language="french"),"language code")
  # the metadata attribute describes the result: in French, with the columns as collected
  expect_equal(attr(french,"metadata")$language,"fr")
  expect_equal(attr(french,"metadata")$columns$name$label,"Nom")
  expect_null(attr(french,"metadata")$columns$name_fr)
  expect_equal(unclass(parquet_metadata(french)),attr(french,"metadata"))
  # the same metadata without collecting; as stored by default
  expect_equal(unclass(parquet_metadata(tbl,language="fr")),attr(french,"metadata"))
  expect_equal(parquet_metadata(tbl)$language,"en")
  expect_equal(unclass(parquet_metadata(spanish)),unclass(parquet_metadata(tbl)))
})

test_that("metadata in several languages without a plain one asks for a language once", {
  df <- data.frame(id=c("a","b"),name_en=c("Canada","Quebec"),name_fr=c("Canada","Québec"),value=1:2)
  path <- withr::local_tempfile(fileext=".parquet")
  metadata <- list(title_en="Census profile",title_fr="Profil du recensement",
                   attribution_en="Statistics Canada",attribution_fr="Statistique Canada",
                   columns=list(id=list(label_en="Identifier",label_fr="Identifiant")))
  data_to_parquet(df,path,metadata=metadata)
  tbl <- parquet_tbl(path)
  withr::local_options(r2parquet.language=NULL)
  assign("language_noted",FALSE,envir=r2parquet:::parquet_cache)
  # nothing resolves without a language, and the message says how to set one, once
  expect_message(collected <- collect_parquet(tbl),'options\\(r2parquet.language = "en"\\).*once per session')
  expect_null(attr(collected$id,"label"))
  expect_equal(names(collected),c("id","name_en","name_fr","value"))
  expect_null(parquet_sources_attribution(collected))
  expect_no_message(collect_parquet(tbl))
  expect_no_message(parquet_sources_attribution(tbl))
  # the sources and attributions give the message too
  assign("language_noted",FALSE,envir=r2parquet:::parquet_cache)
  expect_message(expect_null(parquet_sources_attribution(tbl)),"r2parquet.language")
  assign("language_noted",FALSE,envir=r2parquet:::parquet_cache)
  expect_message(parquet_sources(collected),"r2parquet.language")
  # with a language, or the option, there is nothing to say
  assign("language_noted",FALSE,envir=r2parquet:::parquet_cache)
  expect_no_message(english <- collect_parquet(tbl,language="en"))
  expect_equal(attr(english$id,"label"),"Identifier")
  expect_equal(names(english),c("id","name","value"))
  expect_equal(parquet_sources_attribution(english),"Statistics Canada")
  withr::with_options(list(r2parquet.language="fr"),expect_no_message(expect_equal(parquet_sources_attribution(tbl),"Statistique Canada")))
  # parquet_metadata returns everything and stays quiet
  expect_no_message(parquet_metadata(tbl))
  # a dataset with a language of its own has plain text to fall back on
  other <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,other,metadata=c(metadata,list(language="en",attribution="Statistics Canada")))
  expect_no_message(collect_parquet(parquet_tbl(other)))
  expect_false(isTRUE(get("language_noted",envir=r2parquet:::parquet_cache)))
})

test_that("metadata collected in a language describes the result in that language", {
  df <- data.frame(id=c("a","b"),name=c("Canada","Quebec"),name_fr=c("Canada","Québec"),value=1:2)
  path <- withr::local_tempfile(fileext=".parquet")
  metadata <- list(title="Census profile",title_fr="Profil du recensement",language="en",
                   attribution="Statistics Canada",attribution_fr="Statistique Canada",notes="Only in English",
                   columns=list(id=list(label="Identifier",label_fr="Identifiant",description="The id"),
                                name=list(label="Name",label_fr="Nom",unit="text")))
  data_to_parquet(df,path,metadata=metadata)
  tbl <- parquet_tbl(path)
  french <- unclass(parquet_metadata(tbl,language="fr"))
  # entries in the language take the place of the plain ones, in their place, the others are left out
  expect_equal(names(french),c("title","language","attribution","notes","columns"))
  expect_equal(french$title,"Profil du recensement")
  expect_equal(french$attribution,"Statistique Canada")
  expect_equal(french$notes,"Only in English")
  expect_equal(french$language,"fr")
  # column entries resolve the same way, with the other entries kept, and follow the columns collected
  expect_equal(french$columns,list(id=list(label="Identifiant",description="The id"),name=list(label="Nom",unit="text")))
  # the dataset's language gives the plain entries without the variants
  english <- unclass(parquet_metadata(tbl,language="en"))
  expect_equal(english$title,"Census profile")
  expect_equal(english$columns$name,list(label="Name",unit="text"))
  expect_equal(english$language,"en")
  # a language the dataset doesn't have leaves everything as stored
  expect_equal(unclass(parquet_metadata(tbl,language="es")),unclass(parquet_metadata(tbl)))
  # collect_parquet attaches the resolved metadata, and the sources and attribution follow
  collected <- collect_parquet(tbl,language="fr")
  expect_equal(attr(collected,"metadata"),french)
  expect_equal(attr(collected$name,"label"),"Nom")
  expect_equal(parquet_sources(collected)$attribution,"Statistique Canada")
  expect_equal(parquet_sources_attribution(collected),"Statistique Canada")
  expect_equal(parquet_sources_attribution(tbl,language="fr"),"Statistique Canada")
  expect_equal(parquet_sources_attribution(tbl),"Statistics Canada")
  withr::with_options(list(r2parquet.language="fr"),{
    expect_equal(parquet_sources_attribution(tbl),"Statistique Canada")
    # parquet_metadata ignores the option, it returns what the files hold
    expect_equal(parquet_metadata(tbl)$language,"en")
  })
  # writing the collected data with its metadata gives a French dataset
  other <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(collected,other,metadata=parquet_metadata(collected))
  expect_equal(parquet_metadata(other)$title,"Profil du recensement")
  expect_equal(parquet_metadata(other)$language,"fr")
  expect_equal(attr(collect_parquet(parquet_tbl(other))$name,"label"),"Nom")
  # the resolver on a plain list, including a title in several languages without a plain one
  resolved <- r2parquet:::resolve_language(list(title_en="Profile",title_fr="Profil"),"fr")
  expect_equal(resolved,list(title="Profil",language="fr"))
  expect_equal(r2parquet:::resolve_language(list(title="T"),NULL),list(title="T"))
})

test_that("datasets with only suffixed labels and columns privilege no language", {
  df <- data.frame(id=c("a","b"),name_en=c("Canada","Quebec"),name_fr=c("Canada","Québec"),name_de=c("Kanada","Quebec"),only_fr=c("x","y"),value=1:2)
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(columns=list(
    id=list(label_en="Identifier",label_fr="Identifiant"),name_en=list(label_en="Name",label_fr="Nom"),
    name_fr=list(label_en="Name, French",label_fr="Nom, en français"),only_fr=list(label_fr="Seulement"),value="Value")))
  expect_null(parquet_metadata(path)$language)
  tbl <- parquet_tbl(path)
  # as stored, without a language: labels only where there is a plain one
  plain <- suppressMessages(collect_parquet(tbl))
  expect_equal(names(plain),names(df))
  expect_null(attr(plain$id,"label"))
  expect_equal(attr(plain$value,"label"),"Value")
  # the peers name_en and name_fr stand in for the missing plain column, in its place; de is no language of
  # the dataset, so name_de is no peer
  french <- collect_parquet(tbl,language="fr")
  expect_equal(names(french),c("id","name","name_de","only_fr","value"))
  expect_equal(as.character(french$name),c("Canada","Québec"))
  expect_equal(attr(french$name,"label"),"Nom, en français")
  expect_equal(attr(french$id,"label"),"Identifiant")
  # a lone suffixed column without a plain one or a peer is left alone, with its label in the language
  expect_equal(attr(french$only_fr,"label"),"Seulement")
  english <- collect_parquet(tbl,language="en")
  expect_equal(names(english),c("id","name","name_de","only_fr","value"))
  expect_equal(as.character(english$name),c("Canada","Quebec"))
  expect_equal(attr(english$name,"label"),"Name")
  # asking for German makes name_de a peer, which then stands in for name
  german <- collect_parquet(tbl,language="de")
  expect_equal(names(german),c("id","name","only_fr","value"))
  expect_equal(as.character(german$name),c("Kanada","Quebec"))
  # a language none of the peers have keeps them as stored, there is no plain column to fall back on
  expect_equal(names(collect_parquet(tbl,language="es")),names(df))
  # a dataset language without plain labels labels the plain read in that language
  data_to_parquet(df,path,metadata=list(language="fr",columns=list(id=list(label_en="Identifier",label_fr="Identifiant"))))
  expect_equal(attr(collect_parquet(parquet_tbl(path))$id,"label"),"Identifiant")
  expect_equal(attr(collect_parquet(parquet_tbl(path),language="en")$id,"label"),"Identifier")
  expect_equal(attr(collect_parquet(parquet_tbl(path),language="es")$id,"label"),"Identifiant")
})

test_that("language columns of joined tables get picked too", {
  left <- withr::local_tempfile(fileext=".parquet")
  right <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data.frame(id=c("a","b"),value=1:2),left,metadata=list(columns=list(id="Identifier")))
  data_to_parquet(data.frame(id=c("a","b"),name=c("One","Two"),name_fr=c("Un","Deux")),right,
                  metadata=list(language="en",columns=list(name=list(label="Name",label_fr="Nom"))))
  joined <- dplyr::left_join(parquet_tbl(left),parquet_tbl(right),by="id")
  result <- collect_parquet(joined,language="fr")
  expect_equal(names(result),c("id","value","name"))
  expect_equal(as.character(result$name),c("Un","Deux"))
  expect_equal(attr(result$name,"label"),"Nom")
  expect_equal(attr(result$id,"label"),"Identifier")
})

test_that("select_language picks the columns of a language on the lazy table", {
  df <- data.frame(id=c("a","b"),name_en=c("One","Two"),name_fr=c("Un","Deux"),rate_men=c(0.1,0.2),value=1:2)
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(title_en="Numbers",title_fr="Nombres",columns=list(
    name_en=list(label_en="Name",label_fr="Nom"),name_fr=list(label_en="Name, French",label_fr="Nom"),
    value=list(label_en="Value",label_fr="Valeur"))))
  # without a language the table has the columns as stored
  withr::local_options(r2parquet.language=NULL)
  tbl <- parquet_tbl(path)
  expect_equal(colnames(tbl),names(df))
  expect_null(attr(tbl,"r2parquet_selection"))
  expect_identical(select_language(tbl,NULL),tbl)
  expect_error(select_language(tbl,"english"),"language code")
  expect_error(select_language(df,"en"),"lazy table")
  # the column in the language stands in for the plain one, the other language is left out before reading
  french <- select_language(tbl,"fr")
  expect_equal(colnames(french),c("id","name","rate_men","value"))
  expect_match(dbplyr::remote_query(french),"name_fr")
  expect_no_match(dbplyr::remote_query(french),"name_en")
  expect_equal(attr(french,"r2parquet_selection"),c(id="id",name="name_fr",rate_men="rate_men",value="value"))
  expect_equal(attr(french,"r2parquet_language"),"fr")
  # the table keeps what parquet_tbl recorded, so listing and metadata still work
  expect_equal(attr(french,"r2parquet_paths"),attr(tbl,"r2parquet_paths"))
  expect_equal(list_parquet(french)$path,path)
  # collecting labels the column with the label of the stored column it came from and describes the result
  # in the language the columns were selected in, without the option set
  collected <- collect_parquet(french)
  expect_equal(names(collected),c("id","name","rate_men","value"))
  expect_equal(as.character(collected$name),c("Un","Deux"))
  expect_equal(attr(collected$name,"label"),"Nom")
  expect_equal(attr(collected$value,"label"),"Valeur")
  metadata <- parquet_metadata(collected)
  expect_equal(metadata$title,"Nombres")
  expect_equal(metadata$language,"fr")
  expect_equal(names(metadata$columns),c("name","value"))
  expect_equal(parquet_sources(collected)$title,"Nombres")
  # the metadata as stored stays available from the table, and resolves through the selection
  expect_equal(unclass(parquet_metadata(french))$title_en,"Numbers")
  expect_equal(names(parquet_metadata(french,language="fr")$columns),c("name","value"))
  expect_equal(parquet_metadata(french,language="fr")$columns$name$label,"Nom")
  # dplyr verbs in between keep the selection; the entry of a column selected away stays, as always, the
  # other language's version of a selected column doesn't
  subset <- dplyr::filter(french,value==1L) |> dplyr::select(id,name)
  collected <- collect_parquet(subset)
  expect_equal(as.character(collected$name),"Un")
  expect_equal(attr(collected$name,"label"),"Nom")
  expect_equal(names(parquet_metadata(collected)$columns),c("name","value"))
  # another language asked for when collecting describes the data in that language
  english_labels <- collect_parquet(french,language="en")
  expect_equal(as.character(english_labels$name),c("Un","Deux"))
  expect_equal(attr(english_labels$name,"label"),"Name, French")
  # selecting again in another language has nothing left to swap and keeps the columns
  expect_equal(colnames(select_language(french,"en")),colnames(french))
  # a table without a column in the language is returned as it is, with the language recorded
  data_to_parquet(data.frame(id=c("a","b"),value=1:2),path,metadata=list(columns=list(value=list(label_en="Value",label_fr="Valeur"))))
  tbl <- parquet_tbl(path)
  expect_equal(colnames(select_language(tbl,"fr")),c("id","value"))
  expect_equal(attr(collect_parquet(select_language(tbl,"fr"))$value,"label"),"Valeur")
})

test_that("parquet_tbl selects the columns of the language set", {
  df <- data.frame(id=c("a","b"),name_en=c("One","Two"),name_fr=c("Un","Deux"),value=1:2)
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(columns=list(name_en=list(label_en="Name",label_fr="Nom"),name_fr=list(label_en="Name",label_fr="Nom"))))
  expect_error(parquet_tbl(path,language="english"),"language code")
  # the argument
  tbl <- parquet_tbl(path,language="en")
  expect_equal(colnames(tbl),c("id","name","value"))
  expect_equal(attr(tbl,"r2parquet_selection"),c(id="id",name="name_en",value="value"))
  expect_equal(attr(collect_parquet(tbl)$name,"label"),"Name")
  expect_equal(as.character(collect_parquet(tbl)$name),c("One","Two"))
  # the option, and language = NULL for the columns as stored
  withr::local_options(r2parquet.language="fr")
  tbl <- parquet_tbl(path)
  expect_equal(colnames(tbl),c("id","name","value"))
  expect_equal(as.character(collect_parquet(tbl)$name),c("Un","Deux"))
  expect_equal(attr(collect_parquet(tbl)$name,"label"),"Nom")
  expect_equal(colnames(parquet_tbl(path,language=NULL)),names(df))
  # a table read in another language than the option is described in its own language unless one is passed
  english <- parquet_tbl(path,language="en")
  expect_equal(attr(collect_parquet(english)$name,"label"),"Name")
  expect_equal(parquet_metadata(collect_parquet(english))$language,"en")
  expect_equal(parquet_sources(english)$language,"en")
  expect_equal(parquet_sources(collect_parquet(english))$language,"en")
  expect_equal(attr(collect_parquet(english,language="fr")$name,"label"),"Nom")
  expect_null(attr(collect_parquet(english,language=NULL)$name,"label"))
  expect_equal(parquet_sources(english,language="fr")$language,"fr")
  expect_equal(unclass(parquet_metadata(english))$title_en,NULL)
  # the metadata gets read once per table: the registry keeps it, and a new table on the path reads afresh
  scan <- get(as.character(parquet_tbl(path,language=NULL)$lazy_query$x),envir=r2parquet:::parquet_scans)
  expect_null(scan$metadata)
  tbl <- parquet_tbl(path)
  scan <- get(r2parquet:::table_scans(tbl)[[1]]$key,envir=r2parquet:::parquet_scans)
  expect_equal(names(scan$metadata$columns),c("name_en","name_fr"))
  data_to_parquet(df,path,metadata=list(title="Changed",columns=list(name_en=list(label_en="Name",label_fr="Nom"),name_fr=list(label_en="Name",label_fr="Nom"))))
  expect_null(parquet_metadata(tbl)$title)
  expect_equal(parquet_metadata(tbl,from_files=TRUE)$title,"Changed")
  expect_equal(parquet_metadata(parquet_tbl(path))$title,"Changed")
})

test_that("select_language works on joins of tables read without a language", {
  left <- withr::local_tempfile(fileext=".parquet")
  right <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(data.frame(id=c("a","b"),label_en=c("A","B"),label_fr=c("A (fr)","B (fr)")),left,
                  metadata=list(columns=list(label_en=list(label_en="Label",label_fr="Étiquette"),label_fr=list(label_en="Label",label_fr="Étiquette"))))
  data_to_parquet(data.frame(id=c("a","b"),name=c("One","Two"),name_fr=c("Un","Deux")),right,
                  metadata=list(language="en",columns=list(name=list(label="Name",label_fr="Nom"))))
  withr::local_options(r2parquet.language=NULL)
  joined <- dplyr::left_join(parquet_tbl(left),parquet_tbl(right),by="id")
  expect_equal(colnames(joined),c("id","label_en","label_fr","name","name_fr"))
  french <- select_language(joined,"fr")
  expect_equal(colnames(french),c("id","label","name"))
  collected <- collect_parquet(french)
  expect_equal(as.character(collected$label),c("A (fr)","B (fr)"))
  expect_equal(as.character(collected$name),c("Un","Deux"))
  expect_equal(attr(collected$label,"label"),"Étiquette")
  expect_equal(attr(collected$name,"label"),"Nom")
  expect_equal(parquet_sources(french)$language,c("fr","fr"))
  # tables read in a language join on their plain names
  withr::local_options(r2parquet.language="fr")
  joined <- dplyr::left_join(parquet_tbl(left),parquet_tbl(right),by="id")
  expect_equal(colnames(joined),c("id","label","name"))
  collected <- collect_parquet(joined)
  expect_equal(as.character(collected$name),c("Un","Deux"))
  expect_equal(attr(collected$label,"label"),"Étiquette")
  expect_equal(attr(collected$name,"label"),"Nom")
})

test_that("collecting every column of a remote table suggests a select, once per session", {
  skip_if_no_duckdb_extensions()
  root <- withr::local_tempdir()
  df <- data.frame(id=1:3,name=c("a","b","c"),value=c(1.5,2.5,3.5))
  data_to_parquet(df,file.path(root,"data.parquet"))
  base <- local_http_server(root)
  reset <- function()assign("columns_noted",FALSE,envir=r2parquet:::parquet_cache)
  reset()
  withr::defer(reset())
  # a local table gives no message, nor a remote one with columns selected away or summarised
  expect_silent(collect_parquet(parquet_tbl(file.path(root,"data.parquet"))))
  tbl <- parquet_tbl(paste0(base,"/data.parquet"))
  expect_silent(collect_parquet(dplyr::select(tbl,id,value)))
  expect_silent(collect_parquet(dplyr::summarise(tbl,n=dplyr::n())))
  expect_silent(collect_parquet(dplyr::count(dplyr::left_join(dplyr::select(tbl,id,name),dplyr::select(tbl,id,value),by="id"))))
  # with every column still there, the message gives the select() of the columns to prune, also after a
  # filter or mutate, and only once per session
  expect_message(collect_parquet(dplyr::filter(tbl,id>1L)),"select\\(id, name, value\\)")
  expect_silent(collect_parquet(dplyr::mutate(tbl,double=value*2)))
  reset()
  expect_message(collect_parquet(dplyr::mutate(tbl,double=value*2)),"select\\(id, name, value, double\\)")
  reset()
  # a joined table keeps the message when all columns of a remote table come along
  expect_message(collect_parquet(dplyr::left_join(tbl,dplyr::select(tbl,id,other=value),by="id")),"all 4 columns of http")
})
