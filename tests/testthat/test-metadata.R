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

test_that("lazy tables get written with metadata", {
  df <- data.frame(id=1:3,value=c(3,1,2))
  path <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(df,path,metadata=list(source="s",columns=list(value="Value")))
  path2 <- withr::local_tempfile(fileext=".parquet")
  data_to_parquet(dplyr::filter(parquet_tbl(path),value>1),path2,metadata=list(source="s2"))
  expect_equal(parquet_metadata(path2),list(source="s2"))
  expect_error(data_to_parquet(parquet_tbl(path),path2,metadata=list(columns=list(other="x"))),"Columns other")
})
