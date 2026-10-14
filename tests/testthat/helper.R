# North Carolina counties from sf, NAD27 multipolygons
nc_data <- function(){
  sf::read_sf(system.file("shape/nc.shp",package="sf"),quiet=TRUE)
}

# the spatial and httpfs DuckDB extensions get downloaded on first use
skip_if_no_duckdb_extensions <- function(){
  skip_on_cran()
  skip_if_offline()
}

# R2 tests need credentials and a bucket that the tests can write to
skip_if_no_r2 <- function(){
  skip_on_cran()
  if (Sys.getenv("R2_TEST_BUCKET")=="" || Sys.getenv("R2_ACCOUNT_ID")=="" ||
      Sys.getenv("R2_ACCESS_KEY_ID")=="" || Sys.getenv("R2_SECRET_ACCESS_KEY")=="") {
    skip("R2_TEST_BUCKET and R2 credentials not set")
  }
}

# a temporary cache directory with forward slashes, as cache_directory() returns them, so paths compare on Windows
local_cache_dir <- function(envir=parent.frame()){
  gsub("\\\\","/",withr::local_tempdir(.local_envir=envir))
}

# response objects as returned by httr, with xml text or a list that gets encoded as json, and further
# response headers like etag, named in lower case as httr looks them up
fake_response <- function(status,content="",url="https://example.com",headers=list()){
  type <- if (is.character(content)) "application/xml" else "application/json"
  if (!is.character(content)) content <- jsonlite::toJSON(content,auto_unbox=TRUE)
  structure(list(url=url,status_code=status,content=charToRaw(content),headers=c(list("Content-Type"=type),headers)),
            class="response")
}

# Cloudflare API response with a result, or with errors for a failed request
cloudflare_response <- function(result=NULL,errors=NULL,status=if (is.null(errors)) 200L else 400L){
  fake_response(status,list(success=is.null(errors),errors=if (is.null(errors)) list() else errors,result=result))
}

# a stand-in for parquet_footers, since the parquet files in mocked uploads are not real: the metadata per file,
# looked up by path or basename, and the paths asked for recorded in the calls argument if it is an environment
fake_footers <- function(footers=list(),calls=NULL){
  function(path){
    if (is.environment(calls)) calls$paths <- c(calls$paths,path)
    stats::setNames(lapply(path,function(p){
      hit <- footers[[p]]
      if (is.null(hit)) hit <- footers[[basename(p)]]
      if (is.null(hit)) list() else hit
    }),path)
  }
}

# the text of a manifest listing files, to serve from mocked GETs
manifest_body <- function(path,etag="e",modified=as.POSIXct("2026-10-10 12:00:00",tz="UTC"),metadata=NULL){
  as.character(manifest_json(manifest_files(path,1,etag,modified,metadata)))
}

# a mock of the signed requests over a bucket held in the variables objects (the listing), bodies (manifest texts,
# from where GETs serve them) and puts of the test environment env: PUTs are recorded in puts as the paths listed
# by a manifest, or "data" for other files, GETs in gets
manifest_bucket <- function(env){
  function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
    if (verb=="PUT") {
      text <- rawToChar(body)
      env$puts[[r2_path]] <- if (headers[["content-type"]]=="application/json") jsonlite::fromJSON(text)$files$path else "data"
      env$bodies[[r2_path]] <- text
      if (!(r2_path %in% env$objects$key)) env$objects <- rbind(env$objects,data.frame(key=r2_path,size=1,modified=Sys.time(),etag="e"))
    }
    if (verb=="GET") env$gets <- c(env$gets,r2_path)
    fake_response(if (verb=="GET" && !(r2_path %in% env$objects$key)) 404 else 200,env$bodies[[r2_path]] %||% "")
  }
}
