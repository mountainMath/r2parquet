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

skip_if_no_s3 <- function(){
  skip_on_cran()
  if (Sys.getenv("S3_TEST_BUCKET")=="" || Sys.getenv("AWS_ACCESS_KEY_ID")=="" || Sys.getenv("AWS_SECRET_ACCESS_KEY")=="") {
    skip("S3_TEST_BUCKET and AWS credentials not set")
  }
}

# Amazon S3 applies bucket policy changes with a delay, anonymous GET requests intermittently get 403 for a few
# minutes, so wait for a run of successful requests to a public file; HEAD requests succeed earlier, so the probe
# is a GET of the first byte
wait_until_public <- function(url,successes=60,timeout=900){
  run <- 0
  start <- Sys.time()
  while (run<successes) {
    if (difftime(Sys.time(),start,units="secs")>timeout) stop(paste0(url," did not become public within ",timeout," seconds."))
    run <- if (httr::status_code(httr::GET(url,httr::add_headers(Range="bytes=0-0")))<300) run+1 else 0
    if (run<successes) Sys.sleep(1)
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

# the tests describe what collecting without a language gives; a user's .Rprofile may set the option
options(r2parquet.language=NULL)

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
