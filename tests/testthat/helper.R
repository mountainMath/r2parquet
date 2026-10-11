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

# response objects as returned by httr, with xml text or a list that gets encoded as json
fake_response <- function(status,content="",url="https://example.com"){
  type <- if (is.character(content)) "application/xml" else "application/json"
  if (!is.character(content)) content <- jsonlite::toJSON(content,auto_unbox=TRUE)
  structure(list(url=url,status_code=status,content=charToRaw(content),headers=list("Content-Type"=type)),
            class="response")
}

# Cloudflare API response with a result, or with errors for a failed request
cloudflare_response <- function(result=NULL,errors=NULL,status=if (is.null(errors)) 200L else 400L){
  fake_response(status,list(success=is.null(errors),errors=if (is.null(errors)) list() else errors,result=result))
}
