# Cloudflare R2 storage for parquet files
# This file contains convenience scripts to upload
# data to R2 into (partitioned) parquet files for
# private or public access

# session cache for the duckdb connection
r2_cache <- new.env(parent=emptyenv())

# read R2 credentials from environment variables
r2_credentials <- function(env_vars){
  credentials <- Sys.getenv(env_vars,names=TRUE)
  missing_vars <- env_vars[credentials==""]
  if (length(missing_vars)>0) {
    stop(paste0("Missing environment variables for R2 access: ",paste0(missing_vars,collapse = ", "),"."))
  }
  credentials
}

# endpoint of the S3 compatible API
r2_endpoint <- function(){
  paste0("https://",r2_credentials("R2_ACCOUNT_ID")[["R2_ACCOUNT_ID"]],".r2.cloudflarestorage.com")
}

# R2 filesystem via the S3 compatible API
r2_filesystem <- function(){
  credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
  arrow::S3FileSystem$create(access_key=credentials[["R2_ACCESS_KEY_ID"]],
                             secret_key=credentials[["R2_SECRET_ACCESS_KEY"]],
                             endpoint_override=r2_endpoint(),
                             region="auto")
}

# signed request for an object via the S3 compatible API, for requests without body
r2_object_request <- function(verb,r2_bucket,r2_path,datetime=format(Sys.time(),"%Y%m%dT%H%M%SZ",tz="UTC")){
  credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
  endpoint <- r2_endpoint()
  path <- paste0("/",r2_bucket,"/",
                 paste0(vapply(strsplit(r2_path,"/")[[1]],utils::URLencode,"",reserved=TRUE,repeated=TRUE),collapse="/"))
  headers <- list(host=sub("^https?://","",endpoint),
                  "x-amz-content-sha256"=digest::digest("",algo="sha256",serialize=FALSE),
                  "x-amz-date"=datetime)
  signature <- aws.signature::signature_v4_auth(datetime=datetime,region="auto",service="s3",
                                                verb=verb,action=path,
                                                canonical_headers=headers,request_body="",
                                                key=credentials[["R2_ACCESS_KEY_ID"]],
                                                secret=credentials[["R2_SECRET_ACCESS_KEY"]],
                                                force_credentials=TRUE)
  httr::VERB(verb,paste0(endpoint,path),
             httr::add_headers("x-amz-content-sha256"=headers[["x-amz-content-sha256"]],
                               "x-amz-date"=datetime,
                               Authorization=paste0("AWS4-HMAC-SHA256 Credential=",signature$Credential,
                                                    ", SignedHeaders=",signature$SignedHeaders,
                                                    ", Signature=",signature$Signature)))
}

# delete object via the S3 compatible API
# the arrow filesystem is not used for this as it leaves empty directory marker objects behind when deleting files
r2_delete_object <- function(r2_bucket,r2_path){
  response <- r2_object_request("DELETE",r2_bucket,r2_path)
  if (httr::status_code(response)>=400) {
    stop(paste0("Removing ",r2_path," from R2 bucket ",r2_bucket," failed with status ",httr::status_code(response),". ",
                httr::content(response,"text",encoding="UTF-8")))
  }
}

# check if there is an object, as opposed to a directory, at the path
r2_object_exists <- function(r2_bucket,r2_path){
  status <- httr::status_code(r2_object_request("HEAD",r2_bucket,r2_path))
  if (status>=400 && status!=404) {
    stop(paste0("Checking ",r2_path," in R2 bucket ",r2_bucket," failed with status ",status,"."))
  }
  status<400
}

# call the Cloudflare API
cloudflare_api <- function(verb,path,body=NULL,query=NULL){
  credentials <- r2_credentials("R2_API_TOKEN")
  httr::VERB(verb,paste0("https://api.cloudflare.com/client/v4",path),
             httr::add_headers(Authorization=paste0("Bearer ",credentials[["R2_API_TOKEN"]])),
             body=body,query=query,encode="json")
}

# call the Cloudflare API for R2 buckets, needed for bucket settings not available via the S3 compatible API
r2_api <- function(verb,path="",body=NULL){
  credentials <- r2_credentials("R2_ACCOUNT_ID")
  cloudflare_api(verb,paste0("/accounts/",credentials[["R2_ACCOUNT_ID"]],"/r2/buckets",path),body=body)
}

# find the active Cloudflare zone in the R2 account that a domain belongs to
r2_zone_for_domain <- function(domain){
  account_id <- r2_credentials("R2_ACCOUNT_ID")[["R2_ACCOUNT_ID"]]
  labels <- strsplit(domain,".",fixed=TRUE)[[1]]
  candidates <- vapply(seq_len(max(length(labels)-1,1)),function(i)paste0(labels[i:length(labels)],collapse="."),"")
  for (candidate in candidates) {
    response <- cloudflare_api("GET","/zones",query=list(name=candidate,account.id=account_id))
    r2_stop_for_status(response)
    zones <- httr::content(response)$result
    if (length(zones)>0) {
      zone <- zones[[1]]
      if (zone$status!="active") {
        stop(paste0("Cloudflare zone ",zone$name," is not active yet (status ",zone$status,"), ",
                    "check that the domain uses the Cloudflare nameservers."))
      }
      return(zone)
    }
  }
  stop(paste0("No Cloudflare zone found for ",domain,". The domain has to be added to the Cloudflare account ",
              "of the R2 bucket, and the R2 API token needs Zone Read permission for it."))
}

# connect a custom domain to the bucket for public read access, or enable it if it is connected but disabled
r2_connect_custom_domain <- function(r2_bucket,domain){
  response <- r2_api("GET",paste0("/",r2_bucket,"/domains/custom"))
  r2_stop_for_status(response)
  existing <- Filter(function(d)d$domain==domain,httr::content(response)$result$domains)
  if (length(existing)>0 && isTRUE(existing[[1]]$enabled)) {
    message(paste0("Custom domain ",domain," is already connected to R2 bucket ",r2_bucket,"."))
  } else if (length(existing)>0) {
    response <- r2_api("PUT",paste0("/",r2_bucket,"/domains/custom/",domain),body=list(enabled=TRUE))
    r2_stop_for_status(response)
    message(paste0("Enabled custom domain ",domain," for R2 bucket ",r2_bucket,"."))
  } else {
    zone <- r2_zone_for_domain(domain)
    response <- r2_api("POST",paste0("/",r2_bucket,"/domains/custom"),
                       body=list(domain=domain,zoneId=zone$id,enabled=TRUE))
    r2_stop_for_status(response)
    message(paste0("Connected custom domain ",domain," to R2 bucket ",r2_bucket,". ",
                   "It can take a few minutes until the certificate is issued and the domain is active."))
  }
}

# stop with the error messages returned by the Cloudflare API
r2_stop_for_status <- function(response){
  if (httr::status_code(response)>=400) {
    errors <- tryCatch(httr::content(response)$errors,error=function(e)NULL)
    error_messages <- unlist(lapply(errors,function(e)paste0(e$message," (code ",e$code,")")))
    stop(paste0("Cloudflare API request failed with status ",httr::status_code(response),". ",
                paste0(error_messages,collapse = ", ")))
  }
}

# purge objects served via custom domains of the bucket from the Cloudflare cache
r2_purge_cache <- function(r2_bucket,r2_paths){
  response <- r2_api("GET",paste0("/",r2_bucket,"/domains/custom"))
  r2_stop_for_status(response)
  domains <- Filter(function(d)isTRUE(d$enabled),httr::content(response)$result$domains)
  for (domain in domains) {
    urls <- paste0("https://",domain$domain,"/",vapply(r2_paths,utils::URLencode,"",USE.NAMES=FALSE))
    # the purge API accepts a limited number of urls per request
    for (batch in split(urls,ceiling(seq_along(urls)/30))) {
      response <- cloudflare_api("POST",paste0("/zones/",domain$zoneId,"/purge_cache"),body=list(files=as.list(batch)))
      r2_stop_for_status(response)
    }
  }
}

# purge cache after writing or removing objects, failure to purge only warrants a warning
# as the objects have already been changed at this point
r2_purge_cache_after_change <- function(r2_bucket,r2_paths){
  if (Sys.getenv("R2_API_TOKEN")=="") return(invisible())
  tryCatch(r2_purge_cache(r2_bucket,r2_paths),
           error=function(e)warning(paste0("Purging the Cloudflare cache for R2 bucket ",r2_bucket," failed, ",
                                           "cached copies of the changed files may be served until they expire. ",
                                           conditionMessage(e)),call.=FALSE))
}

# stream local file to arrow filesystem in chunks, large files get uploaded as multipart uploads
file_to_filesystem <- function(path,filesystem,destination,chunk_size=8*1024^2){
  input <- file(path,"rb")
  on.exit(close(input))
  output <- filesystem$OpenOutputStream(destination)
  while (length(chunk <- readBin(input,raw(),chunk_size))>0) {
    output$write(chunk)
  }
  output$close()
}

#' create Cloudflare R2 bucket with private or public access
#'
#' @description
#' Creates the bucket if it does not exist yet. Buckets are private by default. Public read access is granted
#' either by connecting a custom domain to the bucket, or by enabling the `r2.dev` public development URL
#' of the bucket. Cloudflare rate limits access via `r2.dev`, for heavier use connect a custom domain.
#' Public access can also be enabled on existing buckets, but this function never turns off public access.
#'
#' Expects the R2 account id and the value of an R2 API token with admin read and write permissions
#' to be available as `R2_ACCOUNT_ID` and `R2_API_TOKEN` environment variables. For connecting a custom
#' domain the token also needs Zone Read permission for the domain's zone, which has to be an active zone
#' in the same Cloudflare account. With Zone Cache Purge permission as well, `parquet_to_r2` and
#' `remove_parquet_from_r2` purge changed files from the cache. Each custom domain can only serve one bucket, use subdomains like
#' `data.example.com` to serve several buckets from one zone.
#'
#' @param r2_bucket R2 bucket name
#' @param public if `TRUE`, enable public read access to the bucket via the `r2.dev` URL, default is `FALSE`.
#' Ignored if `custom_domain` is set.
#' @param custom_domain optional domain, like `data.example.com`, to connect to the bucket for public read access.
#' The `r2.dev` URL is not enabled in this case.
#' @return (invisibly) the public base url of the bucket if it is publicly accessible, otherwise `NULL`
#' @export
create_r2_bucket <- function(r2_bucket,public=FALSE,custom_domain=NULL) {
  response <- r2_api("GET",paste0("/",r2_bucket))
  if (httr::status_code(response)==404) {
    message(paste0("Creating R2 bucket ",r2_bucket,"."))
    response <- r2_api("POST",body=list(name=r2_bucket))
  } else if (httr::status_code(response)<400) {
    message(paste0("R2 bucket ",r2_bucket," already exists."))
  }
  r2_stop_for_status(response)
  public_url <- NULL
  if (!is.null(custom_domain)) {
    custom_domain <- sub("/+$","",sub("^https?://","",tolower(custom_domain)))
    r2_connect_custom_domain(r2_bucket,custom_domain)
    public_url <- paste0("https://",custom_domain)
    message(paste0("R2 bucket ",r2_bucket," is publicly accessible at ",public_url,"."))
  } else if (public) {
    response <- r2_api("PUT",paste0("/",r2_bucket,"/domains/managed"),body=list(enabled=TRUE))
    r2_stop_for_status(response)
    public_url <- paste0("https://",httr::content(response)$result$domain)
    message(paste0("R2 bucket ",r2_bucket," is publicly accessible at ",public_url,"."))
  }
  invisible(public_url)
}

#' transfer parquet files to Cloudflare R2
#'
#' @description
#' Uploads a single parquet file, or all parquet files in a directory of (partitioned) parquet files,
#' keeping the directory structure. The bucket has to exist, use `create_r2_bucket` to create it.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' If the bucket is served via a custom domain, the uploaded files get purged from the Cloudflare cache
#' so that the new versions are served right away. This needs the value of an R2 API token with Zone Cache Purge
#' permission in the `R2_API_TOKEN` environment variable, see `create_r2_bucket`, and is skipped if it is not set.
#'
#' @param path path to local parquet file or directory with (partitioned) parquet files
#' @param r2_bucket R2 bucket name
#' @param r2_path path in bucket. If `path` is a file and `r2_path` is a path component ending with a slash (`/`)
#' the basename of the input path will be appended. If `path` is a directory `r2_path` is the path
#' under which the content of the directory gets placed.
#' @param purge_cache if `TRUE`, the default, purge the uploaded files from the Cloudflare cache of custom domains of the bucket
#' @return (invisibly) the paths in the bucket of the uploaded files
#' @export
parquet_to_r2 <- function(path,r2_bucket,r2_path="",purge_cache=TRUE) {
  if (!requireNamespace("arrow",quietly=TRUE)) {
    stop("The arrow package is required to upload to R2.")
  }
  r2_path <- sub("^/+","",r2_path)
  if (dir.exists(path)) {
    files <- dir(path,"\\.parquet$",recursive=TRUE)
    if (length(files)==0) stop(paste0("No parquet files found in ",path,"."))
    if (r2_path!="") r2_path=paste0(sub("/+$","",r2_path),"/")
    r2_paths <- paste0(r2_path,files)
    files <- file.path(path,files)
  } else if (file.exists(path)) {
    if (r2_path=="" || endsWith(r2_path,"/")) {
      r2_path=paste0(r2_path,basename(path))
    }
    r2_paths <- r2_path
    files <- path
  } else {
    stop(paste0("File or directory ",path," does not exist."))
  }
  filesystem <- r2_filesystem()
  for (i in seq_along(files)) {
    file_to_filesystem(files[i],filesystem,paste0(r2_bucket,"/",r2_paths[i]))
  }
  if (purge_cache) r2_purge_cache_after_change(r2_bucket,r2_paths)
  invisible(r2_paths)
}

#' remove parquet files from Cloudflare R2
#'
#' @description
#' Removes a single parquet file, or all parquet files in a directory of (partitioned) parquet files.
#' Other files in the directory are left in place. Removed files can't be recovered.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' If the bucket is served via a custom domain, the removed files get purged from the Cloudflare cache,
#' with the same requirements as for `parquet_to_r2`.
#'
#' @param r2_bucket R2 bucket name
#' @param r2_path path in bucket of the parquet file, or of the directory with (partitioned) parquet files
#' @param purge_cache if `TRUE`, the default, purge the removed files from the Cloudflare cache of custom domains of the bucket
#' @return (invisibly) the paths in the bucket of the removed files
#' @export
remove_parquet_from_r2 <- function(r2_bucket,r2_path,purge_cache=TRUE) {
  if (!requireNamespace("arrow",quietly=TRUE)) {
    stop("The arrow package is required to access R2.")
  }
  r2_path <- sub("/+$","",sub("^/+","",r2_path))
  if (r2_path=="") stop("Please specify the path of the parquet file or directory to remove.")
  filesystem <- r2_filesystem()
  bucket_path <- paste0(r2_bucket,"/",r2_path)
  file_type <- filesystem$GetFileInfo(bucket_path)[[1]]$type
  if (file_type==arrow::FileType$File) {
    r2_paths <- r2_path
  } else if (file_type==arrow::FileType$Directory) {
    file_infos <- filesystem$GetFileInfo(arrow::FileSelector$create(bucket_path,recursive=TRUE))
    r2_paths <- unlist(lapply(file_infos,function(i)if (i$type==arrow::FileType$File) i$path))
    r2_paths <- substring(r2_paths[grepl("\\.parquet$",r2_paths)],nchar(r2_bucket)+2)
    if (length(r2_paths)==0) stop(paste0("No parquet files found in ",r2_path," in R2 bucket ",r2_bucket,"."))
  } else {
    stop(paste0("File or directory ",r2_path," does not exist in R2 bucket ",r2_bucket,"."))
  }
  for (p in r2_paths) {
    r2_delete_object(r2_bucket,p)
  }
  if (purge_cache) r2_purge_cache_after_change(r2_bucket,r2_paths)
  message(paste0("Removed ",length(r2_paths)," file",if (length(r2_paths)>1) "s" else ""," from R2 bucket ",r2_bucket,"."))
  invisible(r2_paths)
}

# path or glob pattern matching the parquet file or all parquet files in the directory
# directories of parquet files are commonly named like a parquet file, so paths ending in .parquet get checked
r2_parquet_glob <- function(r2_bucket,r2_path){
  is_directory <- endsWith(r2_path,"/")
  r2_path <- sub("/+$","",sub("^/+","",r2_path))
  if (is_directory || !grepl("\\.parquet$",r2_path) || !r2_object_exists(r2_bucket,r2_path)) {
    r2_path <- paste0(r2_path,if (r2_path!="") "/","**/*.parquet")
  }
  r2_path
}

# lazy table for parquet files readable by duckdb, filters and column selections get pushed down to the parquet scan
duckdb_parquet_tbl <- function(con,urls,hive_partitioning=TRUE){
  urls <- paste0(DBI::dbQuoteString(con,urls),collapse=", ")
  if (grepl(", ",urls,fixed=TRUE)) urls <- paste0("[",urls,"]")
  dplyr::tbl(con,dplyr::sql(paste0("SELECT * FROM read_parquet(",urls,
                                   ", hive_partitioning = ",if (hive_partitioning) "true" else "false",")")))
}

#' DuckDB connection to query parquet files on Cloudflare R2
#'
#' @description
#' In-memory DuckDB connection with the `httpfs` extension loaded and the R2 credentials registered,
#' so that files on R2 can be accessed as `r2://bucket/path`. The connection is cached and reused for the
#' duration of the session so that tables from several calls to `r2_parquet_tbl` can be joined.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' @param refresh if `TRUE`, close the cached connection and open a new one, default is `FALSE`
#' @return a DuckDB connection
#' @export
r2_duckdb_connection <- function(refresh=FALSE) {
  con <- r2_cache$duckdb_connection
  if (!is.null(con) && (refresh || !DBI::dbIsValid(con))) {
    if (DBI::dbIsValid(con)) DBI::dbDisconnect(con,shutdown=TRUE)
    con <- NULL
  }
  if (is.null(con)) {
    credentials <- r2_credentials(c("R2_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
    con <- DBI::dbConnect(duckdb::duckdb())
    DBI::dbExecute(con,"INSTALL httpfs")
    DBI::dbExecute(con,"LOAD httpfs")
    DBI::dbExecute(con,paste0("CREATE OR REPLACE SECRET r2 (TYPE r2",
                              ", KEY_ID ",DBI::dbQuoteString(con,credentials[["R2_ACCESS_KEY_ID"]]),
                              ", SECRET ",DBI::dbQuoteString(con,credentials[["R2_SECRET_ACCESS_KEY"]]),
                              ", ACCOUNT_ID ",DBI::dbQuoteString(con,credentials[["R2_ACCOUNT_ID"]]),
                              # without this the region gets picked up from AWS_DEFAULT_REGION if set, which R2 rejects
                              ", REGION 'auto')"))
    # avoids downloading the parquet file footers again for every query
    DBI::dbExecute(con,"SET parquet_metadata_cache = true")
    r2_cache$duckdb_connection <- con
  }
  con
}

#' lazy table to query parquet files on Cloudflare R2
#'
#' @description
#' Lazy `dplyr` table backed by DuckDB reading directly from R2, no data is downloaded until the query is executed,
#' e.g. via `dplyr::collect()`. `dplyr` verbs get translated to SQL and DuckDB only requests the data it needs
#' to answer the query. Filters on partition columns restrict which files are read, other filters and
#' the selection of columns restrict which parts of the files are downloaded. When filtering on integer columns
#' use integer values like `2021L`, comparing to a plain number like `2021` is a floating point comparison
#' and DuckDB then downloads considerably more data.
#'
#' @param r2_bucket R2 bucket name
#' @param r2_path path in bucket of the parquet file, or of the directory with (partitioned) parquet files
#' @param hive_partitioning if `TRUE`, the default, partition columns get derived from `key=value` components of the file paths
#' @param con DuckDB connection with access to R2, as returned by `r2_duckdb_connection`
#' @return a lazy `dplyr` table
#' @export
r2_parquet_tbl <- function(r2_bucket,r2_path,hive_partitioning=TRUE,con=r2_duckdb_connection()) {
  duckdb_parquet_tbl(con,paste0("r2://",r2_bucket,"/",r2_parquet_glob(r2_bucket,r2_path)),hive_partitioning=hive_partitioning)
}

#' lazy arrow dataset to query parquet files on Cloudflare R2
#'
#' @description
#' Arrow `Dataset` reading directly from R2, no data is downloaded until the query is executed,
#' e.g. via `dplyr::collect()`. The dataset can be queried with `dplyr` verbs. Filters on partition columns
#' restrict which files are read, other filters and the selection of columns restrict which parts of the files
#' are downloaded. Opening the dataset lists the files and reads the schema from the first file.
#' See `r2_parquet_tbl` for a version using DuckDB and `r2_parquet_polars` for a version using polars.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' @param r2_bucket R2 bucket name
#' @param r2_path path in bucket of the parquet file, or of the directory with (partitioned) parquet files
#' @param hive_partitioning if `TRUE`, the default, partition columns get derived from `key=value` components of the file paths
#' @return an arrow `Dataset`
#' @export
r2_parquet_arrow <- function(r2_bucket,r2_path,hive_partitioning=TRUE) {
  if (!requireNamespace("arrow",quietly=TRUE)) {
    stop("The arrow package is required to query R2 with arrow.")
  }
  r2_path <- sub("/+$","",sub("^/+","",r2_path))
  arrow::open_dataset(paste0(r2_bucket,if (r2_path!="") "/",r2_path),
                      filesystem=r2_filesystem(),
                      format="parquet",
                      partitioning=if (hive_partitioning) arrow::hive_partition() else NULL)
}

#' lazy polars frame to query parquet files on Cloudflare R2
#'
#' @description
#' Polars `LazyFrame` reading directly from R2, no data is downloaded until the query is executed
#' via `$collect()`. Polars only requests the data it needs to answer the query. Filters on partition columns
#' restrict which files are read, other filters and the selection of columns restrict which parts of the files
#' are downloaded. With the `tidypolars` package loaded the `LazyFrame` can be queried with `dplyr` verbs,
#' ending in `dplyr::collect()`. When filtering on integer columns use integer values like `2021L`, comparing to
#' a plain number like `2021` casts the column to a floating point number and polars then downloads all rows of
#' the selected columns before filtering. See `r2_parquet_tbl` for a version using DuckDB
#' and `r2_parquet_arrow` for a version using arrow.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' @param r2_bucket R2 bucket name
#' @param r2_path path in bucket of the parquet file, or of the directory with (partitioned) parquet files
#' @param hive_partitioning if `TRUE`, the default, partition columns get derived from `key=value` components of the file paths
#' @return a polars `LazyFrame`
#' @export
r2_parquet_polars <- function(r2_bucket,r2_path,hive_partitioning=TRUE) {
  if (!requireNamespace("polars",quietly=TRUE)) {
    stop("The polars package is required to query R2 with polars.")
  }
  credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
  polars::pl$scan_parquet(paste0("s3://",r2_bucket,"/",r2_parquet_glob(r2_bucket,r2_path)),
                          hive_partitioning=hive_partitioning,
                          storage_options=c(aws_access_key_id=credentials[["R2_ACCESS_KEY_ID"]],
                                            aws_secret_access_key=credentials[["R2_SECRET_ACCESS_KEY"]],
                                            aws_endpoint_url=r2_endpoint(),
                                            aws_region="auto"))
}
