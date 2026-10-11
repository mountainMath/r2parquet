# Cloudflare R2 storage for parquet files
# This file contains convenience scripts to upload
# data to R2 into (partitioned) parquet files for
# private or public access

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

# current time in the format used by AWS signature version 4
sigv4_datetime <- function(){
  format(Sys.time(),"%Y%m%dT%H%M%SZ",tz="UTC")
}

# hex encoded sha256 hash of a string or raw vector
sha256_hex <- function(x){
  digest::digest(x,algo="sha256",serialize=FALSE)
}

# percent encode everything except unreserved characters, as required for AWS signature version 4
uri_encode <- function(x){
  vapply(x,utils::URLencode,"",reserved=TRUE,repeated=TRUE,USE.NAMES=FALSE)
}

# canonical path of an object in a bucket for path style access, slashes in the object path are kept
r2_canonical_path <- function(r2_bucket,r2_path=""){
  segments <- c(r2_bucket,if (r2_path!="") strsplit(r2_path,"/",fixed=TRUE)[[1]])
  paste0("/",paste0(uri_encode(segments),collapse="/"))
}

# canonical query string, sorted by encoded name
sigv4_query_string <- function(query){
  if (length(query)==0) return("")
  pairs <- paste0(uri_encode(names(query)),"=",uri_encode(as.character(unlist(query))))
  # byte order as the signature requires, order() would otherwise follow the locale
  paste0(pairs[order(uri_encode(names(query)),method="radix")],collapse="&")
}

# AWS signature version 4, see https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
# R2 expects the region auto
sigv4_signature <- function(canonical_request,secret,datetime,region,service="s3"){
  date <- substr(datetime,1,8)
  scope <- paste(date,region,service,"aws4_request",sep="/")
  string_to_sign <- paste("AWS4-HMAC-SHA256",datetime,scope,sha256_hex(canonical_request),sep="\n")
  key <- charToRaw(paste0("AWS4",secret))
  for (part in c(date,region,service,"aws4_request")) key <- digest::hmac(key,part,"sha256",raw=TRUE)
  list(scope=scope,signature=digest::hmac(key,string_to_sign,"sha256"))
}

# headers, including the Authorization header, for a request signed with AWS signature version 4
sigv4_headers <- function(verb,host,path,query=list(),headers=list(),body=raw(),
                          access_key,secret,region,datetime=sigv4_datetime()){
  payload_hash <- sha256_hex(body)
  headers <- c(headers,list(host=host,"x-amz-content-sha256"=payload_hash,"x-amz-date"=datetime))
  names(headers) <- tolower(names(headers))
  headers <- headers[order(names(headers),method="radix")]
  signed_headers <- paste0(names(headers),collapse=";")
  canonical_request <- paste(verb,path,sigv4_query_string(query),
                             paste0(names(headers),":",trimws(unlist(headers)),"\n",collapse=""),
                             signed_headers,payload_hash,sep="\n")
  signature <- sigv4_signature(canonical_request,secret,datetime,region)
  headers$authorization <- paste0("AWS4-HMAC-SHA256 Credential=",access_key,"/",signature$scope,
                                  ",SignedHeaders=",signed_headers,",Signature=",signature$signature)
  headers[names(headers)!="host"]
}

# url with the signature in the query string, valid for expires seconds
sigv4_presigned_url <- function(verb,base_url,path,expires,access_key,secret,region,datetime=sigv4_datetime()){
  host <- sub("^https?://","",base_url)
  query <- list("X-Amz-Algorithm"="AWS4-HMAC-SHA256",
                "X-Amz-Credential"=paste(access_key,substr(datetime,1,8),region,"s3","aws4_request",sep="/"),
                "X-Amz-Date"=datetime,
                "X-Amz-Expires"=as.character(as.integer(expires)),
                "X-Amz-SignedHeaders"="host")
  query_string <- sigv4_query_string(query)
  canonical_request <- paste(verb,path,query_string,paste0("host:",host,"\n"),"host","UNSIGNED-PAYLOAD",sep="\n")
  signature <- sigv4_signature(canonical_request,secret,datetime,region)
  paste0(base_url,path,"?",query_string,"&X-Amz-Signature=",signature$signature)
}

# signed request via the S3 compatible API
r2_request <- function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
  credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
  endpoint <- r2_endpoint()
  path <- r2_canonical_path(r2_bucket,r2_path)
  if (is.character(body)) body <- charToRaw(enc2utf8(body))
  headers <- sigv4_headers(verb,sub("^https?://","",endpoint),path,query=query,headers=headers,body=body,
                           access_key=credentials[["R2_ACCESS_KEY_ID"]],
                           secret=credentials[["R2_SECRET_ACCESS_KEY"]],region="auto")
  query_string <- sigv4_query_string(query)
  httr::VERB(verb,paste0(endpoint,path,if (query_string!="") "?",query_string),
             do.call(httr::add_headers,headers),
             body=if (length(body)>0) body)
}

# escape text for use in xml
xml_escape <- function(x){
  for (i in seq_along(xml_entities)) x <- gsub(names(xml_entities)[i],xml_entities[[i]],x,fixed=TRUE)
  x
}
xml_entities <- c("&"="&amp;","<"="&lt;",">"="&gt;","\""="&quot;","'"="&apos;")

# undo xml escaping
xml_unescape <- function(x){
  for (i in rev(seq_along(xml_entities))) x <- gsub(xml_entities[[i]],names(xml_entities)[i],x,fixed=TRUE)
  x
}

# delete objects via the S3 compatible API, in batches of up to 1000 objects per request
# the arrow filesystem is not used for this as it leaves empty directory marker objects behind when deleting files
r2_delete_objects <- function(r2_bucket,r2_paths){
  for (batch in split(r2_paths,ceiling(seq_along(r2_paths)/1000))) {
    body <- paste0('<?xml version="1.0" encoding="UTF-8"?><Delete><Quiet>true</Quiet>',
                   paste0("<Object><Key>",xml_escape(batch),"</Key></Object>",collapse=""),
                   "</Delete>")
    body <- charToRaw(enc2utf8(body))
    response <- r2_request("POST",r2_bucket,query=list(delete=""),body=body,
                           headers=list("content-md5"=base64_md5(body),"content-type"="application/xml"))
    content <- httr::content(response,"text",encoding="UTF-8")
    if (httr::status_code(response)>=400) {
      stop(paste0("Removing files from R2 bucket ",r2_bucket," failed with status ",httr::status_code(response),". ",content))
    }
    # with quiet mode the response only lists the objects that could not be deleted
    errors <- regmatches(content,gregexpr("(?s)<Error>.*?</Error>",content,perl=TRUE))[[1]]
    if (length(errors)>0) {
      error_messages <- vapply(errors,function(e){
        field <- function(name)xml_unescape(sub(paste0("(?s).*<",name,">(.*?)</",name,">.*"),"\\1",e,perl=TRUE))
        paste0(field("Key"),": ",field("Message")," (",field("Code"),")")
      },"",USE.NAMES=FALSE)
      stop(paste0("Removing ",length(errors)," file",if (length(errors)>1) "s"," from R2 bucket ",r2_bucket," failed. ",
                  paste0(error_messages,collapse=", ")))
    }
  }
}

# base64 encoded md5 hash, the S3 API requires it for batch deletes
base64_md5 <- function(body){
  jsonlite::base64_enc(digest::digest(body,algo="md5",serialize=FALSE,raw=TRUE))
}

# check if there is an object, as opposed to a directory, at the path
r2_object_exists <- function(r2_bucket,r2_path){
  status <- httr::status_code(r2_request("HEAD",r2_bucket,r2_path))
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

# parquet files below a directory on R2, as paths within the bucket
r2_list_parquet <- function(filesystem,r2_bucket,r2_path){
  file_infos <- filesystem$GetFileInfo(arrow::FileSelector$create(paste0(r2_bucket,"/",r2_path),recursive=TRUE))
  paths <- unlist(lapply(file_infos,function(i)if (i$type==arrow::FileType$File) i$path))
  substring(paths[grepl("\\.parquet$",paths)],nchar(r2_bucket)+2)
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
#' in the same Cloudflare account. With Zone Cache Purge permission as well, `upload_parquet` and
#' `remove_parquet` purge changed files from the cache. Each custom domain can only serve one bucket, use subdomains like
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
#' @param destination destination on R2 as `r2://bucket/path`. If `path` is a file and `destination` ends with a slash (`/`)
#' or is just the bucket, the basename of `path` gets appended. If `path` is a directory, `destination` is the directory
#' under which the content of the directory gets placed.
#' @param purge_cache if `TRUE`, the default, purge the uploaded files from the Cloudflare cache of custom domains of the bucket
#' @return (invisibly) the uploaded files as `r2://bucket/path`
#' @export
upload_parquet <- function(path,destination,purge_cache=TRUE) {
  if (!requireNamespace("arrow",quietly=TRUE)) {
    stop("The arrow package is required to upload to R2.")
  }
  destination <- r2_split_path(destination)
  r2_bucket <- destination$bucket
  r2_path <- destination$path
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
  invisible(paste0("r2://",r2_bucket,"/",r2_paths))
}

#' remove parquet files from Cloudflare R2
#'
#' @description
#' Removes parquet files, or all parquet files in directories of (partitioned) parquet files.
#' Other files in the directories are left in place. Removed files can't be recovered.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' All paths get resolved to files first, so a path that does not exist stops the call before anything is
#' removed. The files are then removed in one batch per bucket. If the bucket is served via a custom domain,
#' the removed files get purged from the Cloudflare cache, with the same requirements as for `upload_parquet`.
#'
#' @param path parquet file, or directory with (partitioned) parquet files, on R2 as `r2://bucket/path`, or a
#' vector of these
#' @param purge_cache if `TRUE`, the default, purge the removed files from the Cloudflare cache of custom domains of the bucket
#' @return (invisibly) the removed files as `r2://bucket/path`
#' @export
remove_parquet <- function(path,purge_cache=TRUE) {
  if (!requireNamespace("arrow",quietly=TRUE)) {
    stop("The arrow package is required to access R2.")
  }
  if (!is.character(path) || length(path)==0) stop("path has to be one or more R2 paths of the form r2://bucket/path.")
  filesystem <- r2_filesystem()
  resolved <- lapply(path,function(p){
    p <- r2_split_path(p)
    r2_bucket <- p$bucket
    r2_path <- sub("/+$","",p$path)
    if (r2_path=="") stop("Please specify the path of the parquet file or directory to remove in R2 bucket ",r2_bucket,".")
    file_type <- filesystem$GetFileInfo(paste0(r2_bucket,"/",r2_path))[[1]]$type
    if (file_type==arrow::FileType$File) {
      r2_paths <- r2_path
    } else if (file_type==arrow::FileType$Directory) {
      r2_paths <- r2_list_parquet(filesystem,r2_bucket,r2_path)
      if (length(r2_paths)==0) stop(paste0("No parquet files found in ",r2_path," in R2 bucket ",r2_bucket,"."))
    } else {
      stop(paste0("File or directory ",r2_path," does not exist in R2 bucket ",r2_bucket,"."))
    }
    list(bucket=r2_bucket,paths=r2_paths)
  })
  buckets <- vapply(resolved,`[[`,"",i="bucket")
  removed <- character()
  for (r2_bucket in unique(buckets)) {
    r2_paths <- unique(unlist(lapply(resolved[buckets==r2_bucket],`[[`,"paths")))
    r2_delete_objects(r2_bucket,r2_paths)
    if (purge_cache) r2_purge_cache_after_change(r2_bucket,r2_paths)
    message(paste0("Removed ",length(r2_paths)," file",if (length(r2_paths)>1) "s" else ""," from R2 bucket ",r2_bucket,"."))
    removed <- c(removed,paste0("r2://",r2_bucket,"/",r2_paths))
  }
  invisible(removed)
}

#' presigned URL for a file on Cloudflare R2
#'
#' @description
#' URLs that grant time limited access to single files in a private bucket, without credentials.
#' Anyone with the URL can download the file (`method="GET"`) or, for `method="PUT"`, upload a file
#' to that path until the URL expires, so treat the URL like a password. The URLs are signed locally,
#' creating them does not check that the files exist.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables. The URLs stop working when the API token is revoked.
#'
#' @param path file on R2 as `r2://bucket/path`, or a vector of these
#' @param expires number of seconds the URLs are valid for, between 1 second and 7 days (604800 seconds),
#' default is one hour
#' @param method HTTP method the URLs are valid for, `"GET"` (the default) to download, `"HEAD"` to
#' get the file's metadata, or `"PUT"` to upload
#' @return the presigned URLs, one per path
#' @export
presigned_url <- function(path,expires=3600,method=c("GET","HEAD","PUT")) {
  method <- match.arg(method)
  if (!is.numeric(expires) || length(expires)!=1 || expires<1 || expires>604800) {
    stop("expires has to be between 1 and 604800 seconds (7 days).")
  }
  if (!is.character(path)) stop("path has to be one or more R2 paths of the form r2://bucket/path.")
  credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
  endpoint <- r2_endpoint()
  vapply(path,function(p){
    p <- r2_split_path(p)
    if (p$path=="" || endsWith(p$path,"/")) stop("The path has to be the path of a file, got r2://",p$bucket,"/",p$path,".")
    sigv4_presigned_url(method,endpoint,r2_canonical_path(p$bucket,p$path),expires,
                        access_key=credentials[["R2_ACCESS_KEY_ID"]],
                        secret=credentials[["R2_SECRET_ACCESS_KEY"]],region="auto")
  },character(1),USE.NAMES=FALSE)
}

# split r2://bucket/path into bucket and path
r2_split_path <- function(path){
  if (!is.character(path) || length(path)!=1) stop("Expected a single R2 path, got ",length(path),".")
  parts <- regmatches(path,regexec("^r2://([^/]+)/?(.*)$",path))[[1]]
  if (length(parts)!=3) stop("R2 paths have to be of the form r2://bucket/path, got ",path,".")
  list(bucket=parts[2],path=sub("^/+","",parts[3]))
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
