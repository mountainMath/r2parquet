# Cloudflare R2 storage for parquet files: uploading, listing and removing (partitioned) parquet files and
# presigning URLs via the S3 compatible API with the package's own signature version 4 implementation, and
# bucket settings via the Cloudflare API.

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

# signed request via the S3 compatible API, the body is a raw vector or a string, config is further httr
# configuration like httr::write_disk
r2_request <- function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
  credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
  endpoint <- r2_endpoint()
  path <- r2_canonical_path(r2_bucket,r2_path)
  if (is.character(body)) body <- charToRaw(enc2utf8(body))
  headers <- sigv4_headers(verb,sub("^https?://","",endpoint),path,query=query,headers=headers,body=body,
                           access_key=credentials[["R2_ACCESS_KEY_ID"]],
                           secret=credentials[["R2_SECRET_ACCESS_KEY"]],region="auto")
  query_string <- sigv4_query_string(query)
  request_config <- do.call(httr::add_headers,headers)
  if (!is.null(config)) request_config <- c(request_config,config)
  httr::VERB(verb,paste0(endpoint,path,if (query_string!="") "?",query_string),request_config,
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

# text of the first <name> element in each xml snippet, NA where there is none
# the S3 API responses are flat enough that a proper xml parser is not needed
xml_field <- function(x,name){
  matches <- regmatches(x,regexec(paste0("(?s)<",name,">(.*?)</",name,">"),x,perl=TRUE))
  xml_unescape(vapply(matches,function(m)if (length(m)==2) m[2] else NA_character_,""))
}

# all <name> elements in an xml document, with their content
xml_elements <- function(x,name){
  regmatches(x,gregexpr(paste0("(?s)<",name,">.*?</",name,">"),x,perl=TRUE))[[1]]
}

# delete objects via the S3 compatible API, in batches of up to 1000 objects per request
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
    errors <- xml_elements(content,"Error")
    if (length(errors)>0) {
      error_messages <- paste0(xml_field(errors,"Key"),": ",xml_field(errors,"Message")," (",xml_field(errors,"Code"),")")
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

# upload a local file as an object in a single PUT request with a signed payload, so the file gets read into
# memory (the S3 API allows up to 5GB per PUT, httr's streaming upload_file leaks file connections)
r2_put_object <- function(r2_bucket,r2_path,file){
  r2_put_body(r2_bucket,r2_path,readBin(file,raw(),file.size(file)),r2_content_type(r2_path),what=file)
}

# content type of an object from its extension, parquet files and unknown extensions as binary data
r2_content_type <- function(r2_path){
  ifelse(grepl("\\.parquet$",r2_path),"application/octet-stream",mime::guess_type(r2_path,empty="application/octet-stream"))
}

# upload a raw vector or string as an object
r2_put_body <- function(r2_bucket,r2_path,body,content_type,what="the file"){
  if (!is.raw(body)) body <- charToRaw(enc2utf8(as.character(body)))
  response <- r2_request("PUT",r2_bucket,r2_path,body=body,headers=list("content-type"=content_type))
  if (httr::status_code(response)>=400) {
    stop(paste0("Uploading ",what," to r2://",r2_bucket,"/",r2_path," failed with status ",httr::status_code(response),". ",
                httr::content(response,"text",encoding="UTF-8")))
  }
  invisible(response)
}

# key of the manifest of a directory, "" is the bucket itself
r2_manifest_key <- function(r2_dir){
  paste0(r2_dir,if (r2_dir!="") "/",manifest_name)
}

# directory an object is in, "" for the bucket itself
r2_dirname <- function(r2_path){
  ifelse(grepl("/",r2_path,fixed=TRUE),sub("/[^/]*$","",r2_path),"")
}

# the directories above a key or directory, nearest first, ending with "" for the bucket itself
r2_ancestors <- function(r2_path){
  ancestors <- character()
  while (r2_path!="") {
    r2_path <- r2_dirname(r2_path)
    ancestors <- c(ancestors,r2_path)
  }
  ancestors
}

# the footer metadata of parquet files, local paths or r2://bucket/key, as a list per file named by the path,
# without the GeoParquet entry. Reads on a connection of its own so that DuckDB's parquet metadata cache can't
# hand back the footer of an earlier version of a file; a remote footer costs a request or two per file
parquet_footers <- function(path){
  metadata <- stats::setNames(rep(list(list()),length(path)),path)
  if (length(path)==0) return(metadata)
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con,shutdown=TRUE))
  if (any(startsWith(path,"r2://"))) {
    DBI::dbExecute(con,"INSTALL httpfs")
    DBI::dbExecute(con,"LOAD httpfs")
    duckdb_r2_secret(con)
  }
  files <- paste0("[",paste0(DBI::dbQuoteString(con,path),collapse=", "),"]")
  kv <- DBI::dbGetQuery(con,paste0("SELECT file_name, decode(key) AS k, decode(value) AS v FROM parquet_kv_metadata(",files,")"))
  kv <- kv[kv$k!="geo",]
  for (file in unique(kv$file_name)) {
    entries <- kv[kv$file_name==file,]
    metadata[[file]] <- lapply(stats::setNames(entries$v,entries$k),parse_metadata_value)
  }
  metadata
}

# the parsed manifest at a key, NULL if there is none, and an empty one without metadata if it can't be parsed
r2_get_manifest <- function(r2_bucket,key){
  response <- r2_request("GET",r2_bucket,key)
  status <- httr::status_code(response)
  if (status==404) return(NULL)
  if (status>=400) stop(paste0("Reading r2://",r2_bucket,"/",key," failed with status ",status,"."))
  tryCatch(parse_manifest(httr::content(response,"text",encoding="UTF-8")),
           error=function(e)structure(manifest_files(),directory=TRUE,has_metadata=FALSE))
}

# write the manifest listing the parquet files below a directory with their footer metadata, or remove it if no
# parquet files are left. Without create only an existing manifest gets updated, so the manifest gets read first,
# since listing the directory, or the whole bucket for "", costs more. The metadata of files that were just
# uploaded is passed in as uploaded, a list named by key, read from the local files; files the existing manifest
# lists with the same etag keep the metadata recorded there; the footers of the remaining files get read from R2.
# Returns the key of the manifest if it changed.
r2_update_manifest <- function(r2_bucket,r2_dir,create=FALSE,uploaded=list()){
  key <- r2_manifest_key(r2_dir)
  existing <- r2_get_manifest(r2_bucket,key)
  if (!create && is.null(existing)) return(NULL)
  objects <- r2_objects(r2_bucket,r2_dir)
  exists <- key %in% objects$key
  if (!create && !exists) return(NULL)
  files <- objects[grepl("\\.parquet$",objects$key),]
  if (nrow(files)==0) {
    if (exists) r2_delete_objects(r2_bucket,key)
    return(if (exists) key)
  }
  paths <- substring(files$key,nchar(r2_dir)+1+(r2_dir!=""))
  known <- if (isTRUE(attr(existing,"has_metadata"))) existing else manifest_files()
  metadata <- rep(list(NULL),nrow(files))
  for (i in seq_len(nrow(files))) {
    j <- match(paths[i],known$path)
    if (files$key[i] %in% names(uploaded)) {
      metadata[i] <- list(uploaded[[files$key[i]]])
    } else if (!is.na(j) && identical(known$etag[j],files$etag[i])) {
      metadata[i] <- list(known$metadata[[j]])
    }
  }
  unknown <- vapply(metadata,is.null,logical(1))
  if (any(unknown)) metadata[unknown] <- unname(parquet_footers(paste0("r2://",r2_bucket,"/",files$key[unknown])))
  files <- manifest_files(paths,files$size,files$etag,files$modified,metadata)
  r2_put_body(r2_bucket,key,manifest_json(files),"application/json",what="the manifest")
  key
}

# update the manifests of the directories among r2_dirs that have one, returns the keys of the manifests that changed
r2_update_manifests <- function(r2_bucket,r2_dirs,uploaded=list()){
  manifests <- character()
  for (r2_dir in unique(r2_dirs)) manifests <- c(manifests,r2_update_manifest(r2_bucket,r2_dir,uploaded=uploaded))
  manifests
}

# upload a local file or a directory of parquet files, maintaining the manifests: directory uploads write the
# manifest of the directory, and the manifests of the directories above get updated where they exist, since a
# manifest lists all parquet files below its directory, with the metadata of the uploaded files from their
# footers. A single file of any type goes up with the content type for its extension, a parquet file updates the
# manifests above it. With remove_stale, parquet files below the destination directory that are not part of the
# upload get removed first. Returns the uploaded keys, the removed keys and the keys of the manifests that changed.
r2_upload <- function(path,r2_bucket,r2_path,remove_stale=FALSE){
  stale <- character()
  if (dir.exists(path)) {
    files <- dir(path,"\\.parquet$",recursive=TRUE)
    if (length(files)==0) stop(paste0("No parquet files found in ",path,"."))
    r2_dir <- sub("/+$","",r2_path)
    r2_paths <- paste0(r2_dir,if (r2_dir!="") "/",files)
    files <- file.path(path,files)
    create <- TRUE
  } else if (file.exists(path)) {
    if (r2_path=="" || endsWith(r2_path,"/")) {
      r2_path=paste0(r2_path,basename(path))
    }
    if (basename(r2_path)==manifest_name) {
      stop(paste0(manifest_name," files are the manifests of directories of parquet files, use write_manifest to write them."))
    }
    r2_dir <- r2_dirname(r2_path)
    r2_paths <- r2_path
    files <- path
    create <- FALSE
  } else {
    stop(paste0("File or directory ",path," does not exist."))
  }
  # the footers get read before anything goes up, so a file that is not parquet stops the upload
  parquet <- grepl("\\.parquet$",r2_paths)
  uploaded <- stats::setNames(parquet_footers(files[parquet]),r2_paths[parquet])
  if (remove_stale && create) {
    objects <- r2_objects(r2_bucket,r2_dir)
    stale <- setdiff(objects$key[grepl("\\.parquet$",objects$key)],r2_paths)
    if (length(stale)>0) {
      message("Removing ",length(stale)," stale parquet file",if (length(stale)>1) "s"," from r2://",r2_bucket,"/",r2_dir,".")
      r2_delete_objects(r2_bucket,stale)
    }
  }
  for (i in seq_along(files)) {
    r2_put_object(r2_bucket,r2_paths[i],files[i])
  }
  manifests <- character()
  if (any(parquet)) {
    manifests <- c(r2_update_manifest(r2_bucket,r2_dir,create=create,uploaded=uploaded),
                   r2_update_manifests(r2_bucket,r2_ancestors(r2_dir),uploaded=uploaded))
  }
  list(files=r2_paths,stale=stale,manifests=manifests)
}

# objects in the bucket whose key starts with prefix, with size in bytes, modification time and etag, via the
# ListObjectsV2 API that returns up to 1000 objects per page
r2_list_objects <- function(r2_bucket,prefix=""){
  pages <- list()
  token <- NULL
  repeat {
    query <- c(list("list-type"="2"),if (prefix!="") list(prefix=prefix),if (!is.null(token)) list("continuation-token"=token))
    response <- r2_request("GET",r2_bucket,query=query)
    content <- httr::content(response,"text",encoding="UTF-8")
    if (httr::status_code(response)>=400) {
      stop(paste0("Listing R2 bucket ",r2_bucket," failed with status ",httr::status_code(response),". ",content))
    }
    objects <- xml_elements(content,"Contents")
    pages[[length(pages)+1]] <- data.frame(key=xml_field(objects,"Key"),
                                           size=as.numeric(xml_field(objects,"Size")),
                                           modified=as.POSIXct(xml_field(objects,"LastModified"),format="%Y-%m-%dT%H:%M:%OS",tz="UTC"),
                                           etag=clean_etag(xml_field(objects,"ETag")))
    if (!identical(xml_field(content,"IsTruncated"),"true")) break
    token <- xml_field(content,"NextContinuationToken")
  }
  do.call(rbind,pages)
}

# the object at an R2 path together with the objects below it as a directory (path/...), the whole bucket
# for an empty path. Objects whose keys merely start with the path, like path.bak, are not included.
r2_objects <- function(r2_bucket,r2_path){
  objects <- r2_list_objects(r2_bucket,prefix=r2_path)
  if (r2_path=="") return(objects)
  objects[objects$key==r2_path | startsWith(objects$key,paste0(r2_path,"/")),]
}

# the parquet file at an R2 path, or the parquet files below the directory at the path, with size and
# modification time, nothing for a path that does not exist
r2_parquet_files <- function(path){
  path <- r2_split_path(path)
  r2_path <- sub("/+$","",path$path)
  objects <- r2_objects(path$bucket,r2_path)
  objects <- if (r2_path %in% objects$key) objects[objects$key==r2_path,] else objects[grepl("\\.parquet$",objects$key),]
  parquet_files(paste0("r2://",path$bucket,"/",objects$key),objects$size,objects$modified,objects$etag)
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
#' @param r2_bucket R2 bucket name, or the bucket as `r2://bucket` like the other functions address it
#' @param public if `TRUE`, enable public read access to the bucket via the `r2.dev` URL, default is `FALSE`.
#' Ignored if `custom_domain` is set.
#' @param custom_domain optional domain, like `data.example.com`, to connect to the bucket for public read access.
#' The `r2.dev` URL is not enabled in this case.
#' @return (invisibly) the public base url of the bucket if it is publicly accessible, otherwise `NULL`
#' @export
create_r2_bucket <- function(r2_bucket,public=FALSE,custom_domain=NULL) {
  if (!is.character(r2_bucket) || length(r2_bucket)!=1) stop("Expected a single bucket name.")
  r2_bucket <- sub("/+$","",sub("^r2://","",r2_bucket))
  if (r2_bucket=="" || grepl("/",r2_bucket,fixed=TRUE)) stop("r2_bucket has to be a bucket name or r2://bucket, got ",r2_bucket,".")
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

#' transfer files to Cloudflare R2
#'
#' @description
#' Uploads a single file, or all parquet files in a directory of (partitioned) parquet files, keeping the
#' directory structure. The bucket has to exist, use `create_r2_bucket` to create it.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' A single file can be of any type, like a JSON index or a README that gets published next to a dataset. It
#' goes up with the content type for its extension (`application/json` for `.json`, `application/geo+json` for
#' `.geojson`, `text/csv` for `.csv`, and so on, as `mime::guess_type` guesses it), so that browsers and other
#' clients handle it correctly; parquet files and files with unknown extensions are sent as
#' `application/octet-stream`. `remove_parquet` removes such files again. `_manifest.json` files are reserved
#' for the manifests described below and can't be uploaded, use `write_manifest` to write them.
#'
#' If the bucket is served via a custom domain, the uploaded files get purged from the Cloudflare cache
#' so that the new versions are served right away. This needs the value of an R2 API token with Zone Cache Purge
#' permission in the `R2_API_TOKEN` environment variable, see `create_r2_bucket`, and is skipped if it is not set.
#'
#' Directory uploads write a manifest, a `_manifest.json` file in the destination directory listing the parquet
#' files below it with their size, etag and modification time and the metadata in their footers, see
#' `write_manifest` for the format. The manifest lets `cache_parquet` and `parquet_tbl(cache = TRUE)` cache the
#' directory from a public URL, where the files can't be listed, and lets other clients read the dataset's
#' metadata without parquet footers. Since a manifest lists all parquet files below its directory, uploads also
#' update the existing manifests of the directories above the destination, so that uploading a single file or a
#' new partition into a dataset keeps the dataset's manifest current. The metadata of the uploaded parquet
#' files is read from the local files, the manifests keep what they record for files that did not change.
#'
#' @param path path to a local file, or to a directory with (partitioned) parquet files
#' @param destination destination on R2 as `r2://bucket/path`. If `path` is a file and `destination` ends with a slash (`/`)
#' or is just the bucket, the basename of `path` gets appended. If `path` is a directory, `destination` is the directory
#' under which the content of the directory gets placed.
#' @param purge_cache if `TRUE`, the default, purge the uploaded files from the Cloudflare cache of custom domains of the bucket
#' @param remove_stale if `TRUE`, and `path` is a directory, remove the parquet files below `destination` on R2 that
#' are not in the directory, so that the destination mirrors the directory after the upload, e.g. when a dataset
#' got rebuilt without a partition or file. The default `FALSE` leaves them in place. The files are removed before
#' the upload, other files in the directory and files above `destination` are never touched
#' @return (invisibly) the uploaded files as `r2://bucket/path`
#' @export
upload_parquet <- function(path,destination,purge_cache=TRUE,remove_stale=FALSE) {
  destination <- r2_split_path(destination)
  r2_bucket <- destination$bucket
  uploaded <- r2_upload(path,r2_bucket,destination$path,remove_stale=remove_stale)
  if (purge_cache) r2_purge_cache_after_change(r2_bucket,c(uploaded$files,uploaded$stale,uploaded$manifests))
  invisible(paste0("r2://",r2_bucket,"/",uploaded$files))
}

#' write manifests for directories of parquet files on Cloudflare R2
#'
#' @description
#' Writes a `_manifest.json` file into directories on R2, listing the parquet files below the directory with
#' their size, etag and modification time and the metadata in their footers. `upload_parquet` and
#' `data_to_parquet` write manifests for the directories they upload, this writes them for directories uploaded
#' before. Manifests let `cache_parquet` and `parquet_tbl(cache = TRUE)` cache a directory from its public URL,
#' since directories can't be listed over https, and let clients other than R read what a dataset holds.
#' Expects the same credentials as `upload_parquet`, and purges the manifest from the Cloudflare cache of the
#' bucket's custom domains like `upload_parquet` does.
#'
#' The metadata comes from the parquet footers, as `parquet_metadata` reads it. Files that an existing manifest
#' lists with the same etag keep the metadata recorded there, the footers of all other files get read from R2,
#' which costs a request or two per file, so writing the manifest of a large directory for the first time takes
#' a moment.
#'
#' @section Manifest format:
#' A manifest is a JSON object with three members:
#' * `directory`, `true` for the manifests on R2, which list every parquet file below their directory;
#' * `metadata`, an object with the metadata entries that all files share, with identical values, the dataset's
#'   metadata, which is what `parquet_metadata` returns for the directory. The entries are the ones
#'   `data_to_parquet(metadata = )` wrote, `columns` included; the GeoParquet entry `geo` is left out;
#' * `files`, an array with one object per parquet file, listed in the order of their keys: `path`, relative to the
#'   manifest's directory; `size` in bytes; `etag`, for files uploaded by this package the MD5 hash of the file in
#'   hex, which is what R2 assigns to single request uploads; `modified`, the modification time in UTC as
#'   `YYYY-MM-DDTHH:MM:SSZ`; and `metadata`, an object with the entries of the file's footer that differ from those
#'   of other files or that only this file has, left out if there are none. So the column descriptions of a
#'   partitioned dataset sit in the manifest's `metadata`, and in a directory holding different datasets with
#'   each file.
#'
#' The metadata of a file is the manifest's `metadata` with the file's own `metadata` added. Values are stored
#' as `parquet_metadata` returns them, single values as strings and everything else as the JSON objects and
#' arrays that `data_to_parquet` wrote.
#'
#' @param path directory with (partitioned) parquet files on R2 as `r2://bucket/path`, or a vector of these
#' @param purge_cache if `TRUE`, the default, purge the manifests from the Cloudflare cache of custom domains of the bucket
#' @return (invisibly) the manifests as `r2://bucket/path/_manifest.json`
#' @export
write_manifest <- function(path,purge_cache=TRUE) {
  if (!is.character(path) || length(path)==0) stop("path has to be one or more R2 paths of the form r2://bucket/path.")
  manifests <- vapply(path,function(p){
    p <- r2_split_path(p)
    r2_dir <- sub("/+$","",p$path)
    objects <- r2_objects(p$bucket,r2_dir)
    if (r2_dir %in% objects$key) stop(paste0("r2://",p$bucket,"/",r2_dir," is a file, manifests describe directories."))
    if (!any(grepl("\\.parquet$",objects$key))) stop(paste0("No parquet files found in ",r2_dir," in R2 bucket ",p$bucket,"."))
    key <- r2_update_manifest(p$bucket,r2_dir,create=TRUE)
    if (purge_cache) r2_purge_cache_after_change(p$bucket,key)
    paste0("r2://",p$bucket,"/",key)
  },character(1),USE.NAMES=FALSE)
  invisible(manifests)
}

#' remove files from Cloudflare R2
#'
#' @description
#' Removes files, or all parquet files in directories of (partitioned) parquet files, together with the
#' manifests (`_manifest.json`, see `write_manifest`) below the directories. The path of a file removes that
#' file whatever its type, like a JSON index uploaded with `upload_parquet`, or a manifest, which takes the
#' directory's manifest away until `write_manifest` or the next upload writes it again; a directory only loses
#' its parquet files and manifests, other files in it are left in place. The existing manifests of the
#' directories above the removed files and directories get updated, since a manifest lists all parquet files
#' below its directory, so removing a file or a partition from a dataset keeps the dataset's manifest current.
#' Removed files can't be recovered.
#' Expects the R2 account id and the access key id and secret access key
#' of an R2 API token to be available as `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`
#' environment variables.
#'
#' All paths get resolved to files first, so a path that does not exist stops the call before anything is
#' removed. The files are then removed in one batch per bucket. If the bucket is served via a custom domain,
#' the removed files get purged from the Cloudflare cache, with the same requirements as for `upload_parquet`.
#'
#' @param path file, or directory with (partitioned) parquet files, on R2 as `r2://bucket/path`, or a
#' vector of these
#' @param purge_cache if `TRUE`, the default, purge the removed files from the Cloudflare cache of custom domains of the bucket
#' @return (invisibly) the removed files as `r2://bucket/path`
#' @export
remove_parquet <- function(path,purge_cache=TRUE) {
  if (!is.character(path) || length(path)==0) stop("path has to be one or more R2 paths of the form r2://bucket/path.")
  resolved <- lapply(path,function(p){
    p <- r2_split_path(p)
    r2_bucket <- p$bucket
    r2_path <- sub("/+$","",p$path)
    if (r2_path=="") stop("Please specify the path of the parquet file or directory to remove in R2 bucket ",r2_bucket,".")
    objects <- r2_objects(r2_bucket,r2_path)
    manifests <- character()
    # the manifests above the removed path list its files, the ones below a directory go with it
    directories <- r2_ancestors(r2_path)
    if (r2_path %in% objects$key) {
      r2_paths <- r2_path
    } else if (nrow(objects)>0) {
      r2_paths <- objects$key[grepl("\\.parquet$",objects$key)]
      if (length(r2_paths)==0) stop(paste0("No parquet files found in ",r2_path," in R2 bucket ",r2_bucket,"."))
      manifests <- objects$key[basename(objects$key)==manifest_name]
    } else {
      stop(paste0("File or directory ",r2_path," does not exist in R2 bucket ",r2_bucket,"."))
    }
    list(bucket=r2_bucket,paths=r2_paths,manifests=manifests,directories=directories)
  })
  buckets <- vapply(resolved,`[[`,"",i="bucket")
  removed <- character()
  for (r2_bucket in unique(buckets)) {
    in_bucket <- resolved[buckets==r2_bucket]
    r2_paths <- unique(unlist(lapply(in_bucket,`[[`,"paths")))
    manifests <- unique(unlist(lapply(in_bucket,`[[`,"manifests")))
    r2_delete_objects(r2_bucket,c(r2_paths,manifests))
    # manifests of the directories above the removed paths, unless they went with a removed directory
    directories <- setdiff(unlist(lapply(in_bucket,`[[`,"directories")),r2_dirname(manifests))
    manifests <- c(manifests,r2_update_manifests(r2_bucket,directories))
    if (purge_cache) r2_purge_cache_after_change(r2_bucket,c(r2_paths,manifests))
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
