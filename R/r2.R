# Cloudflare R2 storage for parquet files: uploading, listing and removing (partitioned) parquet files and
# presigning URLs via the S3 compatible API with the package's own signature version 4 implementation, and
# bucket settings via the Cloudflare API.

# read credentials from environment variables
r2_credentials <- function(env_vars){
  credentials <- Sys.getenv(env_vars,names=TRUE)
  missing_vars <- env_vars[credentials==""]
  if (length(missing_vars)>0) {
    stop(paste0("Missing environment variables for ",if (all(startsWith(env_vars,"R2_"))) "R2" else "S3"," access: ",
                paste0(missing_vars,collapse = ", "),"."))
  }
  credentials
}

# endpoint of the S3 compatible API
r2_endpoint <- function(){
  paste0("https://",r2_credentials("R2_ACCOUNT_ID")[["R2_ACCOUNT_ID"]],".r2.cloudflarestorage.com")
}

# whether paths are in a bucket, r2://bucket/path on R2 or s3://bucket/path on Amazon S3 or another S3 compatible service
is_bucket_path <- function(path){
  grepl("^(r2|s3)://",path)
}

# paths at URLs, as opposed to bucket paths and local paths
is_url <- function(path){
  !is_bucket_path(path) & grepl("^[a-z0-9]+://",path)
}

# internally a bucket on R2 is its name and a bucket on an S3 service is s3://name, so the code written for R2
# serves both; the path of keys in a bucket as r2://bucket/key or s3://bucket/key
bucket_url <- function(r2_bucket,key=""){
  paste0(if (!startsWith(r2_bucket,"s3://")) "r2://",r2_bucket,"/",key)
}

# the bucket for messages, "R2 bucket name" or "S3 bucket name"
bucket_label <- function(r2_bucket){
  if (startsWith(r2_bucket,"s3://")) paste0("S3 bucket ",sub("^s3://","",r2_bucket)) else paste0("R2 bucket ",r2_bucket)
}

# what a denied request (401 or 403) for path depends on, the credentials for buckets, the signature and expiry
# for presigned URLs, public access for other URLs; "" for other statuses
access_hint <- function(status,path){
  if (!(status %in% c(401,403))) return("")
  if (startsWith(path,"r2://")) {
    return(paste0(" Access was denied: check that R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY hold a valid R2 API token ",
                  "for the account in R2_ACCOUNT_ID with access to the bucket ",sub("^r2://([^/]*).*$","\\1",path),"."))
  }
  if (startsWith(path,"s3://")) {
    return(paste0(" Access was denied: check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and AWS_SESSION_TOKEN for ",
                  "temporary credentials), and that the IAM permissions or the bucket policy allow the request. S3 ",
                  "also denies requests for missing objects when the credentials can't list the bucket."))
  }
  if (grepl("[?&]X-Amz-Signature=",path)) {
    signed <- as.POSIXct(sub("^.*[?&]X-Amz-Date=([0-9TZ]+).*$","\\1",path),format="%Y%m%dT%H%M%SZ",tz="UTC")
    expires <- signed+as.numeric(sub("^.*[?&]X-Amz-Expires=([0-9]+).*$","\\1",path))
    if (!is.na(expires) && expires<Sys.time()) {
      return(paste0(" Access was denied: the presigned URL expired at ",format(expires,"%Y-%m-%d %H:%M:%S UTC"),
                    ", create a new one with presigned_url()."))
    }
    return(" Access was denied: the presigned URL is not valid, create a new one with presigned_url().")
  }
  paste0(" Access was denied: the file is not public. Files in private buckets can be read as r2://bucket/path ",
         "or s3://bucket/path with credentials.")
}

# an error of class r2parquet_not_found or r2parquet_access_denied, so callers can tell these failures apart
r2parquet_error <- function(type,message,call=NULL){
  structure(class=c(paste0("r2parquet_",type),"r2parquet_error","error","condition"),list(message=message,call=call))
}

# an r2parquet_network_error for a server that could not be reached, with the reason the connection failed
network_error <- function(url,reason,call=NULL){
  r2parquet_error("network_error",paste0("Could not reach ",sub("^[a-z0-9]+://([^/?#]+).*$","\\1",url),
                                         ", check the internet connection. ",reason),call)
}

# run a request to url, with connection failures (unresolvable host, refused connection, timeout) as
# r2parquet_network_error conditions
with_network <- function(url,expr){
  tryCatch(expr,curl_error=function(e)stop(network_error(url,sub("^.*\n","",conditionMessage(e)))))
}

# stop for the response to a failed request (status 400 and up): an r2parquet_access_denied for 401 and 403
# with what access to the path depends on, an r2parquet_not_found for 404, a plain error otherwise. what names
# the request, like "Reading r2://bucket/key", detail, like the response body, goes on a line of its own
stop_for_status <- function(response,what,path,detail=NULL){
  status <- httr::status_code(response)
  if (status<400) return(invisible(response))
  message <- paste0(what," failed with status ",status,".",access_hint(status,path),
                    if (length(detail)==1 && nzchar(detail)) paste0("\n",detail))
  call <- sys.call(-1)
  stop(if (status %in% c(401,403)) r2parquet_error("access_denied",message,call)
       else if (status==404) r2parquet_error("not_found",message,call)
       else simpleError(message,call))
}

# a count with its noun, "1 file", "2 files"
plural <- function(n,noun)paste0(n," ",noun,if (n!=1) "s")

# the first of the environment variables that is set, "" if none is
first_env <- function(env_vars){
  values <- Sys.getenv(env_vars)
  c(values[values!=""],"")[[1]]
}

# settings of the S3 service for s3:// paths from the environment variables AWS SDKs use: the credentials,
# AWS_SESSION_TOKEN for temporary ones, the region, and AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL for services
# other than Amazon S3, like MinIO, which get path style addressing unless R2PARQUET_S3_URL_STYLE is vhost
s3_settings <- function(){
  credentials <- r2_credentials(c("AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY"))
  region <- first_env(c("AWS_REGION","AWS_DEFAULT_REGION"))
  if (region=="") region <- "us-east-1"
  endpoint <- sub("/+$","",first_env(c("AWS_ENDPOINT_URL_S3","AWS_ENDPOINT_URL")))
  url_style <- first_env("R2PARQUET_S3_URL_STYLE")
  if (url_style=="") url_style <- if (endpoint=="") "vhost" else "path"
  if (!(url_style %in% c("path","vhost"))) stop("R2PARQUET_S3_URL_STYLE has to be path or vhost, got ",url_style,".")
  list(endpoint=if (endpoint=="") paste0("https://s3.",region,".amazonaws.com") else endpoint,custom=endpoint!="",
       region=region,url_style=url_style,access_key=credentials[["AWS_ACCESS_KEY_ID"]],
       secret=credentials[["AWS_SECRET_ACCESS_KEY"]],token=Sys.getenv("AWS_SESSION_TOKEN"))
}

# endpoint, region, addressing and credentials for requests to a bucket; virtual host addressing puts the
# bucket into the endpoint, except for bucket names with dots, which don't match the TLS certificates
bucket_store <- function(r2_bucket){
  if (!startsWith(r2_bucket,"s3://")) {
    credentials <- r2_credentials(c("R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"))
    return(list(endpoint=r2_endpoint(),bucket=r2_bucket,region="auto",url_style="path",
                access_key=credentials[["R2_ACCESS_KEY_ID"]],secret=credentials[["R2_SECRET_ACCESS_KEY"]],token=""))
  }
  store <- s3_settings()
  store$bucket <- sub("^s3://","",r2_bucket)
  if (store$url_style=="vhost" && grepl(".",store$bucket,fixed=TRUE)) store$url_style <- "path"
  if (store$url_style=="vhost") store$endpoint <- sub("://",paste0("://",store$bucket,"."),store$endpoint,fixed=TRUE)
  store
}

# the directory below the cache directory that s3:// paths get cached in, per service since bucket names are
# only unique within one: aws for Amazon S3, the host (and port) of the endpoint otherwise
s3_cache_host <- function(){
  endpoint <- first_env(c("AWS_ENDPOINT_URL_S3","AWS_ENDPOINT_URL"))
  if (endpoint=="") return("aws")
  cache_host(httr::parse_url(endpoint))
}

# the directory a host gets cached in, with the port as host_port since colons aren't allowed in Windows paths
cache_host <- function(url){
  paste0(url$hostname,if (!is.null(url$port)) paste0("_",url$port))
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

# canonical path of an object in a bucket for path style access, slashes in the object path are kept, without
# the bucket (NULL) for virtual host access
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
sigv4_presigned_url <- function(verb,base_url,path,expires,access_key,secret,region,datetime=sigv4_datetime(),token=""){
  host <- sub("^https?://","",base_url)
  query <- c(list("X-Amz-Algorithm"="AWS4-HMAC-SHA256",
                  "X-Amz-Credential"=paste(access_key,substr(datetime,1,8),region,"s3","aws4_request",sep="/"),
                  "X-Amz-Date"=datetime,
                  "X-Amz-Expires"=as.character(as.integer(expires)),
                  "X-Amz-SignedHeaders"="host"),
             if (token!="") list("X-Amz-Security-Token"=token))
  query_string <- sigv4_query_string(query)
  canonical_request <- paste(verb,path,query_string,paste0("host:",host,"\n"),"host","UNSIGNED-PAYLOAD",sep="\n")
  signature <- sigv4_signature(canonical_request,secret,datetime,region)
  paste0(base_url,path,"?",query_string,"&X-Amz-Signature=",signature$signature)
}

# signed request via the S3 compatible API, the body is a raw vector or a string, config is further httr
# configuration like httr::write_disk
r2_request <- function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw(),config=NULL){
  store <- bucket_store(r2_bucket)
  path <- r2_canonical_path(if (store$url_style=="path") store$bucket,r2_path)
  if (is.character(body)) body <- charToRaw(enc2utf8(body))
  if (store$token!="") headers <- c(headers,list("x-amz-security-token"=store$token))
  headers <- sigv4_headers(verb,sub("^https?://","",store$endpoint),path,query=query,headers=headers,body=body,
                           access_key=store$access_key,secret=store$secret,region=store$region)
  query_string <- sigv4_query_string(query)
  request_config <- do.call(httr::add_headers,headers)
  if (!is.null(config)) request_config <- c(request_config,config)
  with_network(store$endpoint,httr::VERB(verb,paste0(store$endpoint,path,if (query_string!="") "?",query_string),request_config,
                                         body=if (length(body)>0) body))
}

# escape text for use in xml
xml_escape <- function(x){
  for (i in seq_along(xml_entities)) x <- gsub(names(xml_entities)[i],xml_entities[[i]],x,fixed=TRUE)
  x
}
xml_entities <- c("&"="&amp;","<"="&lt;",">"="&gt;","\""="&quot;","'"="&apos;")

# undo xml escaping, the named entities and numeric character references like &#34;, which MinIO uses for the
# quotes around etags
xml_unescape <- function(x){
  for (i in rev(seq_along(xml_entities)[-1])) x <- gsub(xml_entities[[i]],names(xml_entities)[i],x,fixed=TRUE)
  x <- vapply(x,function(s){
    if (is.na(s) || !grepl("&#",s,fixed=TRUE)) return(s)
    m <- gregexpr("&#(x[0-9a-fA-F]+|[0-9]+);",s,perl=TRUE)
    regmatches(s,m) <- list(vapply(regmatches(s,m)[[1]],function(r){
      code <- sub("^&#(.*);$","\\1",r)
      intToUtf8(if (startsWith(code,"x")) strtoi(substring(code,2),16L) else as.integer(code))
    },""))
    s
  },"",USE.NAMES=FALSE)
  gsub("&amp;","&",x,fixed=TRUE)
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
    stop_for_status(response,paste0("Removing files from ",bucket_label(r2_bucket)),bucket_url(r2_bucket),content)
    # with quiet mode the response only lists the objects that could not be deleted
    errors <- xml_elements(content,"Error")
    if (length(errors)>0) {
      error_messages <- paste0(xml_field(errors,"Key"),": ",xml_field(errors,"Message")," (",xml_field(errors,"Code"),")")
      stop(paste0("Removing ",plural(length(errors),"file")," from ",bucket_label(r2_bucket)," failed. ",
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
  response <- r2_request("HEAD",r2_bucket,r2_path)
  status <- httr::status_code(response)
  if (status!=404) stop_for_status(response,paste0("Checking ",r2_path," in ",bucket_label(r2_bucket)),bucket_url(r2_bucket,r2_path))
  status<400
}

# call the Cloudflare API
cloudflare_api <- function(verb,path,body=NULL,query=NULL){
  credentials <- r2_credentials("R2_API_TOKEN")
  with_network("https://api.cloudflare.com",
               httr::VERB(verb,paste0("https://api.cloudflare.com/client/v4",path),
                          httr::add_headers(Authorization=paste0("Bearer ",credentials[["R2_API_TOKEN"]])),
                          body=body,query=query,encode="json"))
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

# origins of web pages reading the bucket, from R2PARQUET_PURGE_ORIGINS (comma or whitespace separated)
purge_origins <- function(){
  origins <- strsplit(Sys.getenv("R2PARQUET_PURGE_ORIGINS"),"[,[:space:]]+")[[1]]
  unique(sub("/+$","",origins[origins!=""]))
}

# purge objects served via custom domains of the bucket from the Cloudflare cache; responses to requests
# with an Origin header (browsers reading cross-origin) are cached separately per origin, a purge of the
# plain URL leaves them in place, so each URL also gets purged once per origin in R2PARQUET_PURGE_ORIGINS
r2_purge_cache <- function(r2_bucket,r2_paths){
  response <- r2_api("GET",paste0("/",r2_bucket,"/domains/custom"))
  r2_stop_for_status(response)
  domains <- Filter(function(d)isTRUE(d$enabled),httr::content(response)$result$domains)
  origins <- purge_origins()
  for (domain in domains) {
    urls <- paste0("https://",domain$domain,"/",url_path_encode(r2_paths))
    files <- c(as.list(urls),do.call(c,lapply(origins,function(o)
      lapply(urls,function(u)list(url=u,headers=list(Origin=o))))))
    # the purge API accepts a limited number of urls per request
    for (batch in split(files,ceiling(seq_along(files)/30))) {
      response <- cloudflare_api("POST",paste0("/zones/",domain$zoneId,"/purge_cache"),body=list(files=as.list(batch)))
      r2_stop_for_status(response)
    }
  }
}

# purge cache after writing or removing objects, failure to purge only warrants a warning
# as the objects have already been changed at this point
r2_purge_cache_after_change <- function(r2_bucket,r2_paths){
  if (Sys.getenv("R2_API_TOKEN")=="" || startsWith(r2_bucket,"s3://")) return(invisible())
  tryCatch(r2_purge_cache(r2_bucket,r2_paths),
           error=function(e)warning(paste0("Purging the Cloudflare cache for R2 bucket ",r2_bucket," failed, ",
                                           "cached copies of the changed files may be served until they expire. ",
                                           conditionMessage(e)),call.=FALSE))
}

# upload a local file as an object in a single PUT request with a signed payload, so the file gets read into
# memory (the S3 API allows up to 5GB per PUT, httr's streaming upload_file leaks file connections)
r2_put_object <- function(r2_bucket,r2_path,file){
  response <- r2_put_body(r2_bucket,r2_path,readBin(file,raw(),file.size(file)),r2_content_type(r2_path),what=file)
  clean_etag(httr::headers(response)$etag)
}

# content type of an object from its extension, parquet files and unknown extensions as binary data
r2_content_type <- function(r2_path){
  ifelse(grepl("\\.parquet$",r2_path),"application/octet-stream",mime::guess_type(r2_path,empty="application/octet-stream"))
}

# upload a raw vector or string as an object, with further request headers such as the conditions
# If-Match and If-None-Match, in which case a 412 (precondition failed) response is returned for the caller
# to handle; any other failure stops
r2_put_body <- function(r2_bucket,r2_path,body,content_type,what="the file",headers=list()){
  if (!is.raw(body)) body <- charToRaw(enc2utf8(as.character(body)))
  response <- r2_request("PUT",r2_bucket,r2_path,body=body,headers=c(list("content-type"=content_type),headers))
  status <- httr::status_code(response)
  if (status>=400 && !(status==412 && length(headers)>0)) {
    stop_for_status(response,paste0("Uploading ",what," to ",bucket_url(r2_bucket,r2_path)),bucket_url(r2_bucket,r2_path),
                    httr::content(response,"text",encoding="UTF-8"))
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

# the parsed manifest at a key with the etag of the object as an attribute, NULL if there is none, and, with a
# warning, an empty one without metadata if it can't be parsed, which makes readers fall back to the footers and
# r2_update_manifest rebuild it from a listing
r2_get_manifest <- function(r2_bucket,key){
  manifest_response(r2_request("GET",r2_bucket,key),bucket_url(r2_bucket,key))
}

# the manifest from the response to the GET of its path, for r2_get_manifest and url_get_manifest
manifest_response <- function(response,path){
  status <- httr::status_code(response)
  if (status==404) return(NULL)
  stop_for_status(response,paste0("Reading the manifest at ",path),path)
  manifest <- tryCatch(parse_manifest(httr::content(response,"text",encoding="UTF-8")),error=function(e){
    warning(paste0(path," is not a valid manifest, write_manifest() or the next upload to its directory rewrites it."),call.=FALSE)
    structure(manifest_files(),directory=TRUE,has_metadata=FALSE)
  })
  etag <- clean_etag(httr::headers(response)$etag)
  attr(manifest,"etag") <- if (length(etag)==1 && nzchar(etag)) etag
  manifest
}

# manifest entries for local parquet files that are about to be uploaded, keyed by their destination keys,
# with the etag R2 assigns to single request uploads (the MD5 of the file) and the metadata of their footers,
# read before anything goes up
r2_upload_entries <- function(files,keys){
  entries <- manifest_files(keys,file.size(files),vapply(files,digest::digest,"",algo="md5",file=TRUE),Sys.time(),
                            unname(parquet_footers(files)))
  names(entries)[names(entries)=="path"] <- "key"
  entries
}

# whether keys are below a directory, "" being the bucket itself, and their paths relative to it
r2_below <- function(keys,r2_dir)r2_dir=="" | startsWith(keys,paste0(r2_dir,"/"))
r2_relative <- function(keys,r2_dir)substring(keys,nchar(r2_dir)+1+(r2_dir!=""))

# manifest entries for the parquet files below a directory in a listing, except the keys in exclude, with the
# metadata of files that known, an existing manifest with metadata, lists with the same etag carried over and
# the footers of the others read from R2, one range request per file
r2_manifest_from_listing <- function(r2_bucket,r2_dir,objects,known=manifest_files(),exclude=character()){
  keys <- objects$key[grepl("\\.parquet$",objects$key) & r2_below(objects$key,r2_dir) & !(objects$key %in% exclude)]
  files <- objects[match(keys,objects$key),]
  paths <- r2_relative(keys,r2_dir)
  j <- match(paths,known$path)
  carry <- !is.na(j) & known$etag[j]==files$etag
  carry[is.na(carry)] <- FALSE
  metadata <- rep(list(NULL),length(keys))
  metadata[carry] <- known$metadata[j[carry]]
  if (any(!carry)) metadata[!carry] <- unname(parquet_footers(bucket_url(r2_bucket,keys[!carry])))
  manifest_files(paths,files$size,files$etag,files$modified,metadata)
}

# bring the manifest of a directory up to date after parquet files below it were uploaded (uploaded, entries
# from r2_upload_entries) or removed (removed, keys), and write it with a conditional PUT, so that a manifest
# someone else wrote in the meantime is not overwritten: the existing manifest gets read and, when it carries
# metadata, patched with the uploaded entries and without the removed keys, which costs nothing beyond the two
# requests, whatever the directory holds. It gets rebuilt from a listing instead with create, the repair path
# of write_manifest and of directory uploads, and when the existing manifest has no metadata (written before the
# metadata was added, or not parseable), see r2_manifest_from_listing; objects is a listing that covers
# everything below the directory if the caller has one, otherwise the directory gets listed. Without create a
# directory without a manifest is left alone, so that the directories above an upload, the bucket root in
# particular, don't get one. A manifest left without files gets deleted. If the manifest changed on R2 between
# the read and the write (412) the update starts over. Returns the key of the manifest if it was written or
# deleted.
r2_update_manifest <- function(r2_bucket,r2_dir,create=FALSE,objects=NULL,uploaded=r2_upload_entries(character(),character()),
                               removed=character()){
  key <- r2_manifest_key(r2_dir)
  uploaded <- uploaded[r2_below(uploaded$key,r2_dir),]
  uploaded$path <- r2_relative(uploaded$key,r2_dir)
  removed <- removed[r2_below(removed,r2_dir)]
  columns <- names(manifest_files())
  for (attempt in 1:3) {
    existing <- r2_get_manifest(r2_bucket,key)
    if (!create && is.null(existing)) return(NULL)
    known <- if (isTRUE(attr(existing,"has_metadata"))) existing else manifest_files()
    if (create || !isTRUE(attr(existing,"has_metadata"))) {
      if (is.null(objects)) objects <- r2_objects(r2_bucket,r2_dir)
      files <- r2_manifest_from_listing(r2_bucket,r2_dir,objects,known=known,exclude=c(removed,uploaded$key))
    } else {
      files <- existing[!(existing$path %in% c(r2_relative(removed,r2_dir),uploaded$path)),]
    }
    files <- sorted_files(rbind(files[,columns],uploaded[,columns]))
    if (nrow(files)==0) {
      if (!is.null(existing)) r2_delete_objects(r2_bucket,key)
      return(if (!is.null(existing)) key)
    }
    condition <- if (is.null(existing)) {
      list("if-none-match"="*")
    } else if (!is.null(attr(existing,"etag"))) {
      list("if-match"=paste0("\"",attr(existing,"etag"),"\""))
    }
    response <- r2_put_body(r2_bucket,key,manifest_json(files),"application/json",what="the manifest",headers=condition)
    if (httr::status_code(response)!=412) return(key)
  }
  stop(paste0(bucket_url(r2_bucket,key)," kept changing while it was being updated."))
}

# update the manifests of several directories, the ones that have a manifest and the ones in create, for
# uploaded entries and removed keys, with objects a listing covering everything below the directory listed,
# which saves the directories at or below it their own listing when they need one. A failed update only warns,
# since the data has already changed, naming the manifests that are now out of date, write_manifest rewrites
# them. Returns the keys of the manifests that changed
r2_update_manifests <- function(r2_bucket,r2_dirs,create=character(),objects=NULL,listed=NULL,
                                uploaded=r2_upload_entries(character(),character()),removed=character()){
  manifests <- character()
  failed <- character()
  for (r2_dir in unique(r2_dirs)) {
    covered <- !is.null(objects) && (r2_dir==listed || r2_below(r2_dir,listed))
    manifest <- tryCatch(r2_update_manifest(r2_bucket,r2_dir,create=r2_dir %in% create,objects=if (covered) objects,
                                            uploaded=uploaded,removed=removed),
                         error=function(e){
                           failed <<- c(failed,paste0(bucket_url(r2_bucket,r2_manifest_key(r2_dir)),": ",conditionMessage(e)))
                           NULL
                         })
    manifests <- c(manifests,manifest)
  }
  if (length(failed)>0) {
    warning(paste0(plural(length(failed),"manifest")," could not be updated and ",
                   if (length(failed)>1) "are" else "is"," out of date, write_manifest rewrites ",
                   if (length(failed)>1) "them" else "it",":\n",paste0(failed,collapse="\n")),call.=FALSE)
  }
  manifests
}

# the keys to remove before an upload so that the destination mirrors the uploaded directory: every parquet
# file below it that is not in the upload. For the stale argument of r2_upload
r2_stale_mirror <- function(objects,r2_paths){
  setdiff(objects$key[grepl("\\.parquet$",objects$key)],r2_paths)
}

# upload a local file or a directory of parquet files, maintaining the manifests. A directory goes up with
# its parquet files, keeping the structure, and gets its manifest written, a single file of any type goes up
# with the content type for its extension. Since a manifest lists all parquet files below its directory, the
# existing manifests of the directories above the destination, and of the directories below it, get updated for
# the uploaded parquet files, with their metadata from the local footers. stale, if given, picks the keys to
# remove before the upload from the listing of the destination and the keys of the upload, like
# r2_stale_mirror does, and the manifests get updated for the removals too. Returns the uploaded keys, the
# removed keys and the keys of the manifests that changed. A single file goes into r2_path if that is a
# directory (given with a trailing slash), otherwise r2_path is the file's key.
r2_upload <- function(path,r2_bucket,r2_path,stale=NULL,directory=FALSE){
  if (dir.exists(path)) {
    files <- dir(path,"\\.parquet$",recursive=TRUE)
    if (length(files)==0) stop(paste0("No parquet files found in ",path,"."))
    r2_dir <- r2_path
    if (r2_dir=="") {
      stop(paste0("Please specify the directory in ",bucket_label(r2_bucket)," to upload the directory into, ",
                  "write_manifest writes a manifest for the whole bucket."))
    }
    r2_paths <- paste0(r2_dir,"/",files)
    files <- file.path(path,files)
    create <- TRUE
  } else if (file.exists(path)) {
    if (r2_path=="" || directory) {
      r2_path=paste0(r2_path,if (r2_path!="") "/",basename(path))
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
  # DuckDB reads these characters as patterns, such files could never be queried by their name
  patterns <- grepl("[*?\\[]",r2_paths)
  if (any(patterns)) {
    stop(paste0("File names can't contain *, ? or [, DuckDB reads them as patterns: ",paste0(r2_paths[patterns],collapse=", "),"."))
  }
  # the footers get read before anything goes up, so a file that is not parquet stops the upload
  parquet <- grepl("\\.parquet$",r2_paths)
  uploaded <- r2_upload_entries(files[parquet],r2_paths[parquet])
  # one listing serves the stale files, the manifest of the destination and the manifests below it
  listed <- if (create) r2_dir else r2_path
  objects <- if (create || !is.null(stale)) r2_objects(r2_bucket,listed)
  removed <- as.character(if (!is.null(stale)) stale(objects,r2_paths))
  if (length(removed)>0) {
    message("Removing ",plural(length(removed),"stale parquet file")," from ",bucket_url(r2_bucket,listed),".")
    r2_delete_objects(r2_bucket,removed)
  }
  for (i in seq_along(files)) {
    etag <- r2_put_object(r2_bucket,r2_paths[i],files[i])
    if (parquet[i] && length(etag)==1) uploaded$etag[uploaded$key==r2_paths[i]] <- etag
  }
  manifests <- character()
  if (any(parquet) || length(removed)>0) {
    # the manifests below the destination show in its listing, the ones above get looked up
    below <- if (is.null(objects)) character() else r2_dirname(objects$key[basename(objects$key)==manifest_name])
    r2_dirs <- c(if (create) r2_dir,setdiff(below,if (create) r2_dir),r2_ancestors(listed))
    manifests <- r2_update_manifests(r2_bucket,r2_dirs,create=if (create) r2_dir,objects=objects,listed=listed,
                                     uploaded=uploaded,removed=removed)
  }
  list(files=r2_paths,stale=removed,manifests=manifests)
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
    stop_for_status(response,paste0("Listing ",bucket_label(r2_bucket)),bucket_url(r2_bucket),content)
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

# the object at an R2 path if it is a file, or else the parquet files below the directory at the path, with
# whether the path is a file; nothing for a path that does not exist
r2_dataset_objects <- function(r2_bucket,r2_path){
  objects <- r2_objects(r2_bucket,r2_path)
  is_file <- r2_path %in% objects$key
  objects <- if (is_file) objects[objects$key==r2_path,] else objects[grepl("\\.parquet$",objects$key),]
  list(is_file=is_file,objects=objects)
}

# the parquet file at an R2 path, or the parquet files below the directory at the path, with size and
# modification time, nothing for a path that does not exist
r2_parquet_files <- function(path){
  path <- r2_split_path(path)
  objects <- r2_dataset_objects(path$bucket,path$path)$objects
  parquet_files(bucket_url(path$bucket,objects$key),objects$size,objects$modified,objects$etag)
}

#' create Cloudflare R2 bucket with private or public access
#'
#' @description
#' Creates the bucket if it does not exist yet. Buckets are private by default, public read access gets
#' granted by connecting a custom domain or by enabling the `r2.dev` public development URL of the bucket.
#' Public access can also be enabled on existing buckets, but this function never turns it off.
#'
#' @details
#' Cloudflare rate limits access via `r2.dev`, for heavier use connect a custom domain. Each custom domain can
#' only serve one bucket, use subdomains like `data.example.com` to serve several buckets from one zone.
#'
#' # Credentials
#' The R2 account id and the value of an R2 API token with admin read and write permissions have to be in the
#' `R2_ACCOUNT_ID` and `R2_API_TOKEN` environment variables. For connecting a custom domain the token also
#' needs Zone Read permission for the domain's zone, which has to be an active zone in the same Cloudflare
#' account. With Zone Cache Purge permission as well, [upload_parquet()] and [remove_parquet()] purge changed files
#' from the cache.
#'
#' @param r2_bucket bucket name, or the bucket as `r2://bucket` like the other functions address it.
#' @param public `FALSE` (default) or `TRUE`: enable public read access via the `r2.dev` URL. Ignored if
#'   `custom_domain` is set.
#' @param custom_domain domain to connect to the bucket for public read access, like `data.example.com`,
#'   optional. The `r2.dev` URL does not get enabled then.
#' @return (invisibly) the public base URL of the bucket if it is publicly accessible, otherwise `NULL`.
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

#' transfer files to Cloudflare R2 or S3
#'
#' @description
#' Uploads a single file, or all parquet files in a directory of (partitioned) parquet files keeping the
#' directory structure, to a bucket that has to exist (see [create_r2_bucket()]). Directory uploads write a
#' manifest (`_manifest.json`), and all uploads keep the existing manifests of the dataset current.
#'
#' @details
#' Each file goes up in a single request and is read into memory for that, so a file has to fit into memory,
#' and the 5 GB limit of single requests applies; split larger data into partitions with
#' [data_to_parquet()]. File names with `*`, `?` or `[` are refused, since DuckDB reads them as patterns when the
#' files are queried. If an R2 bucket is served via a custom domain, the uploaded files get purged from the
#' Cloudflare cache, see the Credentials section.
#'
#' A single file can be of any type, like a JSON index or a README that gets published next to a dataset. It
#' goes up with the content type for its extension, as [mime::guess_type()] guesses it (`application/json` for
#' `.json`, `text/csv` for `.csv`, and so on), so that browsers and other clients handle it correctly; parquet
#' files and unknown extensions are sent as `application/octet-stream`. [remove_parquet()] removes such files
#' again. `_manifest.json` files are reserved for manifests and can't be uploaded, use [write_manifest()] to
#' write them.
#'
#' # Manifests
#' A manifest lists the parquet files below its directory with their size, etag, modification time and footer
#' metadata, see [write_manifest()] for the format. It lets [cache_parquet()] and [parquet_tbl()] read a directory
#' from a public URL, where the files can't be listed, and lets other clients read the dataset's metadata
#' without the parquet footers.
#' * The manifest of the destination of a directory upload is built from a listing of the directory, so it also
#'   repairs a manifest that fell behind.
#' * Since a manifest lists all parquet files below its directory, the existing manifests of the directories
#'   above the destination get updated too, and for directory uploads those below it, so that uploading a
#'   single file or a new partition into a dataset keeps its manifest current. These get patched from what
#'   they record, without listing their directories: the uploaded and removed files are swapped in and out, and
#'   the metadata of the uploaded files is read from the local files. [write_manifest()] rebuilds a manifest from
#'   a listing when files were changed by other means.
#' * Manifests are written conditionally, so that an upload running at the same time can't overwrite an
#'   update, and the update starts over from the current manifest when that happens. The upload warns, after
#'   the files are up, when a manifest could not be updated this way.
#' * Uploading a single file never creates a manifest. A directory can't be uploaded to the root of the
#'   bucket, since its manifest would list the whole bucket and `remove_stale` would empty it;
#'   [write_manifest()] writes a manifest of a whole bucket on request.
#'
#' @section Credentials:
#' Credentials come from environment variables, e.g. set in `.Renviron`, read at call time. Public URLs and
#' local paths need none.
#'
#' Paths on R2 (`r2://bucket/path`):
#' * `R2_ACCOUNT_ID`: the R2 account id.
#' * `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`: the access key id and secret access key of an R2 API
#'   token with access to the bucket.
#' * `R2_API_TOKEN`, optional: the value of an API token with Zone Cache Purge permission (see
#'   [create_r2_bucket()]). Files that change on R2 then get purged from the Cloudflare cache of the bucket's
#'   custom domains, so that the new versions are served right away; the purge is skipped without it.
#' * `R2PARQUET_PURGE_ORIGINS`, optional: the origins of web pages reading the bucket from a browser, comma
#'   separated, like `https://example.com,https://www.example.com`. Cloudflare caches the responses to their
#'   requests separately per `Origin`, which a purge of the plain URL misses, so each changed file also gets
#'   purged once per origin; copies cached for origins not listed are refreshed when the edge TTL runs out.
#'   This only concerns web apps, whose browsers send the `Origin` header; requests without one, like those of
#'   this package, DuckDB or curl, get the plain URL's copy, which is always purged.
#'
#' Paths on S3 (`s3://bucket/path`), on Amazon S3 or another S3 compatible service like MinIO, use the
#' variables AWS tools use:
#' * `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` for temporary credentials.
#' * `AWS_REGION` or `AWS_DEFAULT_REGION`: the region, default `us-east-1`.
#' * `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL`: the endpoint of other S3 compatible services, like
#'   `http://localhost:9000`, which get addressed with path style URLs (`endpoint/bucket/key`).
#' * `R2PARQUET_S3_URL_STYLE`: `path` or `vhost`, overrides the URL style.
#'
#' AWS profiles and single sign-on are not read, `aws configure export-credentials --format env` prints the
#' variables for them.
#'
#' @param path local file, or directory of (partitioned) parquet files.
#' @param destination destination as `r2://bucket/path` or `s3://bucket/path`:
#'   * for a file: the path of the file, or with a trailing slash (`/`), or just the bucket, the directory it
#'     goes into under its basename;
#'   * for a directory: the directory its content goes into, which has to be a directory in the bucket, not
#'     the bucket itself.
#' @param purge_cache `TRUE` (default) or `FALSE`: purge the uploaded files from the Cloudflare cache of the
#'   bucket's custom domains. Ignored for S3.
#' @param remove_stale what happens to parquet files below `destination` that are not in the uploaded
#'   directory:
#'   * `FALSE` (default): they are left in place.
#'   * `TRUE`: they get removed before the upload, so that the destination mirrors the directory, e.g. when a
#'     dataset got rebuilt without a partition or file. Other files in the directory and files above
#'     `destination` are never touched; manifests of directories below `destination` lose the removed files,
#'     and go when nothing is left.
#' @return (invisibly) the uploaded files as `r2://bucket/path` or `s3://bucket/path`.
#' @export
upload_parquet <- function(path,destination,purge_cache=TRUE,remove_stale=FALSE) {
  destination <- r2_split_path(destination)
  r2_bucket <- destination$bucket
  uploaded <- r2_upload(path,r2_bucket,destination$path,stale=if (remove_stale && dir.exists(path)) r2_stale_mirror,
                        directory=destination$directory)
  if (purge_cache) r2_purge_cache_after_change(r2_bucket,c(uploaded$files,uploaded$stale,uploaded$manifests))
  # tables reading the changed files, or the directory they went into, read their metadata again
  forget_scans(bucket_url(r2_bucket,c(destination$path[nzchar(destination$path)],uploaded$files,uploaded$stale)))
  invisible(bucket_url(r2_bucket,uploaded$files))
}

#' write manifests for directories of parquet files on Cloudflare R2 or S3
#'
#' @description
#' Writes a `_manifest.json` file into directories on R2 or S3, listing the parquet files below the directory with
#' their size, etag, modification time and footer metadata. For directories uploaded before, and to rebuild
#' the manifest of a directory that fell behind.
#'
#' @details
#' [upload_parquet()] and [data_to_parquet()] write manifests for the directories they upload. A manifest falls
#' behind after files were changed with other tools, after an upload or removal warned that it could not be
#' updated, or after it was removed. Manifests let [cache_parquet()] and [parquet_tbl()] read a directory from its
#' public URL, since directories can't be listed over https, and let clients other than R read what a dataset
#' holds. On R2 the manifest gets purged from the Cloudflare cache of the bucket's custom domains like
#' [upload_parquet()] does. `write_manifest("r2://bucket")` writes a manifest of the whole bucket at its root,
#' which uploads and removals anywhere in the bucket then keep current.
#'
#' The metadata comes from the parquet footers, as [parquet_metadata()] reads it. Files that an existing manifest
#' lists with the same etag keep the metadata recorded there, the footers of all other files get read from the
#' bucket, which costs a request or two per file, so writing the manifest of a large directory for the first
#' time takes a moment. The manifest is written conditionally, like [upload_parquet()] writes them, and the
#' call fails if it keeps changing underneath, e.g. because of uploads running at the same time.
#'
#' @section Manifest format:
#' A manifest is a JSON object with three members:
#' * `directory`: `true` for the manifests in buckets, which list every parquet file below their directory.
#' * `metadata`: an object with the metadata entries that all files share with identical values, the
#'   dataset's metadata, which is what [parquet_metadata()] returns for the directory. The entries are the ones
#'   `data_to_parquet(metadata = )` wrote, `columns` included; the GeoParquet entry `geo` is left out.
#' * `files`: an array with one object per parquet file, in the order of their keys, with
#'   * `path`: relative to the manifest's directory;
#'   * `size`: in bytes;
#'   * `etag`: for files uploaded by this package the MD5 hash of the file in hex, which is what R2 and S3 assign
#'     to single request uploads without encryption by KMS or customer keys;
#'   * `modified`: the modification time in UTC as `YYYY-MM-DDTHH:MM:SSZ`;
#'   * `metadata`: an object with the entries of the file's footer that differ from those of other files or
#'     that only this file has, left out if there are none.
#'
#' The metadata of a file is the manifest's `metadata` with the file's own `metadata` added. So the column
#' descriptions of a partitioned dataset sit in the manifest's `metadata`, and in a directory holding
#' different datasets with each file. Values are stored as [parquet_metadata()] returns them, single values as
#' strings and everything else as the JSON objects and arrays that [data_to_parquet()] wrote.
#'
#' @inheritSection upload_parquet Credentials
#' @param path directory of (partitioned) parquet files as `r2://bucket/path` or `s3://bucket/path`, or a
#'   vector of these.
#' @param purge_cache `TRUE` (default) or `FALSE`: purge the manifests from the Cloudflare cache of the
#'   bucket's custom domains. Ignored for S3.
#' @return (invisibly) the manifests as `r2://bucket/path/_manifest.json` or the `s3://` equivalent.
#' @export
write_manifest <- function(path,purge_cache=TRUE) {
  if (!is.character(path) || length(path)==0) stop("path has to be one or more paths of the form r2://bucket/path or s3://bucket/path.")
  manifests <- vapply(path,function(p){
    p <- r2_split_path(p)
    r2_dir <- p$path
    objects <- r2_objects(p$bucket,r2_dir)
    if (r2_dir %in% objects$key) stop(paste0(bucket_url(p$bucket,r2_dir)," is a file, manifests describe directories."))
    if (!any(grepl("\\.parquet$",objects$key))) stop(paste0("No parquet files found in ",r2_dir," in ",bucket_label(p$bucket),"."))
    key <- r2_update_manifest(p$bucket,r2_dir,create=TRUE,objects=objects)
    if (purge_cache) r2_purge_cache_after_change(p$bucket,key)
    bucket_url(p$bucket,key)
  },character(1),USE.NAMES=FALSE)
  # tables reading below the directory may have read a manifest that was behind
  forget_scans(path)
  invisible(manifests)
}

#' remove parquet files from Cloudflare R2, S3 or local directories
#'
#' @description
#' Removes files, or all parquet files in directories of (partitioned) parquet files together with the
#' manifests below them, from buckets or local directories, and updates the manifests of the bucket directories
#' above. Removed files can't be recovered.
#'
#' @details
#' * The path of a file in a bucket removes that file whatever its type, like a JSON index uploaded with
#'   [upload_parquet()], or a manifest, which takes the directory's manifest away until [write_manifest()] or the
#'   next upload writes it again. Local files have to be parquet files, [unlink()] removes others.
#' * The path of a directory removes its parquet files and the manifests (`_manifest.json`, see
#'   [write_manifest()]) below it; other files in it are left in place. Local directories left empty get
#'   removed as well.
#' * A lazy table from [parquet_tbl()] removes the paths it was created from, the remote ones also when the
#'   table was read from the local cache.
#'
#' All paths get resolved to files first, so a path that does not exist stops the call before anything is
#' removed. The files are then removed in one batch per bucket. If an R2 bucket is served via a custom domain,
#' the removed files get purged from the Cloudflare cache, with the same requirements as for [upload_parquet()].
#'
#' Since a manifest lists all parquet files below its directory, the existing manifests of the bucket
#' directories above removed files get updated, so removing a file or a partition from a dataset keeps the
#' dataset's manifest current: the removed files are dropped from what the manifests record, without listing
#' their directories, and a manifest left without files is removed. The manifests are written conditionally,
#' like [upload_parquet()] writes them, and a manifest that could not be updated is reported with a warning
#' after the files are gone. [data_to_parquet()] writes no manifests for local paths, so there are none to update
#' above local files. Copies in the cache directory are removed with [remove_parquet_from_cache()] instead, which
#' keeps the manifests there in line, and files at URLs can't be removed, only through their bucket path.
#'
#' @inheritSection upload_parquet Credentials
#' @param path what to remove:
#'   * `r2://bucket/path`, `s3://bucket/path` or local path of a file or of a directory of (partitioned)
#'     parquet files, or a vector of these;
#'   * a lazy DuckDB table from [parquet_tbl()].
#' @param purge_cache `TRUE` (default) or `FALSE`: purge the removed files from the Cloudflare cache of the
#'   bucket's custom domains. Ignored for S3 and local paths.
#' @return (invisibly) the removed files as `r2://bucket/path`, `s3://bucket/path` or local paths.
#' @export
remove_parquet <- function(path,purge_cache=TRUE) {
  files <- NULL
  if (inherits(path,"tbl_lazy")) {
    files <- attr(path,"r2parquet_paths")
    path <- table_source(path)
  }
  if (!is.character(path) || length(path)==0) {
    stop("path has to be one or more paths of the form r2://bucket/path or s3://bucket/path, local paths, or a lazy table from parquet_tbl.")
  }
  is_bucket <- is_bucket_path(path)
  at_url <- is_url(path)
  if (any(at_url)) {
    # a table whose files are local was read from copies in the cache
    stop(paste0("Files at URLs can't be removed, use the r2://bucket/path or s3://bucket/path form instead: ",
                paste0(path[at_url],collapse=", "),".",
                if (!is.null(files) && !any(is_remote(files))) " The copies the table reads from the cache are removed with remove_parquet_from_cache()."))
  }
  local <- lapply(path[!is_bucket],local_removal)
  resolved <- lapply(path[is_bucket],function(p){
    p <- r2_split_path(p)
    r2_bucket <- p$bucket
    r2_path <- p$path
    if (r2_path=="") stop("Please specify the path of the parquet file or directory to remove in ",bucket_label(r2_bucket),".")
    objects <- r2_objects(r2_bucket,r2_path)
    manifests <- character()
    directories <- character()
    if (r2_path %in% objects$key) {
      r2_paths <- r2_path
      # the manifests above a parquet file list it, those of other files don't
      if (grepl("\\.parquet$",r2_path)) directories <- r2_ancestors(r2_path)
    } else if (nrow(objects)>0) {
      r2_paths <- objects$key[grepl("\\.parquet$",objects$key)]
      if (length(r2_paths)==0) stop(paste0("No parquet files found in ",r2_path," in ",bucket_label(r2_bucket),"."))
      # the manifests above the directory list its files, the ones below go with it
      manifests <- objects$key[basename(objects$key)==manifest_name]
      directories <- r2_ancestors(r2_path)
    } else {
      stop(paste0("File or directory ",r2_path," does not exist in ",bucket_label(r2_bucket),"."))
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
    manifests <- c(manifests,r2_update_manifests(r2_bucket,directories,removed=r2_paths))
    if (purge_cache) r2_purge_cache_after_change(r2_bucket,c(r2_paths,manifests))
    message(paste0("Removed ",plural(length(r2_paths),"file")," from ",bucket_label(r2_bucket),"."))
    removed <- c(removed,bucket_url(r2_bucket,r2_paths))
  }
  if (length(local)>0) {
    files <- unique(unlist(lapply(local,`[[`,"files")))
    manifests <- unique(unlist(lapply(local,`[[`,"manifests")))
    unlink(c(files,manifests))
    # directories left empty, the deepest first
    directories <- unique(unlist(lapply(local,`[[`,"directories")))
    for (d in directories[order(-nchar(directories))]) {
      if (dir.exists(d) && length(dir(d,all.files=TRUE,no..=TRUE))==0) unlink(d,recursive=TRUE)
    }
    message(paste0("Removed ",plural(length(files),"local file"),"."))
    removed <- c(removed,files)
  }
  forget_scans(path)
  invisible(removed)
}

# whether a local file starts with the parquet magic bytes, PARE for encrypted footers
is_parquet_file <- function(path){
  con <- file(path,"rb")
  on.exit(close(con))
  magic <- readBin(con,"raw",4)
  # compared as bytes, rawToChar() fails on files starting with a nul byte
  identical(magic,charToRaw("PAR1")) || identical(magic,charToRaw("PARE"))
}

# the files, manifests and directories that removing a local path removes, nothing removed yet
local_removal <- function(path){
  path <- sub("(.)[/\\\\]+$","\\1",path)
  normalized <- function(p)gsub("\\\\","/",normalizePath(p,mustWork=FALSE))
  cache_dir <- normalized(cache_directory())
  if (file.exists(path) && (normalized(path)==cache_dir || startsWith(normalized(path),paste0(cache_dir,"/")))) {
    stop(paste0(path," is in the cache directory, remove copies with remove_parquet_from_cache() and the remote path."))
  }
  if (dir.exists(path)) {
    files <- dir(path,recursive=TRUE,full.names=TRUE,all.files=TRUE)
    parquet <- files[grepl("\\.parquet$",files)]
    if (length(parquet)==0) stop(paste0("No parquet files found in ",path,"."))
    directories <- c(path,dir(path,recursive=TRUE,full.names=TRUE,include.dirs=TRUE,all.files=TRUE))
    list(files=parquet,manifests=files[basename(files)==manifest_name],directories=directories[dir.exists(directories)])
  } else if (file.exists(path)) {
    # a guard against removing other files by mistake, truncated parquet files still start with the magic bytes
    if (!is_parquet_file(path)) stop(paste0(path," is not a parquet file, use unlink() to remove other local files."))
    list(files=path,manifests=character(),directories=character())
  } else {
    stop(paste0("File or directory ",path," does not exist (relative to the working directory ",getwd(),")."))
  }
}

#' presigned URL for a file on Cloudflare R2 or S3
#'
#' @description
#' URLs that grant time limited access to single files in a private bucket, without credentials. Anyone with
#' the URL can download the file or, for `method = "PUT"`, upload a file to that path until the URL expires, so
#' treat the URL like a password.
#'
#' @details
#' The URLs are signed locally, creating them does not check that the files exist. The URLs stop working when
#' the credentials they were signed with are revoked, and for temporary S3 credentials when these expire.
#'
#' @inheritSection upload_parquet Credentials
#' @param path file as `r2://bucket/path` or `s3://bucket/path`, or a vector of these.
#' @param expires seconds the URLs are valid for, from 1 to 604800 (7 days), default `3600` (one hour).
#' @param method HTTP method the URLs are valid for:
#'   * `"GET"` (default): download the file;
#'   * `"HEAD"`: get the file's metadata;
#'   * `"PUT"`: upload a file.
#' @return the presigned URLs, one per path.
#' @export
presigned_url <- function(path,expires=3600,method=c("GET","HEAD","PUT")) {
  method <- match.arg(method)
  if (!is.numeric(expires) || length(expires)!=1 || expires<1 || expires>604800) {
    stop("expires has to be between 1 and 604800 seconds (7 days).")
  }
  if (!is.character(path)) stop("path has to be one or more paths of the form r2://bucket/path or s3://bucket/path.")
  vapply(path,function(p){
    p <- r2_split_path(p)
    if (p$path=="" || p$directory) stop("The path has to be the path of a file, got ",bucket_url(p$bucket,p$path),".")
    store <- bucket_store(p$bucket)
    sigv4_presigned_url(method,store$endpoint,r2_canonical_path(if (store$url_style=="path") store$bucket,p$path),expires,
                        access_key=store$access_key,secret=store$secret,region=store$region,token=store$token)
  },character(1),USE.NAMES=FALSE)
}

# bucket and path of r2://bucket/path, the path without leading or trailing slashes, and whether the path was
# given as a directory with a trailing slash; the bucket of s3://bucket/path is s3://bucket, see bucket_url
r2_split_path <- function(path){
  if (!is.character(path) || length(path)!=1) stop("Expected a single r2:// or s3:// path, got ",length(path),".")
  parts <- regmatches(path,regexec("^(r2|s3)://([^/]+)/?(.*)$",path))[[1]]
  if (length(parts)!=4) stop("Paths have to be of the form r2://bucket/path or s3://bucket/path, got ",path,".")
  r2_path <- sub("^/+","",parts[4])
  list(bucket=paste0(if (parts[2]=="s3") "s3://",parts[3]),path=sub("/+$","",r2_path),directory=endsWith(r2_path,"/"))
}

# path or glob pattern matching the parquet file or all parquet files in the directory
# directories of parquet files are commonly named like a parquet file, so paths ending in .parquet get checked
r2_parquet_glob <- function(r2_bucket,r2_path,is_directory=FALSE){
  if (is_directory || !grepl("\\.parquet$",r2_path) || !r2_object_exists(r2_bucket,r2_path)) {
    r2_path <- paste0(r2_path,if (r2_path!="") "/","**/*.parquet")
  }
  r2_path
}
