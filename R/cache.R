# Local copies of parquet files on R2 or at URLs, kept current by comparing etags, and the manifests that
# list the files of a dataset with their etags. Manifests are JSON files named _manifest.json with one entry per
# parquet file, paths relative to the manifest's directory. upload_parquet writes them next to the data on R2
# so that directories can be cached from public URLs, where listing is not possible, and the cache keeps one
# per directory to know which files it holds.

manifest_name <- "_manifest.json"

# directory of the cache: TRUE picks R2PARQUET_CACHE_PATH or the user cache directory, otherwise the path given
cache_directory <- function(cache){
  if (isTRUE(cache)) {
    cache <- Sys.getenv("R2PARQUET_CACHE_PATH")
    if (cache=="") cache <- tools::R_user_dir("r2parquet","cache")
  } else if (!is.character(cache) || length(cache)!=1 || is.na(cache) || cache=="") {
    stop("cache has to be TRUE or the path of the cache directory.")
  }
  sub("(.)/+$","\\1",path.expand(cache))
}

# the local path of a remote path in the cache, local paths are not cached and have no place there
remote_cache_location <- function(path,cache_dir){
  if (!startsWith(path,"r2://") && !grepl("^https?://",path)) stop("Expected an r2://bucket/path or a URL, got ",path,".")
  cache_location(path,cache_dir)
}

# etag without the quotes and the weak validator prefix headers and listings put around it
clean_etag <- function(etag){
  etag <- sub("^W/","",trimws(as.character(etag)))
  gsub("\"","",etag,fixed=TRUE)
}

# the files of a manifest as a data frame, modification time in UTC
manifest_files <- function(path=character(),size=NA_real_,etag=NA_character_,modified=NA_real_){
  if (inherits(modified,"POSIXt")) modified <- as.numeric(modified)
  if (is.character(modified)) modified <- as.numeric(as.POSIXct(modified,format="%Y-%m-%dT%H:%M:%OS",tz="UTC"))
  n <- length(path)
  data.frame(path=as.character(path),size=rep_len(as.numeric(size),n),etag=rep_len(as.character(etag),n),
             modified=.POSIXct(rep_len(as.numeric(modified),n),tz="UTC"))
}

# JSON text of a manifest, directory says whether it lists all parquet files below its directory, as the
# manifests on R2 and those of cached directories do, or only the cached files in the directory
manifest_json <- function(files,directory=TRUE){
  entries <- data.frame(path=files$path,size=files$size,etag=files$etag,
                        modified=format(files$modified,"%Y-%m-%dT%H:%M:%SZ",tz="UTC"))
  jsonlite::toJSON(list(directory=directory,files=entries),dataframe="rows",auto_unbox=TRUE,pretty=TRUE,na="null")
}

# the files listed in manifest JSON text, with the directory flag as attribute, an error if it is not a manifest
parse_manifest <- function(text){
  manifest <- tryCatch(jsonlite::fromJSON(text,simplifyVector=TRUE),error=function(e)NULL)
  if (!is.list(manifest) || !("files" %in% names(manifest))) stop("Not a manifest.")
  files <- manifest$files
  files <- if (length(files)==0) manifest_files() else manifest_files(files$path,files$size,files$etag,files$modified)
  structure(files,directory=isTRUE(manifest$directory))
}

# the files listed in the manifest at a local path, none if there is no manifest
read_local_manifest <- function(file){
  if (!file.exists(file)) return(manifest_files())
  tryCatch(parse_manifest(paste0(readLines(file,warn=FALSE,encoding="UTF-8"),collapse="\n")),
           error=function(e)manifest_files())
}

# plain GET requests to public URLs
http_get <- function(url){
  httr::GET(url)
}

# the headers of a file at a URL via a request for its first byte, a HEAD request would do but fails on presigned
# URLs, which are signed for GET only
http_probe <- function(url){
  httr::GET(url,httr::add_headers(Range="bytes=0-0"))
}

# download from a public URL to a local file
http_download <- function(url,file){
  response <- httr::GET(url,httr::write_disk(file,overwrite=TRUE))
  if (httr::status_code(response)>=400) {
    unlink(file)
    stop(paste0("Downloading ",url," failed with status ",httr::status_code(response),"."))
  }
  invisible(file)
}

# download an object from R2 to a local file
r2_get_object <- function(r2_bucket,r2_path,file){
  response <- r2_request("GET",r2_bucket,r2_path,config=httr::write_disk(file,overwrite=TRUE))
  if (httr::status_code(response)>=400) {
    unlink(file)
    stop(paste0("Downloading r2://",r2_bucket,"/",r2_path," failed with status ",httr::status_code(response),"."))
  }
  invisible(file)
}

# where a path on R2 or a URL gets cached, the local path mirrors r2://bucket/path as r2/<account id>/bucket/path,
# since bucket names are only unique within an account, and URLs as <scheme>/<host>/<path> without the query string
cache_location <- function(path,cache_dir){
  if (startsWith(path,"r2://")) {
    p <- r2_split_path(path)
    r2_path <- sub("/+$","",p$path)
    if (r2_path=="") stop("Please specify the path of the parquet file or directory to cache in R2 bucket ",p$bucket,".")
    account <- r2_credentials("R2_ACCOUNT_ID")[["R2_ACCOUNT_ID"]]
    return(list(local=file.path(cache_dir,"r2",account,p$bucket,r2_path),bucket=p$bucket,path=r2_path,url=NULL))
  }
  url <- httr::parse_url(path)
  segments <- strsplit(url$path %||% "","/",fixed=TRUE)[[1]]
  segments <- vapply(segments[segments!=""],utils::URLdecode,"",USE.NAMES=FALSE)
  if (length(segments)==0 || any(segments %in% c(".","..")) || is.null(url$hostname)) {
    stop("The URL has to point to a parquet file or a directory on a host, got ",path,".")
  }
  list(local=do.call(file.path,as.list(c(cache_dir,url$scheme,url$hostname,segments))),bucket=NULL,path=NULL,url=path)
}

# the remote files of the dataset with their etags, relative to the manifest directory, and a function to download one
r2_cache_source <- function(location){
  objects <- r2_objects(location$bucket,location$path)
  is_file <- location$path %in% objects$key
  if (!is_file) objects <- objects[grepl("\\.parquet$",objects$key),]
  if (nrow(objects)==0) stop("File or directory ",location$path," does not exist in R2 bucket ",location$bucket,".")
  files <- manifest_files(if (is_file) basename(location$path) else substring(objects$key,nchar(location$path)+2),
                          objects$size,objects$etag,objects$modified)
  files$key <- objects$key
  bucket <- location$bucket
  list(is_file=is_file,files=files,download=function(key,file)r2_get_object(bucket,key,file))
}

# the same for a URL: a file answers a request with its etag in the headers, a directory needs a manifest next
# to the files
url_cache_source <- function(location){
  url <- location$url
  base <- sub("[?#].*$","",url)
  if (!endsWith(base,"/")) {
    response <- http_probe(url)
    status <- httr::status_code(response)
    if (status<400) {
      headers <- httr::headers(response)
      # a partial response gives the total size in the content range, a server that ignores the range the whole file
      size <- if (status==206) sub("^.*/","",headers[["content-range"]] %||% "") else headers[["content-length"]] %||% ""
      files <- manifest_files(utils::URLdecode(basename(base)),size=if (grepl("^[0-9]+$",size)) as.numeric(size) else NA_real_,
                              etag=clean_etag(headers[["etag"]] %||% NA_character_),
                              modified=if (!is.null(headers[["last-modified"]])) httr::parse_http_date(headers[["last-modified"]]) else NA_real_)
      files$key <- url
      return(list(is_file=TRUE,files=files,download=http_download))
    }
    if (status!=404) stop(paste0("Checking ",url," failed with status ",status,"."))
  }
  base <- sub("/+$","",base)
  manifest_url <- paste0(base,"/",manifest_name)
  response <- http_get(manifest_url)
  if (httr::status_code(response)>=400) {
    stop(paste0("No file at ",url," and no manifest at ",manifest_url,
                ". Directories at URLs can only be cached if they have a manifest, see write_manifest, ",
                "or pass the URLs of the files or the r2://bucket/path form with credentials."))
  }
  files <- parse_manifest(httr::content(response,"text",encoding="UTF-8"))
  if (nrow(files)==0) stop(paste0("The manifest at ",manifest_url," lists no files."))
  files$key <- paste0(base,"/",vapply(files$path,utils::URLencode,"",USE.NAMES=FALSE))
  list(is_file=FALSE,files=files,download=http_download)
}

# download a remote file to its place in the cache, via a temporary file so interrupted downloads leave nothing
# behind, checking the content against the etag where it is an MD5 hash, as for files uploaded in one request
cache_download <- function(source,file,target){
  dir.create(dirname(target),recursive=TRUE,showWarnings=FALSE)
  tmp <- paste0(target,".download")
  on.exit(unlink(tmp))
  source$download(file$key,tmp)
  if (grepl("^[0-9a-f]{32}$",file$etag) && digest::digest(file=tmp,algo="md5")!=file$etag) {
    stop(paste0("The download of ",file$key," does not match its etag, the file may have changed while downloading. ",
                "Try again."))
  }
  if (!file.rename(tmp,target)) stop(paste0("Could not move the download of ",file$key," to ",target,"."))
}

# bring the local copy of the dataset in line with the remote files, returns the local path
sync_cache <- function(location,source){
  local <- location$local
  manifest_dir <- if (source$is_file) dirname(local) else local
  dir.create(manifest_dir,recursive=TRUE,showWarnings=FALSE)
  manifest_file <- file.path(manifest_dir,manifest_name)
  known <- read_local_manifest(manifest_file)
  remote <- source$files
  targets <- file.path(manifest_dir,remote$path)
  known_etag <- known$etag[match(remote$path,known$path)]
  current <- !is.na(known_etag) & !is.na(remote$etag) & known_etag==remote$etag & file.exists(targets) &
    (is.na(remote$size) | file.size(targets)==remote$size)
  current[is.na(current)] <- FALSE
  if (any(!current)) {
    size <- sum(remote$size[!current],na.rm=TRUE)
    message("Downloading ",sum(!current)," file",if (sum(!current)>1) "s",
            if (size>0) paste0(" (",format(structure(size,class="object_size"),units="auto",standard="SI"),")"),
            " to ",local,".")
    for (i in which(!current)) cache_download(source,remote[i,],targets[i])
  }
  if (!source$is_file) {
    # the table reads every parquet file below the directory, so files that are gone remotely have to go
    stale <- setdiff(dir(local,"\\.parquet$",recursive=TRUE),remote$path)
    unlink(file.path(local,stale))
    for (directory in rev(list.dirs(local,recursive=TRUE)[-1])) {
      if (length(dir(directory,all.files=TRUE,no..=TRUE))==0) unlink(directory,recursive=TRUE)
    }
    files <- remote
    directory <- TRUE
  } else {
    files <- rbind(known[known$path!=remote$path,c("path","size","etag","modified")],remote[,c("path","size","etag","modified")])
    directory <- isTRUE(attr(known,"directory"))
  }
  writeLines(manifest_json(files,directory),manifest_file,useBytes=TRUE)
  local
}

# the cached copy without checking the remote, for when the remote can't be reached: a directory whose
# manifest covers it, or a file listed in the manifest of its directory
cached_copy <- function(location){
  local <- location$local
  if (isTRUE(attr(read_local_manifest(file.path(local,manifest_name)),"directory"))) return(local)
  known <- read_local_manifest(file.path(dirname(local),manifest_name))
  if (basename(local) %in% known$path && file.exists(local)) return(local)
  NULL
}

#' local copies of parquet files on R2 or at URLs
#'
#' @description
#' Downloads parquet files, or directories of (partitioned) parquet files, to a local cache directory and
#' keeps them current, so that data used regularly can be queried locally instead of over the network.
#' `parquet_tbl(cache = TRUE)` uses it to read from the cache. Each call checks the remote files and only
#' downloads files whose etag changed or that are new, removes files from the cache that are gone remotely,
#' and records the etags in a manifest (`_manifest.json`) in the cache directory. If the remote can't be
#' reached, e.g. offline, an existing cached copy gets used with a warning.
#'
#' Paths on R2 (`r2://bucket/path`) get listed with the same credentials as `upload_parquet`. URLs of files,
#' including presigned URLs, get checked with a request for their first byte, which returns the etag.
#' Directories at URLs can't be listed, they need a manifest next to the files, which `upload_parquet` and
#' `data_to_parquet` write for directories uploaded to R2 and `write_manifest` writes for existing ones.
#' Local paths are returned as they are.
#'
#' The cache mirrors the remote layout: `r2://bucket/path` is stored as `r2/<account id>/bucket/path`, since bucket
#' names are only unique within an account, and URLs as `<scheme>/<host>/<path>`. The package manages the
#' files below the cache directory, files it does not know about get removed when a directory is synced.
#' Downloads go through a temporary file, and files uploaded by this package get checked against their etag,
#' which is the MD5 hash of the content, so an interrupted download never ends up in the cache.
#' `list_cache` lists what is in the cache, `list_parquet` on the remote paths tells whether the copies are
#' current, and `remove_from_cache` removes copies.
#'
#' @param path URL or `r2://bucket/path` of the parquet file or the directory with (partitioned) parquet
#' files, or a vector of these
#' @param cache `TRUE` to use the directory in the `R2PARQUET_CACHE_PATH` environment variable, or the user
#' cache directory (`tools::R_user_dir("r2parquet", "cache")`) if that is not set, or the path of the cache directory
#' @return the local paths of the cached files or directories, one per path, ready for `parquet_tbl`
#' @export
cache_parquet <- function(path,cache=TRUE) {
  if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
  cache_dir <- cache_directory(cache)
  vapply(path,function(p){
    is_r2 <- startsWith(p,"r2://")
    if (!is_r2 && !grepl("^https?://",p)) return(p)
    location <- cache_location(p,cache_dir)
    source <- tryCatch(if (is_r2) r2_cache_source(location) else url_cache_source(location),error=function(e)e)
    if (inherits(source,"error")) {
      local <- cached_copy(location)
      if (is.null(local)) stop(source)
      warning(paste0("Checking ",p," failed, using the cached copy in ",local,". ",conditionMessage(source)),call.=FALSE)
      return(local)
    }
    sync_cache(location,source)
  },character(1),USE.NAMES=FALSE)
}

# the remote paths that local paths in the cache stand for, the reverse of cache_location
cache_remote_path <- function(local,cache_dir){
  vapply(local,function(l){
    segments <- strsplit(substring(l,nchar(cache_dir)+2),"/",fixed=TRUE)[[1]]
    if (segments[1]=="r2") {
      paste0("r2://",segments[3],"/",paste0(segments[-(1:3)],collapse="/"))
    } else {
      paste0(segments[1],"://",segments[2],"/",paste0(vapply(segments[-(1:2)],utils::URLencode,"",USE.NAMES=FALSE),collapse="/"))
    }
  },"",USE.NAMES=FALSE)
}

# the files the manifests in the cache list and that are there, with the remote path they are a copy of
cache_contents <- function(cache_dir){
  manifests <- dir(cache_dir,"^_manifest\\.json$",recursive=TRUE,full.names=TRUE,all.files=TRUE)
  files <- lapply(manifests,function(manifest){
    known <- read_local_manifest(manifest)
    known$local <- file.path(dirname(manifest),known$path)
    known
  })
  empty <- manifest_files()
  empty$local <- character()
  files <- do.call(rbind,c(list(empty),files))
  files <- files[!duplicated(files$local) & file.exists(files$local),]
  files$path <- cache_remote_path(files$local,cache_dir)
  files <- files[order(files$path),]
  rownames(files) <- NULL
  files[,c("path","local","size","modified","etag")]
}

# whether remote files have a copy in the cache, and whether it is current, by the etags the manifests recorded
cache_status <- function(path,etag){
  status <- rep(NA_character_,length(path))
  remote <- startsWith(path,"r2://") | grepl("^https?://",path)
  if (!any(remote)) return(status)
  cache_dir <- cache_directory(TRUE)
  local <- vapply(path[remote],function(p)tryCatch(cache_location(p,cache_dir)$local,error=function(e)NA_character_),"",USE.NAMES=FALSE)
  known <- cache_contents(cache_dir)
  known <- known$etag[match(local,known$local)]
  remote_etag <- etag[remote]
  status[remote] <- ifelse(!file.exists(local),"not cached",
                           ifelse(is.na(remote_etag),NA_character_,
                                  ifelse(!is.na(known) & known==remote_etag,"current","stale")))
  status[remote][is.na(local)] <- NA_character_
  status
}

# after removing a path from the cache, drop it from the manifests of the directories above it, a directory manifest
# that lost entries no longer covers its directory, and remove directories that are left empty
prune_manifests <- function(local,cache_dir){
  directory <- dirname(local)
  relative <- basename(local)
  while (startsWith(directory,paste0(cache_dir,"/"))) {
    manifest_file <- file.path(directory,manifest_name)
    if (file.exists(manifest_file)) {
      known <- read_local_manifest(manifest_file)
      keep <- known$path!=relative & !startsWith(known$path,paste0(relative,"/"))
      if (!any(keep)) {
        unlink(manifest_file)
      } else if (!all(keep)) {
        writeLines(manifest_json(known[keep,],directory=FALSE),manifest_file,useBytes=TRUE)
      }
    }
    if (length(dir(directory,all.files=TRUE,no..=TRUE))==0) unlink(directory,recursive=TRUE)
    relative <- paste0(basename(directory),"/",relative)
    directory <- dirname(directory)
  }
}

#' list the files in the local cache
#'
#' @description
#' Lists the parquet files that `cache_parquet` and `parquet_tbl(cache = TRUE)` downloaded into the cache
#' directory, the one in the `R2PARQUET_CACHE_PATH` environment variable or the user cache directory, with the
#' remote path each is a copy of and the etag recorded when it was downloaded. `list_parquet` on the remote
#' paths tells whether the copies are still current. A cache at a directory of your own, passed to
#' `cache_parquet` as a path, is a plain directory, list it with `list_parquet` and remove it with `unlink`.
#'
#' @param path `r2://bucket/path` or URL of a parquet file or directory, or a vector of these, to list only
#' the copies of these. `r2://` paths need the `R2_ACCOUNT_ID` environment variable, since the cache is kept
#' per account. By default everything in the cache gets listed, which needs no credentials.
#' @return a tibble with one row per cached parquet file and the columns `path`, the `r2://bucket/path` or
#' URL it is a copy of, `local`, the path of the copy, `size` in bytes, `modified`, the modification time of
#' the remote file in UTC, and `etag`, the etag of the remote file when it was downloaded
#' @export
list_cache <- function(path=NULL) {
  cache_dir <- cache_directory(TRUE)
  files <- dplyr::as_tibble(cache_contents(cache_dir))
  if (!is.null(path)) {
    if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
    local <- vapply(path,function(p)remote_cache_location(p,cache_dir)$local,"",USE.NAMES=FALSE)
    keep <- Reduce(`|`,lapply(local,function(l)files$local==l | startsWith(files$local,paste0(l,"/"))),logical(nrow(files)))
    files <- files[keep,]
  }
  files
}

#' remove files from the local cache
#'
#' @description
#' Removes the copies of parquet files or directories from the cache directory, the one in the
#' `R2PARQUET_CACHE_PATH` environment variable or the user cache directory, and drops them from the manifests
#' there. The remote files stay as they are, use `remove_parquet` for those. The next `cache_parquet` or
#' `parquet_tbl(cache = TRUE)` on the paths downloads them again. All paths get resolved before anything is
#' removed, a path without a copy in the cache is an error.
#'
#' @param path `r2://bucket/path` or URL of a parquet file or directory with (partitioned) parquet files, or a
#' vector of these. `r2://` paths need the `R2_ACCOUNT_ID` environment variable, since the cache is kept per account.
#' @return (invisibly) the removed files as `r2://bucket/path` or URL, as `list_cache` lists them
#' @export
remove_from_cache <- function(path) {
  if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
  cache_dir <- cache_directory(TRUE)
  local <- vapply(path,function(p)remote_cache_location(p,cache_dir)$local,"",USE.NAMES=FALSE)
  missing <- !file.exists(local)
  if (any(missing)) stop(paste0("Not in the cache: ",paste0(path[missing],collapse=", "),"."))
  removed <- list_cache(path)$path
  for (l in unique(local)) {
    # may have gone with a directory removed before it
    if (!file.exists(l)) next
    unlink(l,recursive=TRUE)
    prune_manifests(l,cache_dir)
  }
  message(paste0("Removed ",length(removed)," file",if (length(removed)!=1) "s"," from the cache."))
  invisible(removed)
}

`%||%` <- function(x,y) if (is.null(x)) y else x
