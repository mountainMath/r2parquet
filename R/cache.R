# Local copies of parquet files on R2 or at URLs, kept current by comparing etags, and the manifests that
# list the files of a dataset with their etags. Manifests are JSON files named _manifest.json with one entry per
# parquet file, paths relative to the manifest's directory, plus the footer metadata of the files: the entries
# all files share as the manifest's metadata, the rest per file. upload_parquet writes them next to the data on
# R2 so that directories can be cached from public URLs, where listing is not possible, and so that clients
# other than this package can read the dataset's metadata without parquet footers, and the cache keeps one per
# directory to know which files it holds. The format is documented in write_manifest.

manifest_name <- "_manifest.json"

# directory of the cache: R2PARQUET_CACHE_PATH or the user cache directory
cache_directory <- function(){
  cache <- Sys.getenv("R2PARQUET_CACHE_PATH")
  if (cache=="") cache <- tools::R_user_dir("r2parquet","cache")
  # forward slashes throughout, dirname() and dir() return them on Windows too, so paths compare
  sub("(.)/+$","\\1",gsub("\\\\","/",path.expand(cache)))
}

# the local path of a remote path in the cache, local paths are not cached and have no place there
# the checks of the session: for each local path cache_lookup checked against the remote, whether the copy was
# current, so a dataset gets checked once per session, remote data rarely changes. clear_cache_checks() resets it
cache_checks <- new.env(parent=emptyenv())

# drop the checks of a local path, of the files below it and of the directories above it, since any of these may
# have changed when the path was updated or removed
forget_checks <- function(local){
  # all.names, since a relative cache directory like .cache gives keys that ls() would otherwise hide
  for (checked in ls(cache_checks,all.names=TRUE)) {
    if (checked==local || startsWith(checked,paste0(local,"/")) || startsWith(local,paste0(checked,"/"))) rm(list=checked,envir=cache_checks)
  }
}

remote_cache_location <- function(path,cache_dir){
  if (!is_remote(path)) stop("Expected an r2://bucket/path or a URL, got ",path,".")
  cache_location(path,cache_dir)
}

# etag without the quotes and the weak validator prefix headers and listings put around it
clean_etag <- function(etag){
  etag <- sub("^W/","",trimws(as.character(etag)))
  gsub("\"","",etag,fixed=TRUE)
}

# the files of a manifest as a data frame, modification time in UTC, metadata a list with the footer metadata of
# each file (the whole of it, the common entries included), empty lists where it is not known
manifest_files <- function(path=character(),size=NA_real_,etag=NA_character_,modified=NA_real_,metadata=NULL){
  if (inherits(modified,"POSIXt")) modified <- as.numeric(modified)
  if (is.character(modified)) modified <- as.numeric(as.POSIXct(modified,format="%Y-%m-%dT%H:%M:%OS",tz="UTC"))
  n <- length(path)
  files <- data.frame(path=as.character(path),size=rep_len(as.numeric(size),n),etag=rep_len(as.character(etag),n),
                      modified=.POSIXct(rep_len(as.numeric(modified),n),tz="UTC"))
  files$metadata <- if (is.null(metadata)) rep(list(list()),n) else unname(metadata)
  files
}

# the metadata entries all files share, with identical values, what parquet_metadata returns for the directory
common_metadata <- function(metadata){
  if (length(metadata)==0) return(list())
  common <- metadata[[1]]
  for (entry in metadata[-1]) {
    common <- common[vapply(names(common),function(key)identical(common[[key]],entry[[key]]),logical(1))]
    if (length(common)==0) break
  }
  common
}

# JSON text of a manifest, directory says whether it lists all parquet files below its directory, as the
# manifests on R2 and those of cached directories do, or only the cached files in the directory. The metadata
# the files share goes into the manifest's metadata, entries that differ between files or that only some files
# have into the files' metadata, except for the column descriptions, which are too large to repeat per file
manifest_json <- function(files,directory=TRUE){
  metadata <- if (is.null(files$metadata)) rep(list(list()),nrow(files)) else files$metadata
  common <- common_metadata(metadata)
  if (length(common)==0) common <- structure(list(),names=character())
  modified <- format(files$modified,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  entries <- lapply(seq_len(nrow(files)),function(i){
    own <- metadata[[i]][setdiff(names(metadata[[i]]),names(common))]
    c(list(path=files$path[i],size=files$size[i],etag=files$etag[i],modified=modified[i]),
      if (length(own)>0) list(metadata=own))
  })
  jsonlite::toJSON(list(directory=directory,metadata=common,files=entries),auto_unbox=TRUE,pretty=TRUE,na="null",null="null",digits=NA)
}

# the files listed in manifest JSON text, with the directory flag and whether the manifest carries metadata as
# attributes (manifests written before the metadata was added don't), an error if it is not a manifest. The
# metadata of each file is the manifest's metadata with the file's own entries added
parse_manifest <- function(text){
  manifest <- tryCatch(jsonlite::fromJSON(text,simplifyDataFrame=FALSE,simplifyMatrix=FALSE),error=function(e)NULL)
  if (!is.list(manifest) || !("files" %in% names(manifest))) stop("Not a manifest.")
  has_metadata <- is.list(manifest$metadata)
  common <- if (has_metadata) manifest$metadata else list()
  entries <- manifest$files
  if (length(entries)==0) return(structure(manifest_files(),directory=isTRUE(manifest$directory),has_metadata=has_metadata))
  field <- function(name,default){
    coerce <- if (is.numeric(default)) as.numeric else as.character
    vapply(entries,function(entry)if (is.null(entry[[name]])) default else coerce(entry[[name]]),default)
  }
  metadata <- lapply(entries,function(entry){
    own <- if (is.list(entry$metadata)) entry$metadata else list()
    common[names(own)] <- own
    common
  })
  files <- manifest_files(field("path",NA_character_),field("size",NA_real_),field("etag",NA_character_),field("modified",NA_character_),metadata)
  structure(files,directory=isTRUE(manifest$directory),has_metadata=has_metadata)
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

# the headers of a file at a URL: a HEAD request, or a request for its first byte where HEAD is not allowed, as
# on presigned URLs, which are signed for GET only; a server that ignores the range sends the whole file
http_probe <- function(url){
  response <- httr::HEAD(url)
  if (!(httr::status_code(response) %in% c(403L,405L))) return(response)
  httr::GET(url,httr::add_headers(Range="bytes=0-0"))
}

# a key or relative path encoded for the path of a URL: URLencode leaves the characters that are reserved in
# URLs alone, which is right for the slashes and the = of hive partitions but not for # and ?, which would end
# the path
url_path_encode <- function(path){
  gsub("?","%3F",gsub("#","%23",vapply(path,utils::URLencode,"",USE.NAMES=FALSE),fixed=TRUE),fixed=TRUE)
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
    r2_path <- p$path
    if (r2_path=="") stop("Please specify the path of the parquet file or directory to cache in R2 bucket ",p$bucket,".")
    account <- r2_credentials("R2_ACCOUNT_ID")[["R2_ACCOUNT_ID"]]
    return(list(local=file.path(cache_dir,"r2",account,p$bucket,r2_path),bucket=p$bucket,path=r2_path,url=NULL,cache_dir=cache_dir))
  }
  url <- httr::parse_url(path)
  segments <- strsplit(url$path %||% "","/",fixed=TRUE)[[1]]
  segments <- vapply(segments[segments!=""],utils::URLdecode,"",USE.NAMES=FALSE)
  if (length(segments)==0 || any(segments %in% c(".","..")) || is.null(url$hostname)) {
    stop("The URL has to point to a parquet file or a directory on a host, got ",path,".")
  }
  list(local=do.call(file.path,as.list(c(cache_dir,url$scheme,url$hostname,segments))),bucket=NULL,path=NULL,url=path,
       cache_dir=cache_dir)
}

# the remote files of the dataset with their etags, relative to the manifest directory, and a function to download one
r2_cache_source <- function(location){
  dataset <- r2_dataset_objects(location$bucket,location$path)
  is_file <- dataset$is_file
  objects <- dataset$objects
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
  files$key <- paste0(base,"/",url_path_encode(files$path))
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

# whether paths are on R2 or at URLs, which is what the cache mirrors, as opposed to local paths
is_remote <- function(path){
  startsWith(path,"r2://") | grepl("^https?://",path)
}

# the directory holding the manifest of a cached path, a file is listed in the manifest of its directory
manifest_directory <- function(location,source){
  if (source$is_file) dirname(location$local) else location$local
}

# the directories above a path in the cache, nearest first, up to the top-level directories of the cache (the
# cache directory itself holds no manifests), with the path relative to each, as that directory's manifest
# would list it
cache_ancestors <- function(path,cache_dir){
  directory <- dirname(path)
  relative <- basename(path)
  directories <- character()
  relatives <- character()
  while (startsWith(directory,paste0(cache_dir,"/"))) {
    directories <- c(directories,directory)
    relatives <- c(relatives,relative)
    relative <- paste0(basename(directory),"/",relative)
    directory <- dirname(directory)
  }
  list(directory=directories,relative=relatives)
}

# which of the remote files have a current copy: the etag the manifest recorded is the remote one and the
# file is there with the remote size
current_files <- function(manifest_dir,remote){
  known <- read_local_manifest(file.path(manifest_dir,manifest_name))
  targets <- file.path(manifest_dir,remote$path)
  known_etag <- known$etag[match(remote$path,known$path)]
  current <- !is.na(known_etag) & !is.na(remote$etag) & known_etag==remote$etag & file.exists(targets) &
    (is.na(remote$size) | file.size(targets)==remote$size)
  current[is.na(current)] <- FALSE
  current
}

# the manifest to check a cached path against: the one in its manifest directory, or, for a file or partition of
# a cached directory, the nearest one above it, which lists it by its path relative to that directory, with
# the prefix that makes the paths relative to that directory, and whether that manifest exists
covering_manifest <- function(location,source){
  manifest_dir <- manifest_directory(location,source)
  prefix <- ""
  exists <- file.exists(file.path(manifest_dir,manifest_name))
  if (!exists) {
    ancestors <- cache_ancestors(manifest_dir,location$cache_dir)
    found <- which(file.exists(file.path(ancestors$directory,manifest_name)))
    if (length(found)>0) {
      exists <- TRUE
      manifest_dir <- ancestors$directory[found[1]]
      prefix <- paste0(ancestors$relative[found[1]],"/")
    }
  }
  list(dir=manifest_dir,prefix=prefix,path=paste0(prefix,source$files$path),exists=exists)
}

# whether the local copy of the dataset is in line with the remote files without touching it: every remote
# file has a current copy, and a directory holds no parquet files beyond the remote ones, since the table
# reads every file below it
cache_current <- function(location,source){
  manifest <- covering_manifest(location,source)
  remote <- source$files
  remote$path <- manifest$path
  all(current_files(manifest$dir,remote)) &&
    (source$is_file || length(setdiff(dir(location$local,"\\.parquet$",recursive=TRUE),source$files$path))==0)
}

# bring the local copy of the dataset in line with the remote files, returns the local path. A file, or a
# directory below a directory whose manifest lists everything below it, gets recorded in the manifest that
# covers it, so a file or partition of a cached directory does not get a second manifest of its own that the
# directory's syncs would leave behind, and a directory's manifest replaces the manifests below it, which
# it covers
sync_cache <- function(location,source){
  local <- location$local
  manifest <- covering_manifest(location,source)
  known <- read_local_manifest(file.path(manifest$dir,manifest_name))
  if (manifest$prefix!="" && (!manifest$exists || (!source$is_file && !isTRUE(attr(known,"directory"))))) {
    manifest <- list(dir=manifest_directory(location,source),prefix="",path=source$files$path)
    known <- read_local_manifest(file.path(manifest$dir,manifest_name))
  }
  manifest_dir <- manifest$dir
  dir.create(manifest_dir,recursive=TRUE,showWarnings=FALSE)
  manifest_file <- file.path(manifest_dir,manifest_name)
  remote <- source$files
  remote$path <- manifest$path
  targets <- file.path(manifest_dir,remote$path)
  current <- current_files(manifest_dir,remote)
  if (any(!current)) {
    size <- sum(remote$size[!current],na.rm=TRUE)
    message("Downloading ",sum(!current)," file",if (sum(!current)>1) "s",if (size>0) paste0(" (",format_size(size),")"),
            " to ",local,".")
    for (i in which(!current)) cache_download(source,remote[i,],targets[i])
  }
  if (!source$is_file) {
    # the table reads every parquet file below the directory, so files that are gone remotely have to go
    stale <- setdiff(dir(local,"\\.parquet$",recursive=TRUE),source$files$path)
    unlink(file.path(local,stale))
    unlink(dir(local,"^_manifest\\.json$",recursive=TRUE,full.names=TRUE,all.files=TRUE))
    for (directory in rev(list.dirs(local,recursive=TRUE)[-1])) {
      if (length(dir(directory,all.files=TRUE,no..=TRUE))==0) unlink(directory,recursive=TRUE)
    }
    keep <- !startsWith(known$path,manifest$prefix)
    directory <- manifest$prefix=="" || isTRUE(attr(known,"directory"))
  } else {
    keep <- known$path!=remote$path
    directory <- isTRUE(attr(known,"directory"))
  }
  writeLines(manifest_json(sorted_files(rbind(known[keep,manifest_columns],remote[,manifest_columns])),directory),
             manifest_file,useBytes=TRUE)
  patch_ancestor_manifests(manifest_dir,if (source$is_file) remote$path else manifest$prefix,source$is_file,remote,location$cache_dir)
  local
}

manifest_columns <- c("path","size","etag","modified")

# manifest entries in the order of their paths, as listings return them, so patched manifests stay in order
sorted_files <- function(files){
  files <- files[order(files$path),]
  rownames(files) <- NULL
  files
}

# after syncing a path (a file, or a directory as a prefix relative to the manifest directory), replace what
# the manifests of the directories above record below it, so that the manifests above agree with the nearest
# one, which is the one the checks use
patch_ancestor_manifests <- function(manifest_dir,synced,is_file,files,cache_dir){
  ancestors <- cache_ancestors(manifest_dir,cache_dir)
  for (i in seq_along(ancestors$directory)) {
    manifest_file <- file.path(ancestors$directory[i],manifest_name)
    if (!file.exists(manifest_file)) next
    known <- read_local_manifest(manifest_file)
    prefix <- paste0(ancestors$relative[i],"/")
    below <- paste0(prefix,synced)
    keep <- if (is_file) known$path!=below else !startsWith(known$path,below)
    fresh <- files[,manifest_columns]
    fresh$path <- paste0(prefix,files$path)
    writeLines(manifest_json(sorted_files(rbind(known[keep,manifest_columns],fresh)),isTRUE(attr(known,"directory"))),
               manifest_file,useBytes=TRUE)
  }
}

# the cached copy without checking the remote: a directory whose manifest covers it, or a file or directory
# below a directory whose manifest lists or covers it, as the files of a cached partitioned dataset are
cached_copy <- function(location){
  local <- location$local
  if (isTRUE(attr(read_local_manifest(file.path(local,manifest_name)),"directory"))) return(local)
  ancestors <- cache_ancestors(local,location$cache_dir)
  for (i in seq_along(ancestors$directory)) {
    known <- read_local_manifest(file.path(ancestors$directory[i],manifest_name))
    if (ancestors$relative[i] %in% known$path && file.exists(local)) return(local)
    if (isTRUE(attr(known,"directory")) && dir.exists(local)) return(local)
  }
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
#' reached, e.g. offline, an existing cached copy gets used with a warning. Once a dataset is cached,
#' `parquet_tbl` reads the copy by default as long as it is current, and `parquet_tbl(cache = "offline")` reads
#' it without checking, see its `cache` argument.
#'
#' The cache directory is the one in the `R2PARQUET_CACHE_PATH` environment variable, or the user cache
#' directory (`tools::R_user_dir("r2parquet", "cache")`) if that is not set. Set the environment variable, e.g.
#' in `.Renviron`, for a cache local to a project.
#'
#' URLs of files, including presigned URLs, get checked with a request for their first byte, which returns the etag.
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
#' @inheritSection upload_parquet Credentials
#' @param path URL or `r2://bucket/path` of the parquet file or the directory with (partitioned) parquet
#' files, or a vector of these
#' @return the local paths of the cached files or directories, one per path, ready for `parquet_tbl`
#' @export
cache_parquet <- function(path) {
  if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
  cache_dir <- cache_directory()
  vapply(path,function(p){
    is_r2 <- startsWith(p,"r2://")
    if (!is_r2 && !grepl("^https?://",p)) return(p)
    location <- cache_location(p,cache_dir)
    source <- tryCatch(cache_source(location),error=function(e)e)
    if (inherits(source,"error")) {
      local <- cached_copy(location)
      if (is.null(local)) stop(source)
      warning(paste0("Checking ",p," failed, using the cached copy in ",local,". ",conditionMessage(source)),call.=FALSE)
      return(local)
    }
    local <- sync_cache(location,source)
    forget_checks(local)
    cache_checks[[local]] <- TRUE
    local
  },character(1),USE.NAMES=FALSE)
}

# the remote files of a cached path, from R2 or at a URL
cache_source <- function(location){
  if (is.null(location$url)) r2_cache_source(location) else url_cache_source(location)
}

# the paths parquet_tbl(cache = "offline") reads: the copies in the cache, whether current or not, without any
# request to the remote, an error for a path that has no copy
cache_offline <- function(path){
  cache_dir <- cache_directory()
  vapply(path,function(p){
    if (!is_remote(p)) return(p)
    local <- cached_copy(cache_location(p,cache_dir))
    if (is.null(local)) stop(paste0("No copy of ",p," in the cache. Cache it with cache_parquet() or parquet_tbl(cache = TRUE) first."))
    local
  },character(1),USE.NAMES=FALSE)
}

# the paths parquet_tbl(cache = NULL) reads: the copy in the cache where there is one and it is current, the
# remote path otherwise. The remote only gets checked when there is a copy, so paths that were never cached
# cost nothing, and a copy that is out of date is reported but left alone. Each copy gets checked once per
# session, later calls reuse the result from cache_checks until clear_cache_checks(), cache_parquet() or
# remove_from_cache() drop it, so a stale copy is also only reported once. A copy whose remote can't be
# checked, e.g. offline, gets used with a warning like cache_parquet does; that is not a verdict on the copy, so
# it is not remembered and the next call checks again.
cache_lookup <- function(path){
  cache_dir <- cache_directory()
  vapply(path,function(p){
    if (!is_remote(p)) return(p)
    # a path that can't be located in the cache, like r2:// without credentials, is left to the remote read
    location <- tryCatch(cache_location(p,cache_dir),error=function(e)NULL)
    if (is.null(location)) return(p)
    local <- cached_copy(location)
    if (is.null(local)) return(p)
    current <- cache_checks[[local]]
    if (is.null(current)) {
      source <- tryCatch(cache_source(location),error=function(e)e)
      if (inherits(source,"error")) {
        warning(paste0("Checking ",p," failed, using the cached copy in ",local,". ",conditionMessage(source)),call.=FALSE)
        return(local)
      }
      current <- cache_current(location,source)
      cache_checks[[local]] <- current
      if (!current) warning(paste0("The cached copy of ",p," is out of date, reading the remote files. ",
                                   "Update it with cache_parquet() or parquet_tbl(cache = TRUE)."),call.=FALSE)
    }
    if (current) local else p
  },character(1),USE.NAMES=FALSE)
}

#' forget the cache checks of the session
#'
#' @description
#' `parquet_tbl` checks a cached copy against the remote files once per session, the first time it reads the
#' path with the default `cache = NULL`, and reuses the result afterwards, so that reading a dataset repeatedly
#' costs one request and not one per call. Remote data that changes during the session therefore goes
#' unnoticed until the session is restarted or the checks are forgotten with this function, after which the next
#' `parquet_tbl` call on each path checks the remote again. `cache_parquet` and `parquet_tbl(cache = TRUE)`
#' always check, and update the remembered result along with the copy, and `remove_from_cache` forgets the
#' checks of the paths it removes.
#'
#' @return (invisibly) `NULL`
#' @export
clear_cache_checks <- function(){
  rm(list=ls(cache_checks,all.names=TRUE),envir=cache_checks)
  invisible(NULL)
}

# the remote paths that local paths in the cache stand for, the reverse of cache_location
cache_remote_path <- function(local,cache_dir){
  vapply(local,function(l){
    segments <- strsplit(substring(l,nchar(cache_dir)+2),"/",fixed=TRUE)[[1]]
    if (segments[1]=="r2") {
      paste0("r2://",segments[3],"/",paste0(segments[-(1:3)],collapse="/"))
    } else {
      paste0(segments[1],"://",segments[2],"/",url_path_encode(paste0(segments[-(1:2)],collapse="/")))
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
  remote <- is_remote(path)
  if (!any(remote)) return(status)
  cache_dir <- cache_directory()
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
  ancestors <- cache_ancestors(local,cache_dir)
  for (i in seq_along(ancestors$directory)) {
    directory <- ancestors$directory[i]
    relative <- ancestors$relative[i]
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
  }
}

#' list the files in the local cache
#'
#' @description
#' Lists the parquet files that `cache_parquet` and `parquet_tbl(cache = TRUE)` downloaded into the cache
#' directory, the one in the `R2PARQUET_CACHE_PATH` environment variable or the user cache directory, with the
#' remote path each is a copy of and the etag recorded when it was downloaded. `list_parquet` on the remote
#' paths tells whether the copies are still current.
#'
#' @param path `r2://bucket/path` or URL of a parquet file or directory, or a vector of these, to list only
#' the copies of these. `r2://` paths need the `R2_ACCOUNT_ID` environment variable, since the cache is kept
#' per account. By default everything in the cache gets listed, which needs no credentials.
#' @return a tibble with one row per cached parquet file and the columns of `list_parquet`: `file`, the file
#' name, `size`, formatted for reading, `sizeRaw`, the size in bytes, `modified`, the modification time of the
#' remote file in UTC, and `etag`, the etag of the remote file when it was downloaded, followed by `local`,
#' the path of the copy, and `path`, the `r2://bucket/path` or URL it is a copy of
#' @export
list_cache <- function(path=NULL) {
  cache_dir <- cache_directory()
  files <- cache_contents(cache_dir)
  if (!is.null(path)) {
    if (!is.character(path) || length(path)==0) stop("path has to be a character vector of paths.")
    local <- vapply(path,function(p)remote_cache_location(p,cache_dir)$local,"",USE.NAMES=FALSE)
    keep <- Reduce(`|`,lapply(local,function(l)files$local==l | startsWith(files$local,paste0(l,"/"))),logical(nrow(files)))
    files <- files[keep,]
  }
  dplyr::tibble(file=basename(files$local),size=format_size(files$size),sizeRaw=files$size,modified=files$modified,
                etag=files$etag,local=files$local,path=files$path)
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
  cache_dir <- cache_directory()
  local <- vapply(path,function(p)remote_cache_location(p,cache_dir)$local,"",USE.NAMES=FALSE)
  missing <- !file.exists(local)
  if (any(missing)) stop(paste0("Not in the cache: ",paste0(path[missing],collapse=", "),"."))
  removed <- list_cache(path)$path
  for (l in unique(local)) {
    # may have gone with a directory removed before it
    if (!file.exists(l)) next
    unlink(l,recursive=TRUE)
    prune_manifests(l,cache_dir)
    forget_checks(l)
  }
  message(paste0("Removed ",length(removed)," file",if (length(removed)!=1) "s"," from the cache."))
  invisible(removed)
}

`%||%` <- function(x,y) if (is.null(x)) y else x
