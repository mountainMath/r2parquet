# Public read access to prefixes of S3 buckets via the bucket policy, which unlike R2's bucket settings is part
# of the S3 API, so it goes through r2_request like everything else.

# the bucket policy as a list, an empty policy if the bucket has none
s3_get_policy <- function(r2_bucket){
  response <- r2_request("GET",r2_bucket,query=list(policy=""))
  status <- httr::status_code(response)
  if (status==404) return(list(Version="2012-10-17",Statement=list()))
  content <- httr::content(response,"text",encoding="UTF-8")
  if (status>=400) stop(paste0("Reading the policy of bucket ",bucket_url(r2_bucket)," failed with status ",status,". ",content))
  policy <- jsonlite::fromJSON(content,simplifyVector=FALSE)
  # a single statement may be given as an object instead of an array
  if (!is.null(names(policy$Statement))) policy$Statement <- list(policy$Statement)
  policy
}

# write the bucket policy, or delete it when no statements are left
s3_put_policy <- function(r2_bucket,policy){
  response <- if (length(policy$Statement)==0) {
    r2_request("DELETE",r2_bucket,query=list(policy=""))
  } else {
    body <- charToRaw(enc2utf8(jsonlite::toJSON(policy,auto_unbox=TRUE)))
    r2_request("PUT",r2_bucket,query=list(policy=""),body=body,
               headers=list("content-md5"=base64_md5(body),"content-type"="application/json"))
  }
  status <- httr::status_code(response)
  if (status>=400) {
    stop(paste0("Writing the policy of bucket ",bucket_url(r2_bucket)," failed with status ",status,". ",
                if (status==403) paste0("The credentials need the s3:PutBucketPolicy and s3:DeleteBucketPolicy permissions, ",
                                        "and Block Public Access of the AWS account must allow public policies. "),
                httr::content(response,"text",encoding="UTF-8")))
  }
}

# stop if the Block Public Access settings of the bucket would reject or neutralize a public policy; services
# without these settings, like MinIO, answer with an error and are taken to have none
s3_check_public_access_block <- function(r2_bucket){
  response <- r2_request("GET",r2_bucket,query=list(publicAccessBlock=""))
  if (httr::status_code(response)!=200) return(invisible())
  content <- httr::content(response,"text",encoding="UTF-8")
  blocking <- c("BlockPublicPolicy","RestrictPublicBuckets")
  blocked <- blocking[vapply(blocking,function(b)identical(xml_field(content,b),"true"),TRUE)]
  if (length(blocked)>0) {
    stop(paste0("Block Public Access of bucket ",bucket_url(r2_bucket)," has ",paste0(blocked,collapse=" and "),
                " turned on, which keeps the bucket private whatever its policy says. Turn ",
                if (length(blocked)>1) "them" else "it"," off in the AWS console (Permissions tab of the bucket) ",
                "to allow public prefixes."))
  }
}

# the statement granting anonymous downloads of the objects below a prefix, identified by its Sid
s3_public_statement <- function(bucket,prefix){
  resource <- paste0("arn:aws:s3:::",bucket,"/",if (prefix!="") paste0(prefix,"/"),"*")
  list(Sid=paste0("r2parquetPublicRead",substr(digest::digest(resource,algo="md5",serialize=FALSE),1,12)),
       Effect="Allow",Principal="*",Action="s3:GetObject",Resource=resource)
}

# the public URL of a prefix of a bucket, on the endpoint the requests go to
s3_public_url <- function(r2_bucket,prefix){
  store <- bucket_store(r2_bucket)
  paste0(store$endpoint,if (store$url_style=="path") paste0("/",uri_encode(store$bucket)),"/",
         if (prefix!="") paste0(url_path_encode(prefix),"/"))
}

#' public read access to a prefix of an S3 bucket
#'
#' @description
#' Makes the files below a prefix of a bucket on Amazon S3, or on another S3 compatible service like MinIO,
#' publicly readable, or private again, by adding or removing a statement in the bucket policy that grants
#' everyone `s3:GetObject` on the objects below the prefix. Other statements of the policy are kept.
#'
#' @details
#' Whether a file is public is then decided by where it gets uploaded: files that [upload_parquet()] and
#' [data_to_parquet()] write below the prefix are public, files elsewhere in the bucket stay private. Public files
#' can be read by anyone at the URL returned, with [parquet_tbl()], [cache_parquet()] and the other readers, and
#' directories of parquet files are found through their manifests, since the policy does not allow listing the
#' bucket.
#'
#' This is the S3 counterpart of [create_r2_bucket()]'s public access, which R2 grants per bucket. Per-object ACLs
#' are not used, they are disabled on buckets created since April 2023, and a public manifest listing private
#' files would leak their names and metadata; keep public and private data under separate prefixes, a manifest
#' of a directory above both, like one [write_manifest()] writes, stays private.
#'
#' Requirements:
#' * The credentials need the `s3:GetBucketPolicy`, `s3:PutBucketPolicy` and `s3:DeleteBucketPolicy`
#'   permissions.
#' * Block Public Access has to allow public bucket policies, for the bucket and for the AWS account; the call
#'   stops when the bucket's own settings would keep it private.
#'
#' Amazon S3 applies policy changes with a delay, for a few minutes after a call anonymous requests can still
#' get 403 Forbidden now and then, or succeed after the access has been removed.
#'
#' @inheritSection upload_parquet Credentials
#' @param path prefix in a bucket as `s3://bucket/path`, or `s3://bucket` for the whole bucket.
#' @param public `TRUE` (default) to make the files below the prefix public, `FALSE` to remove the public
#'   access this function granted.
#' @return (invisibly) the public URL of the prefix, ending in a slash.
#' @export
set_s3_public_access <- function(path,public=TRUE){
  if (!is.character(path) || length(path)!=1 || !startsWith(path,"s3://")) {
    stop("path has to be a single prefix in an S3 bucket of the form s3://bucket/path, ",
         "public access to R2 buckets is set with create_r2_bucket.")
  }
  p <- r2_split_path(path)
  bucket <- sub("^s3://","",p$bucket)
  statement <- s3_public_statement(bucket,p$path)
  policy <- s3_get_policy(p$bucket)
  existing <- vapply(policy$Statement,function(s)identical(s$Sid,statement$Sid),TRUE)
  url <- s3_public_url(p$bucket,p$path)
  if (public) {
    if (any(existing)) {
      message(paste0(path," is already public at ",url,"."))
      return(invisible(url))
    }
    s3_check_public_access_block(p$bucket)
    policy$Statement <- c(policy$Statement,list(statement))
    s3_put_policy(p$bucket,policy)
    message(paste0("The files below ",path," are publicly readable at ",url,"."))
  } else {
    if (!any(existing)) {
      message(paste0(path," has no public access granted by set_s3_public_access."))
      return(invisible(url))
    }
    policy$Statement <- policy$Statement[!existing]
    s3_put_policy(p$bucket,policy)
    message(paste0("Removed public access to the files below ",path,"."))
  }
  invisible(url)
}
