# response objects as returned by httr
fake_response <- function(status,content=""){
  structure(list(url="https://abc.r2.cloudflarestorage.com/bucket?delete=",status_code=status,
                 content=charToRaw(content),headers=list("Content-Type"="application/xml")),
            class="response")
}

test_that("deletes get batched by 1000 with escaped keys", {
  requests <- list()
  local_mocked_bindings(r2_request=function(verb,r2_bucket,r2_path="",query=list(),headers=list(),body=raw()){
    requests[[length(requests)+1]] <<- list(verb=verb,r2_bucket=r2_bucket,query=query,headers=headers,body=rawToChar(body))
    fake_response(200,"<DeleteResult></DeleteResult>")
  })
  paths <- c("a&b<c>.parquet",paste0("dir/part-",seq_len(2499),".parquet"))
  r2parquet:::r2_delete_objects("bucket",paths)
  expect_length(requests,3)
  expect_equal(requests[[1]]$verb,"POST")
  expect_equal(requests[[1]]$query,list(delete=""))
  expect_equal(lengths(regmatches(vapply(requests,`[[`,"",i="body"),gregexpr("<Key>",vapply(requests,`[[`,"",i="body")))),
               c(1000,1000,500))
  expect_match(requests[[1]]$body,"<Key>a&amp;b&lt;c&gt;.parquet</Key>",fixed=TRUE)
  expect_equal(requests[[1]]$headers[["content-md5"]],
               jsonlite::base64_enc(digest::digest(charToRaw(requests[[1]]$body),algo="md5",serialize=FALSE,raw=TRUE)))
})

test_that("failed deletes of individual objects are reported", {
  local_mocked_bindings(r2_request=function(...){
    fake_response(200,paste0("<DeleteResult>\n<Error>\n<Key>a&amp;b.parquet</Key><Code>AccessDenied</Code>",
                             "<Message>Access Denied</Message></Error></DeleteResult>"))
  })
  expect_error(r2parquet:::r2_delete_objects("bucket",c("a&b.parquet","c.parquet")),
               "Removing 1 file from R2 bucket bucket failed. a&b.parquet: Access Denied \\(AccessDenied\\)")
})

test_that("failed delete requests are reported", {
  local_mocked_bindings(r2_request=function(...)fake_response(403,"<Error><Code>InvalidAccessKeyId</Code></Error>"))
  expect_error(r2parquet:::r2_delete_objects("bucket","a.parquet"),"failed with status 403")
})
