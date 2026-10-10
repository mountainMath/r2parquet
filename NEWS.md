# r2parquet 0.1.1

* `r2_presigned_url()` creates time limited URLs to download (or upload) single files in private buckets without credentials.
* `remove_parquet_from_r2()` deletes files in batches of up to 1000 per request instead of one request per file.
* Requests to the S3 compatible API are signed by the package itself, `aws.signature` is no longer needed.

# r2parquet 0.1.0

* Initial version, moved from `mountainmathHelpers` 0.1.4 with unchanged function names and arguments.
* `duckdb`, `DBI` and `dbplyr` are now hard dependencies; `arrow` and `polars` stay optional.
