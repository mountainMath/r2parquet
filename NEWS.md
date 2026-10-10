# r2parquet 0.2.0

First release, moved from `mountainmathHelpers`.

* Reading public data: `parquet_tbl()` gives lazy DuckDB tables on public URLs, presigned URLs and local files, without credentials.
* Spatial queries: `filter_spatial()` and `collect_sf()` work on any lazy DuckDB table and only download the row groups that intersect the query.
* Writing: `data_to_parquet()` writes data frames, sf objects and lazy DuckDB tables to parquet, optionally sorted (`sort_by`) and hive partitioned (`partition_by`). sf objects and tables with GEOMETRY columns become GeoParquet 2.0 files sorted along a Hilbert curve, with a `bbox` column unless all geometries are points. `data_to_r2_parquet()` writes and uploads in one go, removing earlier uploads at the same path so no stale partitions remain.
* Managing R2: `create_r2_bucket()` creates private or public buckets, `parquet_to_r2()` and `remove_parquet_from_r2()` upload and delete files and purge them from the Cloudflare cache of the bucket's custom domains.
* Reading private buckets: `r2_parquet_tbl()` (DuckDB / `dbplyr`), `r2_parquet_arrow()` and `r2_parquet_polars()`; `r2_presigned_url()` creates time limited download (or upload) URLs for single files.
