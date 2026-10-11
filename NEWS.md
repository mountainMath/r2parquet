# r2parquet 0.2.0

First release, moved from `mountainmathHelpers`.

* Reading: `parquet_tbl()` gives lazy tables on public URLs, presigned URLs and local files without credentials, and on private R2 buckets as `r2://bucket/path`. The `engine` argument returns a DuckDB table (default), an arrow `Dataset` or a polars `LazyFrame`.
* Spatial queries: `filter_spatial()` works on any lazy DuckDB table and only downloads the row groups that intersect the query, `collect_parquet()` collects as sf if the table has geometry.
* Writing: `data_to_parquet()` writes data frames, sf objects and lazy DuckDB tables to parquet, optionally sorted (`sort_by`) and hive partitioned (`partition_by`). sf objects and tables with GEOMETRY columns become GeoParquet 2.0 files sorted along a Hilbert curve. Given an `r2://bucket/path` destination it writes and uploads in one go, removing earlier uploads at the same path so no stale partitions remain.
* Metadata: `data_to_parquet(metadata = list(source = ..., license = ..., columns = ...))` writes source, license, notes and column labels into the parquet footer, `parquet_metadata()` reads them back and `collect_parquet()` attaches them to the collected data as attributes, column labels as `label` attributes.
* Managing R2: `create_r2_bucket()` creates private or public buckets, `upload_parquet()` and `remove_parquet()` upload and delete files and purge them from the Cloudflare cache of the bucket's custom domains.
* Sharing private files: `presigned_url()` creates time limited download (or upload) URLs for single files.
* Files on R2 are given as `r2://bucket/path` throughout, for reading, writing, uploading, removing and presigning.
