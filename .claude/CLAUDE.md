# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`r2parquet` publishes parquet and GeoParquet files on Cloudflare R2 and reads them lazily. It was split out of `mountainmathHelpers` (October 2026), which no longer carries wrappers for these functions. Installed from GitHub (`mountainMath/r2parquet`), not on CRAN.

## Commands

```sh
Rscript -e 'devtools::document()'   # regenerate NAMESPACE and man/*.Rd from roxygen comments
Rscript -e 'devtools::test()'       # testthat suite
Rscript -e 'rcmdcheck::rcmdcheck(args = c("--no-manual", "--as-cran"), error_on = "warning")'  # what CI runs
```

- `NAMESPACE` and `man/` are roxygen-generated, never edit them by hand.
- Vignettes are precomputed: edit the sources in `vignettes.orig/` (`.Rbuildignore`'d), then `Rscript vignettes.orig/precompute.R [name ...]` knits them with live data (and R2 credentials where needed) into `vignettes/`, figures included. Never edit `vignettes/*.Rmd` directly. The pkgdown action (`.github/workflows/pkgdown.yaml`) only renders the precomputed files and deploys to GitHub Pages, it has no credentials.
- `test-signing.R` and `test-delete.R` run offline (the latter mocks `r2_request()`). Most tests in `test-write.R` and all in `test-geoparquet.R` download the DuckDB `spatial`/`httpfs` extensions and are skipped on CRAN and offline. `test-r2.R` is an integration test against a real bucket, skipped unless `R2_TEST_BUCKET` and the R2 credentials below are set; it writes under a random `r2parquet-tests/` prefix and removes it again.

## Configuration

Read at call time from environment variables:

| Setting | Used by |
| --- | --- |
| `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | `r2_presigned_url()`, `parquet_to_r2()`, `remove_parquet_from_r2()`, `r2_duckdb_connection()`, `r2_parquet_tbl()`, `r2_parquet_arrow()`, `r2_parquet_polars()`, `data_to_r2_parquet()` |
| `R2_ACCOUNT_ID`, `R2_API_TOKEN` | `create_r2_bucket()` (Cloudflare REST API via `httr`; bucket settings like public access are not available through R2's S3-compatible API). With `custom_domain` it looks up the domain's zone via `/zones`, so the token also needs Zone Read on that zone, and the zone has to be active in the same account. `parquet_to_r2()` and `remove_parquet_from_r2()` also use this token (if set) to purge changed files from the Cloudflare cache of the bucket's custom domains (`r2_purge_cache()`, needs Zone Cache Purge); a failed purge only warns since the objects have already changed |

## Architecture

**R2 access (`R/r2.R`).** Uploads and listings go through `arrow::S3FileSystem` (`r2_filesystem()`), not `aws.s3`, which cannot address R2 correctly when `AWS_DEFAULT_REGION` is set. Deletes and `HEAD` requests go through `r2_request()`, signed by the package's own SigV4 implementation (`sigv4_headers()`, `sigv4_presigned_url()`; `test-signing.R` checks them against AWS's published examples). Deletes use the batch `POST ?delete` API (1000 keys per request, needs a `Content-MD5` header) rather than arrow because arrow's `DeleteFile` leaves empty directory marker objects behind. `r2_presigned_url()` signs locally with `X-Amz-Expires` capped at 7 days, as for S3. Reading: `r2_parquet_tbl()` (lazy `dbplyr` table on a session-cached in-memory DuckDB connection with `httpfs` and an R2 secret), `r2_parquet_polars()` (`pl$scan_parquet()` with `storage_options`), `r2_parquet_arrow()` (`arrow::open_dataset()` on `r2_filesystem()`). Directories of parquet files are often themselves named `*.parquet`, so `r2_parquet_glob()` (DuckDB and polars; arrow resolves this itself) sends a signed `HEAD` request for such paths to tell a file from a directory. The DuckDB secret sets `REGION 'auto'` explicitly, otherwise DuckDB takes the region from `AWS_DEFAULT_REGION` and R2 rejects it.

**Writing (`R/write.R`).** `data_to_parquet()` writes data frames, sf objects and lazy DuckDB tables (the latter via `COPY` on the table's own connection, without collecting) with a single DuckDB `COPY`, rows ordered by `sort_by`, optionally hive partitioned by `partition_by`. sf objects, and lazy tables with GEOMETRY columns, become GeoParquet 2.0 (native parquet `GEOMETRY` type with the crs in the column type and a bounding box per row group) via `GEOPARQUET_VERSION 'V2'`; for sf the geometry column is always named `geometry` and invalid geometries are fixed with `sf::st_make_valid()`, casting back to the original multi type so geometry types stay consistent (StatCan boundary files contain invalid geometries). Spatial rows are sorted by `ST_Hilbert` over the data's extent (after `sort_by`) so the row group bounding boxes are selective. Unless all geometries are points, a `bbox` struct column is added for arrow/polars readers that can't use native geometry statistics (~2% size for polygons, ~80% for points, hence skipped there); a user column named `bbox` is kept and no bbox added. Partitioned writes run with `SET threads = 1` because multi-threaded `PARTITION_BY` writes interleave chunks within partition files (DuckDB 1.5.6), and stale partitions are removed first since DuckDB's `OVERWRITE` keeps them. Partition columns are only in the directory names (arrow would infer `"001"` as integer and fails if they are also in the files). `data_to_r2_parquet()` writes to a temp dir and uploads via `parquet_to_r2()`, removing a directory at `r2_path` first with `overwrite = TRUE`.

**GeoParquet reading (`R/geoparquet.R`).** `filter_spatial()` works on any lazy DuckDB table and always emits a `geom && envelope` condition before `ST_Intersects`: as of DuckDB 1.5.6 only `&&`/`ST_Intersects_Extent` prune row groups via the geometry statistics, a plain `ST_Intersects` downloads all geometry. It also filters on the `bbox` column if present, recognized by its exact struct type (`bbox_column_type`) so a user column named `bbox` is not mistaken for it. `collect_sf()` turns collected GEOMETRY columns (WKB raw lists) into sf, taking the crs from the column type via `DESCRIBE`, and drops the `bbox` column.

**Public parquet (`R/parquet.R`).** `parquet_tbl()` is the credential-free counterpart of `r2_parquet_tbl()` for public URLs (like a public bucket's custom domain) and local paths, on its own session-cached DuckDB connection. The R duckdb package sets `autoinstall_known_extensions = false`, so extensions have to be `INSTALL`ed explicitly even though they autoload afterwards: `httpfs` gets installed for URLs, and `spatial` only when `DESCRIBE` (footer only) shows a GEOMETRY column, since spatial functions like `ST_MakeEnvelope`/`&&` do not autoload. GEOMETRY columns and `ST_CRS` work without `spatial` (the crs then comes back as PROJJSON instead of `EPSG:n`).

**Why not duckdbfs.** `duckdbfs` covers similar reading ground, but the CRAN version lacks crs reading for GeoParquet and the package does not seem actively maintained, so the thin DuckDB readers here are kept on purpose.

**Dependencies.** Everything is called with explicit `pkg::fn()` qualification, there are no namespace imports. `duckdb`, `DBI`, `dbplyr`, `sf` are hard dependencies. `arrow` and `polars` are in `Suggests` and checked with `requireNamespace()` in the functions that need them; `polars` is not on CRAN and comes from r-multiverse (`Additional_repositories`, and `extra-repositories` in CI).

## Conventions

- Two-space indentation, `=` and `<-` both used for assignment, compact spacing (`function(d)d`). Match the surrounding file.
- Every exported function has a roxygen block with `@param` for each argument, `@return`, and `@export`.
