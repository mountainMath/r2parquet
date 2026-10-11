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
| `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | `presigned_url()`, `upload_parquet()`, `remove_parquet()`, `parquet_tbl()` and `data_to_parquet()` on `r2://` paths |
| `R2_ACCOUNT_ID`, `R2_API_TOKEN` | `create_r2_bucket()` (Cloudflare REST API via `httr`; bucket settings like public access are not available through R2's S3-compatible API). With `custom_domain` it looks up the domain's zone via `/zones`, so the token also needs Zone Read on that zone, and the zone has to be active in the same account. `upload_parquet()` and `remove_parquet()` also use this token (if set) to purge changed files from the Cloudflare cache of the bucket's custom domains (`r2_purge_cache()`, needs Zone Cache Purge); a failed purge only warns since the objects have already changed |

## Architecture

**R2 access (`R/r2.R`).** Uploads and listings go through `arrow::S3FileSystem` (`r2_filesystem()`), not `aws.s3`, which cannot address R2 correctly when `AWS_DEFAULT_REGION` is set. Deletes and `HEAD` requests go through `r2_request()`, signed by the package's own SigV4 implementation (`sigv4_headers()`, `sigv4_presigned_url()`; `test-signing.R` checks them against AWS's published examples). Deletes use the batch `POST ?delete` API (1000 keys per request, needs a `Content-MD5` header) rather than arrow because arrow's `DeleteFile` leaves empty directory marker objects behind. `presigned_url()` signs locally with `X-Amz-Expires` capped at 7 days, as for S3. All functions address files on R2 as `r2://bucket/path` (`r2_split_path()`), the functions return uploaded/removed files in that form too. Directories of parquet files are often themselves named `*.parquet`, so `r2_parquet_glob()` (used by the DuckDB and polars engines of `parquet_tbl()`; arrow resolves this itself) sends a signed `HEAD` request for such paths to tell a file from a directory.

**Writing (`R/write.R`).** `data_to_parquet()` writes data frames, sf objects and lazy DuckDB tables (the latter via `COPY` on the table's own connection, without collecting) with a single DuckDB `COPY`, rows ordered by `sort_by`, optionally hive partitioned by `partition_by`. sf objects, and lazy tables with GEOMETRY columns, become GeoParquet 2.0 (native parquet `GEOMETRY` type with the crs in the column type and a bounding box per row group) via `GEOPARQUET_VERSION 'V2'`; for sf the geometry column is always named `geometry` and invalid geometries are fixed with `sf::st_make_valid()`, casting back to the original multi type so geometry types stay consistent (StatCan boundary files contain invalid geometries). Spatial rows are sorted by `ST_Hilbert` over the data's extent (after `sort_by`) so the row group bounding boxes are selective. No separate `bbox` covering column is written (removed before the first release): DuckDB prunes on the native geometry statistics, polars can't read the files at all, and without GeoParquet `covering` metadata other tools wouldn't use it either. Partitioned writes run with `SET threads = 1` because multi-threaded `PARTITION_BY` writes interleave chunks within partition files (DuckDB 1.5.6), and stale partitions are removed first since DuckDB's `OVERWRITE` keeps them. Partition columns are only in the directory names (arrow would infer `"001"` as integer and fails if they are also in the files). For `r2://` destinations `data_to_parquet()` (`r2_data_to_parquet()`) writes to a temp dir via `local_data_to_parquet()` and uploads via `upload_parquet()`, removing a directory at the destination first with `overwrite = TRUE`.

**GeoParquet reading (`R/geoparquet.R`).** `filter_spatial()` works on any lazy DuckDB table and always emits a `geom && envelope` condition before `ST_Intersects`: as of DuckDB 1.5.6 only `&&`/`ST_Intersects_Extent` prune row groups via the geometry statistics, a plain `ST_Intersects` downloads all geometry. `collect_parquet()` collects any lazy DuckDB table, turning GEOMETRY columns (WKB raw lists) into sf with the crs from the column type via `DESCRIBE`, and attaches the file metadata.

**Metadata (`R/metadata.R`).** `data_to_parquet(metadata=)` writes a named list into the parquet footer via DuckDB's `KV_METADATA` copy option, single values as strings and everything else (notably `columns`, the per-column labels/descriptions, which also picks up `label` attributes of data frame columns) as JSON; `geo` is reserved for GeoParquet. `parquet_metadata()` reads it back with `parquet_kv_metadata()`, parsing JSON values and dropping `geo`, from paths or from a lazy table. `parquet_tbl()` stores the resolved paths as the `r2parquet_paths` attribute of the lazy table, which dbplyr (2.6) keeps through all verbs on the left-hand table, so `collect_parquet()` can look up the metadata at collect time and attach it as the `metadata` attribute plus `label` attributes on columns. Footer metadata can only be changed by writing the data again.

**Reading (`R/parquet.R`).** `parquet_tbl()` is the one reader, for public URLs (like a public bucket's custom domain or presigned URLs), private R2 files as `r2://bucket/path` and local paths, with `engine` `"duckdb"` (default, lazy `dbplyr` table on a single session-cached in-memory DuckDB connection, so all tables can be joined), `"arrow"` (`arrow::open_dataset()`, on `r2_filesystem()` for R2; arrow can't read URLs) or `"polars"` (`pl$scan_parquet()`, with `storage_options` for R2). For `r2://` paths the DuckDB engine runs `CREATE OR REPLACE SECRET` on every call, so credentials are only needed once R2 is read and changed credentials get picked up; the secret sets `REGION 'auto'` explicitly, otherwise DuckDB takes the region from `AWS_DEFAULT_REGION` and R2 rejects it. As of polars 1.16 polars can't read GeoParquet 2.0 files at all (it rejects the native GEOMETRY logical type), plain parquet is fine. The R duckdb package sets `autoinstall_known_extensions = false`, so extensions have to be `INSTALL`ed explicitly even though they autoload afterwards: `httpfs` gets installed for URLs, and `spatial` only when `DESCRIBE` (footer only) shows a GEOMETRY column, since spatial functions like `ST_MakeEnvelope`/`&&` do not autoload. GEOMETRY columns and `ST_CRS` work without `spatial` (the crs then comes back as PROJJSON instead of `EPSG:n`).

**Why not duckdbfs.** `duckdbfs` covers similar reading ground, but the CRAN version lacks crs reading for GeoParquet and the package does not seem actively maintained, so the thin DuckDB readers here are kept on purpose.

**Dependencies.** Everything is called with explicit `pkg::fn()` qualification, there are no namespace imports. `duckdb`, `DBI`, `dbplyr`, `sf` are hard dependencies. `arrow` and `polars` are in `Suggests` and checked with `requireNamespace()` in the functions that need them; `polars` is not on CRAN and comes from r-multiverse (`Additional_repositories`, and `extra-repositories` in CI).

## Conventions

- Two-space indentation, `=` and `<-` both used for assignment, compact spacing (`function(d)d`). Match the surrounding file.
- Every exported function has a roxygen block with `@param` for each argument, `@return`, and `@export`.
