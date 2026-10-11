# r2parquet

<!-- badges: start -->
[![R-CMD-check](https://github.com/mountainMath/r2parquet/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/mountainMath/r2parquet/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

Publish parquet and GeoParquet files on [Cloudflare R2](https://developers.cloudflare.com/r2/) and query them lazily from R.

## Reading public Parquet and GeoParquet data from R2

- **Reading public URLs and local files**: `parquet_tbl()`, no credentials needed. Returns a lazy DuckDB table by default, or an arrow `Dataset` or polars `LazyFrame` with `engine = "arrow"` or `"polars"`.
- **Spatial queries**: `filter_spatial()` on any lazy DuckDB table, `collect_parquet()` collects as sf if the table has geometry.
- **Metadata**: `parquet_metadata()` reads source, license, notes and column labels from the parquet footer, `collect_parquet()` attaches them to the collected data.
- **Joining with other data**: all tables share one DuckDB connection, `parquet_connection()`, so they can be joined with each other and with data frames copied there via `dplyr::copy_to()`. `parquet_tbl(con = )` puts a table on a connection of your own instead.

## Uploading and managing data on R2

- **Buckets**: `create_r2_bucket()` creates private or public buckets, public via the `r2.dev` URL or a custom domain.
- **Files**: `upload_parquet()` uploads single parquet files or directories of (partitioned) parquet files, `remove_parquet()` removes them. Both purge the changed files from the Cloudflare cache of the bucket's custom domains.
- **Writing**: `data_to_parquet()` writes data frames, sf objects and lazy DuckDB tables to parquet, optionally sorted (`sort_by`) and partitioned (`partition_by`), so remote queries only download the row groups they need. sf objects become GeoParquet 2.0 files sorted along a Hilbert curve. Given an `r2://bucket/path` destination it writes and uploads in one go, replacing earlier uploads including stale partitions. `metadata` like the source URL, license and column labels goes into the parquet footer.
- **Reading private buckets**: `parquet_tbl("r2://bucket/path")` with any of the engines.
- **Sharing private files**: `presigned_url()` creates time limited download (or upload) URLs for files in private buckets.

## Installation

```r
remotes::install_github("mountainMath/r2parquet")
```

`arrow` and `polars` are optional and only needed for `upload_parquet()`, `remove_parquet()`, `data_to_parquet()` to R2 and the arrow and polars engines of `parquet_tbl()`. `polars` is not on CRAN:

```r
install.packages("arrow")
install.packages("polars", repos = "https://community.r-multiverse.org")
```

## Credentials

Set these environment variables, e.g. in `~/.Renviron`:

| Variable | Needed for |
| --- | --- |
| `R2_ACCOUNT_ID` | everything on R2 |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | reading, uploading and removing files (S3 API credentials of an R2 API token) |
| `R2_API_TOKEN` | `create_r2_bucket()` (Admin Read & Write); Zone Read for custom domains; Zone Cache Purge for cache purging on upload/removal |

See [Setting up R2](https://mountainmath.github.io/r2parquet/articles/setting_up_r2.html) for the token permissions, caching and rate limiting of public buckets on a custom domain.

## Documentation

Function reference and articles are on the package website: <https://mountainmath.github.io/r2parquet/>.
See [Accessing public buckets](https://mountainmath.github.io/r2parquet/articles/accessing_public_buckets.html) for an example of querying a large public GeoParquet file.

## Example

### Publishing data

Needs the R2 credentials.

```r
library(r2parquet)

create_r2_bucket("census-data", custom_domain = "data.example.com")

nc <- sf::read_sf(system.file("shape/nc.shp", package = "sf"))
data_to_parquet(nc, "r2://census-data/nc/counties.parquet")

# private access with credentials
counties <- parquet_tbl("r2://census-data/nc/counties.parquet")

# one day download link to share a file in a private bucket
presigned_url("r2://census-data/nc/counties.parquet", expires = 24 * 3600)
```

### Accessing published data

No credentials needed, only the public URL of the file or a presigned URL shared by the publisher.

```r
library(r2parquet)

# public access via the custom domain
counties <- parquet_tbl("https://data.example.com/nc/counties.parquet")

# or via a presigned URL
counties <- parquet_tbl("https://<account id>.r2.cloudflarestorage.com/census-data/nc/counties.parquet?X-Amz-...")

nc <- sf::read_sf(system.file("shape/nc.shp", package = "sf"))
wake <- nc[nc$NAME == "Wake", ]
counties |>
  filter_spatial(wake) |>
  dplyr::select(NAME, BIR74, geometry) |>
  collect_parquet()
```
