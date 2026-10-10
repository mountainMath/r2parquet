# r2parquet

<!-- badges: start -->
[![R-CMD-check](https://github.com/mountainMath/r2parquet/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/mountainMath/r2parquet/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

Publish parquet and GeoParquet files on [Cloudflare R2](https://developers.cloudflare.com/r2/) and query them lazily from R.

- **Buckets**: `create_r2_bucket()` creates private or public buckets, public via the `r2.dev` URL or a custom domain.
- **Files**: `parquet_to_r2()` uploads single parquet files or directories of (partitioned) parquet files, `remove_parquet_from_r2()` removes them. Both purge the changed files from the Cloudflare cache of the bucket's custom domains.
- **GeoParquet**: `sf_to_geoparquet()` writes GeoParquet 2.0 files sorted along a Hilbert curve so remote spatial queries only download the row groups they need, `sf_to_r2_geoparquet()` writes and uploads in one go.
- **Reading private buckets**: `r2_parquet_tbl()` (DuckDB / `dbplyr`), `r2_parquet_arrow()` (arrow `Dataset`) and `r2_parquet_polars()` (polars `LazyFrame`).
- **Reading public URLs and local files**: `parquet_tbl()`, no credentials needed.
- **Spatial queries**: `filter_spatial()` and `collect_sf()` on any lazy DuckDB table.

## Installation

```r
remotes::install_github("mountainMath/r2parquet")
```

`arrow` and `polars` are optional and only needed for `parquet_to_r2()`, `remove_parquet_from_r2()`, `r2_parquet_arrow()` and `r2_parquet_polars()`. `polars` is not on CRAN:

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

## Example

```r
library(r2parquet)

create_r2_bucket("census-data", custom_domain = "data.example.com")

nc <- sf::read_sf(system.file("shape/nc.shp", package = "sf"))
sf_to_r2_geoparquet(nc, "census-data", "nc/counties.parquet")

# private access with credentials
counties <- r2_parquet_tbl("census-data", "nc/counties.parquet")

# public access via the custom domain
counties <- parquet_tbl("https://data.example.com/nc/counties.parquet")

wake <- nc[nc$NAME == "Wake", ]
counties |>
  filter_spatial(wake) |>
  dplyr::select(NAME, BIR74) |>
  collect_sf()
```
