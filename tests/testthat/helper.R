# North Carolina counties from sf, NAD27 multipolygons
nc_data <- function(){
  sf::read_sf(system.file("shape/nc.shp",package="sf"),quiet=TRUE)
}

# the spatial and httpfs DuckDB extensions get downloaded on first use
skip_if_no_duckdb_extensions <- function(){
  skip_on_cran()
  skip_if_offline()
}

# R2 tests need credentials and a bucket that the tests can write to
skip_if_no_r2 <- function(){
  skip_on_cran()
  if (Sys.getenv("R2_TEST_BUCKET")=="" || Sys.getenv("R2_ACCOUNT_ID")=="" ||
      Sys.getenv("R2_ACCESS_KEY_ID")=="" || Sys.getenv("R2_SECRET_ACCESS_KEY")=="") {
    skip("R2_TEST_BUCKET and R2 credentials not set")
  }
}
