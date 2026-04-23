# terraform/bigquery.tf
# ----------------------
# BigQuery datasets (one per medallion layer) and partitioned tables.
# Schema changes are managed here — never via the BQ console.

locals {
  bq_location = "US"
}

# ─────────────────────────────────────────────────────────────────────────────
# Datasets
# ─────────────────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "bronze" {
  dataset_id                  = "bronze"
  friendly_name               = "Bronze — Raw ingested data"
  description                 = "Raw data loaded directly by the ingestion pipeline. Never modified."
  location                    = local.bq_location
  delete_contents_on_destroy  = false

  labels = {
    layer       = "bronze"
    environment = var.environment
  }
}

resource "google_bigquery_dataset" "silver" {
  dataset_id    = "silver"
  friendly_name = "Silver — Cleaned and typed data"
  description   = "Transformed data: deduplication, type safety, derived columns."
  location      = local.bq_location

  labels = {
    layer       = "silver"
    environment = var.environment
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id    = "gold"
  friendly_name = "Gold — Business KPIs"
  description   = "Aggregated KPIs ready for BI tools."
  location      = local.bq_location

  labels = {
    layer       = "gold"
    environment = var.environment
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Bronze tables
# ─────────────────────────────────────────────────────────────────────────────

resource "google_bigquery_table" "weather_daily" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "weather_daily"
  deletion_protection = true

  description = "Daily weather data per location from Open-Meteo API."

  time_partitioning {
    type          = "DAY"
    field         = "date"
    expiration_ms = null  # no expiry on raw data
  }

  clustering = ["location_id"]

  schema = jsonencode([
    { name = "date",               type = "DATE",      mode = "REQUIRED" },
    { name = "location_id",        type = "STRING",    mode = "REQUIRED" },
    { name = "location_name",      type = "STRING",    mode = "REQUIRED" },
    { name = "latitude",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "longitude",          type = "FLOAT64",   mode = "NULLABLE" },
    { name = "temp_max_c",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "temp_min_c",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "temp_mean_c",        type = "FLOAT64",   mode = "NULLABLE" },
    { name = "precipitation_mm",   type = "FLOAT64",   mode = "NULLABLE" },
    { name = "wind_speed_max_kmh", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "wind_direction_deg", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "fetched_at",         type = "TIMESTAMP", mode = "REQUIRED" },
  ])

  labels = {
    layer  = "bronze"
    source = "open_meteo"
  }
}

resource "google_bigquery_table" "exchange_rates_daily" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "exchange_rates_daily"
  deletion_protection = true

  description = "Daily closing exchange rates from Frankfurter API."

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["base_currency", "target_currency"]

  schema = jsonencode([
    { name = "date",            type = "DATE",      mode = "REQUIRED" },
    { name = "base_currency",   type = "STRING",    mode = "REQUIRED" },
    { name = "target_currency", type = "STRING",    mode = "REQUIRED" },
    { name = "rate",            type = "FLOAT64",   mode = "REQUIRED" },
    { name = "fetched_at",      type = "TIMESTAMP", mode = "REQUIRED" },
  ])

  labels = {
    layer  = "bronze"
    source = "exchange_rates"
  }
}
