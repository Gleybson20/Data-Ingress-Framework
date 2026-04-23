# terraform/gcs.tf
# ------------------
# GCS buckets for raw file storage, one per medallion layer.
# Bronze bucket is the source of truth for all BQ load jobs.

resource "google_storage_bucket" "bronze" {
  name          = "${var.gcp_project_id}-bronze-raw"
  location      = "US"
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true  # keeps previous versions for auditability
  }

  lifecycle_rule {
    action { type = "Delete" }
    # Raw files older than 365 days are deleted to control storage costs.
    # BigQuery remains the queryable record — GCS is the landing zone only.
    condition { age = 365 }
  }

  labels = {
    layer       = "bronze"
    environment = var.environment
  }
}

resource "google_storage_bucket" "silver" {
  name          = "${var.gcp_project_id}-silver-processed"
  location      = "US"
  force_destroy = false

  uniform_bucket_level_access = true

  labels = {
    layer       = "silver"
    environment = var.environment
  }
}

resource "google_storage_bucket" "gold" {
  name          = "${var.gcp_project_id}-gold-exports"
  location      = "US"
  force_destroy = false

  uniform_bucket_level_access = true

  labels = {
    layer       = "gold"
    environment = var.environment
  }
}
