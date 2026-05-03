resource "google_storage_bucket" "bronze" {
  name          = "${var.gcp_project_id}-bronze-raw"
  location      = "US"
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action { type = "Delete" }
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
