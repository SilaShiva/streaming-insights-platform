# Terraform: BigQuery datasets + GCS staging bucket for the platform.
# Backend "local" for portability; swap to gcs backend for team use.
# Sandbox-friendly: no billing-account resources, free-tier only.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.gcp_project
  region  = var.region
}

variable "gcp_project" {
  type        = string
  description = "GCP project id (sandbox)"
}

variable "region" {
  type        = string
  default     = "US"
  description = "Dataset location; US multi-region works with free-tier sandbox"
}

variable "dataset_raw" {
  type    = string
  default = "streaming_raw"
}

variable "dataset_marts" {
  type    = string
  default = "streaming_marts"
}

variable "staging_bucket" {
  type        = string
  description = "Globally unique GCS bucket name for dbt/Spark staging"
}

# Raw layer: append-only events landed by the consumer
resource "google_bigquery_dataset" "raw" {
  dataset_id    = var.dataset_raw
  friendly_name = "Raw streaming events"
  location      = var.region
  labels = {
    layer = "raw"
    env   = "sandbox"
  }
}

# Marts layer: dbt-built models live here
resource "google_bigquery_dataset" "marts" {
  dataset_id    = var.dataset_marts
  friendly_name = "Analytics marts"
  location      = var.region
  labels = {
    layer = "marts"
    env   = "sandbox"
  }
}

# GCS: dbt artifacts / exported aggregates / query fallback staging
resource "google_storage_bucket" "staging" {
  name          = var.staging_bucket
  location      = var.region == "US" ? "US" : var.region
  force_destroy = true # sandbox only
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

output "raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "marts_dataset" {
  value = google_bigquery_dataset.marts.dataset_id
}

output "staging_bucket_url" {
  value = google_storage_bucket.staging.url
}
