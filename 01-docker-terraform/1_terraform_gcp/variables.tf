variable "credentials" {
  description = "Service account credentials"
  type        = string
  default     = "~/.google/credentials/google_credentials.json"
}

variable "project" {
  description = "Project"
  default = "dtc-de-448703"

}

variable "region" {
  description = "region"
  default = "us-central1"

}


variable "location" {
  description = "The location/region of the resources"
  type        = string
  default     = "US"
  
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  type        = string
  default     = "trips_data_all"
}

variable "gcs_bucket_name" {
  description = "My GCS bucket name"
  type        = string
  default     = "dtc_data_lake_448703"
}
