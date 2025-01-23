variable "credentials" {
  description = "Service account credentials"
  type        = string
  default     = "./keys/terraform-demo-448504-917e55ff73cf.json"
}

variable "project" {
  description = "Project"
  default = "terraform-demo-448504"

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
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My GCS bucket name"
  type        = string
  default     = "terraform-demo-448504-terra-bucket"
}
