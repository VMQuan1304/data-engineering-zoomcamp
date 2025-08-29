variable "credentials" {
  description = "My gg cloud service account"
  default     = "./gg_terraform_key.json"
}

variable "project" {
  description = "My project"
  default     = "data-470504"
}

variable "region" {
  description = "resource region"
  default     = "asia-southeast1"
}

variable "location" {
  description = "resource location"
  default     = "ASIA-SOUTHEAST1"
}


variable "google_storage_bucket_name" {
  description = "google storage bucket name"
  default     = "data-470504-demo-bucket"
}

variable "google_bigquery_dataset_id" {
  description = "google bigquery dataset id"
  default     = "demo_dataset"
}










