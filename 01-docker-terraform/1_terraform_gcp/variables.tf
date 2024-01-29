variable "credentials" {
  description = "My Credentials/Key"
  default     = "C:/Users/user/data-engineering-zoomcamp/01-docker-terraform/1_terraform_gcp/terrademo/terraforms/keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "terraform-demo-412601"
}

variable "bq_dataset_name" {
  description = "My Dataset Name"
  default     = "demo_dataset"
}

variable "server_location" {
  description = "Server Location"
  default     = "asia-southeast1"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-demo-412601-terra-bucket"
}
