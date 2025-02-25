variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = ""
}

variable "service_account_roles" {
  description = "List of IAM roles to assign to the service account"
  type        = list(string)
  default     = [
    "roles/storage.admin",           # Access to Cloud Storage
    "roles/bigquery.admin",          # Access to BigQuery
    "roles/composer.worker",         # Access for Airflow (Composer)
    "roles/container.admin"          # Access for Kubernetes (e.g., GKE)
  ]
}