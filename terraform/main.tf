provider "google" {
    project = var.project_id
    region  = var.region
}

resource "google_service_account" "finboard_sa" {
    account_id   = "finboard_sa"
    display_name = "Service Account for finboard project"
    description  = "Service account for managing finboard pipelines on GCP"
}

resource "google_service_account_key" "finboard_sa_key" {
    service_account_id = google_service_account.finboard_sa.name
}

resource "google_project_iam_member" "finboard_sa_iam" {
    for_each = toset(var.service_account_roles)
    project = var.project_id
    role    = each.value
    member  = "serviceAccount:${google_service_account.finboard_sa.email}"
}

output "service_account_email" {
    value = google_service_account.finboard_sa.email
    description = "Email of the created service account"
}

output "service_account_key" {
    value = google_service_account_key.finboard_sa_key.private_key
    description = "Private key of the created service account (base64 encoded)"
    sensitive = true
}