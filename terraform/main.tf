provider "google" {
    project = var.project_id
    region  = var.region
}

resource "google_service_account" "finboard-sa" {
    account_id   = "finboard-sa"
    display_name = "Service Account for finboard project"
    description  = "Service account for managing finboard pipelines on GCP"
    create_ignore_already_exists = true

    lifecycle {
      prevent_destroy = true # Prevention contre la destruction de cette clé
    }
}

resource "google_service_account_key" "finboard-sa-key" {
    service_account_id = google_service_account.finboard-sa.name
}

resource "google_project_iam_member" "finboard-sa-iam" {
    for_each = toset(var.service_account_roles)
    project = var.project_id
    role    = each.value
    member  = "serviceAccount:${google_service_account.finboard-sa.email}"
}

output "service_account_email" {
    value = google_service_account.finboard-sa.email
    description = "Email of the created service account"
}

output "service_account_key" {
    value = google_service_account_key.finboard-sa-key.private_key
    description = "Private key of the created service account (base64 encoded)"
    sensitive = true
}