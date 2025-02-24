# Finboard

App Diagram

[User] --> [Cloud Run (Streamlit)] --> [APIs (Real-time)]
                     |                      |
                [BigQuery] <--> [Airflow] --> [DBT]
                     |                      |
               [Cloud Storage] <--> [CSV Files + API Dumps]

CI/CD:
[Github Actions] --> [Cloud Build] --> [Artifact Registry] --> [Cloud Run]
                    [Terraform] --> [GCP Infra]
