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

# Finboard: Green Finance Dashboard
## Overview
Finboard is a comprehensive platform that provides real-time financial metrics for environmentally-focused stocks and cryptocurrencies. The dashboard features the proprietary "Green Efficiency Score" which helps investors assess the environmental impact alongside traditional financial performance.
## Features
- Real-time Stock Metrics: Track performance of green stocks using Finnhub API
- Cryptocurrency Analysis: Monitor eco-friendly cryptocurrencies via CoinCap API
- Green Efficiency Score: Proprietary metric combining financial performance with ecological impact
- Interactive Visualizations: Explore trends and patterns through dynamic charts
- Comparative Analysis: Benchmark performance across different green investment options
## Technology Stack
### Data Infrastructure
- Google Cloud Storage (GCS): Raw data storage and archiving
- BigQuery: Data warehousing and analytics
- Cloud Composer: Orchestrating data pipelines
- DBT Cloud: Data transformation and modeling
### Application Layer
- Streamlit: Interactive web application and dashboard
- Docker: Containerization for consistent deployment
- Terraform: Infrastructure as code for cloud resources
- GitHub Actions: CI/CD pipeline automation
### Data Sources
- Finnhub API: Financial data for publicly traded green companies
- CoinCap API: Cryptocurrency market data
- CCRI Datasets: Climate change risk and impact metrics
- Green Stock Lists (from greenstocknews.com): Curated collections of environmentally responsible investments
### Architecture
```
                                  ┌───────────────┐
                                  │  Data Sources │
                                  └───────┬───────┘
                                          │
                                          ▼
┌───────────────────────────────────────────────────────────────────┐
│                        Data Pipeline                              │
│                                                                   │
│  ┌───────────┐    ┌───────────┐    ┌────────────┐   ┌───────────┐ │
│  │  Extract  │ ─► │  Google   │ ─► │ BigQuery + │ ─►│ Processed │ │
│  │  (APIs)   │    │  Cloud    │    │  DBT Cloud │   │   Data    │ │
│  └───────────┘    │  Storage  │    └────────────┘   └───────────┘ │
│                   └───────────┘                                   │
└───────────────────────────┬───────────────────────────────────────┘
                            │
                            │  Orchestrated by Cloud Composer
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Web Application                           │
│                                                                  │
│                         ┌────────────┐                           │
│                         │  Streamlit │                           │
│                         │ Dashboard  │                           │
│                         └────────────┘                           │
│                               ▲                                  │
│                               │                                  │
│          ┌───────────────────┴───────────────────┐               │
│          │                                        │              │
│    ┌───────────┐                          ┌────────────┐         │
│    │  Docker   │                          │ Terraform  │         │
│    │ Container │                          │ Resources  │         │
│    └───────────┘                          └────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                           Deployed with
                         GitHub Actions CI/CD
```
## Green Efficiency Score
The Green Efficiency Score combines:
- Financial performance metrics
- Carbon footprint data
- Sustainability initiatives
- Environmental impact assessments
- Regulatory compliance metrics
# This proprietary scoring model helps investors make informed decisions that balance financial returns with environmental responsibility.
## Getting Started
### Prerequisites
- Google Cloud Platform account with required APIs enabled
- Docker and Docker Compose
- Python 3.9+
- Terraform CLI
- API access keys for Finnhub and CoinCap
### Installation
* Clone the repository
* git clone https://github.com/ycnsk75/Finboard.git
* cd Finboard
* Set up environment variables
* cp .env.example .env
* Edit .env with your API keys and configuration
#### Deploy infrastructure with Terraform
* cd terraform
* terraform init
* terraform apply
#### Build and run the Docker container
* docker build -t finboard .
* docker run -p 8501:8501 finboard
* Access the dashboard at http://localhost:8501
## Development
### Data Pipeline
The data pipeline is orchestrated using Cloud Composer (managed Apache Airflow) with the following stages:
- Extract data from APIs and websites
- Store raw data in Google Cloud Storage
- Load data into BigQuery
- Transform data using DBT models
- Create aggregated views for dashboard consumption
### Dashboard Development
The Streamlit application is structured as follows:
- app.py: Main entry point
To run the dashboard locally for development:
- pip install -r requirements.txt
- streamlit run app.py
### Deployment
This project uses GitHub Actions for CI/CD:
- Code changes are pushed to GitHub
- GitHub Actions runs tests and builds Docker image
- Terraform applies any infrastructure changes
- New Docker image is deployed to Google Cloud Run
### License
This project is licensed under the MIT License - see the LICENSE file for details.
### Acknowledgments
Climate Change Risk Index (CCRI) for ecological datasets
Green Stock News for curated green stock lists
The open-source community for various libraries and tools used in this project
