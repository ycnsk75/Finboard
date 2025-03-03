# Use official dbt BigQuery image as base
FROM ghcr.io/dbt-labs/dbt-bigquery:1.8.2

# Set working directory
WORKDIR /app

# Copy dbt project files
COPY . .

# Install any additional dependencies (if needed)
# RUN pip install <additional-packages>

# Set entrypoint to run dbt
ENTRYPOINT ["dbt", "run", "--profiles-dir", "/app", "--project-dir", "/app"]