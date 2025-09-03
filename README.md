# Capstone Project: Data Engineering with Snowflake, LangGraph, Fast AI, and Docker

## Project Overview

This project aims to build a data engineering pipeline and natural language querying system using Snowflake for data warehousing, LangGraph and Fast AI for natural language data access, and Docker for containerization and deployment.

---

## Step 1: Database Design and ETL Pipeline

- Create a Snowflake account and set up a new database.
- Design the database schema based on the provided Excel sheet structure.
- Develop SQL scripts to insert and query data.
- Build a Python-based ETL script utilizing `pandas`:
  - Extract data from CSV files.
  - Transform data through cleaning and standardization.
  - Load transformed data into Snowflake.

---

## Step 2: Automation

- Schedule the ETL job using a cloud scheduler (e.g., AWS CloudWatch, Azure Scheduler) or local scheduler (e.g., cron).
- Implement Apache Airflow to manage and orchestrate ETL workflows and scheduling.

---

## Step 3: Data Access Layer

- Create a Fast AI endpoint to enable natural language querying of the data.
- Integrate LangGraph with Fast AI to facilitate natural language-based data queries.
- Provide traditional SQL query access for users needing direct database interaction.

---

## Step 4: Dockerisation

- Develop a Dockerfile to containerize the ETL pipeline and Fast AI endpoint.
- Create a `Docker Compose` file to manage and orchestrate multi-container services.
- Deploy the Dockerized application on cloud platforms such as AWS ECS or Azure Kubernetes Service (AKS).

---

## Step 5: Microservice Development

- Design a RESTful API to handle:
  - File uploads.
  - ETL process triggering.
  - Data querying and access.
- Build the API using a web framework FastAPI.
- Integrate the RESTful API within the Dockerized environment for seamless operation.

---