# Capstone Project: Data Engineering with Django, PostgreSQL LangGraph and Docker

![Database schema diagram showing tables, columns, and relationships for the capstone project. The diagram is organized with tables representing users, projects, and datasets, connected by lines indicating foreign key relationships. The environment is a digital workspace with a neutral tone. No visible text in the image.](assets/images/Screenshot%202025-08-22%20at%2007.04.33.png)

## Project Overview

This project aims to build a data engineering pipeline and natural language querying system using Django, Docker, PostgreSQL and LangGraph.

## Construction Steps

---

### Step 1: Database Design

- Design the database schema based on the provided Excel sheet structure.

---

### Step 2: ETL Pipeline

- Develop an ETL pipeline to populate a PostgreSQL database using the Django ORM.

---

### Step 3: Data Access Layer

- Create Views and CTEs to Query the Data, Generate Downstream Datasets & Reports.

---

### Step 4: AI Automation

- Add A RAG agent connection to the DB to enable natural language querying.

---

### Step 5: Dockerisation

- Develop a Dockerfile to containerize the ETL pipeline and API endpoint.

---

### Step 6: Micro-service Development

- Design a RESTful API using a web framework to handle:
  - File uploads.
  - ETL process triggering.
  - Data querying and access.
  - File downloads.

---

## Step 7: Deployment

- Deploy the service on a cloud hosting provider.

___

## Operational Steps

1. Clone this repo.
2. Change directories to the root of "katlego-engineering-c4-project"
3. Run the `docker build .` command.
4. Run the `docker compose up` command.
5. Open `localhost:8000/api/docs` in your favorite browser.
6. Upload the `Cohort 4 Capstone Project - Dataset.xlsx` spreadsheet via the `etl` endpoint.
7. Get database info via the `db-meta` endpoint.
8. List available datasets and download them via the `datasets` endpoint.
9. Get information from the database via natural language queries using the `agent` endpoint.
10. Generate and download visual report from the data using the `report` endpoint.

![Database schema diagram illustrating tables for users, projects, and datasets, each with labeled columns and connecting lines that indicate foreign key relationships. The layout is organized and clear, set within a digital workspace featuring a neutral background. No visible text is present in the image. The overall tone is professional and informative.](assets/images/Screenshot%202025-08-22%20at%2016.29.26.png)