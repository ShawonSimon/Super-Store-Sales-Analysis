# SuperStore-Sales-Analysis

## Project Overview
This data engineering project implements an end-to-end data pipeline for sales analytics, transforming raw transactional data from a PostgreSQL OLTP database into actionable business insights using a modern cloud-based data warehouse and visualization solution.

## Dataset
The original dataset was obtained from the [GTS.AI](https://gts.ai/dataset-download/superstore-sales-dataset/). It contains 9993 sales transactions that occurred from 2019 to 2022. This dataset encompasses a wide range of information, including order specifics, geographical data, and product-related data. There are no missing values or any irrelevant data types and values

## Architecture
![Project Architecture](https://github.com/ShawonSimon/SuperStore-Sales-Data-Engineering/blob/main/screenshots/ProjectArchitecture.jpg?raw=true)
Technology Stack
    - OLTP Database: PostgreSQL
    - ETL Orchestration: Apache Airflow
    - Cloud Storage: Azure Blob Storage
    - Data Warehouse: Azure Synapse Analytics
    - BI Tool: Power BI
    - Local Development Environment: Windows Subsystem for Linux (WSL)

1. Source Database: PostgreSQL stores transactional sales data.
2. ETL Process:
   - Extract: Data fetched from PostgreSQL.
   - Transform: Data cleaning, deduplication, Standardizing data formats and creation of fact/dimension tables (star schema).
   - Load: Processed data uploaded to Azure Blob Storage.
3. Data Warehousing:
   - Data moved to Azure Synapse Analytics.
   - Dedicated SQL pool (shawonDSQL) tables populated with cleaned data.
   - Analytical queries run for insights.
4. Visualization: Power BI dashboards for presenting KPIs and trends.
5. Orchestration: Airflow DAG automates the ETL and data movement processes.

## Dashboards
![](https://github.com/ShawonSimon/SuperStore-Sales-Data-Engineering/blob/main/screenshots/Dashboard2.png?raw=true)
![](https://github.com/ShawonSimon/SuperStore-Sales-Data-Engineering/blob/main/screenshots/Dashboard.png?raw=true)
