# Fitbit Data Engineering Project

## Overview

This project is designed to process, transform, and analyze granular health behavior data captured by Fitbit devices. It showcases an end-to-end data engineering pipeline — from raw data ingestion to curated insights delivered through Power BI dashboards.

The main objective is to derive personalized health and lifestyle insights such as physical activity patterns, heart rate trends, sleep quality, and BMI trends by processing CSV data sourced from Fitbit participants.

## Architecture

The data pipeline follows a **Bronze → Silver → Gold** layered architecture:

- **Bronze Layer (RAW + STAGING)**: Raw CSV ingestion, light cleansing, timestamp formatting.
- **Silver Layer (CORE)**: dbt transformations, joins across multiple sources (activity, sleep, heart rate), and derived KPIs.
- **Gold Layer (MART + REPORTING)**: Star/Galaxy schema models with fact/dimension tables, optimized for reporting.

## Tech Stack

- **Snowflake**: Cloud data warehouse for storing and processing data
- **Python + SnowSQL**: For automation and data ingestion
- **dbt (Data Build Tool)**: For transformations and model orchestration
- **Power BI**: For dashboarding and visual insights
- **Pandas & Snowflake Connector**: Used in the ingestion script

## Project Structure

```
fitbit-data-engineering/
├── ingestion/
│   └── ingest_fitbit_csvs.py        # Python script for ingestion & table creation
├── dbt/
│   ├── models/
│   │   ├── staging/                 # stg_ models (Bronze Layer)
│   │   ├── core/                    # int_ models (Silver Layer)
│   │   ├── marts/                   # fact_ and dim_ models (Gold Layer)
│   │   └── reporting/              # report_ models for BI
│   └── dbt_project.yml
├── README.md
└── requirements.txt
```

## Ingestion Steps

1. **Upload CSVs to Snowflake Stage**  
   Compressed CSVs from two time windows are uploaded using `PUT` commands:
   - `Fitabase Data 3.12.16-4.11.16`
   - `Fitabase Data 4.12.16-5.12.16`
  - source: https://www.kaggle.com/datasets/arashnic/fitbit
2. **Automated Table Creation and Loading**  
   The Python script:
   - Reads each CSV
   - Infers schema using pandas
   - Creates tables in the `RAW_LAYER`
   - Loads data with `COPY INTO` using a defined file format

## Transformation Logic with dbt

- Models are structured as:
  - **Staging**: Field renaming, deduplication, timestamp parsing
  - **Core**: Joins and intermediate logic
  - **Marts**: Fact/dim models based on Galaxy schema
- Key facts include:
  - `fact_daily_sleep_activity`
  - `fact_minute_heart_rate_analysis`
  - `fact_daily_weight_metrics`
  - `fact_hourly_metrics`

- Shared dimensions:
  - `dim_users`, `dim_heart_rate_zone`, `dim_intensity_level`, etc.

## KPIs and Insights

- **Calories Burned per Step**
- **Sleep Efficiency & Time in Bed**
- **Weight Trend & BMI Health Status**
- **Heart Rate Zone Analysis**
- **Daily and Hourly Activity Metrics**

These are visualized using **Power BI**, providing interactive reports on user health.

## Getting Started

1. Clone the repo:
   ```bash
   git clone https://github.com/Swathijettiboina/FitBit_Data_Analysis.git
   ```

2. Set up Python environment:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure Snowflake credentials in your `.env` or Python script.

4. Run the ingestion script:
   ```bash
   python ingestion/ingest_fitbit_csvs.py
   ```

5. Navigate to `dbt/` and run transformations:
   ```bash
   dbt run
   dbt docs generate
   ```

6. Load models in Power BI from the Reporting schema.

## Author

**Swathi Jettiboina**  
Email: swathi.j@jmangroup.com
