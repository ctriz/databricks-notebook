
----------

# Data Pipeline Using Databricks Delta Live Tables (DLT)

This project implements a Slowly Changing Dimension Type 2 (SCD2) data pipeline using **Databricks Delta Live Tables (DLT)**. It simulates transactional data ingestion from Postgres into a Bronze Delta table and orchestrates transformations through Silver and Gold layers to support business intelligence and advanced analytics.

----------

## Architecture Overview

The pipeline follows a **Medallion Architecture** with three core layers:

**Raw Data (CSV) → Bronze Raw Table (Delta) → Silver Staging Layer (DLT) → Gold Analytics Tables (DLT)**

-   **Bronze Layer**  
    This is the raw data landing zone. Simulated transactional data files are ingested into a monitored DataBricks folder. A Spark Streaming job processes these files and writes the data into a Bronze Delta table. This layer acts as the immutable source of truth for all downstream transformations. (Note: Airflow orchestration is out of scope for this experiment.)
    
-   **Silver Layer (Staging)**  
    The data in this layer is cleansed and enhanced. The DLT pipeline reads from the Bronze layer, enriches data by adding a `surrogate_key`, and enforces data quality rules to ensure validity and consistency.
    
-   **Gold Layer (SCD2 History and Analytics)**  
    The final layer is optimized for specific business use cases:
    
    -   Maintains an SCD2 history table to track changes over time, serving as the core source of truth for business analysis.
        
    -   The `clean_golden_tbl.py` script produces a simplified table containing only currently active employee records, ideal for standard BI reporting.
        
    -   The `training_salary_attribution.py` script merges clean salary data with external training datasets to create a feature-rich dataset suited for machine learning models (e.g., salary attribution).
        
    -   The `salary_gender_bias.py` script aggregates data to compute average salary by gender, enabling fairness and bias analytics.
        ----------

## How It Works

-   **Raw Data Ingestion**  
    The `raw_data_ingestion.py` script ingests CSV files into the Bronze Delta table using Delta Lake’s efficient `MERGE` statements to apply inserts, updates, and deletes.
    
-   **DLT Pipeline Orchestration**  
    The pipeline structure is defined across multiple scripts: `staging_tbl.py`, `golden_tbl.py`, `clean_golden_tbl.py`, `training_salary_attribution.py`, and `salary_gender_bias.py`. Databricks DLT manages dependency resolution, scheduling, and table creation declaratively, simplifying pipeline management.
    
-   **Automated Change Data Capture (CDC)**  
    Utilizing DLT’s `create_auto_cdc_flow` API, the complex SCD2 logic is automated, ensuring accurate historical record-keeping with minimal manual overhead.
    
-   **Downstream Analytics and Modeling**  
    The Gold layer tables provide tailored datasets ready for diverse use cases, ranging from BI dashboarding to machine learning and statistical fairness analysis, showcasing the adaptability of the layered architecture.
    
----------

This pipeline demonstrates modern best practices in building scalable, maintainable data flows with Delta Live Tables, making it easier to deliver reliable analytics from raw data through to actionable business insights.

----------
