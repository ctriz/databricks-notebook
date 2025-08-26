
# Delta Live Tables SCD Type 2 Pipeline

This project implements a Slowly Changing Dimension Type 2 (SCD2) pipeline using Databricks Delta Live Tables (DLT). This pipeline:

- Ingests raw CSV data files (batch or streaming) into a bronze Delta table.  
- Applies incremental transformations in a staging layer.  
- Uses the `dlt.create_auto_cdc_flow` API to implement SCD Type 2 logic, tracking history with time-bound versions.  
- Supports inserts, updates, and soft deletes via an `is_deleted` flag.
- Designed for seamless integration with Databricks workflows and Auto Loader for continuous updates.

---

## Architecture

Raw Data --> Bronze Raw Table (Delta) --> Staging Table (DLT Streaming) --> SCD2 Target Table (DLT Streaming)

- **Bronze Layer:** Raw ingested data stored as Delta table.  
- **Silver Layer (Staging):** Cleansed and enhanced data with metadata columns.  
- **Gold Layer (SCD2 Target):** Historical tracking of changes with CDC.

---

## How It Works

- **Raw Data Ingestion:** Ingest raw CSVs by running `raw_data_ingestion.py`. Batch or streaming ingestion into a bronze Delta table. Run this one time manually or you can create a job to pull this from anywhere.
- **Streaming Staging:** DLT streaming table reads from raw bronze table with transformations. Create and run the DLT pipeline with `staging_tbl.py` and `transformation.py`
- **SCD2 Processing:** DLT `create_auto_cdc_flow` maintains versions of records, handling inserts/updates/deletes.  
- **CDC Columns:** Includes `effective_ts`, `expiry_ts`, `is_deleted`, `surrogate_key`, `__START_AT`, and `__END_AT`. Use Databricks Workflows to orchestrate notebook runs and pipeline execution.  Monitor pipeline run results and validate CDC behavior in target table.

- **Incremental Updates:** Changes (inserts, updates, deletes) are applied using Delta MERGE into the bronze table. Set `is_deleted` flag for deletes. DLT pipelines read changes incrementally and update the SCD2 target table seamlessly without full refreshes.

## Next Upcoming

- **Airflow orchestrated Kafka ingestion with Schema Registry
- **dbt Core implementation for Medallion layering 

