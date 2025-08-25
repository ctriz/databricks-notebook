
# Delta Live Tables SCD Type 2 Pipeline

This project implements a Slowly Changing Dimension Type 2 (SCD2) pipeline using Databricks Delta Live Tables (DLT). It demonstrates how to ingest raw CSV data, stage it, and apply Change Data Capture (CDC) with SCD2 logic to maintain a historical record of data changes over time.

---

## Overview

This pipeline:

- Ingests raw CSV data files (batch or streaming) into a bronze Delta table.  
- Applies incremental transformations in a staging layer.  
- Uses the `dlt.create_auto_cdc_flow` API to implement SCD Type 2 logic, tracking history with time-bound versions.  
- Supports inserts, updates, and soft deletes via an `is_deleted` flag.
- Designed for seamless integration with Databricks workflows and Auto Loader for continuous updates.

---

## Architecture

CSV Files --> Bronze Raw Table (Delta) --> Staging Table (DLT Streaming) --> SCD2 Target Table (DLT Streaming)

- **Bronze Layer:** Raw ingested data stored as Delta table.  
- **Silver Layer (Staging):** Cleansed and enhanced data with metadata columns.  
- **Gold Layer (SCD2 Target):** Historical tracking of changes with CDC.

---

## Project Structure

- 'raw_data_ingestion.py': Notebook to ingest CSV files into the bronze Delta table. 
- 'daily_snapshots.py': Notebook to ingest updated data (snapshots) files into the bronze Delta table.
- 'staging_tbl.py': DLT pipeline script that processes the bronze table into a streaming staging table, adding metadata and generating keys.  
- 'golden_tbl.py': DLT pipeline applying SCD Type 2 CDC logic using `create_auto_cdc_flow` to maintain historical target table.  

---

## How It Works

- **Raw Data Ingestion:** Ingest raw CSVs by running `raw_data_ingestion.py`. Batch or streaming ingestion into a bronze Delta table. Run this one time manually or you can create a job to pull this from anywhere.
- **Streaming Staging:** DLT streaming table reads from raw bronze table with transformations. Create and run the DLT pipeline with `staging_tbl.py` and `transformation.py`
- **SCD2 Processing:** DLT `create_auto_cdc_flow` maintains versions of records, handling inserts/updates/deletes.  
- **CDC Columns:** Includes `effective_ts`, `expiry_ts`, `is_deleted`, `surrogate_key`, `__START_AT`, and `__END_AT`.
- Use Databricks Workflows to orchestrate notebook runs and pipeline execution.  
- Monitor pipeline run results and validate CDC behavior in target table.

---

## Incremental Updates

- Changes (inserts, updates, deletes) are applied using Delta MERGE into the bronze table.  
- Set `is_deleted` flag for deletes.  
- DLT pipelines read changes incrementally and update the SCD2 target table seamlessly without full refreshes.
