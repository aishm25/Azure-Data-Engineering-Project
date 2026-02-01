# Azure AdventureWorks End-to-End Data Engineering Pipeline (Medallion Architecture)

An end-to-end Azure data engineering project using **Azure Data Factory** for ingestion, **ADLS Gen2** for storage, **Azure Databricks** for transformations, **Azure Synapse (Serverless SQL)** for analytics, and **Power BI** for reporting.

The pipeline follows the **Medallion Architecture**:
- **Bronze**: raw ingested files
- **Silver**: cleaned + standardized datasets (Parquet)
- **Gold**: curated, analytics-ready datasets for BI

> Data source: AdventureWorks dataset (ingested via HTTP from GitHub).


## Architecture

**Flow**
1. **Ingest (ADF)**: Copy activity pulls data via HTTP and lands it in **Bronze**.
2. **Transform (Databricks)**: Cleans/standardizes data → writes **Silver** in **Parquet**.
3. **Serve (Synapse Serverless)**: External tables/views on Silver/Gold for easy querying.
4. **Visualize (Power BI)**: Reports built on Synapse (serverless SQL).


## Tech Stack

- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks (Spark)
- Azure Synapse Analytics (Serverless SQL)
- Power BI
