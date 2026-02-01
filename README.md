## Azure AdventureWorks Data Engineering Project ##
**End-to-End Medallion Architecture using Azure Cloud**

---

## 📌 Project Overview

This project demonstrates an **end-to-end data engineering pipeline on Azure** using the **AdventureWorks dataset**.  
The goal is to design a **scalable, analytics-ready platform** following the **Medallion Architecture (Bronze, Silver, Gold)**, enabling reliable ingestion, transformation, and reporting.

<img width="1536" height="1024" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/cc724db4-b115-4b92-bee6-5ab82393f257" />


The pipeline:
- Ingests raw data from an **HTTP source**
- Processes data using **Apache Spark in Azure Databricks**
- Serves analytics-ready data via **Azure Synapse Serverless SQL**
- Visualizes insights using **Power BI**

---

## 🏗️ Architecture Overview

The solution follows a **layered architecture** to ensure **data quality, scalability, and performance**.


### High-Level Flow
- **Data ingestion** from an HTTP source using **Azure Data Factory**
- **Raw data storage** in **ADLS Gen2 (Bronze layer)**
- **Data transformation** using **Databricks**, written in **Parquet format (Silver layer)**
- **Analytics-ready data exposure** via **Synapse Serverless SQL (Gold layer)**
- **Reporting and visualization** through **Power BI**

## ⚙️ Step 1: Azure Environment Setup

The following Azure resources were provisioned to support the end-to-end data pipeline:

- **Azure Data Factory (ADF):** Used for data ingestion, orchestration, and automation  
- **Azure Data Lake Storage Gen2:** Acts as the centralized data lake, storing **Bronze, Silver, and Gold** data layers  
- **Azure Databricks:** Performs scalable data transformations using **Apache Spark**  
- **Azure Synapse Analytics (Serverless SQL):** Enables analytics and BI querying without dedicated infrastructure  

All services were configured with appropriate **Identity and Access Management (IAM)** roles and permissions to ensure:
- Secure service-to-service communication  
- Seamless data access across Azure components  
- Adherence to best practices for cloud security

<img width="1717" height="532" alt="image" src="https://github.com/user-attachments/assets/c6065b99-4a03-41ec-bed5-402274535e5c" />

---

## 🥉🥈🥇 Medallion Architecture

### 🥉 Bronze Layer – Raw Data
- Stores **raw, unprocessed data**
- Acts as a **landing zone** for ingestion
- Data is stored in **ADLS Gen2** without transformations

**Technologies Used**
- **Azure Data Factory**
- **Azure Data Lake Storage Gen2**

---

### 🥈 Silver Layer – Transformed Data
- Data is **cleaned, standardized, and optimized**
- Handles **invalid records and inconsistencies**
- Stored in **Parquet format** for improved performance

**Technologies Used**
- **Azure Databricks (Apache Spark)**
- **Azure Data Lake Storage Gen2**

---

### 🥇 Gold Layer – Analytics-Ready Data
- **Curated datasets** optimized for reporting
- Exposed using **SQL views and external tables**
- Designed for **BI and analytics consumption**

**Technologies Used**
- **Azure Synapse Analytics (Serverless SQL)**

<img width="1672" height="803" alt="image" src="https://github.com/user-attachments/assets/d33e740b-33c5-4b09-a678-36d7dfac80b0" />

---

## 🔧 Technologies Used
- **Azure Data Factory** – Data ingestion & orchestration  
- **Azure Data Lake Storage Gen2** – Centralized data storage  
- **Azure Databricks** – Data transformation using Apache Spark  
- **Azure Synapse Analytics (Serverless SQL)** – Data serving layer  
- **Power BI** – Reporting and visualization  

---

## ⚙️ Data Pipeline Workflow

### 1️⃣ Data Ingestion
- ADF pipelines ingest data from an **HTTP endpoint**

<img width="1867" height="786" alt="image" src="https://github.com/user-attachments/assets/79ca1dfb-a501-4acc-8ae8-136dc283d796" />

- Data is landed into the **Bronze container** in ADLS Gen2

- <img width="1880" height="776" alt="image" src="https://github.com/user-attachments/assets/a18b6ffa-3fcb-49df-b474-bd53bd4ebabc" />


---

### 2️⃣ Data Transformation
- Databricks notebooks read raw data from the **Bronze layer**

<img width="1783" height="900" alt="image" src="https://github.com/user-attachments/assets/b1552997-4b03-4c2a-8efd-dd3f6586a00a" />

- Apply **cleaning, formatting, and schema standardization**

<img width="1693" height="911" alt="image" src="https://github.com/user-attachments/assets/11e4aa63-dbc8-4a10-8ff3-4bf7ca179353" />

- Output is written to the **Silver layer** in Parquet format

<img width="1542" height="722" alt="image" src="https://github.com/user-attachments/assets/456671b8-0a2c-424d-91e1-670c1c075576" />

---

### 3️⃣ Data Serving
- **Synapse Serverless SQL** creates **external tables and views**

<img width="1868" height="760" alt="image" src="https://github.com/user-attachments/assets/279333df-a91c-4ac4-b5aa-2288845ff6f1" />

<img width="1641" height="730" alt="image" src="https://github.com/user-attachments/assets/d2200b0b-a482-4467-8d93-b36e37254d1e" />


- Queries directly read **Parquet files from ADLS**

<img width="1596" height="438" alt="image" src="https://github.com/user-attachments/assets/1593793f-d11d-48b7-b906-fa9a324f2e99" />

---

### 4️⃣ Reporting
- **Power BI** connects to **Synapse Serverless SQL**
- Interactive dashboards are built on **Gold layer datasets**

<img width="1912" height="1016" alt="image" src="https://github.com/user-attachments/assets/344beb40-40d5-42d5-8d57-56365c9ff6f1" />


---

## 🌐 Key Takeaways

This project highlights how Azure services can be orchestrated to build a **reliable and scalable data engineering solution**.  
By integrating **Azure Data Factory, Databricks, Synapse Analytics, and Power BI**, the pipeline delivers:

- **End-to-end automation** for smooth data movement across layers  
- **Scalable processing** capable of handling growing data volumes  
- **Performance optimization** using Parquet storage and serverless SQL  
- **Business-ready insights** through interactive Power BI dashboards  

Overall, this implementation showcases how Azure enables organizations to convert **raw data into analytics-ready datasets**, supporting faster and more informed decision-making.

---

## 🎉 Acknowledgment

This project was inspired by learning resources and community-driven content available online, including an excellent walkthrough by **Ansh Lamba**.  
