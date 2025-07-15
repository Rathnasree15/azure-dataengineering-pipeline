## Project Execution

This project implements a **complete Azure data engineering pipeline** using the Medallion Architecture (Bronze → Silver → Gold), integrating multiple Azure services: **Data Factory, ADLS Gen2, Databricks, Synapse Analytics**, and **Power BI**.

---

### Azure Setup

- Created a **Resource Group** and a **Storage Account** with **ADLS Gen2** enabled (via hierarchical namespace).
- Set up four containers in the Data Lake:
  - `bronze/` – Raw ingested data
  - `silver/` – Transformed/cleaned data
  - `gold/` – Serving layer for analytics
  - `parameters/` – Metadata config for dynamic pipelines

---

### Ingestion: Azure Data Factory (ADF)

- Created an **HTTP Linked Service** to GitHub API and an **ADLS Linked Service** to target the Data Lake.
- Built an initial pipeline to **copy data from GitHub to Bronze layer**, testing with one CSV file.
- Upgraded to a **dynamic pipeline**:
  - Created **parameterized datasets** for source and sink
  - Stored filenames, folder names, and URLs in a **JSON file**
  - Uploaded the JSON to the `parameters/` container in ADLS
  - Used **Lookup + ForEach activity** in ADF to loop through the JSON entries and **dynamically ingest multiple files**
- Clicked **Debug** to test, and then **Publish All** to finalize the pipeline.

---

### Transformation: Azure Databricks (Silver Layer)

- Created a **Databricks workspace and cluster** with auto-termination enabled.
- Registered an app in **Microsoft Entra ID (Azure AD)** and captured:
  - Application ID
  - Tenant ID
  - Secret key
- Assigned **Storage Blob Data Contributor** role via IAM to the service principal for secure access.
- Built Databricks notebooks using **PySpark** to:
  - Clean and transform the raw Bronze data
  - Apply business logic, string/date functions, joins, filtering
  - Write transformed data to **Silver layer in Parquet format**
- Also created basic **visualizations within the notebook** to verify transformation outputs.

---

### Serving Layer: Azure Synapse Analytics (Gold Layer)

- Created a **Synapse workspace** and a **Serverless SQL pool**.
- Assigned the **Storage Blob Data Contributor** role to:
  - Synapse’s **Managed Identity**
  - My own **user account** (for querying)
- Created a **Gold schema** and used:
  - `OPENROWSET` to create external views on Silver layer data
  - `CETAS` (Create External Table As Select) to:
    - Materialize Gold layer Parquet files
    - Register them as **external tables** in Synapse

---

### Visualization: Power BI Integration

- Used **Synapse Serverless SQL endpoint** to connect Power BI Desktop.
- Built a basic dashboard with:
  - Sales trend analysis
  - Product/category insights
  - Return rate summaries
  - Regional performance charts
