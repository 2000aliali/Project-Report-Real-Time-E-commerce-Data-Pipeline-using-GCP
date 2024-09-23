# Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP
# **Real-Time E-commerce Data Pipeline using GCP**

## **Objective**
The objective of this project is to build a real-time data pipeline to simulate e-commerce transactions, ingest them via Pub/Sub, process the data using Dataflow, and store it in BigQuery for analysis and visualization.

---

## **1. Project Setup**

### Step 1: **Create a New Google Cloud Project**
1. Go to the Google Cloud Console.
2. Select or create a new project: 
   - **Project Name**: `dataengineering-project-2024`
   - **Project ID**: `dataengineering-project-2024`
   
### Step 2: **Create a Service Account**
1. In the Google Cloud Console, navigate to **IAM & Admin**.
2. Create a new service account with the following details:
   - **Name**: `data-pipeline-service-account`
   - **Role**: Assign necessary roles such as **Pub/Sub Publisher**, **BigQuery Admin**, and **Dataflow Admin**.
3. **Generate a Key**: 
   - Choose `JSON` format and download the service account key file (e.g., `dataengineering-project-2024-c303494e3939.json`).

### Step 3: **Enable Necessary APIs**
1. Enable the following APIs for the project:
   - **Pub/Sub API**
   - **BigQuery API**
   - **Dataflow API**

---

## **2. BigQuery Table Creation**

### Step 4: **Create Tables in BigQuery**

#### 1. Location Table
The following SQL command creates the `location_table` in BigQuery:

```sql
CREATE TABLE `dataengineering-project-2024.ecommerce.location_table` (
  location_id STRING NOT NULL,
  city STRING,
  state STRING,
  country STRING
);
```
2. User Table
```sql

CREATE TABLE `dataengineering-project-2024.ecommerce.user_table` (
  user_id STRING NOT NULL,
  name STRING,
  email STRING,
  signup_date DATE
);
```

3. Product Table
```sql
CREATE TABLE `dataengineering-project-2024.ecommerce.product_table` (
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  price FLOAT64
);
```
![ScreenEDEFshot](https://github.com/2000aliali/Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP/blob/main/IMAGES/Bq1.png)


