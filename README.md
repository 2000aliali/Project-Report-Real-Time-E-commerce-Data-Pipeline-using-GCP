# Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP
# **Real-Time E-commerce Data Pipeline using GCP**

## **Objective**
The objective of this project is to build a real-time data pipeline to simulate e-commerce transactions, ingest them via Pub/Sub, process the data using Dataflow, and store it in BigQuery for analysis and visualization.

---

## **1. Project Setup**
### system archicture 
![ScreenEDEFshot](https://github.com/2000aliali/Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP/blob/main/IMAGES/arche.png)

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

## **3. Data Pipeline**

### Step 5: **Setup Pub/Sub Topic**
In the Google Cloud Console, go to Pub/Sub and create a topic:
Topic Name: `realtime-dashboard-data`
Configure Pub/Sub Permissions: Ensure that the service account has the `roles/pubsub.publisher` permission to publish data to the topic.
![ScreenEDEFshot](https://github.com/2000aliali/Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP/blob/main/IMAGES/PUB%20SUB%201.png)
 
Step 6: Python Code to Simulate Data and Publish to Pub/Sub
In your Python script `main.py` , use the Google Cloud Pub/Sub client library to publish e-commerce transactions in real-time.


#### 1. Setup the Google Cloud Clients:
Initialize the Pub/Sub and BigQuery clients using the service account credentials.

#### 2. Preload Dimension Tables:
Insert predefined user, product, and location data into the respective BigQuery tables (user_table, product_table, location_table).
#### 3. Simulate Transaction Data:
Generate random e-commerce transaction data for a user purchasing a product at a particular location.

#### 4. Publish to Pub/Sub:
Publish each transaction to Pub/Sub in real-time.


### Step 7: **Create Dataflow Pipeline**
Create a Dataflow Template:

 `bash
gcloud dataflow jobs run dataflow-pubsub-to-bq-job --gcs-location gs://dataflow-templates/latest/PubSub_to_BigQuery --region europe-west2 --staging-location gs://dataengineering_staging/tmp/ --parameters "inputTopic=projects/dataengineering-project-2024/topics/realtime-dashboard-data,outputTableSpec=dataengineering-project-2024:ecommerce.raw_data"
 `

 In Google Cloud Console, create a Dataflow pipeline that reads messages from Pub/Sub and writes them to BigQuery.
 
![ScreenEDEFshot](https://github.com/2000aliali/Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP/blob/main/IMAGES/DATAFLOW1.png)
![ScreenEDEFshot](https://github.com/2000aliali/Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP/blob/main/IMAGES/DATAFLOW%202.png)

 Create the Materialized View
You can create a materialized view that aggregates the total sales by product, user, and location, and also includes the timestamp for time-based analysis. Here's an example SQL statement to create the materialized view:

```sql
Copier le code
CREATE MATERIALIZED VIEW `dataengineering-project-2024.ecommerce.sales_summary_mv` AS
SELECT 
    r.transaction_id,
    r.timestamp,
    r.user_id,
    r.product_id,
    r.location_id,
    r.amount,
    DATE(r.timestamp) AS transaction_date,
    EXTRACT(HOUR FROM r.timestamp) AS transaction_hour
FROM 
    `dataengineering-project-2024.ecommerce.raw_data` r
```

![ScreenEDEFshot](https://github.com/2000aliali/Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP/blob/main/IMAGES/bq_mv.png)



##### For the final result, feel free to check out this link https://lookerstudio.google.com/reporting/f075cb18-8fa9-437a-a534-737eca95ec04/page/tEnnC
![ScreenEDEFshot](https://github.com/2000aliali/Project-Report-Real-Time-E-commerce-Data-Pipeline-using-GCP/blob/main/IMAGES/DASH1.png)
## Conclusion
This project demonstrates the full data pipeline from simulating e-commerce transaction data to ingesting and streaming it using Pub/Sub, processing it with Dataflow, and finally storing it in BigQuery for analysis. The use of GCP services such as Pub/Sub, Dataflow, and BigQuery allows for real-time, scalable, and reliable data ingestion and processing, which can be applied to various real-time analytics and reporting scenarios.
