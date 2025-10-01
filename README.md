# 📡 Milan Telecom Data Engineering Project

This project implements a **complete data engineering pipeline** for processing telecom data from Milan.  
It includes both **batch and streaming ETL processes** using:

- **Apache Spark** (batch + streaming ETL)
- **Kafka** (real-time ingestion)
- **MinIO** (object storage, S3-compatible)
- **Airflow** (orchestration)
- **Zookeeper** (Kafka coordination)

The pipeline handles **data ingestion, transformation, aggregation, and storage**.

---

## 🚀 Project Overview

- **Batch Pipeline**:  
  Processes raw telecom data files in batch mode, performs cleaning + aggregations (daily & hourly), and stores results in MinIO as Parquet.

- **Streaming Pipeline**:  
  Simulates real-time data ingestion using a Kafka producer, processes streams with Spark Structured Streaming, and writes aggregated results to MinIO.

- **Orchestration**:  
  Airflow DAGs manage workflows for both batch and streaming pipelines.

- **Storage**:  
  MinIO acts as S3-compatible storage for processed data.

- **Core Components**:
  - Spark Cluster (1 Master + 3 Workers)
  - Kafka + Zookeeper
  - MinIO
  - PostgreSQL (Airflow metadata)
  - Airflow

**Data Format**:  
Telecom events with fields like: `cell_id`, `timestamp`, `country_code`, `sms_in/out`, `call_in/out`, `internet`.

---

## 🧩 ETL Process

### Data Cleaning
Both batch and streaming pipelines include data cleaning steps to ensure quality:
- **Timestamp Conversion**: Convert millisecond timestamps to proper datetime, extracting `date` and `hour`.
- **Handle Missing/Invalid Values**: Fill null or negative values in numeric columns (`sms_in`, `sms_out`, `call_in`, `call_out`, `internet`) with 0.0 (indicating no activity).
- **Filter Invalid Records**: Retain only records where `cell_id` is between 1 and 10,000.

### Aggregations
- **Daily**: Sum metrics (`sms_in_total`, etc.) grouped by `date` and `cell_id`.
- **Hourly**: Sum metrics grouped by `date`, `hour`, and `cell_id` (streaming focuses on hourly).

### ETL Flowchart
Below is a high-level diagram of the ETL pipelines (batch and streaming):

```mermaid
graph TD
    A[Raw Data Files] -->|Batch Load| B[Spark Batch ETL]
    A -->|Streaming Producer| C[Kafka Topic]
    C -->|Spark Streaming| D[Spark Streaming ETL]
    B --> E{Transform & Clean}
    D --> E
    E -->|Clean Data| F[Filter Records]
    F -->|Extract Time| G[Aggregate Data]
    G --> H[Write to MinIO]
    I[Airflow DAGs] -->|Orchestrate| B
    I -->|Orchestrate| D

## ⚙️ Prerequisites

- Docker & Docker Compose  
- Python 3.x (local dev; most runs in containers)  
- 16GB+ RAM recommended (Spark workers use 5GB each)  
- Open ports: 8080 (Airflow), 8081 (Spark UI), 9000/9001 (MinIO), etc.  

---

## 📂 Project Structure

```
milan_project/
├── dags/                     # Airflow DAGs
│   ├── milan_batch_dag.py
│   └── milan_streaming_dag.py
├── scripts/                  # ETL scripts
│   ├── etl.py                # Batch ETL
│   ├── streaming_etl.py      # Streaming ETL
│   └── producer.py           # Kafka producer
├── data/
│   └── raw/                  # Raw telecom data files (*.txt)
├── Dockerfile.spark          # Dockerfile for Spark/Producer images
├── docker-compose.yml        # Main compose file
├── init-airflow.sh           # Airflow initialization script
├── requirements.txt          # Python dependencies for Spark
├── .env                      # Env vars (MinIO creds, etc.)
└── README.md
```

---

## 🛠️ Setup

### Clone the repository
```bash
git clone https://github.com/John-Wassef/Milan-CDR-Batch-Stream-Pipeline.git
cd Milan-CDR-Batch-Stream-Pipeline
```

### Prepare data
Place raw telecom files (*.txt) in:
```
data/raw/
```

### Build images
```bash
docker-compose build
```

### Start services
```bash
docker-compose up -d
```

This starts: Zookeeper, Kafka, Spark cluster, MinIO, PostgreSQL, Airflow.

### Access UIs
- **Airflow**: http://localhost:8080 (user: admin, pass: admin)  
- **Spark Master UI**: http://localhost:8081  
- **MinIO**: http://localhost:9001 (user: minioadmin, pass: minioadmin)  

---

# 🔄 Transformation & Aggregation Summary

## Data Cleaning
- **Timestamp Conversion**: Convert milliseconds to datetime, extract `date` and `hour`
- **Null/Invalid Handling**: Replace nulls/negatives with 0.0 in numeric columns
- **Validation**: Filter `cell_id` between 1-10,000
- **Schema Enforcement**: Strict typing with predefined structure

## Aggregations
### **Daily Aggregates**
- **Group by**: `date`, `cell_id`
- **Metrics**: Sum of `sms_in`, `sms_out`, `call_in`, `call_out`, `internet`

### **Hourly Aggregates** 
- **Group by**: `date`, `hour`, `cell_id`
- **Same metrics** as daily but hourly granularity

## Pipeline Flow
```mermaid
graph LR
    A[Raw Data] --> B[Clean & Transform] --> C[Daily Aggregates]
    B --> D[Hourly Aggregates]
    C --> E[MinIO Storage]
    D --> E

---

## ▶️ Usage

### 📊 Batch Pipeline
Enable & trigger the `milan_batch_pipeline` DAG in Airflow UI.

- Creates MinIO bucket `telecom-data` if missing.  
- Processes raw files → Parquet in `s3a://telecom-data/processed`.

**Command-line alternative:**
```bash
docker exec -it producer bash
spark-submit --master spark://spark:7077     /opt/spark/app/etl.py /opt/spark/data/raw s3a://telecom-data/processed
```

---

### ⚡ Streaming Pipeline
Enable & trigger `milan_streaming_pipeline` in Airflow.

- Producer streams raw file → Kafka topic `telecom_events`.  
- Spark Structured Streaming aggregates hourly → MinIO.  

**Command-line alternative:**

Producer:
```bash
docker exec -it producer python3 /opt/spark/app/producer.py /opt/spark/data/raw/*.txt
```

Spark Streaming Job:
```bash
docker exec -it spark-worker-1 bash
spark-submit --master spark://spark:7077     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0     /opt/spark/app/streaming_etl.py
```

(Streaming job runs ~120s by default, change in code for production)

---

## ⚙️ Configuration

- **MinIO**: Bucket = `telecom-data`, creds in `.env`  
- **Spark**: Configured with MinIO connector; adjust memory in `docker-compose.yml`  
- **Kafka**: Topic = `telecom_events` (auto-created)  
- **Airflow**: Batch DAG = `@daily`, Streaming DAG = manual  

---

## 🛠️ Troubleshooting

View logs:
```bash
docker-compose logs -f <service>
```

- Spark UI: check job status  
- MinIO: check via web console or boto3  
- Ensure raw files exist in `data/raw/`  
- Resolve port conflicts by editing `docker-compose.yml`  

---

## 🧹 Cleanup

```bash
docker-compose down -v
```

(Removes containers + volumes, including data)

---

