# Cost-Optimised Analytics Platform (Athena vs Snowflake vs Redshift)

## Overview

This project implements a **cost-optimised analytics platform** to compare **serverless querying** and **cloud data warehouses** using real-world data engineering tools and constraints.

Using the **NYC Taxi Trips dataset**, the platform benchmarks **query performance, ingestion complexity, and operational trade-offs** across:

- **AWS Athena** (serverless, pay-per-scan)
- **Snowflake** (cloud data warehouse)
- **Amazon Redshift Serverless** (MPP analytics engine)

The project highlights how identical datasets behave differently across engines due to **schema enforcement, file format compatibility, and ingestion patterns**.

---

## Architecture

```
NYC Taxi Raw Data
        |
        v
Apache Spark (Docker)
  - Cleaning & transformations
  - Schema enforcement
  - Derived columns
        |
        +-----------------------------+
        |                             |
        v                             v
 Parquet (S3)                    CSV (S3)
  (Athena / Snowflake)           (Redshift)
        |                             |
        v                             v
 Athena Queries              Redshift Serverless
 Snowflake Queries
```

---

## Key Technologies

- **Apache Spark** – data transformation and schema handling  
- **Apache Airflow** – orchestration and scheduling  
- **Docker / Docker Compose** – local development environment  
- **AWS S3** – data lake storage  
- **AWS Athena** – serverless SQL querying  
- **Snowflake** – cloud data warehouse  
- **Amazon Redshift Serverless** – MPP analytics engine  

---

## Data Processing

### Spark Transformations

Spark performs the following transformations:

- Schema normalisation across NYC Taxi data  
- Explicit casting and cleaning of numeric and integer columns  
- Handling of invalid values (e.g. `"."`, empty strings, NaN)  
- Derived fields:
  - `pickup_hour`
  - `trip_duration_minutes`
  - `pickup_year`
  - `pickup_month`

### Outputs

Two outputs are produced from the same Spark job:

1. **Partitioned Parquet**
   - Used by Athena and Snowflake
   - Partitioned by `pickup_year` and `pickup_month`

2. **Single CSV file**
   - Used by Redshift Serverless
   - Required due to strict Parquet schema enforcement in Redshift

---

## Orchestration (Airflow)

An Airflow DAG orchestrates the pipeline:

- Raw data ingestion  
- Spark transformation job  
- Writing Parquet and CSV outputs to S3  

Spark runs inside a container to mirror production-style workflows.

---

## Query Benchmarks

Each engine runs the same analytical queries:

### 1. Row Count
```sql
SELECT COUNT(*) FROM fact_trips;
```

### 2. Group-By Aggregation
```sql
SELECT
  PULocationID,
  COUNT(*) AS trip_count
FROM fact_trips
GROUP BY PULocationID
ORDER BY trip_count DESC
LIMIT 10;
```

### 3. Revenue Aggregation
```sql
SELECT
  SUM(total_amount) AS total_revenue
FROM fact_trips;
```

Execution time and cost behaviour are recorded for comparison.

---

## Platform Observations

### Athena
- Very low setup overhead  
- Pay-per-scan pricing  
- Excellent for exploratory analytics  
- Performance dependent on partitioning  

### Snowflake
- Strong schema handling  
- Very tolerant of Parquet schema drift  
- Predictable performance  
- Requires warehouse management  

### Redshift Serverless
- Strong analytical performance  
- Strict schema enforcement  
- CSV ingestion required for reliability  
- Highlights real-world ingestion trade-offs  

---

## Key Learnings

- Parquet schema drift is common in real datasets  
- Different engines handle schema enforcement very differently  
- Spark → CSV is often the most reliable ingestion path for Redshift  
- IAM role design is critical for serverless data platforms  
- Cost-optimised architectures require engine-specific decisions  

---

## Project Value

This project demonstrates:

- End-to-end data engineering design  
- Real-world debugging across distributed systems  
- Practical trade-off analysis between analytics platforms  
- Production-style Spark, Airflow, and AWS workflows  

---

## Future Improvements

- Enforced unified Parquet schema across all partitions  
- Iceberg / Hudi table formats  
- Automated cost tracking per query  
- CI validation of schema compatibility  

---

## Author

**Chris McD**  
Graduate Data Engineer / Applied Data Science  
