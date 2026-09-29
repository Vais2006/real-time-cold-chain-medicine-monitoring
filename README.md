# Data Processing Using Spark DataFrames for Cold-Chain Medicine Monitoring

**Academic Case Study & Practical Implementation**  
**Syllabus Topic Alignment:** Topic 5.2 — Spark DataFrames  
**Core Academic Tools:** PySpark, Pandas  
**Application Extension:** Apache Kafka, PostgreSQL, Grafana, Docker Compose  

---

## 1. Project Title & Overview

**Title:** Data Processing Using Spark DataFrames for Cold-Chain Medicine Monitoring  

### Objective
To build a resilient cold-chain telemetry monitoring and analytics system for temperature-sensitive pharmaceuticals (vaccines, insulins, biologics) during transit. The system processes simulated IoT sensor data, assesses temperature compliance according to safety thresholds, and computes multi-dimensional aggregations to safeguard medicine efficacy.

### Important Academic Separation
This project is engineered specifically for the curriculum requirement:
- **Topic:** Data Processing using Spark DataFrames
- **Task:** Load dataset, perform queries, transformations, and aggregations
- **Required Tools:** PySpark and Pandas

The primary academic focus is **Spark DataFrames**. Secondary tools like Apache Kafka, PostgreSQL, and Grafana serve solely as an application extension. **Grafana does not perform data processing**; all business logic, thresholding, and aggregations are executed via **PySpark DataFrames**.

---

## 2. Cold-Chain Transportation Case Study

Temperature-sensitive medicines must be transported within strictly controlled thermal boundaries:

| Parameter | Demonstration Range | Classification | System Action |
|---|---|---|---|
| **Nominal Thermal Range** | $2.0^\circ\text{C}$ to $8.0^\circ\text{C}$ | **NORMAL** | Standard operation; safe transit conditions |
| **Moderate Excursion** | $0.0^\circ\text{C}$ to $<2.0^\circ\text{C}$ OR $>8.0^\circ\text{C}$ to $10.0^\circ\text{C}$ | **WARNING** | Fleet telemetry warning; inspect cooling units |
| **Severe Excursion** | $< 0.0^\circ\text{C}$ OR $> 10.0^\circ\text{C}$ | **CRITICAL** | Critical alarm; product safety compromised |

> **IMPORTANT CLINICAL & ACADEMIC NOTICE:**  
> These threshold values ($2^\circ\text{C}-8^\circ\text{C}$) are standard demonstration parameters for common biologics (e.g., standard vaccines). Actual pharmaceutical storage/transport requirements vary significantly by medicine, manufacturer, and cold-chain protocol (e.g., ultra-cold $-70^\circ\text{C}$ mRNA vaccines vs. $2^\circ\text{C}-8^\circ\text{C}$ insulin).

### Transportation Fleet & Transit Hubs
- **Pharmaceutical Products:** `Vaccine-A`, `Insulin-B`, `Vaccine-C`, `Medicine-D`, `Vaccine-E`
- **Transit Hubs / Locations:** `Mumbai`, `Navi Mumbai`, `Thane`, `Pune`, `Nashik`
- **Telemetry Monitored:** Temperature ($^\circ\text{C}$), Humidity ($\%RH$), Container Door Status (`OPEN`/`CLOSED`), Vehicle Battery Level ($\%$), Timestamp.

---

## 3. Project Architecture

The system is separated into two decoupled processing stages:

```
=============================================================================
STAGE A: MAIN ACADEMIC DATAFRAME PROCESSING (Standalone & Independent)
=============================================================================
  [Raw Sensor CSV Dataset] (cold_chain_sensor_data.csv)
             │
             ▼
    [Pandas Pipeline] (pandas/data_preparation.py)
      - Dataset inspection (head, info, describe)
      - Missing value auditing (isnull.sum)
      - Data cleaning & invalid record rectification
      - Datatype casting & validation
             │
             ▼
  [Cleaned Sensor CSV] (cleaned_cold_chain_data.csv)
             │
             ▼
    [PySpark Engine] (spark/dataframe_processing.py)
      - SparkSession & inferSchema DataFrame creation
      - Column selection (select)
      - Temperature excursion queries (filter)
      - Compliance classification (withColumn + when)
      - Multi-level grouping (groupBy)
      - Statistical aggregations (count, avg, min, max, agg)
             │
             ▼
  [Evaluator Console Output & Verification]

=============================================================================
STAGE B: REAL-TIME APPLICATION EXTENSION (Streaming & Dashboard)
=============================================================================
  [Cleaned Dataset]
             │
             ▼
  [Pandas Kafka Producer] (producer/producer.py)
             │ (JSON Telemetry via localhost:9093)
             ▼
  [Apache Kafka Message Broker] (Topic: cold-chain-events)
             │ (cc-kafka:29092)
             ▼
  [PySpark Structured Streaming] (spark/cold_chain_stream.py)
      - Spark DataFrame stream parsing
      - Real-time condition & alert_type classification
      - Micro-batch filtering & group-by aggregations
      - foreachBatch sink
             │
             ▼
  [PostgreSQL Relational DB] (Tables: sensor_events, temperature_alerts, shipment_summary)
             │
             ▼
  [Grafana Dashboard] (http://localhost:3001 - Purely Visualization)
```

---

## 4. Directory Structure

```
cold_chain_medicine_monitoring_project/
├── data/
│   ├── cold_chain_sensor_data.csv       # Raw synthetic sensor dataset (with deliberate dirty records)
│   └── cleaned_cold_chain_data.csv      # Cleaned dataset generated by Pandas
│
├── pandas/
│   └── data_preparation.py             # Stage 1: Pandas inspection, cleaning, validation, and export
│
├── spark/
│   ├── dataframe_processing.py          # Stage 2: Main Academic PySpark DataFrame program (Batch)
│   └── cold_chain_stream.py             # Stage 3: PySpark Structured Streaming with foreachBatch sink
│
├── producer/
│   └── producer.py                      # Real-time event publisher using Pandas & KafkaProducer
│
├── sql/
│   └── init.sql                         # PostgreSQL schema (sensor_events, temperature_alerts, shipment_summary)
│
├── grafana/
│   ├── README.md                        # Documentation of all 10 panels, queries, and color legends
│   └── provisioning/
│       ├── datasources/postgres.yml     # Automated PostgreSQL connection config
│       └── dashboards/
│           ├── dashboard_provider.yml   # Automated Grafana dashboard provider
│           └── json/
│               └── medicine_cold_chain_dashboard.json # Pre-built Grafana Dashboard
│
├── docker-compose.yml                   # Docker topology (Zookeeper, Kafka, Postgres, Spark, Grafana)
├── requirements.txt                     # Python dependencies (pandas, pyspark, kafka-python)
├── README.md                            # Comprehensive project & viva documentation
└── run_project.bat                      # Windows interactive runner script
```

---

## 5. Technology Stack & Component Responsibilities

| Technology | Role in Project | Why Used? |
|---|---|---|
| **Pandas** | Data Preparation & Cleaning | Best-in-class tool for exploratory single-node data profiling, missing-value imputation, and preliminary sanitization. |
| **PySpark** | Core Academic DataFrame Analytics | Distributed processing engine implementing DataFrame APIs: `select`, `filter`, `withColumn`, `when`, `groupBy`, and `agg`. |
| **Apache Kafka** | Real-Time Message Bus | High-throughput distributed broker that decouples telemetry producers from real-time stream consumers. |
| **Spark Streaming** | Real-Time DataFrame Processing | Uses Structured Streaming micro-batches to apply condition logic and summarize high-velocity telemetry. |
| **PostgreSQL** | Relational Analytical Store | Persistent storage for processed events, active alerts, and consignment rollups. |
| **Grafana** | Visual Analytics Dashboard | Renders real-time gauges, time-series charts, and alert tables for fleet controllers. |

---

## 6. Dataset Description

The dataset simulates IoT sensors mounted inside reefer (refrigerated) medicine shipping containers:

| Column Name | Data Type | Example Value | Description |
|---|---|---|---|
| `event_id` | String | `EVT-00042` | Unique reading identifier |
| `shipment_id` | String | `SHP-003` | Cargo consignment batch code |
| `medicine` | String | `Vaccine-A` | High-value pharmaceutical product |
| `vehicle_id` | String | `VEH-102` | Dedicated refrigerated vehicle |
| `location` | String | `Pune` | Transit checkpoint / waypoint |
| `temperature` | Float | `5.4` | Cargo chamber temperature in $^\circ\text{C}$ |
| `humidity` | Float | `58.2` | Relative humidity percentage |
| `door_status` | String | `CLOSED` | Chamber seal status (`OPEN` / `CLOSED`) |
| `battery_level` | Float | `94.5` | IoT sensor telematics battery percentage |
| `event_time` | Timestamp | `2026-09-29 08:35:00` | Telemetry transmission timestamp |

The synthetic dataset contains 620+ records featuring:
- **Normal records** ($2.0^\circ\text{C} - 8.0^\circ\text{C}$): ~65%
- **Warning records** ($0.0^\circ\text{C} - 2.0^\circ\text{C}$ and $8.0^\circ\text{C} - 10.0^\circ\text{C}$): ~22%
- **Critical records** ($< 0.0^\circ\text{C}$ and $> 10.0^\circ\text{C}$): ~13%
- **Imperfect records**: Missing values, blank strings, invalid battery values, and duplicate rows to visibly demonstrate Pandas data cleaning.

---

## 7. Step-by-Step Execution Guide (Windows PowerShell)

Open PowerShell and navigate to the project directory:

```powershell
cd C:\Users\PC\Desktop\DE_ISE\cold_chain_medicine_monitoring_project
```

### STEP 1: Run Pandas Data Preparation
Demonstrates data loading, inspection, cleaning, and export using Pandas.

```powershell
python pandas\data_preparation.py
```

**What it does:**
- Runs `read_csv()`, `head()`, `info()`, `describe()`, `isnull().sum()`.
- Strips whitespace, removes duplicate rows, drops invalid rows missing critical identifiers or temperature.
- Imputes missing humidity values with median.
- Saves the clean dataset to `data\cleaned_cold_chain_data.csv`.

---

### STEP 2: Run Main PySpark DataFrame Processing (Academic Core)
Demonstrates all syllabus requirements for Spark DataFrames.

```powershell
python spark\dataframe_processing.py
```

*(Alternatively, if running via the Spark Docker Master:)*
```powershell
docker exec -it cc-spark-master spark-submit /opt/spark/dataframe_processing.py
```

**What it demonstrates:**
1. SparkSession initialization & `inferSchema` DataFrame creation.
2. `printSchema()` and `show()`.
3. `select()` for attribute projection.
4. `filter()` for upper/lower thermal excursions ($>8^\circ\text{C}$ and $<2^\circ\text{C}$).
5. `withColumn()` and `when()` for `condition` and `alert_type` derivation.
6. `groupBy()` and `count()` for medicine and condition distributions.
7. `agg()` with `avg()`, `min()`, `max()`, and `round()`.
8. Consignment alert summary calculations.

---

### STEP 3: Start Real-Time Extension (Docker Infrastructure)

Start all services (Kafka, Zookeeper, PostgreSQL, Spark Master & Worker, Grafana):

```powershell
docker compose up -d
```

Verify that all 6 containers are healthy:

```powershell
docker compose ps
```

---

### STEP 4: Start PySpark Structured Streaming (RUN ONCE)

Submit the streaming pipeline inside the Spark Master container:

```powershell
docker exec -it cc-spark-master spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --master spark://cc-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 /opt/spark/cold_chain_stream.py
```

> [!IMPORTANT]
> Run this command **ONLY ONCE**. The application will continuously listen for Kafka messages and process micro-batches.

---

### STEP 5: Start the Real-Time IoT Telemetry Producer

In a **separate PowerShell terminal**, start the telemetry publisher:

```powershell
cd C:\Users\PC\Desktop\DE_ISE\cold_chain_medicine_monitoring_project
python producer\producer.py
```

The producer reads the cleaned dataset and publishes JSON events to Kafka (`localhost:9093`) with a realistic 1.2-second transmission delay.

---

### STEP 6: Verify PostgreSQL Records

Open a third terminal to inspect database ingestion:

```powershell
# Check total ingested readings
docker exec -it cc-postgres psql -U postgres -d coldchain -c "SELECT COUNT(*) AS total_readings FROM sensor_events;"

# Check temperature alerts categorized by condition
docker exec -it cc-postgres psql -U postgres -d coldchain -c "SELECT condition, alert_type, COUNT(*) FROM temperature_alerts GROUP BY condition, alert_type;"

# View latest 5 events
docker exec -it cc-postgres psql -U postgres -d coldchain -c "SELECT event_id, shipment_id, medicine, temperature, condition, alert_type FROM sensor_events ORDER BY id DESC LIMIT 5;"
```

---

### STEP 7: View the Grafana Intelligence Dashboard

1. Open your web browser and navigate to:
   **[http://localhost:3001](http://localhost:3001)**
2. Login with credentials:
   - **Username:** `admin`
   - **Password:** `admin`
3. The dashboard **"Medicine Cold-Chain Intelligence Dashboard"** is pre-loaded through automated provisioning with 10 real-time panels and a 5-second auto-refresh.

---

## 8. PySpark DataFrame Operations Reference

| Syllabus Requirement | PySpark Method Used | Code Snippet |
|---|---|---|
| **DataFrame Creation** | `spark.read.csv()` | `spark.read.format("csv").option("header","true").option("inferSchema","true").load(path)` |
| **Schema Inspection** | `printSchema()` | `df.printSchema()` |
| **Display Table** | `show()` | `df.show(10, truncate=False)` |
| **Column Projection** | `select()` | `df.select("shipment_id", "medicine", "temperature", "event_time")` |
| **Threshold Filtering** | `filter()` / `where()` | `df.filter(col("temperature") > 8.0)` |
| **Column Engineering** | `withColumn()` | `df.withColumn("condition", when(...).otherwise(...))` |
| **Conditional Logic** | `when()` / `otherwise()` | `when(col("temperature") < 0.0, "CRITICAL").when(col("temperature") < 2.0, "WARNING").otherwise("NORMAL")` |
| **Categorical Grouping**| `groupBy()` | `df.groupBy("medicine").count()` |
| **Aggregation Function**| `agg()` | `df.groupBy("medicine").agg(avg("temperature"), min("temperature"), max("temperature"))` |

---

## 9. Checkpoint Safety & Troubleshooting

### Checkpoint Collision Issue
If you attempt to launch multiple streaming jobs simultaneously, Spark will reject the execution with:
`"Multiple streaming queries are concurrently using file:/tmp/cold-chain-checkpoint"`

**Resolution:**
1. Terminate running background Spark stream jobs:
   ```powershell
   docker exec cc-spark-master pkill -f cold_chain_stream.py
   ```
2. Delete the active checkpoint directory:
   ```powershell
   docker exec cc-spark-master rm -rf /tmp/cold-chain-checkpoint
   ```
3. Start the Spark Structured Streaming command **once**.

*(Do NOT delete the checkpoint during normal runs, as Spark requires it for fault-tolerance and offset tracking.)*

### Port Conflict Troubleshooting
- **Port 9093** (Kafka): If 9093 is in use, modify `docker-compose.yml` to another port (e.g., `9094:9093`).
- **Port 5433** (PostgreSQL): Exposed on host port 5433 to avoid conflicts with any local Windows PostgreSQL instances on 5432.
- **Port 3001** (Grafana): Exposed on 3001 to prevent collisions with applications using 3000.

---

## 10. How to Stop the Project

1. Stop the producer in its terminal: press `Ctrl+C`.
2. Stop the streaming query in its terminal: press `Ctrl+C`.
3. Stop and remove all Docker containers:
   ```powershell
   docker compose down
   ```

---

## 11. Viva Questions & Simple Academic Answers

### 1. What is a Spark DataFrame?
**Answer:** A Spark DataFrame is a distributed collection of structured data organized into named columns, conceptually equivalent to a relational database table or Pandas DataFrame, but optimized with Spark's Catalyst Optimizer for distributed parallel computing across a cluster.

### 2. Why use PySpark?
**Answer:** PySpark enables Python developers to leverage Apache Spark's massive distributed processing engine. It provides high scalability for big data sets that cannot fit into the memory of a single computer, using in-memory distributed computation.

### 3. Why use Pandas?
**Answer:** Pandas is optimized for single-node in-memory data wrangling, initial inspection, schema exploration, and sanitizing raw data before feeding it into distributed engines like PySpark.

### 4. What is the difference between a Pandas DataFrame and a Spark DataFrame?
**Answer:**
- **Pandas DataFrame:** Runs on a single machine, executes operations eagerly (immediately), and is constrained by local RAM.
- **Spark DataFrame:** Runs in a distributed cluster across multiple worker nodes, uses lazy evaluation (builds a Directed Acyclic Graph / DAG until an action is called), and scales to terabytes of data.

### 5. What is `select()` in PySpark?
**Answer:** `select()` is a projection transformation used to choose a specific subset of columns from a DataFrame, similar to the SQL `SELECT` clause.

### 6. What is `filter()` in PySpark?
**Answer:** `filter()` (or `where()`) is a transformation that filters rows based on a boolean condition (e.g., `temperature > 8.0`), returning only records that satisfy the predicate.

### 7. What is `withColumn()` in PySpark?
**Answer:** `withColumn()` is a transformation used to add a new column, rename an existing column, or update values in a column by applying an expression.

### 8. What is `groupBy()` in PySpark?
**Answer:** `groupBy()` groups rows that have matching values in specified columns into summary rows so that aggregation functions (like `count`, `avg`, `min`, `max`) can be computed for each group.

### 9. What is aggregation?
**Answer:** Aggregation is the mathematical process of summarizing multiple rows of data into a single descriptive metric, such as computing an average, sum, minimum, or maximum.

### 10. Why use Apache Kafka?
**Answer:** Kafka acts as a distributed, fault-tolerant message queue that ingests continuous telemetry streams from thousands of IoT sensors and buffers them reliably for downstream processing engines.

### 11. What is Spark Structured Streaming?
**Answer:** It is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine, allowing stream processing queries to be written using the exact same DataFrame APIs used for batch processing.

### 12. Why use PostgreSQL?
**Answer:** PostgreSQL serves as the persistent analytical database that stores structured sensor records, excursion alerts, and shipment rollups for fast querying and indexing.

### 13. Why use Grafana?
**Answer:** Grafana provides an interactive web-based dashboard with customizable charts, gauges, and tables to visually monitor real-time metrics and alerts.

### 14. Does Grafana process the data?
**Answer:** **No.** Grafana only executes read-only SQL queries to display data stored in PostgreSQL. All business logic, temperature threshold classifications, and aggregations are performed by PySpark DataFrames.

### 15. Where is the main syllabus topic demonstrated?
**Answer:** In `spark/dataframe_processing.py`, which loads the cleaned dataset into a PySpark DataFrame and explicitly executes `printSchema()`, `show()`, `select()`, `filter()`, `withColumn()`, `when()`, `groupBy()`, and `agg()` with `count()`, `avg()`, `min()`, and `max()`.

### 16. Why are both Pandas and PySpark used?
**Answer:** Pandas handles local data ingestion, inspection, and missing-data cleaning. PySpark handles large-scale batch DataFrame operations, condition derivations, and streaming analytics.

### 17. What happens when temperature goes outside the specified range?
**Answer:** If temperature is $<0^\circ\text{C}$ or $>10^\circ\text{C}$, the PySpark DataFrame classifies the record as **CRITICAL**. If it is $0^\circ\text{C}$ to $<2^\circ\text{C}$ or $>8^\circ\text{C}$ to $10^\circ\text{C}$, it is classified as **WARNING**. These records are captured in the `temperature_alerts` table and displayed on Grafana.

### 18. What is the difference between batch and streaming processing?
**Answer:**
- **Batch Processing:** Computes transformations on a static, bounded dataset collected over time (e.g., `dataframe_processing.py`).
- **Streaming Processing:** Ingests and processes data continuously in real time as each micro-batch arrives from a message broker (e.g., `cold_chain_stream.py`).

---

## 12. College Demonstration Sequence

Follow this sequence during your academic presentation:

1. **Step 1:** Run `python pandas\data_preparation.py` — explain dataset inspection, missing values, and data cleaning.
2. **Step 2:** Show the generated `data\cleaned_cold_chain_data.csv`.
3. **Step 3:** Run `python spark\dataframe_processing.py` (or via Docker) — walk the evaluator through each numbered output section:
   - Schema and datatypes
   - Selection and filtering
   - Condition classification using `when()`
   - Aggregations (`groupBy`, `count`, `avg`, `min`, `max`, `agg`)
4. **Step 4:** Highlight the academic distinction: explain that DataFrame processing is the primary syllabus objective.
5. **Step 5:** Start Docker (`docker compose up -d`).
6. **Step 6:** Launch Spark Structured Streaming (`docker exec ... spark-submit ...`).
7. **Step 7:** Run the real-time producer (`python producer\producer.py`).
8. **Step 8:** Inspect PostgreSQL records using `psql`.
9. **Step 9:** Open Grafana at `http://localhost:3001` and showcase the real-time cold-chain monitoring dashboard.
