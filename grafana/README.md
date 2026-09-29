# Grafana Visualization Layer: Medicine Cold-Chain Monitoring

> **IMPORTANT ACADEMIC NOTE**:  
> In this architecture, **Grafana is solely the presentation and visualization layer**.  
> Grafana **DOES NOT** perform the data processing, filtering, or condition classification.  
> All ingestion, data transformation, threshold logic (NORMAL/WARNING/CRITICAL), alert categorisation, and batch/streaming aggregations are executed exclusively by **PySpark DataFrames** before being persisted to PostgreSQL.

---

## 1. Connection Details

When connecting Grafana to PostgreSQL:

| Setting | Value (Within Docker Network) | Value (From Windows Host) |
|---|---|---|
| **Host** | `cc-postgres:5432` | `localhost:5433` |
| **Database** | `coldchain` | `coldchain` |
| **Username** | `postgres` | `postgres` |
| **Password** | `postgres` | `postgres` |
| **SSL Mode** | `disable` | `disable` |

---

## 2. Automated Provisioning

This project comes with pre-configured automated provisioning files:
- `provisioning/datasources/postgres.yml`: Automatically registers `ColdChainPostgres` as the default datasource on container boot.
- `provisioning/dashboards/dashboard_provider.yml`: Automatically registers the JSON provider.
- `provisioning/dashboards/json/medicine_cold_chain_dashboard.json`: Preloads the complete **Medicine Cold-Chain Intelligence Dashboard**.

When you open `http://localhost:3001` (login: `admin` / `admin`), the dashboard and datasource are ready immediately.

---

## 3. Dashboard Panels & SQL Queries

The **Medicine Cold-Chain Intelligence Dashboard** includes 10 purpose-built panels:

### Panel 1: Total Sensor Readings
- **Type**: Single Stat Panel
- **Purpose**: Displays the total count of telemetry events processed by Spark and stored in PostgreSQL.
- **SQL**:
  ```sql
  SELECT COUNT(*) AS total_readings
  FROM sensor_events;
  ```

### Panel 2: Total Temperature Alerts
- **Type**: Single Stat Panel (with Color Alert Thresholds)
- **Purpose**: Real-time counter of WARNING and CRITICAL excursion records.
- **SQL**:
  ```sql
  SELECT COUNT(*) AS total_alerts
  FROM temperature_alerts;
  ```

### Panel 3: Average Temperature (°C)
- **Type**: Gauge
- **Purpose**: Shows the fleet-wide average cargo temperature.
- **SQL**:
  ```sql
  SELECT ROUND(AVG(temperature)::numeric, 2) AS average_temperature
  FROM sensor_events;
  ```

### Panel 4: Alerts by Shipment
- **Type**: Bar Chart
- **Purpose**: Pinpoints which consignments have experienced the highest count of excursions.
- **SQL**:
  ```sql
  SELECT shipment_id, COUNT(*) AS alert_count
  FROM temperature_alerts
  GROUP BY shipment_id
  ORDER BY alert_count DESC;
  ```

### Panel 5: Alerts by Location
- **Type**: Bar Chart
- **Purpose**: Identifies geographic hotspots or transit hubs with high temperature violations.
- **SQL**:
  ```sql
  SELECT location, COUNT(*) AS alert_count
  FROM temperature_alerts
  GROUP BY location
  ORDER BY alert_count DESC;
  ```

### Panel 6: Real-Time Temperature Trend
- **Type**: Time Series Line Chart
- **Purpose**: Chronological telemetry plot showing temperature fluctuations over time.
- **SQL**:
  ```sql
  SELECT event_time AS "time", temperature AS "temperature"
  FROM sensor_events
  ORDER BY event_time;
  ```

### Panel 7: Condition Distribution
- **Type**: Pie Chart
- **Purpose**: Visual breakdown of NORMAL vs WARNING vs CRITICAL conditions classified by Spark.
- **SQL**:
  ```sql
  SELECT condition, COUNT(*) AS count
  FROM sensor_events
  GROUP BY condition
  ORDER BY count DESC;
  ```

### Panel 8: Alert Type Distribution
- **Type**: Pie / Donut Chart
- **Purpose**: Categorization of alert specifics (TEMPERATURE_TOO_HIGH, TEMPERATURE_TOO_LOW, etc.).
- **SQL**:
  ```sql
  SELECT alert_type, COUNT(*) AS alert_count
  FROM temperature_alerts
  GROUP BY alert_type
  ORDER BY alert_count DESC;
  ```

### Panel 9: Average Temperature by Medicine
- **Type**: Horizontal Bar Chart
- **Purpose**: Evaluates thermal stability per individual medicine product.
- **SQL**:
  ```sql
  SELECT medicine,
         ROUND(AVG(temperature)::numeric, 2) AS average_temperature
  FROM sensor_events
  GROUP BY medicine
  ORDER BY average_temperature DESC;
  ```

### Panel 10: Latest Temperature Alerts
- **Type**: Table
- **Purpose**: Real-time tabular log of the 10 most recent cold-chain excursions for immediate incident intervention.
- **SQL**:
  ```sql
  SELECT event_time,
         shipment_id,
         medicine,
         location,
         temperature,
         condition,
         alert_type
  FROM temperature_alerts
  ORDER BY event_time DESC
  LIMIT 10;
  ```

---

## 4. Temperature Threshold Legend (Demonstration Reference)

| Category | Temperature Range | Visual Color | Action Required |
|---|---|---|---|
| **NORMAL** | 2.0°C to 8.0°C | Green | Nominal transportation conditions |
| **WARNING** | 0.0°C to <2.0°C OR >8.0°C to 10.0°C | Orange / Yellow | Driver notification / inspect refrigeration unit |
| **CRITICAL** | < 0.0°C OR > 10.0°C | Red | Immediate containment / potential product degradation |

*Note: In commercial logistics, actual temperature ranges differ by pharmaceutical product and regulatory standards.*
