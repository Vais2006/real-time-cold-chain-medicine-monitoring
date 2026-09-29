"""
=============================================================================
COLD-CHAIN MEDICINE MONITORING SYSTEM
STAGE 2: SPARK DATAFRAME BATCH PROCESSING & ANALYTICS
=============================================================================
Academic Topic: Data Processing using Spark DataFrames (Syllabus 5.2)
Main Academic Component: PySpark DataFrame Operations, Transformations,
                         Queries, and Multi-Level Aggregations.

Demonstrates:
- SparkSession & DataFrame Initialization
- Schema Inspection (printSchema) & Table Display (show)
- Projection & Column Subsetting (select)
- Multi-Condition Filtering (filter)
- Conditional Column Engineering (withColumn, when, col)
- Categorical Grouping (groupBy)
- Statistical Aggregations (count, avg, min, max, round, agg)
=============================================================================
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, min, max, round


def resolve_data_path():
    """Locate the cleaned CSV dataset across Windows host and Docker environments."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, "..", "data", "cleaned_cold_chain_data.csv"),
        os.path.join(script_dir, "data", "cleaned_cold_chain_data.csv"),
        "/opt/spark/data/cleaned_cold_chain_data.csv",
        "data/cleaned_cold_chain_data.csv"
    ]
    for p in candidates:
        if os.path.exists(p):
            return os.path.abspath(p)
    # Default fallback
    return os.path.abspath(candidates[0])


def main():
    print("=" * 60)
    print("SPARK DATAFRAME PROCESSING")
    print("Cold-Chain Medicine Transportation Analytics")
    print("=" * 60)

    # Initialize PySpark Session
    spark = (
        SparkSession.builder
        .appName("ColdChainMedicineDataFrameProcessing")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    # Reduce verbose Spark log spam for clean academic demonstration
    spark.sparkContext.setLogLevel("WARN")

    # Locate dataset
    data_path = resolve_data_path()
    print(f"\n[*] Spark Session Initialized: Version {spark.version}")
    print(f"[*] Loading Dataset from: {data_path}\n")

    if not os.path.exists(data_path):
        print(f"[ERROR] Cleaned dataset not found at: {data_path}")
        print("Please run `python pandas/data_preparation.py` first to generate the cleaned dataset.")
        spark.stop()
        sys.exit(1)

    # 1. Spark DataFrame Creation with Schema Inference
    df_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data_path)

    # ============================================================
    # 1. DATAFRAME SCHEMA
    # ============================================================
    print("\n" + "=" * 60)
    print("1. DATAFRAME SCHEMA")
    print("=" * 60)
    print("[Explanation: Spark inferSchema extracts column datatypes into an immutable DataFrame Schema]")
    df_raw.printSchema()

    # ============================================================
    # 2. ORIGINAL DATAFRAME
    # ============================================================
    print("\n" + "=" * 60)
    print("2. ORIGINAL DATAFRAME (show())")
    print("=" * 60)
    print(f"Total Rows Loaded: {df_raw.count()}")
    df_raw.show(10, truncate=False)

    # ============================================================
    # 3. SELECT OPERATION
    # ============================================================
    print("\n" + "=" * 60)
    print("3. SELECT OPERATION")
    print("=" * 60)
    print("[Query A: Select Medicine, Location, Temperature, and Event Time]")
    selected_df = df_raw.select("shipment_id", "medicine", "location", "temperature", "event_time")
    selected_df.show(8, truncate=False)

    # ============================================================
    # 4. FILTER OPERATION - HIGH TEMPERATURE
    # ============================================================
    print("\n" + "=" * 60)
    print("4. FILTER OPERATION - HIGH TEMPERATURE (> 8.0 C)")
    print("=" * 60)
    print("[Query B: Cold-chain integrity breach - Above Normal Upper Bound (> 8.0 C)]")
    high_temp_df = df_raw.filter(col("temperature") > 8.0)
    print(f"High Temperature Excursion Count: {high_temp_df.count()}")
    high_temp_df.select("shipment_id", "medicine", "vehicle_id", "location", "temperature", "door_status").show(8, truncate=False)

    # ============================================================
    # 5. FILTER OPERATION - LOW TEMPERATURE
    # ============================================================
    print("\n" + "=" * 60)
    print("5. FILTER OPERATION - LOW TEMPERATURE (< 2.0 C)")
    print("=" * 60)
    print("[Query C: Cold-chain freezing risk - Below Normal Lower Bound (< 2.0 C)]")
    low_temp_df = df_raw.filter(col("temperature") < 2.0)
    print(f"Low Temperature Excursion Count: {low_temp_df.count()}")
    low_temp_df.select("shipment_id", "medicine", "vehicle_id", "location", "temperature", "battery_level").show(8, truncate=False)

    # Additional queries required by academic syllabus
    print("\n--- Additional Query: Filter by Specific Medicine ('Vaccine-A') ---")
    vaccine_a_df = df_raw.filter(col("medicine") == "Vaccine-A")
    print(f"Total readings for Vaccine-A: {vaccine_a_df.count()}")
    vaccine_a_df.select("event_id", "shipment_id", "medicine", "location", "temperature").show(5, truncate=False)

    print("\n--- Additional Query: Filter by Specific Location ('Pune') ---")
    pune_df = df_raw.filter(col("location") == "Pune")
    print(f"Total readings in Pune transit hub: {pune_df.count()}")
    pune_df.select("event_id", "shipment_id", "vehicle_id", "temperature", "door_status").show(5, truncate=False)

    # ============================================================
    # 6. CONDITION CLASSIFICATION & ALERT TYPE
    # ============================================================
    print("\n" + "=" * 60)
    print("6. CONDITION CLASSIFICATION (withColumn & when)")
    print("=" * 60)
    print("Applying Academic Demo Thresholds:")
    print("  * 2.0C to 8.0C                      => NORMAL")
    print("  * 0.0C to <2.0C OR >8.0C to 10.0C   => WARNING")
    print("  * < 0.0C OR > 10.0C                 => CRITICAL")
    print("  (Note: Demonstration thresholds; actual parameters depend on drug specifications)")

    # Transformation using Spark DataFrame withColumn and when()
    classified_df = df_raw.withColumn(
        "condition",
        when((col("temperature") < 0.0) | (col("temperature") > 10.0), "CRITICAL")
        .when((col("temperature") < 2.0) | (col("temperature") > 8.0), "WARNING")
        .otherwise("NORMAL")
    ).withColumn(
        "alert_type",
        when(col("temperature") < 0.0, "TEMPERATURE_TOO_LOW")
        .when(col("temperature") > 10.0, "TEMPERATURE_TOO_HIGH")
        .when((col("temperature") >= 0.0) & (col("temperature") < 2.0), "TEMPERATURE_BELOW_RANGE")
        .when((col("temperature") > 8.0) & (col("temperature") <= 10.0), "TEMPERATURE_ABOVE_RANGE")
        .otherwise("NONE")
    )

    print("\nClassified DataFrame Preview with Generated Columns:")
    classified_df.select("event_id", "medicine", "temperature", "condition", "alert_type").show(10, truncate=False)

    print("\n[Query D: Critical Excursion Records]")
    critical_df = classified_df.filter(col("condition") == "CRITICAL")
    print(f"Total Critical Excursions: {critical_df.count()}")
    critical_df.select("shipment_id", "medicine", "location", "temperature", "alert_type").show(6, truncate=False)

    # ============================================================
    # 7. GROUPBY AND COUNT
    # ============================================================
    print("\n" + "=" * 60)
    print("7. GROUPBY AND COUNT")
    print("=" * 60)

    print("\n[Agg A: Number of Readings per Medicine (groupBy('medicine').count())]:")
    medicine_count_df = classified_df.groupBy("medicine").count().orderBy(col("count").desc())
    medicine_count_df.show(truncate=False)

    print("\n[Agg D: Distribution of Condition Status (groupBy('condition').count())]:")
    condition_count_df = classified_df.groupBy("condition").count().orderBy(col("count").desc())
    condition_count_df.show(truncate=False)

    # ============================================================
    # 8. TEMPERATURE AGGREGATION
    # ============================================================
    print("\n" + "=" * 60)
    print("8. TEMPERATURE AGGREGATION (avg, min, max, agg)")
    print("=" * 60)

    print("\n[Agg B & C: Temperature Metrics per Medicine (groupBy('medicine').agg(...))]:")
    med_temp_metrics_df = classified_df.groupBy("medicine").agg(
        round(avg("temperature"), 2).alias("avg_temperature"),
        round(min("temperature"), 2).alias("min_temperature"),
        round(max("temperature"), 2).alias("max_temperature"),
        round(avg("humidity"), 2).alias("avg_humidity")
    ).orderBy("medicine")
    med_temp_metrics_df.show(truncate=False)

    print("\n[Agg F: Comprehensive Transit Summary (Multi-Metric Aggregation)]:")
    transit_summary_df = classified_df.groupBy("shipment_id", "medicine").agg(
        count("event_id").alias("reading_count"),
        round(avg("temperature"), 2).alias("average_temperature"),
        round(min("temperature"), 2).alias("minimum_temperature"),
        round(max("temperature"), 2).alias("maximum_temperature")
    ).orderBy("shipment_id")
    transit_summary_df.show(10, truncate=False)

    # ============================================================
    # 9. ALERT SUMMARY
    # ============================================================
    print("\n" + "=" * 60)
    print("9. ALERT SUMMARY (groupBy('shipment_id') on Alert Violations)")
    print("=" * 60)

    # Filter only warning & critical records
    alerts_only_df = classified_df.filter(col("condition") != "NORMAL")
    print(f"Total Non-Normal Alert Records: {alerts_only_df.count()}")

    print("\n[Agg E: Total Alerts by Shipment ID]:")
    alerts_by_shipment = alerts_only_df.groupBy("shipment_id").count() \
        .withColumnRenamed("count", "alert_count") \
        .orderBy(col("alert_count").desc())
    alerts_by_shipment.show(10, truncate=False)

    print("\n[Distribution of Alert Types]:")
    alerts_by_type = alerts_only_df.groupBy("alert_type").count() \
        .withColumnRenamed("count", "alert_count") \
        .orderBy(col("alert_count").desc())
    alerts_by_type.show(truncate=False)

    print("\n[Excursions by Transportation Route/Location]:")
    alerts_by_loc = alerts_only_df.groupBy("location").count() \
        .withColumnRenamed("count", "alert_count") \
        .orderBy(col("alert_count").desc())
    alerts_by_loc.show(truncate=False)

    # ============================================================
    # 10. PROCESSING COMPLETED
    # ============================================================
    print("\n" + "=" * 60)
    print("10. PROCESSING COMPLETED")
    print("=" * 60)
    print("[SUCCESS] All Spark DataFrame syllabus requirements successfully executed:")
    print("  [x] Spark DataFrame Creation & InferSchema")
    print("  [x] printSchema() and show()")
    print("  [x] select() column projection")
    print("  [x] filter() multi-condition queries")
    print("  [x] withColumn() and when() classification")
    print("  [x] groupBy() & count()")
    print("  [x] Statistical aggregations: avg(), min(), max(), agg()")
    print("  [x] Domain analysis: Cold-chain compliance & alert categorization")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
