"""
=============================================================================
COLD-CHAIN MEDICINE MONITORING SYSTEM
STAGE 3: SPARK STRUCTURED STREAMING & REAL-TIME ANALYTICS
=============================================================================
Component: PySpark Structured Streaming Pipeline
Role:
- Ingests cold-chain IoT events from Kafka broker ('cold-chain-events').
- Unpacks JSON payload into a typed Spark DataFrame schema.
- Transforms raw sensor telemetry:
    * Casts timestamps
    * Classifies compliance condition (NORMAL, WARNING, CRITICAL)
    * Generates descriptive alert classifications
- Filters alert excursions in real time.
- Computes tumbling micro-batch aggregations per shipment and medicine.
- Persists processed events, alerts, and summaries into PostgreSQL via foreachBatch.
=============================================================================
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, count, avg, min, max, round, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


def get_spark_session():
    return (
        SparkSession.builder
        .appName("ColdChainMedicineStreamingEngine")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def define_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("shipment_id", StringType(), True),
        StructField("medicine", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("door_status", StringType(), True),
        StructField("battery_level", DoubleType(), True),
        StructField("event_time", StringType(), True)
    ])


def main():
    print("=" * 60)
    print("PYSPARK STRUCTURED STREAMING: COLD-CHAIN MONITORING")
    print("=" * 60)

    # Configuration from environment with robust defaults
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "cc-kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "cold-chain-events")

    pg_host = os.getenv("POSTGRES_HOST", "cc-postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "coldchain")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/cold-chain-checkpoint")

    print(f"[*] Kafka Source:      {kafka_bootstrap} (Topic: {kafka_topic})")
    print(f"[*] PostgreSQL Sink:   {pg_url}")
    print(f"[*] Checkpoint Dir:    {checkpoint_dir}\n")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    sensor_schema = define_schema()

    # 1. Read Stream from Apache Kafka
    raw_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON Value Payload into Structured Columns
    parsed_df = (
        raw_stream_df
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), sensor_schema).alias("data"))
        .select("data.*")
    )

    # 3. Apply Cold-Chain Compliance Transformations
    # Academic Demo Thresholds:
    # 2C to 8C = NORMAL
    # 0C to <2C OR >8C to 10C = WARNING
    # <0C OR >10C = CRITICAL
    condition_expr = (
        when((col("temperature") < 0.0) | (col("temperature") > 10.0), "CRITICAL")
        .when((col("temperature") < 2.0) | (col("temperature") > 8.0), "WARNING")
        .otherwise("NORMAL")
    )

    alert_type_expr = (
        when(col("temperature") < 0.0, "TEMPERATURE_TOO_LOW")
        .when(col("temperature") > 10.0, "TEMPERATURE_TOO_HIGH")
        .when((col("temperature") >= 0.0) & (col("temperature") < 2.0), "TEMPERATURE_BELOW_RANGE")
        .when((col("temperature") > 8.0) & (col("temperature") <= 10.0), "TEMPERATURE_ABOVE_RANGE")
        .otherwise("NONE")
    )

    processed_df = (
        parsed_df
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("condition", condition_expr)
        .withColumn("alert_type", alert_type_expr)
    )

    # 4. Micro-Batch Processing Function for PostgreSQL Storage & Console Output
    def process_micro_batch(batch_df, batch_id):
        batch_count = batch_df.count()
        if batch_count == 0:
            return

        print("\n" + "=" * 60)
        print(f"SPARK STRUCTURED STREAMING - MICRO-BATCH #{batch_id} ({batch_count} records)")
        print("=" * 60)

        # A. Print Schema
        print("\n1. DATAFRAME SCHEMA:")
        batch_df.printSchema()

        # B. Processed DataFrame Preview
        print("\n2. PROCESSED DATAFRAME:")
        batch_df.select(
            "event_id", "shipment_id", "medicine", "location", "temperature", "condition", "alert_type"
        ).show(6, truncate=False)

        # C. Filtered Alert DataFrame (WARNING & CRITICAL only)
        alerts_df = batch_df.filter(col("condition") != "NORMAL")
        alert_count = alerts_df.count()
        print(f"\n3. FILTERED ALERT DATAFRAME ({alert_count} alert(s) in this batch):")
        if alert_count > 0:
            alerts_df.select("shipment_id", "medicine", "location", "temperature", "condition", "alert_type").show(truncate=False)
        else:
            print("   [All readings in this batch are within NORMAL range (2.0C - 8.0C)]")

        # D. GroupBy + Aggregations
        print("\n4. GROUPBY + AGG DATAFRAME (Shipment Summary):")
        summary_df = batch_df.groupBy("shipment_id", "medicine").agg(
            count("event_id").alias("reading_count"),
            round(avg("temperature"), 2).alias("avg_temperature"),
            round(min("temperature"), 2).alias("min_temperature"),
            round(max("temperature"), 2).alias("max_temperature"),
            count(when(col("condition") != "NORMAL", 1)).alias("violation_count")
        )
        summary_df.show(truncate=False)

        # Write to PostgreSQL
        try:
            # 1. sensor_events
            batch_df.write \
                .format("jdbc") \
                .option("url", pg_url) \
                .option("dbtable", "sensor_events") \
                .option("user", pg_user) \
                .option("password", pg_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            # 2. temperature_alerts
            if alert_count > 0:
                alerts_df.write \
                    .format("jdbc") \
                    .option("url", pg_url) \
                    .option("dbtable", "temperature_alerts") \
                    .option("user", pg_user) \
                    .option("password", pg_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

            # 3. shipment_summary
            if summary_df.count() > 0:
                summary_df.withColumn("created_at", current_timestamp()).write \
                    .format("jdbc") \
                    .option("url", pg_url) \
                    .option("dbtable", "shipment_summary") \
                    .option("user", pg_user) \
                    .option("password", pg_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

            print(f"[SUCCESS] Micro-batch #{batch_id} persisted to PostgreSQL successfully.")
        except Exception as ex:
            print(f"[ERROR] Failed writing micro-batch #{batch_id} to PostgreSQL: {ex}")

    # 5. Start Streaming Query with foreachBatch
    query = (
        processed_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    print("[*] Spark Structured Streaming pipeline started.")
    print("[*] Listening for events on Kafka topic... (Press Ctrl+C to terminate)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
