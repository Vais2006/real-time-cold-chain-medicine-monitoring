"""
=============================================================================
COLD-CHAIN MEDICINE MONITORING SYSTEM
REAL-TIME EVENT PRODUCER
=============================================================================
Component: Python / Pandas Kafka Telemetry Producer
Role:
- Reads the cleaned IoT sensor dataset using Pandas.
- Connects to the Apache Kafka message broker (localhost:9093 or cc-kafka:29092).
- Continuously serializes cold-chain telemetry readings into JSON.
- Publishes records to topic 'cold-chain-events' with a realistic transmission cadence.
=============================================================================
"""

import os
import sys
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError


def resolve_data_path():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates = [
        os.path.join(base_dir, "data", "cleaned_cold_chain_data.csv"),
        os.path.join(base_dir, "data", "cold_chain_sensor_data.csv"),
        "/opt/spark/data/cleaned_cold_chain_data.csv",
        "data/cleaned_cold_chain_data.csv"
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return candidates[0]


def create_kafka_producer(bootstrap_servers, max_retries=15, delay=3):
    print(f"[*] Connecting to Kafka Broker at: {bootstrap_servers}")
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                retries=5,
                request_timeout_ms=10000
            )
            print(f"[SUCCESS] Connected to Kafka Broker on attempt {attempt}!")
            return producer
        except BrokerNotAvailableError:
            print(f"[-] Kafka broker not ready yet (attempt {attempt}/{max_retries}). Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            print(f"[-] Connection warning: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    print(f"[ERROR] Could not connect to Kafka Broker at {bootstrap_servers} after {max_retries} attempts.")
    return None


def run_producer():
    # Kafka bootstrap servers: localhost:9093 for Windows host; cc-kafka:29092 inside docker network
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    topic = os.getenv("KAFKA_TOPIC", "cold-chain-events")
    delay_sec = float(os.getenv("SEND_INTERVAL_SEC", "1.2"))

    csv_path = resolve_data_path()
    if not os.path.exists(csv_path):
        print(f"[ERROR] Dataset not found at: {csv_path}")
        print("Please run `python pandas/data_preparation.py` first.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("REAL-TIME COLD-CHAIN PRODUCER")
    print("=" * 60)
    print(f"[*] Reading telemetry records using Pandas from: {csv_path}")
    df = pd.read_csv(csv_path)
    total_records = len(df)
    print(f"[*] Loaded {total_records} cleaned sensor records ready for streaming.")

    producer = create_kafka_producer(bootstrap_servers)
    if not producer:
        print("[ERROR] Exiting due to Kafka broker unavailability.")
        sys.exit(1)

    print(f"[*] Publishing events to Kafka topic: '{topic}'")
    print("------------------------------------------------------------")
    print("Press Ctrl+C to stop publishing.")
    print("------------------------------------------------------------\n")

    event_counter = 0
    cycle = 1

    try:
        while True:
            for idx, row in df.iterrows():
                event_counter += 1
                record = {
                    "event_id": str(row["event_id"]),
                    "shipment_id": str(row["shipment_id"]),
                    "medicine": str(row["medicine"]),
                    "vehicle_id": str(row["vehicle_id"]),
                    "location": str(row["location"]),
                    "temperature": float(row["temperature"]),
                    "humidity": float(row["humidity"]),
                    "door_status": str(row["door_status"]),
                    "battery_level": float(row["battery_level"]),
                    "event_time": str(row["event_time"])
                }

                # Publish to Kafka with shipment_id as partitioning key
                producer.send(
                    topic,
                    key=record["shipment_id"],
                    value=record
                )
                producer.flush()

                # Print formatted academic telemetry output
                print("REAL-TIME COLD-CHAIN PRODUCER")
                print("-----------------------------")
                print("Sending event:")
                print(f"Shipment: {record['shipment_id']}")
                print(f"Medicine: {record['medicine']}")
                print(f"Temperature: {record['temperature']} C")
                print(f"Location: {record['location']}")
                print(f"[Total Sent: #{event_counter} | Cycle: {cycle}]\n")

                time.sleep(delay_sec)

            cycle += 1
            print(f"[*] Completed streaming cycle {cycle - 1}. Repeating sensor dataset...")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[*] Stopping cold-chain producer safely...")
    finally:
        producer.close()
        print(f"[*] Producer stopped. Total events sent: {event_counter}")


if __name__ == "__main__":
    run_producer()
