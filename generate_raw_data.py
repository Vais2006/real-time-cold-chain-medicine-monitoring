"""
Data Generator for Cold-Chain Medicine Monitoring
Generates realistic simulated IoT sensor readings with normal, warning, and critical temperatures,
as well as a small number of dirty/imperfect rows to demonstrate Pandas data cleaning.
"""
import random
import os
from datetime import datetime, timedelta

def generate_cold_chain_data(output_path, num_rows=620):
    random.seed(42)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    medicines = ["Vaccine-A", "Insulin-B", "Vaccine-C", "Medicine-D", "Vaccine-E"]
    locations = ["Mumbai", "Navi Mumbai", "Thane", "Pune", "Nashik"]
    shipments = [f"SHP-{i:03d}" for i in range(1, 11)]
    vehicles = [f"VEH-{i:03d}" for i in range(101, 107)]

    # Map shipments to fixed medicine and vehicle for realistic consistency
    shipment_map = {}
    for idx, s in enumerate(shipments):
        shipment_map[s] = {
            "medicine": medicines[idx % len(medicines)],
            "vehicle_id": vehicles[idx % len(vehicles)]
        }

    start_time = datetime(2026, 9, 29, 6, 0, 0)
    rows = []

    # Header
    header = "event_id,shipment_id,medicine,vehicle_id,location,temperature,humidity,door_status,battery_level,event_time"

    for i in range(1, num_rows + 1):
        event_id = f"EVT-{i:05d}"
        shipment_id = random.choice(shipments)
        medicine = shipment_map[shipment_id]["medicine"]
        vehicle_id = shipment_map[shipment_id]["vehicle_id"]
        location = random.choice(locations)
        
        # Advance time by 1 to 4 minutes
        current_time = start_time + timedelta(minutes=int(i * 2.5) + random.randint(0, 2))
        event_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

        # Determine door status
        door_status = "OPEN" if random.random() < 0.12 else "CLOSED"

        # Generate realistic temperature distribution
        # Normal: 2.0 to 8.0 (~65%)
        # Warning: 0.0 to <2.0 OR >8.0 to 10.0 (~22%)
        # Critical: <0.0 OR >10.0 (~13%)
        rand_category = random.random()
        if door_status == "OPEN":
            # Higher chance of warning or critical when door is open
            if rand_category < 0.35:
                # Normal
                temp = round(random.uniform(5.5, 7.9), 2)
            elif rand_category < 0.70:
                # Warning high
                temp = round(random.uniform(8.1, 9.9), 2)
            else:
                # Critical high
                temp = round(random.uniform(10.1, 14.8), 2)
        else:
            if rand_category < 0.65:
                # Normal (2.0 to 8.0)
                temp = round(random.uniform(2.1, 7.9), 2)
            elif rand_category < 0.76:
                # Warning low (0.0 to < 2.0)
                temp = round(random.uniform(0.1, 1.9), 2)
            elif rand_category < 0.88:
                # Warning high (> 8.0 to 10.0)
                temp = round(random.uniform(8.1, 9.9), 2)
            elif rand_category < 0.94:
                # Critical low (< 0.0)
                temp = round(random.uniform(-4.5, -0.2), 2)
            else:
                # Critical high (> 10.0)
                temp = round(random.uniform(10.2, 13.5), 2)

        humidity = round(random.uniform(42.0, 78.0), 2)
        battery_level = round(random.uniform(72.0, 99.5), 1)

        rows.append({
            "event_id": event_id,
            "shipment_id": shipment_id,
            "medicine": medicine,
            "vehicle_id": vehicle_id,
            "location": location,
            "temperature": str(temp),
            "humidity": str(humidity),
            "door_status": door_status,
            "battery_level": str(battery_level),
            "event_time": event_time
        })

    # Introduce deliberate dirty/missing data for Pandas cleaning demonstration
    # 1. Missing temperature in a few rows
    rows[15]["temperature"] = ""
    rows[88]["temperature"] = ""
    rows[204]["temperature"] = ""
    
    # 2. Missing humidity in a few rows (to demonstrate imputation)
    rows[45]["humidity"] = ""
    rows[160]["humidity"] = ""
    rows[310]["humidity"] = ""

    # 3. Invalid battery level (negative value)
    rows[112]["battery_level"] = "-25.0"
    rows[270]["battery_level"] = "-5.0"

    # 4. Missing shipment_id
    rows[95]["shipment_id"] = ""

    # 5. Duplicate record (copy of row 50)
    duplicate_row = dict(rows[50])
    rows.append(duplicate_row)

    # Write out CSV
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for r in rows:
            line = f"{r['event_id']},{r['shipment_id']},{r['medicine']},{r['vehicle_id']},{r['location']},{r['temperature']},{r['humidity']},{r['door_status']},{r['battery_level']},{r['event_time']}"
            f.write(line + "\n")

    print(f"Generated {len(rows)} cold-chain records at: {output_path}")

if __name__ == "__main__":
    target = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cold_chain_sensor_data.csv")
    generate_cold_chain_data(target, 620)
