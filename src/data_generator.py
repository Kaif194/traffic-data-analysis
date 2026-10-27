import os
import random
import csv
from datetime import datetime, timedelta
import sys

# Ensure parent directory is in sys.path for config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import LOCAL_RAW_PATH, DATA_RAW_DIR

def generate_traffic_dataset(num_records=60000, output_file=LOCAL_RAW_PATH):
    """
    Generates realistic traffic dataset containing Date, Time, Location,
    Vehicle_Count, Average_Speed, Weather, Day_of_Week, and Road_Type.
    Injects standard data quality issues (duplicates, nulls, invalid speeds)
    to demonstrate PySpark data cleaning operations.
    """
    print(f"[Data Generator] Starting generation of {num_records:,} traffic records...")
    
    locations = [
        ("Downtown Highway A", "Highway"),
        ("Midtown Expressway", "Expressway"),
        ("Westside Junction", "Urban Arterial"),
        ("Eastside Toll Plaza", "Expressway"),
        ("Suburban Route 9", "Rural Highway"),
        ("Bridge Crossing North", "Highway"),
        ("Airport Expressway", "Expressway"),
        ("Tech Park Junction", "City Street"),
        ("Central Avenue", "City Street"),
        ("Industrial Ring Road", "Highway")
    ]
    
    weather_conditions = ["Sunny", "Clear", "Rainy", "Foggy", "Snowy"]
    weather_weights = [0.45, 0.30, 0.15, 0.07, 0.03]
    
    start_date = datetime(2026, 1, 1)
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    headers = [
        "Date", "Time", "Location", "Vehicle_Count", 
        "Average_Speed", "Weather", "Day_of_Week", "Road_Type"
    ]
    
    rows = []
    
    for i in range(num_records):
        # Pick date within a 60-day range
        random_days = random.randint(0, 59)
        current_datetime = start_date + timedelta(days=random_days)
        
        # Pick hour (simulate rush hours: 8-10 AM and 5-7 PM)
        rand_val = random.random()
        if rand_val < 0.35: # Morning peak
            hour = random.choice([8, 9])
        elif rand_val < 0.70: # Evening peak
            hour = random.choice([17, 18])
        else: # Normal hours
            hour = random.randint(0, 23)
            
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        date_str = current_datetime.strftime("%Y-%m-%d")
        time_str = f"{hour:02d}:{minute:02d}:{second:02d}"
        day_of_week = current_datetime.strftime("%A")
        
        loc_name, road_type = random.choice(locations)
        weather = random.choices(weather_conditions, weights=weather_weights)[0]
        
        # Determine vehicle count and speed based on hour & weather
        is_rush_hour = hour in [8, 9, 17, 18]
        is_weekend = day_of_week in ["Saturday", "Sunday"]
        
        base_vehicles = 600 if is_rush_hour else 250
        if is_weekend:
            base_vehicles = int(base_vehicles * 0.65)
            
        vehicle_count = max(10, int(random.gauss(base_vehicles, 120)))
        
        # Speed decreases when vehicle count is high or weather is bad
        base_speed = 70.0 if "Highway" in road_type or "Expressway" in road_type else 45.0
        if is_rush_hour:
            base_speed -= 20.0
        if weather in ["Rainy", "Foggy"]:
            base_speed -= 10.0
        elif weather == "Snowy":
            base_speed -= 22.0
            
        avg_speed = round(max(8.0, random.gauss(base_speed, 8.0)), 2)
        
        # Inject deliberate data quality issues for Spark cleaning demo:
        # 1. Null / missing values (0.5%)
        if random.random() < 0.005:
            vehicle_count = ""
        if random.random() < 0.005:
            avg_speed = ""
            
        # 2. Invalid negative / outlier values (0.2%)
        if random.random() < 0.002:
            avg_speed = -15.0
        if random.random() < 0.002:
            vehicle_count = -50
            
        row = [date_str, time_str, loc_name, vehicle_count, avg_speed, weather, day_of_week, road_type]
        rows.append(row)
        
        # 3. Inject duplicate records (0.5%)
        if random.random() < 0.005:
            rows.append(row)
            
    # Write to CSV
    with open(output_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
        
    print(f"[Data Generator] Successfully generated {len(rows):,} records to '{output_file}'.")
    return output_file

if __name__ == "__main__":
    generate_traffic_dataset()
