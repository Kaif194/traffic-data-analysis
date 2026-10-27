# Traffic Data Analysis using Apache Spark and Garage Object Storage

**Domain:** Big Data Analytics & Cloud Computing  
**Big Data Framework:** Apache Spark (via PySpark Python API)  
**Object Storage:** Garage Object Storage (Self-Hosted S3-Compatible) / AWS S3  
**Programming Language:** Python 3.x  

---

## 📌 Project Overview
This project provides an end-to-end Big Data and Cloud Computing solution for processing and analyzing large-scale traffic datasets using **Apache Spark** and **Garage Object Storage** (an open-source, distributed, S3-compatible object storage service). The application generates 60,000+ realistic traffic records, uploads them to **Garage Object Storage**, cleans and processes data using **Apache Spark DataFrames & SQL engine**, calculates key traffic metrics (peak hours, busiest locations, weather impact, weekend comparisons), generates visual charts with **Seaborn/Matplotlib**, and presents an interactive dashboard with **Streamlit**.

---

## 📁 Project Directory Structure
```
traffic-data-analysis/
│
├── data/
│   └── raw/                    # Raw synthetic dataset (traffic_data.csv)
├── src/
│   ├── data_generator.py       # Dataset generator with injected edge cases
│   ├── s3_upload.py            # Uploads dataset to AWS S3 using Boto3
│   ├── traffic_analysis.py     # PySpark Data Cleaning & Analytics Engine
│   ├── visualization.py        # Chart renderer (Seaborn & Matplotlib)
│   └── dashboard.py            # Streamlit Interactive Dashboard
│
├── output/
│   ├── processed/              # Aggregated PySpark CSV outputs
│   └── reports/                # Visual PNG charts & analytical summary
│
├── requirements.txt            # Python dependencies
├── config.example.py           # Configuration template
├── config.py                   # Active environment & path configuration
└── README.md                   # Project Documentation & User Guide
```

---

## 🚀 Quick Start Guide

### 1. Install Dependencies
Ensure Python 3.8+ and Java (JDK 8 or 11 for PySpark) are installed.
```bash
pip install -r requirements.txt
```

### 2. Configure Environment (`.env`) for Garage Object Storage
Create or edit `.env` in the project root:
```ini
AWS_ACCESS_KEY_ID=GKe9af6d8b6e209ad42f25a576
AWS_SECRET_ACCESS_KEY=05c21e7cf4237e50eb939b2b89f661b37b8fa32f2440f852c63b0d4bce0e2a7a
AWS_DEFAULT_REGION=garagenet
S3_BUCKET_NAME=traffic-analysis-bucket
S3_ENDPOINT_URL=http://localhost:3900
S3_PATH_STYLE_ACCESS=true
```

### 3. Start Garage Object Storage (Docker)
```powershell
docker run -d --name garage -p 3900:3900 -p 3901:3901 -v garage_data:/var/lib/garage dxflrs/garage:v1.0.1
```

### 4. Upload Raw Data to Garage Object Storage
```powershell
python src/s3_upload.py
```

### 5. Run Apache Spark Pipeline (with S3/Garage Integration)
```powershell
python src/traffic_analysis.py --use-s3
```
*Loads raw data directly from `s3a://traffic-analysis-bucket/...`, executes PySpark cleaning & aggregations, exports locally, and syncs processed tables back to Garage Object Storage.*

### 6. View & Inspect Stored Datasets
You can view the contents of Garage Object Storage in several ways:
- **Viewer Script (Easiest):**
  ```powershell
  python src/view_garage_data.py
  ```
  *Lists all files in `traffic-analysis-bucket` and previews row data directly from Garage.*
- **Interactive Streamlit Web Dashboard:**
  ```powershell
  streamlit run src/dashboard.py
  ```
  *Features a real-time data source switch between Garage Object Storage (S3) and Local Filesystem.*
- **Static Charts:**
  ```powershell
  python src/visualization.py
  ```
- **Garage Container Status & Bucket CLI:**
  ```powershell
  docker exec garage /garage status
  docker exec garage /garage bucket list
  ```

---

## 📊 Key PySpark Analytical Features
- **Data Cleaning:** Removes duplicate records, filters invalid negative speeds/counts, handles null values.
- **Peak Hour Detection:** Identifies hour-of-day traffic bottlenecks.
- **Location Ranking:** Identifies highest density road stretches.
- **Weather Analysis:** Assesses impact of rainy, foggy, or snowy conditions on average vehicle speeds.
- **Day Type Comparison:** Evaluates weekday vs. weekend traffic distribution.
