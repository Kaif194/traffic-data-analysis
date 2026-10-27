import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Object Storage & Credentials Config (Garage / AWS S3)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("GARAGE_ACCESS_KEY_ID", "YOUR_ACCESS_KEY"))
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("GARAGE_SECRET_ACCESS_KEY", "YOUR_SECRET_KEY"))
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("GARAGE_REGION", "garagenet"))
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", os.getenv("GARAGE_BUCKET_NAME", "traffic-analysis-bucket"))
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", os.getenv("GARAGE_ENDPOINT_URL", "http://localhost:3900"))
S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true").lower() == "true"

# S3 Paths (s3a protocol for Hadoop/PySpark)
S3_RAW_PATH = f"s3a://{S3_BUCKET_NAME}/traffic-analysis/raw/traffic_data.csv"
S3_PROCESSED_BASE = f"s3a://{S3_BUCKET_NAME}/traffic-analysis/processed"

# S3 Keys (Prefixes for Boto3)
S3_RAW_KEY = "traffic-analysis/raw/traffic_data.csv"
S3_PROCESSED_PREFIX = "traffic-analysis/processed"

# Project Local Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_RAW_DIR = os.path.join(DATA_DIR, "raw")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOCAL_PROCESSED_DIR = os.path.join(OUTPUT_DIR, "processed")
LOCAL_REPORTS_DIR = os.path.join(OUTPUT_DIR, "reports")

LOCAL_RAW_PATH = os.path.join(DATA_RAW_DIR, "traffic_data.csv")

# Ensure directories exist
for path in [DATA_RAW_DIR, LOCAL_PROCESSED_DIR, LOCAL_REPORTS_DIR]:
    os.makedirs(path, exist_ok=True)
