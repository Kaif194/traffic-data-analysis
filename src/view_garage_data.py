"""
Utility script to view, inspect, and download datasets stored in Garage Object Storage.
"""

import os
import sys
import argparse
import boto3
import pandas as pd
import io

# Ensure parent directory is in sys.path for config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION,
    S3_BUCKET_NAME, S3_ENDPOINT_URL, OUTPUT_DIR
)

def get_s3_client():
    kwargs = {
        'aws_access_key_id': AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
        'region_name': AWS_REGION,
        'endpoint_url': S3_ENDPOINT_URL
    }
    if AWS_SESSION_TOKEN:
        kwargs['aws_session_token'] = AWS_SESSION_TOKEN
    return boto3.client('s3', **kwargs)

def list_bucket_contents():
    s3 = get_s3_client()
    print("=" * 80)
    print(f"GARAGE OBJECT STORAGE BUCKET: '{S3_BUCKET_NAME}'")
    print(f"Endpoint: {S3_ENDPOINT_URL} | Region: {AWS_REGION}")
    print("=" * 80)
    
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
    contents = response.get('Contents', [])
    
    if not contents:
        print("Bucket is empty or not found.")
        return []
    
    print(f"{'OBJECT KEY':<55} | {'SIZE (BYTES)':<12} | {'LAST MODIFIED'}")
    print("-" * 80)
    for obj in contents:
        print(f"{obj['Key']:<55} | {obj['Size']:<12,} | {obj['LastModified']}")
    print("=" * 80 + "\n")
    return contents

def get_dataframe(key):
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    content = obj['Body'].read().decode('utf-8')
    return pd.read_csv(io.StringIO(content))

def preview_object(key, rows=10):
    print(f"\n--- DATA PREVIEW ({rows} ROWS): {key} ---")
    try:
        df = get_dataframe(key)
        print(df.head(rows).to_string(index=False))
        print(f"Total Rows: {len(df):,} | Total Columns: {len(df.columns)}")
        print("-" * 80)
    except Exception as e:
        print(f"Error previewing object {key}: {e}")

def download_all_objects(dest_dir=None):
    if dest_dir is None:
        dest_dir = os.path.join(OUTPUT_DIR, "downloaded_from_docker")
    os.makedirs(dest_dir, exist_ok=True)
    
    s3 = get_s3_client()
    contents = s3.list_objects_v2(Bucket=S3_BUCKET_NAME).get('Contents', [])
    
    print(f"\n[DOWNLOAD] Downloading all files from Garage S3 (Docker) to: {dest_dir}\n")
    for obj in contents:
        key = obj['Key']
        local_path = os.path.join(dest_dir, key.replace("/", os.sep))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(S3_BUCKET_NAME, key, local_path)
        print(f" -> Downloaded: {key} ({obj['Size']:,} bytes) -> {local_path}")
    print(f"\n[SUCCESS] All files downloaded from Docker to '{dest_dir}'. You can open them directly in Excel/VS Code!\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="View or download data from Garage Object Storage (Docker)")
    parser.add_argument("--rows", type=int, default=5, help="Number of rows to preview per table (default: 5)")
    parser.add_argument("--download", action="store_true", help="Download all files from Docker to local output directory")
    parser.add_argument("--table", type=str, default=None, help="Preview a specific table: raw, peak_hours, location, day_type, weather, summary")
    args = parser.parse_args()

    if args.download:
        download_all_objects()
    else:
        objects = list_bucket_contents()
        for obj in objects:
            key = obj['Key']
            if not key.endswith('.csv'):
                continue
            if args.table and args.table.lower() not in key.lower():
                continue
            preview_object(key, rows=args.rows)
