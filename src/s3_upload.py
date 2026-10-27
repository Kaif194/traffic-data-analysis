import os
import sys
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Ensure parent directory is in sys.path for config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION,
    S3_BUCKET_NAME, S3_RAW_KEY, LOCAL_RAW_PATH, S3_ENDPOINT_URL
)
from src.data_generator import generate_traffic_dataset

def upload_to_s3(local_file=LOCAL_RAW_PATH, bucket_name=S3_BUCKET_NAME, s3_key=S3_RAW_KEY):
    """
    Uploads local dataset to Garage Object Storage (or AWS S3) using boto3.
    """
    if not os.path.exists(local_file):
        print(f"[S3 Upload] Local file '{local_file}' not found. Generating sample dataset...")
        generate_traffic_dataset(num_records=60000, output_file=local_file)

    print(f"[S3 Upload] Connecting to Object Storage (Bucket: {bucket_name}, Endpoint: {S3_ENDPOINT_URL})...")
    
    is_placeholder = AWS_ACCESS_KEY_ID in ["YOUR_AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY"]
    
    try:
        kwargs = {
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
            'region_name': AWS_REGION
        }
        if S3_ENDPOINT_URL:
            kwargs['endpoint_url'] = S3_ENDPOINT_URL
        if AWS_SESSION_TOKEN:
            kwargs['aws_session_token'] = AWS_SESSION_TOKEN

        s3_client = boto3.client('s3', **kwargs)

        print(f"[S3 Upload] Uploading '{local_file}' -> 's3://{bucket_name}/{s3_key}'...")
        s3_client.upload_file(local_file, bucket_name, s3_key)
        print(f"[S3 Upload] SUCCESS: Dataset uploaded to Garage Object Storage -> s3://{bucket_name}/{s3_key}")
        return True

    except (NoCredentialsError, PartialCredentialsError):
        print("\n" + "="*70)
        print("[S3 Upload NOTICE] AWS credentials were not detected.")
        print("To upload to AWS S3, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        print(f"File remains saved locally at: '{local_file}' for local PySpark processing.")
        print("="*70 + "\n")
        return False
    except ClientError as e:
        print(f"[S3 Upload ERROR] AWS Client Error: {e}")
        print(f"Ensure S3 Bucket '{bucket_name}' exists and your IAM policy allows s3:PutObject.")
        return False
    except Exception as e:
        if "Could not connect to the endpoint URL" in str(e) or "EndpointConnectionError" in str(e):
            print("\n" + "="*70)
            print(f"[S3 Upload NOTICE] Garage Object Storage endpoint at '{S3_ENDPOINT_URL}' was not reachable.")
            print("To connect to a live Garage cluster, update S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY in .env.")
            print(f"Dataset remains saved locally at '{local_file}' for local Apache Spark processing.")
            print("="*70 + "\n")
        else:
            print(f"[S3 Upload ERROR] Error during S3 upload: {e}")
        return False

if __name__ == "__main__":
    upload_to_s3()
