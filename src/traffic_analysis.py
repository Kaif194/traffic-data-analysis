import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, to_timestamp, concat_ws, when, avg, sum, count, min, max, round, dayofweek
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Ensure parent directory is in sys.path for config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION,
    S3_BUCKET_NAME, S3_RAW_PATH, S3_PROCESSED_BASE, S3_ENDPOINT_URL, S3_PATH_STYLE_ACCESS,
    LOCAL_RAW_PATH, LOCAL_PROCESSED_DIR, S3_PROCESSED_PREFIX
)
from src.data_generator import generate_traffic_dataset

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TrafficAnalysis")

# Windows Hadoop environment compatibility setup
if sys.platform == "win32":
    hadoop_home = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "hadoop"))
    os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
    if "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = hadoop_home

def create_spark_session(use_s3=False):
    """
    Initializes Apache Spark Session via PySpark. Configures S3A filesystem parameters
    for Garage Object Storage (or AWS S3).
    """
    logger.info("Initializing Apache Spark Session...")
    builder = SparkSession.builder \
        .appName("ApacheSparkTrafficAnalysis") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g")
        
    if use_s3:
        logger.info(f"Configuring S3A Filesystem for Garage Object Storage (Endpoint: {S3_ENDPOINT_URL})...")
        
        # Format endpoint host:port for Hadoop S3A
        endpoint = S3_ENDPOINT_URL
        ssl_enabled = "true"
        if "://" in endpoint:
            scheme, endpoint = endpoint.split("://", 1)
            if scheme.lower() == "http":
                ssl_enabled = "false"
            
        provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        if AWS_SESSION_TOKEN:
            provider = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"

        endpoint_clean = endpoint.replace("localhost", "127.0.0.1")
        builder = builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint_clean) \
            .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider) \
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none") \
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
            .config("spark.hadoop.fs.s3a.multipart.purge", "false") \
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
            .config("spark.hadoop.fs.s3a.retry.interval", "1000") \
            .config("spark.hadoop.fs.s3a.threads.max", "10") \
            .config("spark.hadoop.fs.s3a.max.total.tasks", "1000") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
        if AWS_SESSION_TOKEN:
            builder = builder \
                .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Apache Spark Session created successfully.")
    return spark

def get_traffic_schema():
    """
    Defines explicit PySpark Schema for dataset parsing.
    """
    return StructType([
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Vehicle_Count", IntegerType(), True),
        StructField("Average_Speed", DoubleType(), True),
        StructField("Weather", StringType(), True),
        StructField("Day_of_Week", StringType(), True),
        StructField("Road_Type", StringType(), True)
    ])

def clean_and_preprocess_data(df):
    """
    Performs comprehensive PySpark data cleaning operations:
    1. Removes duplicate records.
    2. Drops rows with missing critical fields.
    3. Filters invalid negative/zero values.
    4. Converts Date & Time into timestamps and extracts 'Hour' feature.
    5. Adds Weekday/Weekend classification and Traffic Congestion Levels.
    """
    logger.info("Starting Data Cleaning and Feature Engineering...")
    initial_count = df.count()
    logger.info(f"Raw record count: {initial_count:,}")

    # 1. Remove duplicate records
    cleaned_df = df.dropDuplicates()
    dedup_count = cleaned_df.count()
    logger.info(f"Records after deduplication: {dedup_count:,} (Removed {initial_count - dedup_count:,} duplicates)")

    # 2. Filter missing null values
    cleaned_df = cleaned_df.dropna(subset=["Date", "Time", "Location", "Vehicle_Count", "Average_Speed"])
    
    # 3. Filter invalid / corrupted values (non-positive vehicles or speed)
    cleaned_df = cleaned_df.filter(
        (col("Vehicle_Count") > 0) & 
        (col("Average_Speed") > 0.0) & 
        (col("Average_Speed") <= 150.0)
    )
    
    valid_count = cleaned_df.count()
    logger.info(f"Records after filtering nulls & invalid values: {valid_count:,} (Filtered {dedup_count - valid_count:,} invalid rows)")

    # 4. Feature Engineering: Timestamp & Hour Extraction
    cleaned_df = cleaned_df.withColumn(
        "Timestamp", 
        to_timestamp(concat_ws(" ", col("Date"), col("Time")), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "Hour", hour(col("Time"))
    )

    # 5. Day Type (Weekday vs Weekend)
    cleaned_df = cleaned_df.withColumn(
        "Day_Type",
        when(col("Day_of_Week").isin("Saturday", "Sunday"), "Weekend").otherwise("Weekday")
    )

    # 6. Congestion Level feature based on vehicle count
    cleaned_df = cleaned_df.withColumn(
        "Congestion_Level",
        when(col("Vehicle_Count") > 500, "High")
        .when((col("Vehicle_Count") >= 250) & (col("Vehicle_Count") <= 500), "Moderate")
        .otherwise("Low")
    )

    return cleaned_df

def analyze_traffic_data(cleaned_df):
    """
    Executes core traffic analytics queries using PySpark DataFrames API:
    - Peak Traffic Hours
    - Location Traffic Ranking
    - Weekday vs Weekend Comparison
    - Weather Impact Analysis
    - Overall Summary Statistics
    """
    logger.info("Executing PySpark Traffic Analysis queries...")

    # 1. Peak Traffic Hours Analysis
    peak_hours_df = cleaned_df.groupBy("Hour") \
        .agg(
            count("Location").alias("Total_Records"),
            sum("Vehicle_Count").alias("Total_Vehicles"),
            round(avg("Vehicle_Count"), 2).alias("Avg_Vehicle_Count"),
            round(avg("Average_Speed"), 2).alias("Avg_Speed_KMH")
        ) \
        .orderBy(col("Avg_Vehicle_Count").desc())

    # 2. Location Traffic Ranking (Busiest Locations)
    location_df = cleaned_df.groupBy("Location", "Road_Type") \
        .agg(
            sum("Vehicle_Count").alias("Total_Vehicles"),
            round(avg("Vehicle_Count"), 2).alias("Avg_Vehicle_Count"),
            round(avg("Average_Speed"), 2).alias("Avg_Speed_KMH"),
            count("Location").alias("Sample_Count")
        ) \
        .orderBy(col("Total_Vehicles").desc())

    # 3. Weekday vs Weekend Comparison
    day_type_df = cleaned_df.groupBy("Day_Type") \
        .agg(
            sum("Vehicle_Count").alias("Total_Vehicles"),
            round(avg("Vehicle_Count"), 2).alias("Avg_Vehicle_Count"),
            round(avg("Average_Speed"), 2).alias("Avg_Speed_KMH")
        )

    # 4. Weather Impact Analysis
    weather_df = cleaned_df.groupBy("Weather") \
        .agg(
            sum("Vehicle_Count").alias("Total_Vehicles"),
            round(avg("Vehicle_Count"), 2).alias("Avg_Vehicle_Count"),
            round(avg("Average_Speed"), 2).alias("Avg_Speed_KMH"),
            count("Weather").alias("Incident_Count")
        ) \
        .orderBy(col("Avg_Speed_KMH").asc())

    # 5. Overall Traffic Summary Statistics
    summary_df = cleaned_df.select(
        sum("Vehicle_Count").alias("Total_Vehicles_Recorded"),
        round(avg("Vehicle_Count"), 2).alias("Overall_Avg_Vehicles"),
        round(avg("Average_Speed"), 2).alias("Overall_Avg_Speed"),
        min("Average_Speed").alias("Min_Recorded_Speed"),
        max("Average_Speed").alias("Max_Recorded_Speed")
    )

    return {
        "peak_hours": peak_hours_df,
        "location_analysis": location_df,
        "day_type_analysis": day_type_df,
        "weather_analysis": weather_df,
        "summary": summary_df
    }

def save_dataframe_to_csv(df, output_path, s3_key=None, use_s3=False):
    """
    Saves PySpark DataFrame to a single clean CSV file locally and optionally syncs to Garage S3.
    """
    os.makedirs(output_path, exist_ok=True)
    # Convert PySpark DF summary result to Pandas for clean single CSV export
    pandas_df = df.toPandas()
    csv_file = os.path.join(output_path, "results.csv")
    pandas_df.to_csv(csv_file, index=False)
    logger.info(f"Saved analysis output -> '{csv_file}'")

    if use_s3 and s3_key:
        try:
            import boto3
            kwargs = {
                'aws_access_key_id': AWS_ACCESS_KEY_ID,
                'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
                'region_name': AWS_REGION,
                'endpoint_url': S3_ENDPOINT_URL
            }
            if AWS_SESSION_TOKEN:
                kwargs['aws_session_token'] = AWS_SESSION_TOKEN
            s3 = boto3.client('s3', **kwargs)
            s3.upload_file(csv_file, S3_BUCKET_NAME, s3_key)
            logger.info(f"Uploaded processed output to Garage S3 -> s3://{S3_BUCKET_NAME}/{s3_key}")
        except Exception as err:
            logger.warning(f"Could not upload {s3_key} to Garage S3: {err}")

def run_pipeline(input_source=None, use_s3=False):
    """
    Main driver function for running the PySpark traffic analysis pipeline.
    """
    spark = None
    try:
        if input_source is None:
            if use_s3 and AWS_ACCESS_KEY_ID != "YOUR_AWS_ACCESS_KEY_ID":
                input_source = S3_RAW_PATH
            else:
                input_source = LOCAL_RAW_PATH
                if not os.path.exists(LOCAL_RAW_PATH):
                    generate_traffic_dataset(60000, LOCAL_RAW_PATH)

        logger.info(f"Starting pipeline processing. Input Source: {input_source}")

        spark = create_spark_session(use_s3=use_s3)

        # Read CSV dataset using PySpark with explicit schema
        logger.info("Reading dataset into PySpark DataFrame...")
        raw_df = spark.read \
            .option("header", "true") \
            .schema(get_traffic_schema()) \
            .csv(input_source)

        # Data Cleaning
        cleaned_df = clean_and_preprocess_data(raw_df)

        # Perform Analytics
        results = analyze_traffic_data(cleaned_df)

        # Save Processed Outputs
        logger.info(f"Exporting processed results to local directory: '{LOCAL_PROCESSED_DIR}'...")
        
        save_dataframe_to_csv(
            results["peak_hours"],
            os.path.join(LOCAL_PROCESSED_DIR, "peak_hours"),
            s3_key=f"{S3_PROCESSED_PREFIX}/peak_hours/results.csv",
            use_s3=use_s3
        )
        save_dataframe_to_csv(
            results["location_analysis"],
            os.path.join(LOCAL_PROCESSED_DIR, "location_analysis"),
            s3_key=f"{S3_PROCESSED_PREFIX}/location_analysis/results.csv",
            use_s3=use_s3
        )
        save_dataframe_to_csv(
            results["day_type_analysis"],
            os.path.join(LOCAL_PROCESSED_DIR, "day_type_analysis"),
            s3_key=f"{S3_PROCESSED_PREFIX}/day_type_analysis/results.csv",
            use_s3=use_s3
        )
        save_dataframe_to_csv(
            results["weather_analysis"],
            os.path.join(LOCAL_PROCESSED_DIR, "weather_analysis"),
            s3_key=f"{S3_PROCESSED_PREFIX}/weather_analysis/results.csv",
            use_s3=use_s3
        )
        save_dataframe_to_csv(
            results["summary"],
            os.path.join(LOCAL_PROCESSED_DIR, "traffic_summary"),
            s3_key=f"{S3_PROCESSED_PREFIX}/traffic_summary/results.csv",
            use_s3=use_s3
        )

        print("\n" + "="*70)
        print("APACHE SPARK TRAFFIC ANALYSIS PIPELINE COMPLETED SUCCESSFULLY!")
        print("======================================================================\n")
        print("\n--- OVERALL SUMMARY STATISTICS ---")
        results["summary"].show(truncate=False)
        print("\n--- TOP 5 PEAK TRAFFIC HOURS ---")
        results["peak_hours"].show(5, truncate=False)
        print("\n--- TOP 5 BUSIEST LOCATIONS ---")
        results["location_analysis"].show(5, truncate=False)
        print("="*70 + "\n")

        return True

    except Exception as e:
        logger.error(f"PySpark Pipeline Execution Failed: {e}", exc_info=True)
        return False
    finally:
        if spark:
            spark.stop()
            logger.info("Spark context stopped.")

if __name__ == "__main__":
    use_s3_flag = "--use-s3" in sys.argv
    run_pipeline(use_s3=use_s3_flag)
