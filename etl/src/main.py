import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from s3_extractor import S3Extractor
from data_transformer import DataTransformer
from db_loader import DatabaseLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_Main")

# Configuration class for S3 and Database settings
class Config:
    BASE_PATH = os.getenv("AWS_S3_KEY")
    S3_BUCKET = os.getenv("AWS_S3_BUCKET")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    S3_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    COUNTRY_CODE = os.getenv("COUNTRY_CODE", "EE")
    GEOJSON_URL = os.getenv("LAU_GEOJSON_URL")

    @staticmethod
    def get_s3_path(dataset: str, date: datetime) -> str:
        """Constructs the S3 path for the given dataset and date."""
        return f"{Config.BASE_PATH}/{dataset}/year={date.year}/month={date.month}/day={date.day}/"

def main():
    """Main function to execute the ETL process."""
    # Initialize Spark session with S3 configurations
    spark = SparkSession.builder \
        .appName("ETL_Job") \
        .config("spark.hadoop.fs.s3a.access.key", Config.AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", Config.AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_REGION", "eu-frankfurt-1")) \
        .config("spark.hadoop.fs.s3a.endpoint", Config.S3_ENDPOINT_URL) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.ssl.enabled", "true") \
        .config("spark.hadoop.com.amazonaws.services.s3.enableV4", "true") \
        .getOrCreate()

    # # Initialize ETL classes
    s3_extractor = S3Extractor(Config.S3_BUCKET, Config.AWS_ACCESS_KEY, Config.AWS_SECRET_KEY, Config.S3_ENDPOINT_URL, spark)
    transformer = DataTransformer(country_code=Config.COUNTRY_CODE)
    db_loader = DatabaseLoader(spark=spark, db_url=Config.POSTGRES_DB_URL, db_properties={
        "user": Config.POSTGRES_USER,
        "password": Config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    })

    # Retrieve processing date from environment variable
    processing_date_str = os.getenv("PROCESSING_DATE")
    if not processing_date_str:
        logger.error("PROCESSING_DATE environment variable is not set.")
        return
    processing_date = datetime.strptime(processing_date_str, "%Y-%m-%d")

    # Step 1: Extract data from S3
    logger.info("Starting data extraction from S3...")
    events_df = s3_extractor.get_parquet_files(Config.get_s3_path("events", processing_date))
    cell_plan_df = s3_extractor.get_parquet_files(Config.get_s3_path("cells", processing_date))

    if events_df is None or cell_plan_df is None:
        logger.error("Data extraction failed. Check S3 configuration and access.")
        return

    # Step 2: Transform data
    logger.info("Starting data transformation...")
    transformed_events_df, transformed_cell_plan_df, estonia_units_gdf = transformer.transform_and_split_data(events_df, cell_plan_df, Config.GEOJSON_URL)

    # Step 3: Load data into PostgreSQL
    logger.info("Loading data into PostgreSQL...")
    db_loader.load_events(transformed_events_df)
    db_loader.load_cell_plan(transformed_cell_plan_df)
    # Uncomment the next line to load administrative units data
    db_loader.load_administrative_units(estonia_units_gdf)

    logger.info("ETL process completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
