import boto3
import logging
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

class S3Extractor:
    """Extractor for loading Parquet files from S3-compatible storage into Spark DataFrames."""

    def __init__(self, bucket_name, access_key, secret_key, endpoint_url, spark: SparkSession):
        """Initializes the S3 extractor with the given credentials and bucket information.

        Args:
            bucket_name (str): The name of the S3 bucket.
            access_key (str): AWS access key for S3 access.
            secret_key (str): AWS secret key for S3 access.
            endpoint_url (str): The endpoint URL for the S3 service.
            spark (SparkSession): An existing Spark session to use for DataFrame operations.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
        )
        self.spark = spark  # Use the passed Spark session

    def get_parquet_files(self, path_prefix: str):
        """Loads all Parquet files in the specified S3 bucket path prefix into a Spark DataFrame.

        Args:
            path_prefix (str): The S3 path prefix to search for Parquet files.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the loaded Parquet files,
            or an empty DataFrame if no files are found.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=path_prefix)
            parquet_files = [
                f"s3a://{self.bucket_name}/{obj['Key']}" for obj in response.get('Contents', [])
                if obj['Key'].startswith(path_prefix) and obj['Key'].endswith('.parquet')
            ]

            if not parquet_files:
                self.logger.error(f"No files found with prefix '{path_prefix}' in bucket '{self.bucket_name}'")
                return self.spark.createDataFrame([], schema=None)

            # Load all Parquet files into a Spark DataFrame
            return self.spark.read.parquet(*parquet_files)

        except ClientError as e:
            self.logger.error(f"Error fetching files from bucket '{self.bucket_name}': {e}")
            return None
