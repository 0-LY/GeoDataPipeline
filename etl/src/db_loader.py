from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
from pyspark.sql.types import StructType, StructField, StringType, BinaryType


class DatabaseLoader:
    """Loads data into PostgreSQL with separate methods for each table."""

    def __init__(self, spark: SparkSession, db_url: str, db_properties: dict):
        """
        Initializes the DatabaseLoader with Spark session, database URL, and properties.

        Args:
            spark (SparkSession): The Spark session to use for DataFrame operations.
            db_url (str): The JDBC URL for the PostgreSQL database.
            db_properties (dict): Additional properties for connecting to the database.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = spark
        self.db_url = db_url
        self.db_properties = db_properties

    def load_events(self, events_df, table_name: str = "mobile_network_events"):
        """Loads event data into the PostgreSQL table."""
        try:
            events_df.write.jdbc(
                url=self.db_url,
                table=table_name,
                mode="append",
                properties=self.db_properties,
            )
            self.logger.info(f"Data loaded successfully into {table_name}.")
        except Exception as e:
            self.logger.error(f"Error loading event data: {e}")

    def load_cell_plan(self, cell_plan_df, table_name: str = "mobile_network_cells"):
        """Loads cell plan data into the PostgreSQL table."""
        try:
            cell_plan_df.write.jdbc(
                url=self.db_url,
                table=table_name,
                mode="append",
                properties=self.db_properties,
            )
            self.logger.info(f"Data loaded successfully into {table_name}.")
        except Exception as e:
            self.logger.error(f"Error loading cell plan data: {e}")

    def load_administrative_units(self, gdf, table_name: str = "local_admin_units"):
        """Loads administrative units data into PostgreSQL."""
        try:
            # Convert 'geometry' column to WKB (Well-Known Binary)
            gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkb if geom is not None else None)
            
            # Define schema explicitly
            schema = StructType([
                StructField("lau_id", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("lau_name", StringType(), True),
                StructField("geometry", BinaryType(), True)
            ])
            
            # Convert GeoDataFrame to Spark DataFrame with explicit schema
            spark_df = self.spark.createDataFrame(gdf, schema=schema)
            
            # Write to the database
            spark_df.write.jdbc(
                url=self.db_url,
                table=table_name,
                mode="append",
                properties=self.db_properties,
            )
            self.logger.info(f"Administrative units data loaded successfully into {table_name}.")
        except Exception as e:
            self.logger.error(f"Error loading administrative units data: {e}")
