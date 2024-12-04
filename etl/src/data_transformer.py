import logging
import geopandas as gpd
from pyspark.sql import functions as F

class DataTransformer:
    def __init__(self, country_code="EE"):
        """Initializes the DataTransformer with a country code."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.country_code = country_code

    def transform_and_split_data(self, events_df, cell_plan_df, geojson_url):
        """Performs sorting and transformation for each dataset.

        Args:
            events_df (DataFrame): DataFrame containing mobile network events.
            cell_plan_df (DataFrame): DataFrame containing cell plan data.
            geojson_url (str): URL or path to the GeoJSON file for administrative units.

        Returns:
            tuple: Transformed DataFrames (events_df, cell_plan_df, admin_units_gdf).
        """
        self.logger.info("Sorting and transforming data for each table...")

        # Sort and transform events and cell plan data
        events_df = self.transform_events(events_df)
        cell_plan_df = self.convert_coordinates(cell_plan_df)

        # Filter and transform administrative units
        admin_units_gdf = self.filter_administrative_units(geojson_url)

        return events_df, cell_plan_df, admin_units_gdf

    def transform_events(self, events_df):
        """Transforms Mobile Network Events data.

        Args:
            events_df (DataFrame): DataFrame containing mobile network events.

        Returns:
            DataFrame: Transformed DataFrame containing relevant columns.
        """
        self.logger.info("Transforming data for Mobile Network Events...")
        
        # Rename and cast timestamp column if missing
        if 'timestamp' not in events_df.columns:
            events_df = events_df.withColumn('timestamp', F.current_timestamp())

        events_df = events_df.withColumn('timestamp', F.to_timestamp('timestamp'))
        events_df = events_df.withColumn('date', F.to_date('timestamp'))

        # Select necessary columns
        events_df = events_df.select('user_id', 'cell_id', 'timestamp', 'date')
        self.logger.info("Data transformed for Mobile Network Events.")

        return events_df

    def convert_coordinates(self, cell_plan_df):
        """Transforms and selects required columns for Cell Plan.

        Args:
            cell_plan_df (DataFrame): DataFrame containing cell plan data.

        Returns:
            DataFrame: Transformed DataFrame containing relevant columns.
        """
        self.logger.info("Converting coordinates and selecting required columns for Cell Plan...")
        
        # Rename and cast date column if missing
        if 'date' not in cell_plan_df.columns:
            cell_plan_df = cell_plan_df.withColumn('date', F.current_date())

        cell_plan_df = cell_plan_df.withColumn('date', F.to_date('date'))
        cell_plan_df = cell_plan_df.select("cell_id", "latitude", "longitude", "date")
        self.logger.info("Data transformed for Mobile Network Cell Plan.")

        return cell_plan_df

    def filter_administrative_units(self, geojson_url):
        """Filters and selects the required columns for local administrative units.

        Args:
            geojson_url (str): URL or path to the GeoJSON file.

        Returns:
            GeoDataFrame: Filtered GeoDataFrame containing administrative units with geometry.
        """
        self.logger.info("Filtering and selecting columns for local administrative units...")
        try:
            # Load the GeoJSON file
            admin_units = gpd.read_file(geojson_url)
            
            # Check for expected columns
            required_columns = ['LAU_ID', 'CNTR_CODE', 'LAU_NAME', 'geometry']
            if not all(col in admin_units.columns for col in required_columns):
                self.logger.error("Not all required columns are present in the GeoDataFrame.")
                return None
            
            # Filter by country code
            estonia_gdf = admin_units[admin_units['CNTR_CODE'] == self.country_code]
            
            # Select and rename required columns
            estonia_gdf = estonia_gdf[required_columns]
            estonia_gdf.columns = ['lau_id', 'country_code', 'lau_name', 'geometry']
            
            self.logger.info(f"Filtered administrative units for {self.country_code}.")
            return estonia_gdf
        except Exception as e:
            self.logger.error(f"Error filtering administrative units: {str(e)}")
            return None