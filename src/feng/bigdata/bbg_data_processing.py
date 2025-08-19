"""
- Created by: Liu Chang
- Modified by: Zhuang Hao

Module for extracting and transforming Bloomberg data using PySpark.
"""
import re
import os
import sys
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType
import pyspark.sql.functions as F

from src.feng.bigdata import SparkHandler
from src.feng.config.config import SPARK_MASTER
from src.feng.utils import Log
from src.feng.utils.utils import get_file_type_and_region, get_smbclient
from src.feng.database.mongodb import MongoDBHandler

class BloombergDataProcessor:
    """
    Class for processing Bloomberg data files using PySpark.
    """

    def __init__(self):
        """
        Initialize the Bloomberg data processor.
        """
        self.spark_sessions = {}
        logger.info("Initialized BloombergDataProcessor")

    def _get_or_create_spark_session(self, file_type: str) -> SparkSession:
        """
        Get or create a SparkSession for a specific file type.

        Args:
            file_type: Type of file being processed (e.g., 'bs', 'is', 'cf')

        Returns:
            SparkSession for the file type
        """
        if file_type not in self.spark_sessions:
            # Create a new SparkSession optimized for local execution
            spark_handler = SparkHandler(app_name=SPARK_APP_NAME, master_url=SPARK_MASTER)
            spark = spark_handler.start_session()

            # spark = SparkSession.builder \
            #     .master('spark://10.230.252.6:7077') \
            #     .appName(f"{SPARK_APP_NAME}_{file_type}") \
            #     .config("spark.sql.shuffle.partitions", "4") \
            #     .config("spark.memory.fraction", "0.7") \
            #     .config("spark.memory.storageFraction", "0.3") \
            #     .config("spark.sql.files.maxPartitionBytes", "64mb") \
            #     .config("spark.default.parallelism", "4") \
            #     .config("spark.sql.adaptive.enabled", "true") \
            #     .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            #     .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            #     .config("spark.locality.wait", "0s") \
            #     .getOrCreate()
            self.spark_sessions[file_type] = spark

            logger.info(f"Created new SparkSession for file type: {file_type}")

        return self.spark_sessions[file_type]

    def extract_data(self, file_path: str) -> Tuple[List[str], List[List[str]]]:
        """
        Extract data from Bloomberg flat file.

        Args:
            file_path: Path to the Bloomberg flat file

        Returns:
            Tuple of (column_names, data_rows)
        """
        logger.info(f"Extracting data from file: {file_path}")

        if not smbclient.stat(file_path):
            logger.error(f"File not found: {file_path}")
            return [], []

        with smbclient.open_file(file_path, 'r') as f:
            lines = f.readlines()

        # Parse the file
        in_fields_section = False
        in_data_section = False
        column_names = []
        # add three columns for the data
        column_names.append("SECURITY_DESCRIPTION")
        column_names.append("RCODE")
        column_names.append("NFIELDS")

        data_rows = []

        for line in lines:
            line = line.strip()

            if line == "START-OF-FIELDS":
                in_fields_section = True
                continue
            elif line == "END-OF-FIELDS":
                in_fields_section = False
                continue
            elif line == "START-OF-DATA":
                in_data_section = True
                continue
            elif line == "END-OF-DATA" or line == "END-OF-FILE":
                in_data_section = False
                continue

            if in_fields_section:
                column_names.append(line)
            elif in_data_section:
                # Split the line by pipe character
                fields = line.split('|')
                # remove the last field
                fields = fields[:-1]
                # Replace "N.A." with None
                fields = [None if f == "N.A." else f for f in fields]
                data_rows.append(fields)

        logger.info(f"Extracted {len(column_names)} columns and {len(data_rows)} rows from {file_path}")
        return column_names, data_rows

    def transform_data(self, spark: SparkSession, column_names: List[str], data_rows: List[List[str]]) -> DataFrame:
        """
        Transform the extracted data into a PySpark DataFrame using Spark SQL.

        Args:
            spark: SparkSession to use
            column_names: List of column names
            data_rows: List of data rows

        Returns:
            PySpark DataFrame
        """
        logger.info("Transforming data into DataFrame")

        if not column_names or not data_rows:
            logger.warning("No data to transform")
            return spark.createDataFrame([], StructType([]))

        # Create schema
        schema = StructType([StructField(col, StringType(), True) for col in column_names])

        # Create initial DataFrame with smaller number of partitions for local execution
        df = spark.createDataFrame(data_rows, schema).repartition(4)

        # Register the DataFrame as a temporary view
        df.createOrReplaceTempView("bloomberg_data")

        # Add operation_date column with current date using Spark SQL
        df = spark.sql("""
            SELECT *, CURRENT_DATE() as OPERATION_DATE 
            FROM bloomberg_data
        """)

        logger.info(f"Transformed data into DataFrame with {len(df.columns)} columns")
        return df

    def clean_data(self, spark: SparkSession, df: DataFrame, caesars_companies: List[str],
                   cache_id: str = None) -> DataFrame:
        """
        Clean the transformed data using Spark SQL for better performance.

        Args:
            spark: SparkSession to use
            df: PySpark DataFrame
            caesars_companies: List of Caesars companies
            cache_id: Optional unique identifier for cache naming
        Returns:
            Cleaned PySpark DataFrame
        """
        logger.info("Cleaning data")

        if df.count() == 0:
            logger.warning("No data to clean")
            return df

        # Register the DataFrame as a temporary view with unique name if provided
        view_name = f"raw_data_{cache_id}" if cache_id else "raw_data"
        df.createOrReplaceTempView(view_name)

        # Clean data using Spark df
        cleaned_df = df.withColumn("ID_BB_COMPANY",
                                   F.when(F.col("ID_BB_COMPANY") == "", "-1").otherwise(F.col("ID_BB_COMPANY")))
        cleaned_df = cleaned_df.withColumn("FISCAL_YEAR_PERIOD",
                                           F.when(F.col("FISCAL_YEAR_PERIOD") == "", "99999999").otherwise(
                                               F.col("FISCAL_YEAR_PERIOD")))
        cleaned_df = cleaned_df.withColumn("FILING_STATUS", F.when(F.col("FILING_STATUS") == "", "Unknown").otherwise(
            F.col("FILING_STATUS")))
        cleaned_df = cleaned_df.withColumn("EQY_CONSOLIDATED",
                                           F.when(F.col("EQY_CONSOLIDATED") == "", "Unknown").otherwise(
                                               F.col("EQY_CONSOLIDATED")))
        cleaned_df = cleaned_df.withColumn("ACCOUNTING_STANDARD",
                                           F.when(F.col("ACCOUNTING_STANDARD") == "", "Unknown").otherwise(
                                               F.col("ACCOUNTING_STANDARD")))
        cleaned_df = cleaned_df.withColumn("LATEST_PERIOD_END_DT_FULL_RECORD",
                                           F.when(F.col("LATEST_PERIOD_END_DT_FULL_RECORD") == "",
                                                  "99999999").otherwise(F.col("LATEST_PERIOD_END_DT_FULL_RECORD")))

        # Convert date columns
        for col_name in DATE_COLUMNS:
            if col_name in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    col_name,
                    F.to_date(F.col(col_name), "yyyyMMdd")
                )

        # # Filter out companies that are not in the caesars_companies list
        # cleaned_df = cleaned_df.filter(F.col("ID_BB_COMPANY").isin(caesars_companies))
        # # Filter companies that FILLING_STATUS is "MR", EQY_CONSOLIDATED is "Y", ACCOUNTING_STANDARD is "US GAAP", LATEST_PERIOD_END_DT_FULL_RECORD is > 2019-01-01, and FISCAL_YEAR_PERIOD is like "YYYY Q[1-4]"
        # cleaned_df = cleaned_df.filter(F.col("FILING_STATUS") == "MR")
        # cleaned_df = cleaned_df.filter(F.col("EQY_CONSOLIDATED") == "Y")
        # cleaned_df = cleaned_df.filter(F.col("ACCOUNTING_STANDARD") == "US GAAP")
        # cleaned_df = cleaned_df.filter(F.col("LATEST_PERIOD_END_DT_FULL_RECORD") > "2019-01-01")
        # cleaned_df = cleaned_df.filter(F.col("FISCAL_YEAR_PERIOD").rlike("^\\d{4} Q[1-4]$"))

        # Drop unnecessary columns
        # columns_to_drop = [
        #     "FILING_STATUS", "EQY_CONSOLIDATED", "ACCOUNTING_STANDARD",
        #     # "ANNOUNCEMENT_DT",
        #     "FUNDAMENTAL_ENTRY_DT", "FUNDAMENTAL_UPDATE_DT",
        #     "NFIELDS", "RCODE", "SECURITY_DESCRIPTION"
        # ]
        # cleaned_df = cleaned_df.drop(*[col for col in columns_to_drop if col in cleaned_df.columns])

        logger.info("Data cleaning completed")
        return cleaned_df

    def process_file(self, file_path: str, caesars_companies: List[str]) -> DataFrame:
        """
        Process a Bloomberg flat file with optimized memory usage for local execution.

        Args:
            file_path: Path to the Bloomberg flat file
            caesars_companies: List of Caesars company IDs

        Returns:
            Processed PySpark DataFrame
        """
        logger.info(f"Processing file: {file_path}")

        # Get file type and create/get SparkSession
        _, file_type = get_file_type_and_region(file_path)
        spark = self._get_or_create_spark_session(file_type)

        # Create a unique cache prefix that includes timestamp to nanosecond precision
        timestamp = datetime.now().strftime('%H%M%S%f')
        file_id = os.path.basename(file_path).replace(".", "_").replace('\\', '_')
        cache_prefix = f"bloomberg_{file_type}_{file_id}_{timestamp}"

        # Extract data
        column_names, data_rows = self.extract_data(file_path)
        self.save_colnames_to_mongo(file_path, column_names)
        if not column_names or not data_rows:
            logger.warning(f"No data extracted from file: {file_path}")
            return spark.createDataFrame([], StructType([]))

        # Process data in smaller chunks for local execution
        chunk_size = 50000  # Smaller chunk size for local processing
        processed_dfs = []

        for i in range(0, len(data_rows), chunk_size):
            chunk = data_rows[i:i + chunk_size]
            # Create unique identifiers for this chunk
            chunk_id = f"{cache_prefix}_chunk_{i}"
            chunk_id = re.sub(r"[^0-9a-zA-Z]+", "_", chunk_id)
            # Transform chunk
            chunk_df = self.transform_data(spark, column_names, chunk)
            # Register a unique temp view for the chunk before cleaning
            chunk_df.createOrReplaceTempView(f"chunk_{chunk_id}")

            # Clean chunk with unique cache identifier to prevent conflicts
            cleaned_chunk_df = self.clean_data(spark, chunk_df, caesars_companies, cache_id=chunk_id)

            # Create a proper temp view with unique name and cache through that view
            chunk_cache_name = f"cleaned_{chunk_id}"
            cleaned_chunk_df.createOrReplaceTempView(chunk_cache_name)
            # Get from view and cache
            cached_df = spark.table(chunk_cache_name).cache()

            processed_dfs.append(cached_df)

            # Unpersist previous chunks to free up memory
            if len(processed_dfs) > 1:
                processed_dfs[-2].unpersist()

        # Union all chunks
        if len(processed_dfs) > 1:
            final_df = processed_dfs[0]
            for df in processed_dfs[1:]:
                final_df = final_df.unionByName(df, allowMissingColumns=True)
                df.unpersist()  # Unpersist after union
        else:
            final_df = processed_dfs[0] if processed_dfs else spark.createDataFrame([], StructType([]))

        # Optimize the final DataFrame with fewer partitions and unique cache name
        final_df = final_df.repartition(4)
        final_cache_name = f"{cache_prefix}_final"
        final_df.createOrReplaceTempView(final_cache_name)

        # Get from view and cache properly
        final_df = spark.table(final_cache_name).cache()

        # Count rows to materialize the cache
        row_count = final_df.count()
        logger.info(f"Processed file with {row_count} rows and file type {file_type}")
        return final_df

    def process_files_parallel(self, file_paths: List[str], caesars_companies: List[str]) -> Dict[str, DataFrame]:
        """
        Process multiple Bloomberg flat files in parallel using separate SparkSessions for each file type.

        Args:
            file_paths: List of paths to Bloomberg flat files
            caesars_companies: List of Caesars company IDs

        Returns:
            Dictionary mapping file types to their respective DataFrames
        """
        logger.info(f"Processing {len(file_paths)} files in parallel")

        if not file_paths:
            logger.warning("No files to process")
            return {}

        # Group files by type
        files_by_type = defaultdict(list)
        for file_path in file_paths:
            _, file_type = get_file_type_and_region(file_path)
            files_by_type[file_type].append(file_path)

        logger.info(f"Grouped files by type: {dict([(k, len(v)) for k, v in files_by_type.items()])}")

        # Process files by type using separate SparkSessions
        dataframes_by_type = {}

        for file_type, type_files in files_by_type.items():
            logger.info(f"Processing {len(type_files)} files of type {file_type}")

            # Get or create SparkSession for this file type
            spark = self._get_or_create_spark_session(file_type)

            # Process all files of this type
            processed_dfs = []
            for file_path in type_files:
                # try:
                df = self.process_file(file_path, caesars_companies)
                if not df.count() == 0:
                    processed_dfs.append(df)
                    logger.info(f"Processed file {file_path} successfully")
                # except Exception as e:
                #     logger.error(f"Error processing file {file_path}: {e}")

            if processed_dfs:
                # Combine all DataFrames for this type
                logger.info(f"Combining {len(processed_dfs)} DataFrames for type {file_type}")
                combined_df = processed_dfs[0]

                for df in processed_dfs[1:]:
                    combined_df = combined_df.unionByName(df, allowMissingColumns=True)
                    df.unpersist()  # Unpersist after union

                # Cache the final combined DataFrame with timestamp to ensure uniqueness
                timestamp = datetime.now().strftime('%H%M%S%f')
                combined_view_name = f"combined_{file_type}_{timestamp}"
                combined_df.createOrReplaceTempView(combined_view_name)

                # Get from view and cache
                cached_combined_df = spark.table(combined_view_name).cache()
                dataframes_by_type[file_type] = cached_combined_df

                # Count rows to materialize the cache
                row_count = cached_combined_df.count()
                logger.info(f"Created combined DataFrame for {file_type} with {row_count} rows")

        return dataframes_by_type

    def save_colnames_to_mongo(self, file_path, colnames):

        m = re.search(r'(fundamentals_\w+_\w+\.out)', file_path)
        if m:
            filename = m.group(1)

        mg_handler = MongoDBHandler()
        db = mg_handler.get_database()
        col = db['bbg_data_colnames']
        data = [{
            'filename': filename,
            'colnames': colnames
        }]
        mg_handler.insert_raw_data(col, data)


    def save_to_parquet(self, spark: SparkSession, df: DataFrame, output_path: str) -> None:
        """
        Save DataFrame to Parquet format.

        Args:
            spark: SparkSession to use
            df: PySpark DataFrame
            output_path: Path to save the Parquet file
        """
        logger.info(f"Saving DataFrame to Parquet: {output_path}")

        if df.count() == 0:
            logger.warning("No data to save")
            return

        # Create output directory if it doesn't exist
        # output_dir = os.path.dirname(output_path)
        # if output_dir and not os.path.exists(output_dir):
        #     os.makedirs(output_dir)

        # Save DataFrame to Parquet
        df.write.mode("overwrite").parquet(output_path)

        logger.info(f"DataFrame saved to Parquet: {output_path}")

    def save_to_csv(self, spark: SparkSession, df: DataFrame, output_path: str) -> None:
        """
        Save DataFrame to CSV format.

        Args:
            spark: SparkSession to use
            df: PySpark DataFrame
            output_path: Path to save the CSV file
        """
        logger.info(f"Saving DataFrame to CSV: {output_path}")

        if df.count() == 0:
            logger.warning("No data to save")
            return

        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save DataFrame to CSV with proper NULL handling
        # NULL values will be replaced with \N for load data can recognize null
        df.write.mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("nullValue", r"\N") \
            .option("emptyValue", r"\N") \
            .csv(output_path)

        logger.info(f"DataFrame saved to CSV: {output_path}")

    def save_dataframes_by_type(self, dataframes_by_type: Dict[str, DataFrame], output_dir: str) -> Dict[str, str]:
        """
        Save multiple DataFrames to Parquet files, one per file type.

        Args:
            dataframes_by_type: Dictionary mapping file types to DataFrames
            output_dir: Directory to save the Parquet files

        Returns:
            Dictionary mapping file types to their output paths
        """
        logger.info(f"Saving {len(dataframes_by_type)} DataFrames to Parquet files in {output_dir}")

        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_paths = {}

        for file_type, df in dataframes_by_type.items():
            if df.count() == 0:
                logger.warning(f"No data to save for {file_type}")
                continue

            # Get the SparkSession for this file type
            spark = self._get_or_create_spark_session(file_type)

            # Create a standardized file name
            output_path = os.path.join(output_dir,
                                       f"fundamentals_{file_type}_{datetime.now().strftime('%Y%m%d')}.parquet")

            # Save DataFrame to Parquet
            self.save_to_parquet(spark, df, output_path)

            output_paths[file_type] = output_path

        return output_paths

    def save_dataframes_by_type_to_csv(self, dataframes_by_type: Dict[str, DataFrame], output_dir: str) -> Dict[str, str]:
        """
        Save multiple DataFrames to CSV files, one per file type.

        Args:
            dataframes_by_type: Dictionary mapping file types to DataFrames
            output_dir: Directory to save the CSV files

        Returns:
            Dictionary mapping file types to their output paths
        """
        logger.info(f"Saving {len(dataframes_by_type)} DataFrames to CSV files in {output_dir}")

        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_paths = {}

        for file_type, df in dataframes_by_type.items():
            if df.count() == 0:
                logger.warning(f"No data to save for {file_type}")
                continue

            # Get the SparkSession for this file type
            spark = self._get_or_create_spark_session(file_type)

            # Create a standardized file name with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_dir = os.path.join(output_dir, f"fundamentals_{file_type}_{timestamp}")

            # Save DataFrame to CSV
            self.save_to_csv(spark, df, csv_dir)

            output_paths[file_type] = csv_dir

        return output_paths

    def close(self) -> None:
        """
        Close all SparkSessions.
        """
        for file_type, spark in self.spark_sessions.items():
            try:
                spark.stop()
                logger.info(f"Closed SparkSession for file type: {file_type}")
            except Exception as e:
                logger.error(f"Error closing SparkSession for {file_type}: {e}")

        self.spark_sessions.clear()
        logger.info("All SparkSessions closed")


if __name__ == "__main__":

    # Set up logging with a fallback to print statements if logging fails
    SPARK_APP_NAME = "CombineIndustry11"
    DATE_COLUMNS = ["LATEST_PERIOD_END_DT_FULL_RECORD", "ANNOUNCEMENT_DT", "FUNDAMENTAL_ENTRY_DT", "FUNDAMENTAL_UPDATE_DT"]
    logger = Log("bloomberg_data_processor").getlog()
    # Print a message to confirm the module is loading correctly
    print("Bloomberg data processor module loaded successfully")
    smbclient = get_smbclient()
    processor = BloombergDataProcessor()

    # Process sample files
    sample_files = [
        # r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_bs_history.out",
        # r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_cf_history.out",
        # r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_cr_history.out",
        # r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_cr2_history.out",
        # r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_is_history.out",
        # r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_sard_bs_history.out",
        # r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_sard_cf_history.out",
        r"\\10.230.252.5\\BLOOMBERG_FILES\\fundamentals_namr_sard_is_history.out"
    ]

    # Check if files exist
    existing_files = [f for f in sample_files if smbclient.stat(f)]

    if existing_files:
        # Process files in parallel, grouped by type
        dataframes_by_type = processor.process_files_parallel(existing_files, [27000])

        # Show the first few rows of each DataFrame
        for file_type, df in dataframes_by_type.items():
            print(f"\n=== {file_type} Data ===")
            df.show(5)

        # Save to Parquet files
        output_dir = "./data"
        output_paths = processor.save_dataframes_by_type(dataframes_by_type, output_dir)

        print(f"\nSaved DataFrames to: {output_paths}")
    else:
        print("No sample files found")