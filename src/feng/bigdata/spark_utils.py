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

logger = Log("Spark Utils").getlog()

def save_to_parquet(df: DataFrame, output_path: str) -> None:
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