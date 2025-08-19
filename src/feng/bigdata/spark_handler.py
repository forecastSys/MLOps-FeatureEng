from pyspark.sql import SparkSession
import os


class SparkHandler:
    def __init__(self, app_name='SparkApp', master_url='local[*]'):
        self.app_name = app_name
        self.master_url = master_url
        self.spark = None

    def start_session(self):
        if not self.spark:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .remote(self.master_url) \
                .getOrCreate()
        return self.spark

    def stop_session(self):
        if self.spark:
            self.spark.stop()
            self.spark = None
