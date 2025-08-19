import os
from dotenv import load_dotenv
load_dotenv()

SPARK_MASTER = os.getenv("SPARK_MASTER")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

FILEIP05=os.getenv("FILEIP05")
FILEIP_USERNAME=username=os.getenv("FILEIP_USERNAME")
FILEIP_PASSWORD=password=os.getenv("FILEIP_PASSWORD")

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_DB='CAESARS'