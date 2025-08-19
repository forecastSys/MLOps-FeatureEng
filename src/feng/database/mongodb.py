from src.feng.utils import Log
from src.feng.config.config import MONGO_HOST, MONGO_DB
from pymongo import MongoClient
import os
import json
import uuid
import bson
from bson.binary import Binary, UuidRepresentation
import pandas as pd
import warnings
from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

class MongoDBHandler:

    def __init__(self):
        super().__init__()
        self.DB_URL = MONGO_HOST
        self.client = MongoClient(self.DB_URL)

    def get_database(self, DB=MONGO_DB):
        """
        :param DB: Your mongodb database, default is local
        """
        db = self.client[DB]
        return db

    def create_id(self, data):
        """
        This is function is used to create unique id base on article `Title` & `Date`

        :param data: Input dict does not have unique ID
        :return: Output dict with unique ID `_id`
        """

        _id = uuid.uuid3(uuid.NAMESPACE_DNS, data['filename'])
        _id = bson.Binary.from_uuid(_id, uuid_representation=UuidRepresentation.PYTHON_LEGACY)
        data['_id'] = _id
        return data

    def insert_raw_data(self, col, data):

        """
        This function is used to insert raw text data into database `News`

        :param raw_data_path: Path that stored .json file
        :return: length of inserted article
        """
        logger = Log("MongoDB").getlog()
        logger.info(f"Running {os.path.basename(__file__)}")

        out_data = [self.create_id(i) for i in data]
        try:
            col.insert_many(out_data)
        except Exception as e:
            warnings.warn(str(e))
            pass