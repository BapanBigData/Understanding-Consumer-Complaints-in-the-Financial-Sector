import pymongo
import os
from finance_complaint.constant.database import DATABASE_NAME
from finance_complaint.constant.env_constant import MONGO_DB_URL


class MongodbClient:
    client = None

    def __init__(self, database_name=DATABASE_NAME) -> None:
        
        if MongodbClient.client is None:
            mongo_db_url = MONGO_DB_URL
            
            if mongo_db_url is None:
                raise Exception(f"Environment key: MONGO_DB_URL is not set.")
            
            MongodbClient.client = pymongo.MongoClient(mongo_db_url)
            
        self.client = MongodbClient.client
        self.database = self.client[database_name]
        self.database_name = database_name
        
        