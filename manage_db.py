from datetime import datetime, timedelta
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

"""
This module manages the connection to the MongoDB database
"""

class DB_connection():
    """
    Manages the connection to the database
    """

    def __init__(self, data_member_1: str = None, data_member_2: str = None) -> None:
        """
        Class Initialization Function. Gets called when the object is created

        """
        CONNECTION_STRING = f"mongodb+srv://santosgo:{os.environ.get('MONGODB_PW')}@cluster0.s5rkcyp.mongodb.net/test"
        client = pymongo.MongoClient(CONNECTION_STRING)
        db = client.get_database('api-newsfeeds')
        self.newsfeeds_collection = pymongo.collection.Collection(db, 'api-newsfeeds')

   
    def insert_if_not_exists(self, json_record):
        self.newsfeeds_collection.update_one(
            {'pubDate': json_record['pubDate'], 'titulo': json_record['titulo']},
            {"$set": json_record},
            upsert=True
        )
    
    def clean_older_1wk(self):
        one_week_ago =  datetime.today() - timedelta(7)
        self.newsfeeds_collection.delete_many( { 'pubDate' : {'$lt' : one_week_ago } }) 
