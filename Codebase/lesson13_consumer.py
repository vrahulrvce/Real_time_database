from kafka import KafkaConsumer
from pymongo import MongoClient
import json

import pymongo.mongo_client

def mongo_consumer():
    consumer = KafkaConsumer(
        'tech_news',
        bootstrap_servers=['localhost:9092'],
        api_version=(2, 2, 0),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='mongo-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    client = MongoClient('mongodb://localhost:27017/')
    db = client['tech_news']
    db['the_verge_articles'].drop()
    print("start fresh")
    collection = db['the_verge_articles']
    for message in consumer:
        document = message.value
        try:
            collection.insert_one(document)
            print(f"inserted into mongodb : {document['title']}")
        except Exception as e:
            print(f"unable to insert it: {e}")
        
if __name__ == "__main__":
    mongo_consumer()
