import json
from pymongo import MongoClient
with open("google_news_data.json", "r", encoding="utf-8") as f:
    json_data = json.load(f)
articles = json_data.get("items", [])

print(f"Found {len(articles)} articles in file.\n")

mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["data_stream"]
collection = db["articles"]
collection.create_index("newsUrl", unique=True)


for article in articles:
    doc = {
        "title": article.get("title"),
        "newsUrl": article.get("newsUrl"),
        "snippet": article.get("snippet"),
        "image": article.get("images", {}).get("thumbnailProxied"),
        "timestamp": article.get("timestamp"),
        "publisher": article.get("publisher")
    }

    try:
        collection.insert_one(doc)
        print(f"Inserted: {doc['title']}")
    except Exception as e:
        print(f"Error inserting article: {e}")
