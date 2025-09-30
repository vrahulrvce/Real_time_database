import http.client
import urllib.parse
import json
import time
from pymongo import MongoClient, errors
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["data_stream"]
collection = db["articles"]
collection.create_index("newsUrl", unique=True)
while True:
    try:
        print("\nFetching news articles...")
        conn = http.client.HTTPSConnection("google-news13.p.rapidapi.com")
        query = urllib.parse.quote("Penn Uni")
        endpoint = f"/search?keyword={query}&lr=en-US"
        headers = {
            'x-rapidapi-key': "4b43e7f29emsh2a68c6033ea8a77p1fb78ajsn9fff040c6ea5",
            'x-rapidapi-host': "google-news13.p.rapidapi.com"
        }
        conn.request("GET", endpoint, headers=headers)
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data.decode("utf-8"))
        with open("google_news_data.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        print("Saved full JSON response to 'google_news_data.json'")
        articles = json_data.get("items", [])
        print(f"Found {len(articles)} articles.\n")
        for article in articles:
            title = article.get("title")
            url = article.get("newsUrl")
            snippet = article.get("snippet")
            image = article.get("images", {}).get("thumbnailProxied")
            print(f"Title: {title}")
            print(f"URL: {url}")
            print(f"Snippet: {snippet}")
            print(f"Image: {image}")
            print("-" * 60)
            doc = {
                "title": title,
                "newsUrl": url,
                "snippet": snippet,
                "image": image,
                "timestamp": article.get("timestamp"),
                "publisher": article.get("publisher")
            }
            try:
                collection.insert_one(doc)
                print("Inserted")
            except errors.DuplicateKeyError:
                print("Duplicate - already inserted")
            except Exception as e:
                print(f"Error insertion: {e}")
    except Exception as e:
        print(f"An error occurred during fetch or insert: {e}")
    print("Waiting 30 minutes before next fetch...\n")
    time.sleep(1800)
