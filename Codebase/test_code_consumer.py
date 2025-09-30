# File: theverge_consumer.py
#!/usr/bin/env python3
import logging
import signal
import sys
import pickle
import html
from argparse import ArgumentParser
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from cassandra.cluster import Cluster
import uuid

# ───────────────────────────────────────────────────────────────
# Logging
# ───────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ───────────────────────────────────────────────────────────────
# MongoDB Connection
# ───────────────────────────────────────────────────────────────
client     = MongoClient("mongodb://172.17.0.3:27017/")
db         = client["US"]
collection = db["inclasstheverge"]

# ───────────────────────────────────────────────────────────────
# Cassandra Connection
# ───────────────────────────────────────────────────────────────
cluster = Cluster(['172.17.0.2'])
session = cluster.connect()
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS inclassdatabase 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")
session.set_keyspace('inclassdatabase')
session.execute("""
    CREATE TABLE IF NOT EXISTS inclasstheverge (
        id text PRIMARY KEY,
        title text,
        published_on text,
        summary text,
        link text,
        author text,
        categories list<text>
    )
""")
insert_cassandra = session.prepare("""
    INSERT INTO inclasstheverge (
        id, title, published_on, summary, link, author, categories
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
""")
check_cassandra = session.prepare("""
    SELECT id FROM inclasstheverge WHERE link=? ALLOW FILTERING
""")

def is_processed(link: str) -> bool:
    if collection.count_documents({"Link": link}, limit=1) > 0:
        return True
    if session.execute(check_cassandra, (link,)).one():
        return True
    return False

running = True
def shutdown(sig, frame):
    global running
    logger.info("Shutting down consumer…")
    running = False

def main():
    parser = ArgumentParser(description="Consume RSS entries from Kafka into MongoDB & Cassandra")
    parser.add_argument(
        "--bootstrap-servers",
        default="172.17.0.4:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--group-id",
        default="rss-consumer-group",
        help="Kafka consumer group ID",
    )
    parser.add_argument(
        "--topic",
        default="inclasstopic",
        help="Kafka topic to subscribe to",
    )
    args = parser.parse_args()
    conf = {
        'bootstrap.servers':   args.bootstrap_servers,
        'group.id':            args.group_id,
        'auto.offset.reset':   'earliest',
        'enable.auto.commit':  True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([args.topic])
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shutdown)
    logger.info(f"Subscribed to {args.topic}, starting consumption")
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            entry = pickle.loads(msg.value())
            link = entry.get("link")
            if not link or is_processed(link):
                continue
            author     = entry.get("author", "No author available")
            title      = entry.get("title", "")
            published  = entry.get("published", "")
            summary    = html.unescape(entry.get("summary", ""))
            categories = entry.get("categories", [])
            cass_id = str(uuid.uuid4())
            session.execute(insert_cassandra, (
                cass_id, title, published, summary, link, author, categories
            ))
            doc = {
                "Title":        title,
                "Published on": published,
                "Summary":      summary,
                "Link":         link,
                "Author":       author,
                "ID":           entry.get("id"),
                "Categories":   categories
            }
            collection.insert_one(doc)
            logger.info(f"Stored link into both DBs: {link}")
    except Exception:
        logger.exception("Consumer encountered an error")
    finally:
        consumer.close()
        logger.info("Consumer closed, exiting")

if __name__ == "__main__":
    main()
