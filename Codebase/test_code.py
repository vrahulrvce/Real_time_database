# File: theverge_producer.py
#!/usr/bin/env python3
import logging
import signal
import sys
import pickle
import html
import requests
import feedparser
from argparse import ArgumentParser
from confluent_kafka import Producer
import time

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
# RSS fetcher
# ───────────────────────────────────────────────────────────────
def fetch_feed(url):
    resp = requests.get(url)
    resp.encoding = 'utf-8'
    feed = feedparser.parse(resp.text)
    if feed.bozo:
        raise RuntimeError(f"Error parsing feed: {feed.bozo_exception}")
    return feed.entries

# ───────────────────────────────────────────────────────────────
# Graceful shutdown handler
# ───────────────────────────────────────────────────────────────
def handle_exit(sig, frame):
    logger.info("Shutting down producer…")
    producer.flush()
    sys.exit(0)

# ───────────────────────────────────────────────────────────────
# Main Producer Loop
# ───────────────────────────────────────────────────────────────
def main():
    parser = ArgumentParser(description="Stream The Verge RSS into Kafka (Confluent)")
    parser.add_argument(
        "--feed-url",
        default="https://www.theverge.com/rss/tech/index.xml",
        help="RSS feed URL",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="172.17.0.4:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default="inclasstopic",
        help="Kafka topic to produce to",
    )
    args = parser.parse_args()
    global producer
    producer = Producer({'bootstrap.servers': args.bootstrap_servers})
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle_exit)
    seen = set()
    logger.info(f"Starting periodic fetch every 60 seconds from {args.feed_url}")
    while True:
        try:
            entries = fetch_feed(args.feed_url)
            logger.info(f"Fetched {len(entries)} entries, filtering new ones…")
            for entry in entries:
                link = entry.get('link')
                if link in seen:
                    continue
                # Extract author
                if hasattr(entry, 'author') and entry.author:
                    author = entry.author
                elif hasattr(entry, 'dc_creator'):
                    author = entry.dc_creator
                elif 'authors' in entry and entry.authors:
                    author = entry.authors[0].name
                else:
                    author = "No author available"
                data = {
                    'id':        entry.get('id', link),
                    'title':     entry.get('title', ''),
                    'published': entry.get('published', ''),
                    'summary':   html.unescape(entry.get('summary', '')),
                    'link':      link,
                    'categories':[tag.term for tag in entry.get('tags', [])] if 'tags' in entry else [],
                    'author':    author,
                }
                logger.info(f"Producing new item: {data['id']} - {data['title']!r}")
                producer.produce(topic=args.topic, value=pickle.dumps(data))
                seen.add(link)
            producer.flush()
        except Exception:
            logger.exception("Error in periodic fetch loop")
        time.sleep(60)

if __name__ == "__main__":
    main()