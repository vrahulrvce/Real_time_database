"""from kafka import KafkaProducer
import json
import time
import sys

def test_kafka_producer():
    # Try multiple possible addresses
    broker_addresses = [
        '172.17.0.4:9092'

    ]
    
    # Try each address
    for broker in broker_addresses:
        try:
            print(f"Attempting to connect to Kafka at {broker}...")
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                # Set shorter timeouts for quicker feedback
                api_version=(2, 2, 0),
                request_timeout_ms=10000,
                max_block_ms=15000
            )
            
            # Send a test message
            test_message = {
                'test_id': 'test_message',
                'timestamp': str(time.time()),
                'message': f'Kafka test message from {broker}'
            }
            
            print(f"Sending test message to topic 'test_topic'...")
            future = producer.send('inclass', value=test_message)
            # Try to get the result to confirm it sent properly
            record_metadata = future.get(timeout=5)
            
            print(f"SUCCESS! Message sent to: {record_metadata.topic}, partition: {record_metadata.partition}")
            print(f"Successfully connected to Kafka broker at {broker}")
            producer.close()
            return True
            
        except Exception as e:
            print(f"Failed to connect to {broker}: {str(e)}")
    
    print("Failed to connect to any Kafka brokers.")
    return False

if __name__ == "__main__":
    test_kafka_producer()"""


from bs4 import BeautifulSoup
from datetime import datetime
import time
import requests
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Connect to Cassandra
try:
    cluster = Cluster(['172.17.0.1'])
    session = cluster.connect()
    session.set_keyspace('inclasstheverge')
    print("Connected and keyspace set.")
except Exception as e:
    print("Connection failed:", e)

session.set_keyspace('inclasstheverge')

session.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        url text PRIMARY KEY,
        title text,
        publication_date timestamp,
        author text,
        content text,
        source text,
        scraped_time timestamp,
        tags list<text>
    )
""")

print("Start fresh (Cassandra)")

def scrape_the_verge():
    base_url = "https://www.theverge.com"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }

    try:
        response = requests.get(base_url + "/tech", headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('div', class_='duet--content-cards--content-card duet--content-cards--quick-post yy0d3l4 yy0d3l9')

        for article in articles:
            try:
                title = article.find('a')['aria-label'] if article.find('a') and article.find('a').has_attr('aria-label') else "No title"
                link = article.find('a')['href'] if article.find('a') else None

                if link:
                    if not link.startswith('http'):
                        link = base_url + link
                    print(link)

                    article_response = requests.get(link, headers=headers)
                    article_soup = BeautifulSoup(article_response.text, 'html.parser')

                    time_tag = article_soup.find('time')
                    pub_date = datetime.fromisoformat(time_tag['datetime']) if time_tag else datetime.now()

                    author_tag = article_soup.find('span', class_='k8dtcj3 k8dtcj4')
                    author = author_tag.get_text(strip=True) if author_tag else "Unknown"

                    article_body = article_soup.find('p', class_='duet--article--dangerously-set-cms-markup duet--article--standard-paragraph _1ymtmqpi _17nnmdy1 _17nnmdy0 _1xwtict1')
                    text = article_body.get_text(strip=True) if article_body else ""

                    # Check for duplicates
                    check_stmt = session.prepare("SELECT url FROM articles WHERE url=?")
                    if session.execute(check_stmt, [link]).one() is None:
                        insert_stmt = session.prepare("""
                            INSERT INTO articles (url, title, publication_date, author, content, source, scraped_time, tags)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """)
                        session.execute(insert_stmt, (
                            link, 
                            title, 
                            pub_date, 
                            author, 
                            text,
                            'The Verge', 
                            datetime.now(), 
                            ['technology', 'news']
                        ))
                        print(f"Inserted: {title}")
                    else:
                        print(f"Already exists: {title}")

            except Exception as e:
                print(f"Error processing article: {e}")
                continue

    except Exception as e:
        print(f"Error scraping The Verge: {e}")

if __name__ == "__main__":
    scrape_the_verge()
    print("Scraping complete!")