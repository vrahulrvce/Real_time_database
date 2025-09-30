import os
from kafka import KafkaProducer
import json
import feedparser
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime


def fetch_news():
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
        document = []
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
                    ################################
                    pub_date = datetime.fromisoformat(time_tag['datetime']) if time_tag else datetime.now()
                    author_tag = article_soup.find('span', class_='k8dtcj3 k8dtcj4')
                    # article_soup.find('a')[href]
                    author = author_tag.get_text(strip=True) if author_tag else "Unknown"
                    print(author)
                    #################################
                    article_body = article_soup.find('p', class_='duet--article--dangerously-set-cms-markup duet--article--standard-paragraph _1ymtmqpi _17nnmdy1 _17nnmdy0 _1xwtict1')
                    #text = ' '.join([p.get_text(strip=True) for p in article_body.find_all('p')]) if article_body else ""
                    text = article_body = article_body.get_text(strip= True) if article_body else ""
                    print(text)
                    document.append ({
                        'title': title,
                        'url': link,
                        'publication_date': pub_date,
                        'author': author,
                        'content': text,
                        'source': 'The Verge',
                        'scraped_at': datetime.now(),
                        'tags': ['technology', 'news']
                    })
                
            except Exception as e:
                print(f"Error processing article: {e}")
                continue
    except Exception as e:
        print("erorr for website")
    return document

def producer_to_kafka():
    broker_list = ['172.17.0.4:9092']  # Try multiple addresses if needed
    print(f"Attempting to connect to Kafka brokers: {broker_list}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker_list,
            api_version=(2, 2, 0),
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            # Add these parameters for better debugging
            request_timeout_ms=30000,  # 30 seconds timeout (default is 40 seconds)
            max_block_ms=60000,  # 60 seconds timeout for metadata (default is 60 seconds)
        )
        print("Successfully connected to Kafka!")
        
        topic = 'tech_news'
        while True:
            try:
                documents = fetch_news()
                if not documents:
                    print("No new documents to send.")
                for document in documents:
                    print(f"Sending: {document['title']} to Kafka")
                    producer.send(topic, value=document)
                    print(f"Message sent: {document['title']}")
                time.sleep(900)  
            except Exception as e:
                print(f"Error during processing: {e}")
            finally:
                producer.flush(timeout=5)
    except Exception as e:
        print(f"Critical error connecting to Kafka: {e}")
        # Add more detailed error info
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    producer_to_kafka()