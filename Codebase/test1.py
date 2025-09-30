
from bs4 import BeautifulSoup
from datetime import datetime
import time
import requests
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['tech_news123']
db['the_verge_articles'].drop()
print("start fresh")
collection = db['the_verge_articles']
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
                    document = {
                        'title': title,
                        'url': link,
                        'publication_date': pub_date,
                        'author': author,
                        'content': text,
                        'source': 'The Verge',
                        'scraped_at': datetime.now(),
                        'tags': ['technology', 'news']
                    }
                    if not collection.find_one({'url': link}):
                        collection.insert_one(document)
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