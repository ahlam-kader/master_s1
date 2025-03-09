from prefect import flow, task
import requests
from bs4 import BeautifulSoup
import json
from confluent_kafka import Producer
import os
import time
import random

MATRICULE = "21012"
ARTICLE_START = 33500
ARTICLE_END = 34999
KAFKA_BROKER = "10.136.127.1:9092"  
KAFKA_TOPIC = f"alakhbar-news-{MATRICULE}"

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'article_producer'
}
producer = Producer(KAFKA_CONFIG)

def robust_request(url, max_retries=5, initial_timeout=1):
    timeout = initial_timeout
    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return response
            else:
                print(f"Tentative {i+1}: Erreur HTTP {response.status_code} pour {url}")
        except requests.exceptions.ReadTimeout:
            print(f"Timeout ({timeout}s) - Tentative {i+1}/{max_retries} pour {url}")
        except requests.exceptions.RequestException as e:
            print(f"Erreur {e} lors de la requête à {url}")

        timeout *= 2  
        time.sleep(random.uniform(5, 10))  

    print(f"Impossible de récupérer {url} après {max_retries} tentatives.")
    return None


@task(retries=3, retry_delay_seconds=5)
def fetch_article(article_id):
    url = f"https://alakhbar.info/?q=node/{article_id}"
    response = robust_request(url, max_retries=5, initial_timeout=1)

    if response is None:
        print(f"Article {article_id} introuvable après plusieurs tentatives.")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    title = soup.find("meta", property="og:title")["content"] if soup.find("meta", property="og:title") else "Unknown"
    description = soup.find("meta", property="og:description")["content"] if soup.find("meta", property="og:description") else "No description"
    pub_date = soup.find("meta", property="article:published_time")["content"] if soup.find("meta", property="article:published_time") else "Unknown"
    link = soup.find("meta", property="og:url")["content"] if soup.find("meta", property="og:url") else url

    article_tag = soup.find("article")
    if article_tag:
        source = article_tag.get_text(separator=" ", strip=True)
    else:
        body = soup.find("body")
        source = body.get_text(separator=" ", strip=True) if body else None

    return {
        "id": article_id,
        "title": title,
        "description": description,
        "pub_date": pub_date,
        "link": link,
        "source": source
    }

@task
def send_to_kafka(article_data):
    if article_data:
        message = json.dumps(article_data)
        producer.produce(KAFKA_TOPIC, key=str(article_data["id"]), value=message.encode("utf-8"))
        producer.flush()  
        print(f"Article {article_data['id']} envoyé à Kafka.")
    else:
        print("Aucune donnée à envoyer.")


@flow
def etl_pipeline():
    for article_id in range(ARTICLE_START, ARTICLE_END + 1):
        article_data = fetch_article(article_id)
        send_to_kafka(article_data)


if __name__ == "__main__":
    etl_pipeline()
