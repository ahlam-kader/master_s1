import requests  
import json      
from datetime import datetime  
from confluent_kafka import Producer

GOLD_PRICE_URL = "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
MATRICULE = "21012"
KAFKA_SERVER = "10.136.127.1:9092" 
KAFKA_TOPIC = f"gold_price_{MATRICULE}" 

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_SERVER,
    'client.id': 'match_producer'
}
producer = Producer(KAFKA_CONFIG)

def get_gold_data():
    response = requests.get(GOLD_PRICE_URL)  
    if response.status_code == 200:
        return response.json()
    return None  

def extract_gold_price(data):
    if data:
        for item in data:
            if item.get("topo", {}).get("platform") == "MT5" and item.get("topo", {}).get("server") == "Live1":
                spread_prices = item.get("spreadProfilePrices", [])  
                if spread_prices:
                    return spread_prices[0].get("ask")  
    return None  

def build_payload(gold_price_usd):
    if gold_price_usd is None:
        return None
    
    event_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") 
    
    payload = {
        "tp1_exo1_q2_p1": {
            "gold_price_usd": gold_price_usd,
            "event_date": event_date,
            "sender": MATRICULE 
        }
    }
    return payload  

gold_data = get_gold_data()  
gold_price_usd = extract_gold_price(gold_data)  
payload = build_payload(gold_price_usd)

if payload:
    producer.produce(KAFKA_TOPIC, value=json.dumps(payload).encode('utf-8'))
    producer.flush() 
    print(f"donnees envoyees a Kafka sur le topic '{KAFKA_TOPIC}'")
else:
    print("erreur lors de la recuperation des donnees.")