import requests 
import json      
from datetime import datetime  

GOLD_PRICE_URL = "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
EXCHANGE_RATE_URL = "https://open.er-api.com/v6/latest/USD"
API_DESTINATION = f"http://10.136.127.1:8088/{21012}"

def get_gold_data():
    response = requests.get(GOLD_PRICE_URL)  
    if response.status_code == 200: 
        return response.json() 
    return None  

def extract_gold_price(data):
    if data:
        for item in data: 
            if item.get("topo", {}).get("platform") == "MT5" and item.get("topo", {}).get("server") == "Live1":
                return item["spreadProfilePrices"]["ask"]  
    return None 

def extract_gold_price(data):
    if data:
        for item in data:
            if item.get("topo", {}).get("platform") == "MT5" and item.get("topo", {}).get("server") == "Live1":
                spread_prices = item.get("spreadProfilePrices", []) 
                if spread_prices:  
                    return spread_prices[0].get("ask")
    return None

def get_usd_to_mru():
    response = requests.get(EXCHANGE_RATE_URL)  
    if response.status_code == 200:
        data = response.json()  
        return data["rates"].get("MRU") 
    return None  

def build_payload(gold_price_usd, usd_to_mru):
    if gold_price_usd is None or usd_to_mru is None:  
        return None
    
    event_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") 
    gold_price_mru = gold_price_usd * usd_to_mru 
    
    payload = {
        "tp1_exo1_q1_p1": {
            "gold_price_usd": gold_price_usd,
            "event_date": event_date,
            "gold_price_mru": round(gold_price_mru, 2),  
            "sender": 21012
        }
    }
    return payload

def send_data(payload):
    headers = {"Content-Type": "application/json"}  
    response = requests.post(API_DESTINATION, json=payload, headers=headers)
    return response.status_code, response.text  

gold_data = get_gold_data()
gold_price_usd = extract_gold_price(gold_data)
usd_to_mru = get_usd_to_mru() 
payload = build_payload(gold_price_usd, usd_to_mru) 

if payload:
    status, response_text = send_data(payload)  
    print(f"donnees envoyees avec succes ! code {status}\nreponse : {response_text}")
else:
    print("erreur lors de la recuperation des donnees.")
