import json
import requests
from datetime import datetime
from confluent_kafka import Consumer
import geoip2.database

KAFKA_SERVER = "10.136.127.1:9092"
KAFKA_TOPIC = "nginx-logs"
MATRICULE = "21012"

consumer = Consumer({
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'nginx_consumer_group',
    'auto.offset.reset': 'earliest'
})

geoip_db_path = "GeoLite2-City.mmdb"

geoip_reader = geoip2.database.Reader(geoip_db_path)

consumer.subscribe([KAFKA_TOPIC])

def parse_log(log_message):
    try:
        log_data = json.loads(log_message)

        src_ip = log_data.get("src_ip", "0.0.0.0")
        event_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        http_response = log_data.get("http_response", "")
        user_agent = log_data.get("user_agent", "")

        try:
            geo_info = geoip_reader.city(src_ip)
            src_ip_city = geo_info.city.name or "Unknown"
            src_ip_country = geo_info.country.name or "Unknown"
            src_ip_lat = geo_info.location.latitude or 0.0
            src_ip_long = geo_info.location.longitude or 0.0
        except Exception:
            src_ip_city, src_ip_country, src_ip_lat, src_ip_long = "Unknown", "Unknown", 0.0, 0.0

        payload = {
            "tp1_exo2_q1": {
                "src_ip": src_ip,
                "event_date": event_date,
                "http_response": http_response,
                "user_agent": user_agent,
                "src_ip_city": src_ip_city,
                "src_ip_country": src_ip_country,
                "src_ip_lat": src_ip_lat,
                "src_ip_long": src_ip_long,
                "sender": MATRICULE
            }
        }
        return payload

    except json.JSONDecodeError:
        print("erreur de parsing JSON")
        return None

def send_to_api(payload):
    API_URL = f"http://10.136.127.1:8088/{MATRICULE}"
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(API_URL, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            print("donnees envoyees avec succes a l'API HTTP")
        else:
            print(f"erreur lors de l'envoi des donnees : {response.status_code}")
    except Exception as e:
        print(f"erreur de connexion a l'API : {e}")

while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        print(f"erreur Kafka : {msg.error()}")
        continue
    
    log_message = msg.value().decode('utf-8')
    print(f"message reçu : {log_message}")

    enriched_log = parse_log(log_message)
    if enriched_log:
        send_to_api(enriched_log)