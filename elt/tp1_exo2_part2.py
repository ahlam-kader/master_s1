import json
import requests
from datetime import datetime
from confluent_kafka import Consumer
import geoip2.database

KAFKA_SERVER = "10.136.127.1:9092"
KAFKA_TOPIC = "nginx-logs"
MATRICULE = "21012"
API_URL = f"http://10.136.127.1:8088/{MATRICULE}"

consumer = Consumer({
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'logstash_python_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([KAFKA_TOPIC])

geoip_db_path = "GeoLite2-City.mmdb"
geoip_reader = geoip2.database.Reader(geoip_db_path)

def parse_log(log_message):
    try:
        log_data = json.loads(log_message)

        src_ip = log_data.get("src_ip", "0.0.0.0")
        event_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        http_response = log_data.get("http_response", "")
        user_agent = log_data.get("user_agent", "")

        try:
            geo_info = geoip_reader.city(src_ip)
            src_ip_geo = {
                "country_name": geo_info.country.name or "Unknown",
                "region_name": geo_info.subdivisions.most_specific.name or "Unknown",
                "city_name": geo_info.city.name or "Unknown",
                "latitude": geo_info.location.latitude or 0.0,
                "longitude": geo_info.location.longitude or 0.0
            }
        except Exception:
            src_ip_geo = {
                "country_name": "Unknown",
                "region_name": "Unknown",
                "city_name": "Unknown",
                "latitude": 0.0,
                "longitude": 0.0
            }

        payload = {
            "tp1_exo2_q2": {
                "src_ip": src_ip,
                "event_date": event_date,
                "http_response": http_response,
                "user_agent": user_agent,
                "src_ip_geo": src_ip_geo,
                "sender": MATRICULE
            }
        }

        print("\n Payload formaté :")
        print(json.dumps(payload, indent=4, ensure_ascii=False))
        
        return payload

    except json.JSONDecodeError:
        print("Erreur de parsing JSON")
        return None

def send_to_api(payload):
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(API_URL, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            print("Données envoyées avec succès à l'API HTTP")
        else:
            print(f"Erreur lors de l'envoi des données : {response.status_code}")
    except Exception as e:
        print(f"Erreur de connexion à l'API : {e}")

while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        print(f"Erreur Kafka : {msg.error()}")
        continue
    
    log_message = msg.value().decode('utf-8')
    print(f"\n Message reçu : {log_message}")

    enriched_log = parse_log(log_message)
    if enriched_log:
        send_to_api(enriched_log)
